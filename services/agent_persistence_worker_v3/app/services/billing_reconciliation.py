"""Reconcile incomplete settlement and delayed provider-usage lifecycles."""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from common.billing import customer_wallet_document_id, nonnegative_int
from common.firestore import get_transaction_document_snapshot
from common.openai_usage_metadata import (
    aggregate_turn_usage,
    has_complete_turn_token_counts,
)
from common.schemas import TurnCompletedEvent
from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.core.errors import RetryableWorkerError
from services.agent_persistence_worker_v3.app.services.billing_settlement import (
    BillingSettlementService,
)
from services.agent_persistence_worker_v3.app.services.billing_batch_settlement import (
    BillingBatchSettlementService,
)
from services.agent_persistence_worker_v3.app.services.billing_ledger import (
    BillingLedgerRepository,
)
from services.agent_persistence_worker_v3.app.services.firestore_client import (
    get_firestore_client,
)
from services.agent_persistence_worker_v3.app.services.firestore_messages import (
    FirestoreMessagesRepository,
)
from services.agent_persistence_worker_v3.app.services.openai_turn_usage_hydrator import (
    OpenAITurnUsageHydrator,
)

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class BillingReconciliationResult:
    scanned_reservations: int
    settled_reservations: int
    released_reservations: int
    skipped_reservations: int
    queued_batch_entries: int = 0
    settled_batches: int = 0
    settled_batch_turns: int = 0


class BillingReconciliationService:
    """Recover due turn work and retain cleanup for pre-v4 wallet holds."""

    def __init__(
        self,
        *,
        firestore_client_factory: Callable[[], Any] | None = None,
        settlement_service: BillingSettlementService | None = None,
        batch_settlement_service: BillingBatchSettlementService | None = None,
        usage_hydrator: OpenAITurnUsageHydrator | None = None,
        ledger_repository: BillingLedgerRepository | None = None,
        messages_repository: FirestoreMessagesRepository | None = None,
    ) -> None:
        self._settings = get_settings()
        self._firestore_client_factory = firestore_client_factory or get_firestore_client
        self._settlement_service = settlement_service or BillingSettlementService(
            firestore_client_factory=self._firestore_client_factory,
        )
        self._batch_settlement_service = batch_settlement_service or BillingBatchSettlementService(
            firestore_client_factory=self._firestore_client_factory,
        )
        # One non-blocking provider lookup per scheduled pass; the original
        # Pub/Sub worker already makes its short post-response retry attempts.
        self._usage_hydrator = usage_hydrator or OpenAITurnUsageHydrator(attempts=1)
        self._ledger_repository = ledger_repository or BillingLedgerRepository()
        self._messages_repository = messages_repository or FirestoreMessagesRepository()

    async def reconcile_expired(self) -> BillingReconciliationResult:
        return await asyncio.to_thread(self.reconcile_expired_sync)

    def reconcile_expired_sync(self) -> BillingReconciliationResult:
        client = self._firestore_client_factory()
        now = datetime.now(timezone.utc)
        reservations = list(self._pending_turn_markers(client, now))
        late_reservations = list(self._due_usage_reconciliation_reservations(client, now))
        ledger_cache, recovered_reports, deferred_usage_turn_ids = self._hydrate_due_usage_reports(
            client,
            reservations,
            late_reservations,
            now,
        )
        legacy_unpriced = list(self._legacy_unpriced_reservations(client))
        scanned = settled = released = skipped = queued = 0
        settlement_marker_turn_ids: set[str] = set()
        for reservation_snapshot in reservations:
            scanned += 1
            reservation = reservation_snapshot.to_dict() or {}
            status = reservation.get("status")
            if status not in {"reserved", "completed_pending"}:
                skipped += 1
                continue
            # `reserved` is only produced by the pre-v4 hold flow. New v4
            # markers use `completed_pending` and have no wallet hold/expiry.
            expired = status == "reserved" and _reservation_is_expired(reservation, now)
            turn_id = _required_string(reservation.get("turn_id"), "turn_id")
            settlement_marker_turn_ids.add(turn_id)
            if turn_id in ledger_cache:
                ledger = ledger_cache[turn_id]
                ledger_exists = True
            else:
                ledger_snapshot = client.collection(
                    self._settings.billing_ledger_collection
                ).document(turn_id).get()
                ledger = ledger_snapshot.to_dict() or {}
                ledger_exists = ledger_snapshot.exists
            if ledger_exists:
                usage_deadline = _as_utc_or_none(
                    reservation.get("usage_reconciliation_deadline_at")
                )
                usage_expired = (
                    status == "completed_pending"
                    and reservation.get("usage_reconciliation_status") == "pending"
                    and usage_deadline is not None
                    and now >= usage_deadline
                )
                if usage_expired:
                    has_provisional_estimate = (
                        self._settings.billing_batch_settlement_enabled
                        and ledger.get("estimated_cost_usd_nanos") is not None
                    )
                    self._mark_manual_review(
                        client,
                        reservation,
                        now,
                        reason="provider_usage_retention_expired",
                        preserve_settlement=has_provisional_estimate,
                    )
                    reservation = {
                        **reservation,
                        "usage_reconciliation_status": "manual_review",
                    }
                    if not has_provisional_estimate:
                        skipped += 1
                        continue
                if self._has_pending_openai_usage(ledger):
                    if _recovery_is_due(reservation, now) and not usage_expired:
                        if turn_id in recovered_reports:
                            ledger = self._recover_openai_usage(
                                client,
                                ledger,
                                recovered_reports=recovered_reports[turn_id],
                            )
                        elif turn_id not in deferred_usage_turn_ids:
                            ledger = self._recover_openai_usage(client, ledger)
                    if self._has_pending_openai_usage(ledger):
                        estimate_available = ledger.get("estimated_cost_usd_nanos") is not None
                        if (
                            status == "completed_pending"
                            and self._settings.billing_batch_settlement_enabled
                            and estimate_available
                        ):
                            if (
                                _recovery_is_due(reservation, now)
                                and not usage_expired
                                and turn_id not in deferred_usage_turn_ids
                            ):
                                self._schedule_usage_recovery_retry(
                                    client, reservation, now
                                )
                            event = _event_from_ledger_and_reservation(
                                ledger,
                                reservation,
                            )
                            if self._batch_settlement_service.enqueue_turn_sync(event):
                                queued += 1
                            else:
                                skipped += 1
                            continue
                        if usage_expired:
                            skipped += 1
                            continue
                        if (
                            not expired
                            and _recovery_is_due(reservation, now)
                            and turn_id not in deferred_usage_turn_ids
                        ):
                            self._schedule_usage_recovery_retry(
                                client, reservation, now
                            )
                            skipped += 1
                            continue
                        if not expired and turn_id in deferred_usage_turn_ids:
                            skipped += 1
                            continue
                        if not expired:
                            self._defer_pending_settlement_retry(
                                client, reservation, now
                            )
                            skipped += 1
                            continue
                        if _recovery_is_due(reservation, now):
                            self._schedule_usage_recovery_retry(client, reservation, now)
                        # Only pre-v4 `reserved` records can expire here. The v4
                        # completed_pending state has no hold to release; usage
                        # recovery stays asynchronous and never gates new turns.
                        event = _event_from_ledger_and_reservation(
                            ledger,
                            reservation,
                        )
                        settlement = self._settlement_service.settle_if_required_sync(
                            event
                        )
                        if settlement is None:
                            skipped += 1
                        else:
                            settled += 1
                        continue
                event = _event_from_ledger_and_reservation(
                    ledger,
                    reservation,
                )
                if (
                    self._settings.billing_batch_settlement_enabled
                    and ledger.get("estimated_cost_usd_nanos") is not None
                ):
                    if self._batch_settlement_service.enqueue_turn_sync(event):
                        queued += 1
                    else:
                        skipped += 1
                    continue
                settlement = self._settlement_service.settle_if_required_sync(event)
                if settlement is None:
                    skipped += 1
                else:
                    settled += 1
                continue
            if expired and self._release_expired_reservation_sync(client, reservation, now):
                released += 1
            elif (
                status == "completed_pending"
                and reservation.get("usage_reconciliation_status") == "pending"
                and (deadline := _as_utc_or_none(
                    reservation.get("usage_reconciliation_deadline_at")
                )) is not None
                and now >= deadline
            ):
                self._mark_manual_review(
                    client,
                    reservation,
                    now,
                    reason="late_ledger_missing_at_retention_expiry",
                )
                skipped += 1
            else:
                self._defer_pending_settlement_retry(client, reservation, now)
                skipped += 1

        for reservation_snapshot in late_reservations:
            scanned += 1
            reservation = reservation_snapshot.to_dict() or {}
            status = reservation.get("status")
            turn_id = _required_string(reservation.get("turn_id"), "turn_id")
            # A completed_pending marker can also have a due provider-usage
            # retry. It was already handled above, so do not fetch the same
            # session twice in this scheduler pass from the stale second query.
            if reservation.get("usage_reconciliation_status") != "pending":
                skipped += 1
                continue
            deadline = _as_utc_or_none(
                reservation.get("usage_reconciliation_deadline_at")
            )
            if deadline is not None and now >= deadline:
                if turn_id in ledger_cache:
                    ledger_still_exists = True
                else:
                    ledger_snapshot = client.collection(
                        self._settings.billing_ledger_collection
                    ).document(turn_id).get()
                    ledger_still_exists = ledger_snapshot.exists
                preserve_settlement = status in {"batch_pending", "late_batch_pending"}
                self._mark_manual_review(
                    client,
                    reservation,
                    now,
                    reason=(
                        "provider_usage_retention_expired"
                        if ledger_still_exists
                        else "late_ledger_missing_at_retention_expiry"
                    ),
                    preserve_settlement=preserve_settlement,
                )
                skipped += 1
                continue
            if turn_id in settlement_marker_turn_ids:
                continue
            if not _recovery_is_due(reservation, now):
                skipped += 1
                continue

            if turn_id in ledger_cache:
                ledger = ledger_cache[turn_id]
                ledger_exists = True
            else:
                ledger_snapshot = client.collection(
                    self._settings.billing_ledger_collection
                ).document(turn_id).get()
                ledger = ledger_snapshot.to_dict() or {}
                ledger_exists = ledger_snapshot.exists
            if not ledger_exists:
                LOGGER.warning(
                    "Billing ledger is temporarily missing during usage recovery for turn %s; retaining the marker and retrying.",
                    turn_id,
                )
                self._schedule_usage_recovery_retry(client, reservation, now)
                skipped += 1
                continue
            if self._has_pending_openai_usage(ledger):
                if turn_id in recovered_reports:
                    ledger = self._recover_openai_usage(
                        client,
                        ledger,
                        recovered_reports=recovered_reports[turn_id],
                    )
                elif turn_id not in deferred_usage_turn_ids:
                    ledger = self._recover_openai_usage(client, ledger)
            if self._has_pending_openai_usage(ledger):
                if (
                    self._settings.billing_batch_settlement_enabled
                    and ledger.get("estimated_cost_usd_nanos") is not None
                ):
                    partial_event = _event_from_ledger_and_reservation(
                        ledger,
                        reservation,
                    )
                    if status in {"batch_pending", "late_batch_pending"}:
                        self._batch_settlement_service.update_pending_estimate_sync(
                            partial_event
                        )
                    elif status == "late_usage_pending":
                        if self._batch_settlement_service.enqueue_turn_sync(
                            partial_event,
                            late_usage=True,
                            preserve_usage_recovery=True,
                        ):
                            queued += 1
                if turn_id not in deferred_usage_turn_ids:
                    self._schedule_usage_recovery_retry(client, reservation, now)
                skipped += 1
                continue
            if ledger.get("estimated_cost_usd_nanos") is None:
                self._mark_manual_review(
                    client, reservation, now, reason="recovered_usage_unpriced"
                )
                skipped += 1
                continue
            event = _event_from_ledger_and_reservation(ledger, reservation)
            if self._settings.billing_batch_settlement_enabled:
                reconcile_authoritative = getattr(
                    self._batch_settlement_service,
                    "reconcile_authoritative_usage_sync",
                    None,
                )
                adjusted = (
                    reconcile_authoritative(event)
                    if reconcile_authoritative is not None
                    else False
                )
                if adjusted:
                    if status in {
                        "settled",
                        "settled_shortfall",
                        "late_settled",
                        "late_settled_shortfall",
                    }:
                        settled += 1
                    else:
                        queued += 1
                    continue
                if self._batch_settlement_service.enqueue_turn_sync(event, late_usage=True):
                    queued += 1
                else:
                    skipped += 1
                continue
            result = self._settlement_service.settle_late_usage_sync(event)
            if result is None:
                skipped += 1
            else:
                settled += 1

        for reservation_snapshot in legacy_unpriced:
            reservation = reservation_snapshot.to_dict() or {}
            if reservation.get("status") != "unpriced_released":
                continue
            scanned += 1
            turn_id = _required_string(reservation.get("turn_id"), "turn_id")
            ledger_snapshot = client.collection(
                self._settings.billing_ledger_collection
            ).document(turn_id).get()
            register = getattr(
                self._settlement_service,
                "register_late_usage_recovery_sync",
                None,
            )
            registered = (
                register(reservation, now=now)
                if register is not None
                else False
            )
            if registered:
                if not ledger_snapshot.exists:
                    LOGGER.warning(
                        "Billing ledger is missing for legacy usage recovery turn %s; retaining the marker through its retry window.",
                        turn_id,
                    )
                skipped += 1
                continue
            # Keep compatibility with test doubles/older service implementations,
            # but never close a real unpriced reservation without registering its
            # account-level block first.
            self._mark_legacy_unpriced_final(client, reservation, now)
            skipped += 1

        batch_result = (
            self._batch_settlement_service.reconcile_due_sync()
            if self._settings.billing_batch_settlement_enabled
            else None
        )
        return BillingReconciliationResult(
            scanned_reservations=scanned,
            settled_reservations=settled,
            released_reservations=released,
            skipped_reservations=skipped,
            queued_batch_entries=queued,
            settled_batches=batch_result.settled_batches if batch_result else 0,
            settled_batch_turns=batch_result.settled_turns if batch_result else 0,
        )

    def _hydrate_due_usage_reports(
        self,
        client: Any,
        settlement_reservations: list[Any],
        usage_reservations: list[Any],
        now: datetime,
    ) -> tuple[
        dict[str, dict[str, Any]],
        dict[str, list[dict[str, Any]]],
        set[str],
    ]:
        """Fetch due provider usage concurrently with a bounded per-pass budget."""
        candidates: dict[str, tuple[dict[str, Any], str, list[dict[str, Any]]]] = {}
        ledger_cache: dict[str, dict[str, Any]] = {}
        seen_turn_ids: set[str] = set()
        for snapshot in [*settlement_reservations, *usage_reservations]:
            reservation = snapshot.to_dict() or {}
            try:
                turn_id = _required_string(reservation.get("turn_id"), "turn_id")
            except RetryableWorkerError:
                continue
            if turn_id in seen_turn_ids or not _recovery_is_due(reservation, now):
                continue
            deadline = _as_utc_or_none(
                reservation.get("usage_reconciliation_deadline_at")
            )
            if deadline is not None and now >= deadline:
                continue
            seen_turn_ids.add(turn_id)
            ledger_snapshot = client.collection(
                self._settings.billing_ledger_collection
            ).document(turn_id).get()
            if not ledger_snapshot.exists:
                continue
            ledger = ledger_snapshot.to_dict() or {}
            ledger_cache[turn_id] = ledger
            if not self._has_pending_openai_usage(ledger):
                continue
            usage = ledger.get("usage")
            if not isinstance(usage, dict):
                continue
            reports = usage.get("raw_turn_usage_reports")
            session_id = str(ledger.get("session_id") or "").strip()
            if not session_id or not isinstance(reports, list):
                continue
            candidates[turn_id] = (
                reservation,
                session_id,
                [dict(report) for report in reports if isinstance(report, dict)],
            )

        lookup_limit = max(
            1,
            self._settings.billing_usage_reconciliation_lookup_batch_size,
        )
        selected = list(candidates.items())[:lookup_limit]
        deferred = set(candidates) - {turn_id for turn_id, _ in selected}
        for turn_id in deferred:
            reservation = candidates[turn_id][0]
            self._defer_usage_lookup_for_capacity(client, reservation, now)

        recovered: dict[str, list[dict[str, Any]]] = {}
        if not selected:
            return ledger_cache, recovered, deferred

        def hydrate(item: tuple[str, tuple[dict[str, Any], str, list[dict[str, Any]]]]):
            turn_id, (_, session_id, reports) = item
            return turn_id, self._usage_hydrator.hydrate(
                session_id=session_id,
                reports=reports,
            )

        concurrency = max(
            1,
            min(
                self._settings.billing_usage_reconciliation_concurrency,
                len(selected),
            ),
        )
        if concurrency == 1:
            for item in selected:
                turn_id, reports = hydrate(item)
                recovered[turn_id] = reports
        else:
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [executor.submit(hydrate, item) for item in selected]
                for future in as_completed(futures):
                    turn_id, reports = future.result()
                    recovered[turn_id] = reports
        return ledger_cache, recovered, deferred

    def _defer_usage_lookup_for_capacity(
        self,
        client: Any,
        reservation: dict[str, Any],
        now: datetime,
    ) -> None:
        """Move an unattempted lookup to the next pass without counting a retry."""
        turn_id = _required_string(reservation.get("turn_id"), "turn_id")
        next_attempt_at = now + timedelta(
            seconds=max(
                1,
                self._settings.billing_usage_reconciliation_initial_backoff_seconds,
            )
        )
        updates: dict[str, Any] = {
            "usage_reconciliation_next_attempt_at": next_attempt_at,
            "updated_at": now,
        }
        if reservation.get("status") == "completed_pending":
            updates["billing_settlement_next_attempt_at"] = next_attempt_at
        client.collection(self._settings.billing_reservations_collection).document(
            turn_id
        ).update(updates)

    def _due_usage_reconciliation_reservations(self, client: Any, now: datetime):
        collection = client.collection(self._settings.billing_reservations_collection)
        batch_size = self._settings.billing_reconciliation_batch_size
        try:
            from google.cloud.firestore_v1.base_query import FieldFilter

            snapshots = []
            for due_field in (
                "usage_reconciliation_deadline_at",
                "usage_reconciliation_next_attempt_at",
            ):
                snapshots.extend(
                    collection.where(
                        filter=FieldFilter(
                            "usage_reconciliation_status", "==", "pending"
                        )
                    )
                    .where(filter=FieldFilter(due_field, "<=", now))
                    .order_by(due_field)
                    .limit(batch_size)
                    .stream()
                )
            unique: dict[str, Any] = {}
            for snapshot in snapshots:
                reservation = snapshot.to_dict() or {}
                if reservation.get("usage_reconciliation_status") != "pending":
                    continue
                if not (
                    _recovery_is_due(reservation, now)
                    or (
                        _as_utc_or_none(
                            reservation.get("usage_reconciliation_deadline_at")
                        )
                        is not None
                        and now
                        >= _as_utc_or_none(
                            reservation.get("usage_reconciliation_deadline_at")
                        )
                    )
                ):
                    continue
                unique[str(reservation.get("turn_id") or getattr(snapshot, "id", id(snapshot)))] = snapshot
            return list(unique.values())[:batch_size]
        except (AttributeError, ModuleNotFoundError, ImportError):
            candidates = []
            for snapshot in collection.stream():
                reservation = snapshot.to_dict() or {}
                if reservation.get("usage_reconciliation_status") != "pending":
                    continue
                deadline = _as_utc_or_none(
                    reservation.get("usage_reconciliation_deadline_at")
                )
                if _recovery_is_due(reservation, now) or (deadline is not None and now >= deadline):
                    next_at = _as_utc_or_none(
                        reservation.get("usage_reconciliation_next_attempt_at")
                    ) or datetime.min.replace(tzinfo=timezone.utc)
                    candidates.append((0 if deadline is not None and now >= deadline else 1, next_at, snapshot))
            candidates.sort(key=lambda item: (item[0], item[1]))
            return [snapshot for _, _, snapshot in candidates[:batch_size]]

    def _legacy_unpriced_reservations(self, client: Any):
        collection = client.collection(self._settings.billing_reservations_collection)
        batch_size = self._settings.billing_reconciliation_batch_size
        try:
            from google.cloud.firestore_v1.base_query import FieldFilter

            return (
                collection.where(filter=FieldFilter("status", "==", "unpriced_released"))
                .order_by("expires_at")
                .limit(batch_size)
                .stream()
            )
        except (AttributeError, ModuleNotFoundError, ImportError):
            candidates = []
            for snapshot in collection.stream():
                reservation = snapshot.to_dict() or {}
                if reservation.get("status") == "unpriced_released":
                    expires_at = _as_utc_or_none(reservation.get("expires_at"))
                    candidates.append(
                        (
                            expires_at or datetime.max.replace(tzinfo=timezone.utc),
                            snapshot,
                        )
                    )
            candidates.sort(key=lambda item: item[0])
            return [snapshot for _, snapshot in candidates[:batch_size]]

    def _schedule_usage_recovery_retry(
        self,
        client: Any,
        reservation: dict[str, Any],
        now: datetime,
    ) -> bool:
        turn_id = _required_string(reservation.get("turn_id"), "turn_id")
        attempts = _nonnegative_count(
            reservation.get("usage_reconciliation_attempt_count"), default=0
        )
        started_at = _as_utc_or_none(
            reservation.get("usage_reconciliation_started_at")
        ) or now
        deadline = _as_utc_or_none(
            reservation.get("usage_reconciliation_deadline_at")
        ) or started_at + timedelta(
            seconds=max(
                1,
                self._settings.billing_usage_reconciliation_retention_seconds,
            )
        )
        if now >= deadline:
            if reservation.get("status") != "reserved":
                self._mark_manual_review(
                    client,
                    reservation,
                    now,
                    reason="provider_usage_retention_expired",
                )
                return False
        initial_delay = max(
            1,
            self._settings.billing_usage_reconciliation_initial_backoff_seconds,
        )
        maximum_delay = max(
            initial_delay,
            self._settings.billing_usage_reconciliation_max_backoff_seconds,
        )
        # The initial attempt is already scheduled at +initial_delay when the
        # marker is created. After its first failure, wait twice that interval.
        delay = min(initial_delay * (2 ** min(attempts + 1, 20)), maximum_delay)
        client.collection(self._settings.billing_reservations_collection).document(
            turn_id
        ).update(
            {
                "usage_reconciliation_status": "pending",
                "usage_reconciliation_attempt_count": attempts + 1,
                "usage_reconciliation_started_at": started_at,
                "usage_reconciliation_deadline_at": deadline,
                "usage_reconciliation_next_attempt_at": now
                + timedelta(seconds=delay),
                "billing_settlement_next_attempt_at": now + timedelta(seconds=delay)
                if reservation.get("status") == "completed_pending"
                else reservation.get("billing_settlement_next_attempt_at"),
                "updated_at": now,
            }
        )
        return True

    def _defer_pending_settlement_retry(
        self,
        client: Any,
        reservation: dict[str, Any],
        now: datetime,
    ) -> None:
        turn_id = _required_string(reservation.get("turn_id"), "turn_id")
        next_usage_attempt = _as_utc_or_none(
            reservation.get("usage_reconciliation_next_attempt_at")
        )
        delay = max(
            1,
            self._settings.billing_usage_reconciliation_initial_backoff_seconds,
        )
        retry_at = max(
            now + timedelta(seconds=delay),
            next_usage_attempt or now,
        )
        client.collection(self._settings.billing_reservations_collection).document(
            turn_id
        ).update(
            {
                "billing_settlement_next_attempt_at": retry_at,
                "updated_at": now,
            }
        )

    def _mark_manual_review(
        self,
        client: Any,
        reservation: dict[str, Any],
        now: datetime,
        *,
        reason: str,
        preserve_settlement: bool = False,
    ) -> None:
        turn_id = _required_string(reservation.get("turn_id"), "turn_id")
        updates: dict[str, Any] = {
            "usage_reconciliation_status": "manual_review",
            "usage_reconciliation_manual_review_reason": reason,
            "usage_reconciliation_manual_review_at": now,
            "usage_reconciliation_next_attempt_at": None,
            "updated_at": now,
        }
        if preserve_settlement:
            # The provider-usage retry is terminal, but a provisional wallet
            # settlement may still be active and must remain independently due.
            if reservation.get("status") == "completed_pending":
                updates["billing_settlement_next_attempt_at"] = now
        else:
            updates["status"] = "usage_recovery_manual_review"
        client.collection(self._settings.billing_reservations_collection).document(
            turn_id
        ).update(updates)
        LOGGER.error(
            "OpenAI usage recovery requires manual billing review for turn %s (%s).",
            turn_id,
            reason,
        )

    def _mark_legacy_unpriced_final(
        self,
        client: Any,
        reservation: dict[str, Any],
        now: datetime,
    ) -> None:
        turn_id = _required_string(reservation.get("turn_id"), "turn_id")
        client.collection(self._settings.billing_reservations_collection).document(
            turn_id
        ).update(
            {
                "status": "unpriced_final",
                "usage_reconciliation_status": "not_applicable",
                "usage_reconciliation_next_attempt_at": None,
                "updated_at": now,
            }
        )

    def _pending_turn_markers(self, client: Any, now: datetime | None = None):
        collection = client.collection(self._settings.billing_reservations_collection)
        batch_size = self._settings.billing_reconciliation_batch_size
        now = now or datetime.now(timezone.utc)
        try:
            from google.cloud.firestore_v1.base_query import FieldFilter

            legacy_holds = list(
                collection.where(filter=FieldFilter("status", "==", "reserved"))
                .order_by("expires_at")
                .limit(batch_size)
                .stream()
            )
            # A v4 worker can crash after the atomic ledger/marker write but
            # before batch enqueue or direct settlement. Query this state using
            # Firestore's single-field status index; it has no expires_at field.
            completed_pending = list(
                collection.where(
                    filter=FieldFilter("status", "==", "completed_pending")
                )
                .where(
                    filter=FieldFilter(
                        "billing_settlement_next_attempt_at", "<=", now
                    )
                )
                .order_by("billing_settlement_next_attempt_at")
                .limit(batch_size)
                .stream()
            )
            return legacy_holds + completed_pending
        except (AttributeError, ModuleNotFoundError, ImportError):
            # Test doubles and older fakes may not implement Firestore queries.
            # Keep each state bounded and preserve expiry ordering for legacy holds.
            legacy_holds = []
            completed_pending = []
            for snapshot in collection.stream():
                reservation = snapshot.to_dict() or {}
                expires_at = reservation.get("expires_at")
                if reservation.get("status") == "reserved":
                    if isinstance(expires_at, datetime):
                        legacy_holds.append((_as_utc(expires_at), snapshot))
                    else:
                        legacy_holds.append((datetime.max.replace(tzinfo=timezone.utc), snapshot))
                elif reservation.get("status") == "completed_pending":
                    next_attempt = _as_utc_or_none(
                        reservation.get("billing_settlement_next_attempt_at")
                    )
                    if next_attempt is not None and next_attempt > now:
                        continue
                    created_at = _as_utc_or_none(reservation.get("created_at"))
                    completed_pending.append(
                        (created_at or datetime.min.replace(tzinfo=timezone.utc), snapshot)
                    )
            legacy_holds.sort(key=lambda item: item[0])
            completed_pending.sort(key=lambda item: item[0])
            return [
                *(snapshot for _, snapshot in legacy_holds[:batch_size]),
                *(snapshot for _, snapshot in completed_pending[:batch_size]),
            ]

    def _recover_openai_usage(
        self,
        client: Any,
        ledger: dict[str, Any],
        *,
        recovered_reports: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        usage = ledger.get("usage")
        if not isinstance(usage, dict) or not self._has_pending_openai_usage(ledger):
            return ledger
        reports = usage.get("raw_turn_usage_reports")
        if not isinstance(reports, list):
            return self._mark_openai_usage_unpriced_pending(client, ledger, usage)

        session_id = str(ledger.get("session_id") or "").strip()
        if not session_id:
            return self._mark_openai_usage_unpriced_pending(client, ledger, usage)
        normalized_reports = [dict(report) for report in reports if isinstance(report, dict)]
        if recovered_reports is None:
            recovered_reports = self._usage_hydrator.hydrate(
                session_id=session_id,
                reports=normalized_reports,
            )
        token_reports = [
            report["usage"]
            for report in recovered_reports
            if has_complete_turn_token_counts(report.get("usage"))
        ]
        completed_count = _nonnegative_count(
            usage.get("completed_agent_turn_count"),
            default=len(recovered_reports),
        )
        missing_count = sum(
            not has_complete_turn_token_counts(report.get("usage"))
            for report in recovered_reports
        )
        if not recovered_reports and completed_count:
            missing_count = completed_count
        model_name = str(usage.get("pricing_model") or ledger.get("pricing_model") or "").strip()
        normalized = aggregate_turn_usage(
            token_reports,
            model_name=model_name,
            completed_turn_count=completed_count,
            missing_usage_turn_count=missing_count,
        )
        normalized.update(
            {
                "provider": "openai_agents_api",
                "billing_status": (
                    "pending_provider_usage"
                    if missing_count
                    else "processed_by_persistence_worker"
                ),
                "raw_turn_usage_reports": recovered_reports,
            }
        )
        if missing_count:
            normalized["known_cost_lower_bound_usd"] = normalized.get(
                "estimated_cost_usd"
            )
            normalized["estimated_cost_complete"] = False
            if (
                self._settings.billing_batch_settlement_enabled
                and normalized.get("estimated_cost_usd") is not None
            ):
                normalized["cost_status"] = "provisional_estimate"
            else:
                normalized["estimated_cost_usd"] = None
                normalized["cost_status"] = "unavailable"
        else:
            normalized["cost_status"] = (
                "estimated"
                if normalized.get("estimated_cost_usd") is not None
                else "unavailable"
            )

        should_persist = (
            recovered_reports != reports
            or usage.get("billing_status") != normalized.get("billing_status")
            or usage.get("usage_reporting_complete")
            != normalized.get("usage_reporting_complete")
            or usage.get("missing_usage_turn_count")
            != normalized.get("missing_usage_turn_count")
            or usage.get("estimated_cost_usd") != normalized.get("estimated_cost_usd")
            or ledger.get("estimated_cost_usd") != normalized.get("estimated_cost_usd")
            or ledger.get("estimated_cost_usd_nanos")
            != _cost_usd_nanos(normalized.get("estimated_cost_usd"))
        )
        if not should_persist:
            return ledger

        turn_id = _required_string(ledger.get("turn_id"), "turn_id")
        thread_id = str(ledger.get("thread_id") or "").strip()
        if thread_id:
            self._messages_repository.update_assistant_usage(
                client,
                thread_id=thread_id,
                turn_id=turn_id,
                usage=normalized,
            )
        self._ledger_repository.update_recovered_usage(client, turn_id, normalized)
        updated = dict(ledger)
        updated["usage"] = normalized
        updated["estimated_cost_usd"] = normalized.get("estimated_cost_usd")
        updated["estimated_cost_usd_nanos"] = _cost_usd_nanos(
            normalized.get("estimated_cost_usd")
        )
        updated["cost_status"] = normalized.get("cost_status", "unavailable")
        return updated

    def _mark_openai_usage_unpriced_pending(
        self,
        client: Any,
        ledger: dict[str, Any],
        usage: dict[str, Any],
    ) -> dict[str, Any]:
        normalized = dict(usage)
        normalized["known_cost_lower_bound_usd"] = normalized.get(
            "estimated_cost_usd"
        )
        normalized["estimated_cost_complete"] = False
        normalized["usage_reporting_complete"] = False
        normalized["billing_status"] = "pending_provider_usage"
        if (
            not self._settings.billing_batch_settlement_enabled
            or normalized.get("estimated_cost_usd") is None
        ):
            normalized["estimated_cost_usd"] = None
            normalized["cost_status"] = "unavailable"
        else:
            normalized["cost_status"] = "provisional_estimate"
        if (
            usage == normalized
            and ledger.get("estimated_cost_usd") == normalized.get("estimated_cost_usd")
            and ledger.get("estimated_cost_usd_nanos")
            == _cost_usd_nanos(normalized.get("estimated_cost_usd"))
        ):
            return ledger
        turn_id = _required_string(ledger.get("turn_id"), "turn_id")
        thread_id = str(ledger.get("thread_id") or "").strip()
        if thread_id:
            self._messages_repository.update_assistant_usage(
                client,
                thread_id=thread_id,
                turn_id=turn_id,
                usage=normalized,
            )
        self._ledger_repository.update_recovered_usage(client, turn_id, normalized)
        updated = dict(ledger)
        updated["usage"] = normalized
        updated["estimated_cost_usd"] = normalized.get("estimated_cost_usd")
        updated["estimated_cost_usd_nanos"] = _cost_usd_nanos(
            normalized.get("estimated_cost_usd")
        )
        updated["cost_status"] = normalized.get("cost_status", "unavailable")
        return updated

    @staticmethod
    def _has_pending_openai_usage(ledger: dict[str, Any]) -> bool:
        usage = ledger.get("usage")
        if not isinstance(usage, dict):
            return False
        provider = usage.get("provider") or ledger.get("provider")
        if provider != "openai_agents_api":
            return False
        missing_count = usage.get("missing_usage_turn_count", 0)
        if isinstance(missing_count, int) and not isinstance(missing_count, bool):
            if missing_count > 0:
                return True
        reports = usage.get("raw_turn_usage_reports")
        if usage.get("usage_reporting_complete") is False:
            return True
        return isinstance(reports, list) and any(
            isinstance(report, dict)
            and not has_complete_turn_token_counts(report.get("usage"))
            for report in reports
        )

    def _release_expired_reservation_sync(
        self,
        client: Any,
        reservation: dict[str, Any],
        released_at: datetime,
    ) -> bool:
        turn_id = _required_string(reservation.get("turn_id"), "turn_id")
        billing_subject_id = _required_string(
            reservation.get("billing_subject_id"),
            "billing_subject_id",
        )
        owner_uid = _required_string(reservation.get("owner_uid"), "owner_uid")
        reservation_ref = client.collection(
            self._settings.billing_reservations_collection
        ).document(turn_id)
        wallet_ref = client.collection(self._settings.wallets_collection).document(
            customer_wallet_document_id(billing_subject_id)
        )
        transaction_ref = client.collection(
            self._settings.wallet_transactions_collection
        ).document(f"reservation_expired_{turn_id}")

        def operation(transaction: Any) -> bool:
            # Keep all reads before writes for Firestore transaction validity.
            current_reservation_snapshot = get_transaction_document_snapshot(transaction, reservation_ref)
            wallet_snapshot = get_transaction_document_snapshot(transaction, wallet_ref)
            transaction_snapshot = get_transaction_document_snapshot(transaction, transaction_ref)
            if transaction_snapshot.exists or not current_reservation_snapshot.exists:
                return False
            current_reservation = current_reservation_snapshot.to_dict() or {}
            if current_reservation.get("status") != "reserved":
                return False
            if not wallet_snapshot.exists:
                raise RetryableWorkerError(
                    f"Wallet is missing for expired reservation {turn_id}."
                )
            wallet = wallet_snapshot.to_dict() or {}
            if (
                wallet.get("status") != "active"
                or wallet.get("billing_subject_id") != billing_subject_id
                or wallet.get("owner_uid") != owner_uid
            ):
                raise RetryableWorkerError(
                    f"Wallet does not match expired reservation {turn_id}."
                )
            try:
                reserved_amount_nanos = nonnegative_int(
                    current_reservation.get("reserved_amount_nanos"),
                    field_name="reserved_amount_nanos",
                )
                available_credit_nanos = int(wallet.get("available_credit_nanos", 0))
                reserved_credit_nanos = nonnegative_int(
                    wallet.get("reserved_credit_nanos"),
                    field_name="reserved_credit_nanos",
                )
            except ValueError as exc:
                raise RetryableWorkerError(
                    f"Wallet data is invalid for expired reservation {turn_id}."
                ) from exc
            if reserved_credit_nanos < reserved_amount_nanos:
                raise RetryableWorkerError(
                    f"Wallet reserved credit is invalid for expired reservation {turn_id}."
                )
            transaction.create(
                transaction_ref,
                {
                    "schema_version": 1,
                    "transaction_id": f"reservation_expired_{turn_id}",
                    "transaction_type": "reservation_expiry_release",
                    "status": "released",
                    "billing_subject_id": billing_subject_id,
                    "owner_uid": owner_uid,
                    "wallet_document_id": customer_wallet_document_id(billing_subject_id),
                    "currency": current_reservation.get("currency", "USD"),
                    "turn_id": turn_id,
                    "request_id": current_reservation.get("request_id"),
                    "agent_id": current_reservation.get("agent_id"),
                    "reservation_id": turn_id,
                    "reserved_amount_nanos": reserved_amount_nanos,
                    "amount_nanos": 0,
                    "released_amount_nanos": reserved_amount_nanos,
                    "created_at": released_at,
                    "reason": "completed_turn_not_received_before_reservation_expiry",
                },
            )
            transaction.update(
                wallet_ref,
                {
                    "available_credit_nanos": available_credit_nanos + reserved_amount_nanos,
                    "reserved_credit_nanos": reserved_credit_nanos - reserved_amount_nanos,
                    "updated_at": released_at,
                    "last_reservation_release_at": released_at,
                },
            )
            transaction.update(
                reservation_ref,
                {
                    "status": "expired_released",
                    "released_at": released_at,
                    "released_amount_nanos": reserved_amount_nanos,
                    "release_reason": "completed_turn_not_received_before_reservation_expiry",
                },
            )
            return True

        return _run_firestore_transaction(client, operation)


def _event_from_ledger_and_reservation(
    ledger: dict[str, Any],
    reservation: dict[str, Any],
) -> TurnCompletedEvent:
    return TurnCompletedEvent(
        event_id=_required_string(ledger.get("event_id"), "event_id"),
        turn_id=_required_string(ledger.get("turn_id"), "turn_id"),
        agent_id=_required_string(ledger.get("agent_id"), "agent_id"),
        user_id=_required_string(ledger.get("uid"), "uid"),
        thread_id=_required_string(ledger.get("thread_id"), "thread_id"),
        session_id=_required_string(ledger.get("session_id"), "session_id"),
        user_message="",
        assistant_message="",
        created_at=_required_datetime(ledger.get("created_at"), "created_at"),
        metadata={
            "billing": {
                "reservation_id": reservation.get("reservation_id"),
                "billing_subject_id": reservation.get("billing_subject_id"),
            }
        },
    )


def _required_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RetryableWorkerError(f"Billing reconciliation field {field_name} is required.")
    return value.strip()


def _reservation_is_expired(reservation: dict[str, Any], now: datetime) -> bool:
    expires_at = reservation.get("expires_at")
    return isinstance(expires_at, datetime) and _as_utc(expires_at) <= now


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _as_utc_or_none(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    return _as_utc(value)


def _recovery_is_due(reservation: dict[str, Any], now: datetime) -> bool:
    next_attempt = _as_utc_or_none(
        reservation.get("usage_reconciliation_next_attempt_at")
    )
    return next_attempt is None or next_attempt <= now


def _nonnegative_count(value: Any, *, default: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return max(0, default)
    return value


def _cost_usd_nanos(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not amount.is_finite() or amount < 0:
        return None
    return int(
        (amount * Decimal("1000000000")).to_integral_value(rounding=ROUND_HALF_UP)
    )


def _required_datetime(value: Any, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise RetryableWorkerError(f"Billing reconciliation field {field_name} is required.")
    return value


def _run_firestore_transaction(client: Any, operation: Callable[[Any], bool]) -> bool:
    from google.cloud import firestore

    transaction = client.transaction()

    @firestore.transactional
    def run(transaction: Any) -> bool:
        return operation(transaction)

    return run(transaction)
