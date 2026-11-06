"""Idempotent batch settlement for priced managed-agent turns.

Each priced turn keeps its immutable per-turn ledger. The lifecycle document
tracks provisional batch estimates; those estimates are not wallet holds and
do not reduce the balance available to the request-path minimum-balance gate.
At batch close, any confirmed cost that exceeds available credit is recorded as
wallet usage debt for the shared top-up repayment flow.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from common.billing import customer_billing_period_document_id, customer_wallet_document_id, nonnegative_int
from common.firestore import get_transaction_document_snapshot
from common.schemas import TurnCompletedEvent
from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.core.errors import RetryableWorkerError
from services.agent_persistence_worker_v3.app.services.firestore_client import get_firestore_client

NANOS_PER_USD = Decimal("1000000000")


@dataclass(frozen=True)
class BillingBatchReconciliationResult:
    scanned_entries: int = 0
    settled_batches: int = 0
    settled_turns: int = 0
    skipped_entries: int = 0


@dataclass(frozen=True)
class BillingBatchSettlementResult:
    batch_id: str
    status: str
    turn_count: int
    estimated_cost_nanos: int
    charged_amount_nanos: int
    shortfall_nanos: int


class BillingBatchSettlementService:
    """Moves completed priced turns into capped, idempotent wallet debit batches."""

    def __init__(
        self,
        *,
        firestore_client_factory: Callable[[], Any] | None = None,
        transaction_runner: Callable[[Any, Callable[[Any], Any]], Any] | None = None,
    ) -> None:
        self._settings = get_settings()
        self._firestore_client_factory = firestore_client_factory or get_firestore_client
        self._transaction_runner = transaction_runner or _run_firestore_transaction

    async def reconcile_due(self) -> BillingBatchReconciliationResult:
        return await asyncio.to_thread(self.reconcile_due_sync)

    def enqueue_turn_sync(
        self,
        event: TurnCompletedEvent,
        *,
        late_usage: bool = False,
        preserve_usage_recovery: bool = False,
    ) -> bool:
        """Add a priced turn estimate to the wallet's settlement batch."""
        billing = self._billing_metadata(event)
        if billing is None:
            return False
        cost_nanos = _event_cost_nanos(event)
        if cost_nanos is None:
            return False
        reservation_id = _required_string(
            billing.get("reservation_id") or billing.get("billing_reservation_id"),
            "reservation_id",
        )
        subject_id = _required_string(billing.get("billing_subject_id"), "billing_subject_id")
        if reservation_id != event.turn_id or subject_id != event.user_id:
            raise RetryableWorkerError(f"Billing metadata does not match turn {event.turn_id}.")

        client = self._firestore_client_factory()
        wallet_id = customer_wallet_document_id(subject_id)
        reservation_ref = client.collection(self._settings.billing_reservations_collection).document(reservation_id)
        ledger_ref = client.collection(self._settings.billing_ledger_collection).document(event.turn_id)
        wallet_ref = client.collection(self._settings.wallets_collection).document(wallet_id)
        created_at = _as_utc(event.created_at)
        period_key, period_start, period_end = _billing_period(created_at)
        usage = event.usage if isinstance(event.usage, dict) else {}
        queued_at = datetime.now(timezone.utc)

        def operation(transaction: Any) -> bool:
            reservation_snapshot = get_transaction_document_snapshot(transaction, reservation_ref)
            if not reservation_snapshot.exists:
                raise RetryableWorkerError(f"Billing reservation {reservation_id} is missing.")
            reservation = reservation_snapshot.to_dict() or {}
            if reservation.get("status") in {"batch_pending", "late_batch_pending"}:
                return False
            ledger_snapshot = get_transaction_document_snapshot(transaction, ledger_ref)
            wallet_snapshot = get_transaction_document_snapshot(transaction, wallet_ref)
            if not ledger_snapshot.exists:
                raise RetryableWorkerError(f"Billing ledger for turn {event.turn_id} is missing.")
            if not wallet_snapshot.exists:
                raise RetryableWorkerError(f"Wallet for turn {event.turn_id} is missing.")
            ledger = ledger_snapshot.to_dict() or {}
            wallet = wallet_snapshot.to_dict() or {}
            expected_status = "late_usage_pending" if late_usage else "completed_pending"
            if reservation.get("status") != expected_status:
                raise RetryableWorkerError(
                    f"Reservation {reservation_id} is {reservation.get('status')!r}; expected {expected_status!r}."
                )
            self._validate_owner(event, reservation, wallet, subject_id)
            if (
                ledger.get("turn_id") != event.turn_id
                or ledger.get("uid") != event.user_id
                or ledger.get("billing_subject_id") != subject_id
                or ledger.get("estimated_cost_usd_nanos") != cost_nanos
            ):
                raise RetryableWorkerError(f"Immutable ledger does not match batch turn {event.turn_id}.")
            pending_batch = _nonnegative(wallet.get("pending_batch_usage_nanos", 0), "pending_batch_usage_nanos")
            if late_usage:
                transaction.update(
                    reservation_ref,
                    {
                        "status": "late_batch_pending",
                        "billing_batch_status": "pending",
                        "estimated_cost_nanos": cost_nanos,
                        "session_id": event.session_id,
                        "period_key": period_key,
                        "period_start": period_start,
                        "period_end": period_end,
                        "pricing_model": usage.get("pricing_model"),
                        "pricing_version": usage.get("pricing_version"),
                        "late_usage": True,
                        "billing_batch_created_at": queued_at,
                        "billing_settlement_next_attempt_at": None,
                        "usage_reconciliation_status": (
                            reservation.get("usage_reconciliation_status")
                            if preserve_usage_recovery
                            else "complete"
                        ),
                        "usage_reconciliation_next_attempt_at": (
                            reservation.get("usage_reconciliation_next_attempt_at")
                            if preserve_usage_recovery
                            else None
                        ),
                        "usage_reconciliation_completed_at": (
                            reservation.get("usage_reconciliation_completed_at")
                            if preserve_usage_recovery
                            else queued_at
                        ),
                        "turn_created_at": created_at,
                        "updated_at": queued_at,
                    },
                )
            else:
                transaction.update(
                    reservation_ref,
                    {
                        "status": "batch_pending",
                        "billing_batch_status": "pending",
                        "estimated_cost_nanos": cost_nanos,
                        "session_id": event.session_id,
                        "period_key": period_key,
                        "period_start": period_start,
                        "period_end": period_end,
                        "pricing_model": usage.get("pricing_model"),
                        "pricing_version": usage.get("pricing_version"),
                        "late_usage": False,
                        "billing_batch_created_at": queued_at,
                        "billing_settlement_next_attempt_at": None,
                        "turn_created_at": created_at,
                        "released_amount_nanos": 0,
                        "updated_at": queued_at,
                    },
                )
            transaction.update(
                wallet_ref,
                {
                    "pending_batch_usage_nanos": pending_batch + cost_nanos,
                    "updated_at": queued_at,
                    "last_batch_enqueue_at": queued_at,
                },
            )
            return True

        return self._transaction_runner(client, operation)

    def update_pending_estimate_sync(self, event: TurnCompletedEvent) -> bool:
        """Keep an open batch aligned with a refreshed partial usage estimate."""
        billing = self._billing_metadata(event)
        cost_nanos = _event_cost_nanos(event)
        if billing is None or cost_nanos is None:
            return False
        reservation_id = _required_string(
            billing.get("reservation_id") or billing.get("billing_reservation_id"),
            "reservation_id",
        )
        subject_id = _required_string(billing.get("billing_subject_id"), "billing_subject_id")
        if reservation_id != event.turn_id or subject_id != event.user_id:
            raise RetryableWorkerError(
                f"Provisional billing metadata does not match turn {event.turn_id}."
            )

        client = self._firestore_client_factory()
        reservation_ref = client.collection(self._settings.billing_reservations_collection).document(reservation_id)
        ledger_ref = client.collection(self._settings.billing_ledger_collection).document(event.turn_id)
        wallet_ref = client.collection(self._settings.wallets_collection).document(customer_wallet_document_id(subject_id))
        now = datetime.now(timezone.utc)

        def operation(transaction: Any) -> bool:
            reservation_snapshot = get_transaction_document_snapshot(transaction, reservation_ref)
            ledger_snapshot = get_transaction_document_snapshot(transaction, ledger_ref)
            wallet_snapshot = get_transaction_document_snapshot(transaction, wallet_ref)
            if not reservation_snapshot.exists or not ledger_snapshot.exists or not wallet_snapshot.exists:
                raise RetryableWorkerError(
                    f"Provisional usage records for turn {event.turn_id} are incomplete."
                )
            reservation = reservation_snapshot.to_dict() or {}
            ledger = ledger_snapshot.to_dict() or {}
            wallet = wallet_snapshot.to_dict() or {}
            if (
                reservation.get("owner_uid") != event.user_id
                or reservation.get("billing_subject_id") != subject_id
                or ledger.get("turn_id") != event.turn_id
                or ledger.get("uid") != event.user_id
                or ledger.get("estimated_cost_usd_nanos") != cost_nanos
                or wallet.get("owner_uid") != event.user_id
                or wallet.get("billing_subject_id") != subject_id
                or wallet.get("status") != "active"
            ):
                raise RetryableWorkerError(
                    f"Provisional usage records for turn {event.turn_id} do not match."
                )
            if reservation.get("status") not in {"batch_pending", "late_batch_pending"}:
                return False
            old_cost = _nonnegative(reservation.get("estimated_cost_nanos"), "estimated_cost_nanos")
            if old_cost == cost_nanos:
                return True
            pending_total = _nonnegative(wallet.get("pending_batch_usage_nanos", 0), "pending_batch_usage_nanos")
            if pending_total < old_cost:
                raise RetryableWorkerError(
                    f"Pending batch amount is lower than turn {event.turn_id}'s estimate."
                )
            transaction.update(
                wallet_ref,
                {
                    "pending_batch_usage_nanos": pending_total - old_cost + cost_nanos,
                    "updated_at": now,
                },
            )
            transaction.update(
                reservation_ref,
                {
                    "estimated_cost_nanos": cost_nanos,
                    "updated_at": now,
                },
            )
            return True

        return self._transaction_runner(client, operation)

    def reconcile_authoritative_usage_sync(self, event: TurnCompletedEvent) -> bool:
        """Replace a provisional amount or apply its final per-turn difference.

        Provider lookups run independently of batch close. If the batch is
        still pending, update its estimate in place. If it has settled, record
        an idempotent wallet adjustment and route any positive shortfall into
        the shared usage-debt balance.
        """
        billing = self._billing_metadata(event)
        cost_nanos = _event_cost_nanos(event)
        if billing is None or cost_nanos is None:
            return False
        reservation_id = _required_string(
            billing.get("reservation_id") or billing.get("billing_reservation_id"),
            "reservation_id",
        )
        subject_id = _required_string(billing.get("billing_subject_id"), "billing_subject_id")
        if reservation_id != event.turn_id or subject_id != event.user_id:
            raise RetryableWorkerError(
                f"Authoritative billing metadata does not match turn {event.turn_id}."
            )

        client = self._firestore_client_factory()
        reservation_ref = client.collection(self._settings.billing_reservations_collection).document(reservation_id)
        ledger_ref = client.collection(self._settings.billing_ledger_collection).document(event.turn_id)
        wallet_ref = client.collection(self._settings.wallets_collection).document(customer_wallet_document_id(subject_id))
        period_key, period_start, period_end = _billing_period(_as_utc(event.created_at))
        period_ref = client.collection(self._settings.customer_billing_periods_collection).document(
            customer_billing_period_document_id(subject_id, period_key)
        )
        adjustment_id = f"usage_adjustment_{event.turn_id}"
        adjustment_ref = client.collection(self._settings.wallet_transactions_collection).document(adjustment_id)
        now = datetime.now(timezone.utc)

        def operation(transaction: Any) -> bool:
            reservation_snapshot = get_transaction_document_snapshot(transaction, reservation_ref)
            ledger_snapshot = get_transaction_document_snapshot(transaction, ledger_ref)
            wallet_snapshot = get_transaction_document_snapshot(transaction, wallet_ref)
            period_snapshot = get_transaction_document_snapshot(transaction, period_ref)
            adjustment_snapshot = get_transaction_document_snapshot(transaction, adjustment_ref)
            if not reservation_snapshot.exists or not ledger_snapshot.exists or not wallet_snapshot.exists:
                raise RetryableWorkerError(
                    f"Authoritative usage records for turn {event.turn_id} are incomplete."
                )
            reservation = reservation_snapshot.to_dict() or {}
            ledger = ledger_snapshot.to_dict() or {}
            wallet = wallet_snapshot.to_dict() or {}
            if (
                reservation.get("turn_id") != event.turn_id
                or reservation.get("owner_uid") != event.user_id
                or reservation.get("billing_subject_id") != subject_id
                or ledger.get("turn_id") != event.turn_id
                or ledger.get("uid") != event.user_id
                or ledger.get("billing_subject_id") != subject_id
                or wallet.get("owner_uid") != event.user_id
                or wallet.get("billing_subject_id") != subject_id
                or wallet.get("status") != "active"
            ):
                raise RetryableWorkerError(
                    f"Authoritative usage records for turn {event.turn_id} do not match."
                )
            if ledger.get("estimated_cost_usd_nanos") != cost_nanos:
                raise RetryableWorkerError(
                    f"Ledger cost does not match authoritative usage for turn {event.turn_id}."
                )

            status = reservation.get("status")
            if status in {"batch_pending", "late_batch_pending"}:
                old_cost = _nonnegative(reservation.get("estimated_cost_nanos"), "estimated_cost_nanos")
                pending_total = _nonnegative(wallet.get("pending_batch_usage_nanos", 0), "pending_batch_usage_nanos")
                if pending_total < old_cost:
                    raise RetryableWorkerError(
                        f"Pending batch amount is lower than turn {event.turn_id}'s provisional estimate."
                    )
                transaction.update(
                    wallet_ref,
                    {
                        "pending_batch_usage_nanos": pending_total - old_cost + cost_nanos,
                        "updated_at": now,
                    },
                )
                transaction.update(
                    reservation_ref,
                    {
                        "estimated_cost_nanos": cost_nanos,
                        "billing_authoritative_cost_nanos": cost_nanos,
                        "usage_reconciliation_status": "complete",
                        "usage_reconciliation_next_attempt_at": None,
                        "usage_reconciliation_completed_at": now,
                        "updated_at": now,
                    },
                )
                return True

            if status in {"late_usage_pending", "completed_pending"}:
                return False

            assessed_cost = reservation.get("billing_assessed_cost_nanos")
            if assessed_cost is None:
                return False
            assessed_cost = _nonnegative(assessed_cost, "billing_assessed_cost_nanos")
            if adjustment_snapshot.exists:
                return True
            delta = cost_nanos - assessed_cost
            try:
                available = _nonnegative(wallet.get("available_credit_nanos", 0), "available_credit_nanos")
                settled_usage = _nonnegative(wallet.get("settled_usage_nanos", 0), "settled_usage_nanos")
                usage_debt = _nonnegative(wallet.get("usage_debt_nanos", 0), "usage_debt_nanos")
                lifetime_debt = _nonnegative(wallet.get("lifetime_usage_debt_added_nanos", 0), "lifetime_usage_debt_added_nanos")
                assessed_collected = _nonnegative(reservation.get("billing_assessed_collected_nanos", 0), "billing_assessed_collected_nanos")
                assessed_debt = _nonnegative(reservation.get("billing_assessed_debt_nanos", 0), "billing_assessed_debt_nanos")
            except ValueError as exc:
                raise RetryableWorkerError(
                    f"Wallet or assessment amounts for turn {event.turn_id} are invalid."
                ) from exc

            period = period_snapshot.to_dict() or {}
            previous_estimated = _nonnegative(period.get("usage_estimated_nanos", 0), "usage_estimated_nanos")
            previous_collected = _nonnegative(period.get("collected_usage_nanos", 0), "collected_usage_nanos")
            previous_shortfall = _nonnegative(period.get("uncollected_usage_nanos", 0), "uncollected_usage_nanos")
            charged = debt_added = debt_reduced = refunded = collected_reduction = assessed_debt_reduction = 0
            if delta > 0:
                charged = min(delta, available)
                debt_added = delta - charged
                new_available = available - charged
                new_settled = settled_usage + charged
                new_debt = usage_debt + debt_added
                new_lifetime_debt = lifetime_debt + debt_added
                new_assessed_collected = assessed_collected + charged
                new_assessed_debt = assessed_debt + debt_added
                new_period_estimated = previous_estimated + delta
                new_period_collected = previous_collected + charged
                new_period_shortfall = previous_shortfall + debt_added
            elif delta < 0:
                reduction = -delta
                assessed_debt_reduction = min(reduction, assessed_debt)
                collected_reduction = min(reduction - assessed_debt_reduction, assessed_collected)
                debt_reduced = min(reduction, usage_debt)
                refunded = reduction - debt_reduced
                new_available = available + refunded
                new_settled = max(0, settled_usage - collected_reduction)
                new_debt = usage_debt - debt_reduced
                new_lifetime_debt = lifetime_debt
                new_assessed_collected = assessed_collected - collected_reduction
                new_assessed_debt = assessed_debt - assessed_debt_reduction
                new_period_estimated = max(0, previous_estimated - reduction)
                new_period_collected = max(0, previous_collected - collected_reduction)
                new_period_shortfall = max(0, previous_shortfall - assessed_debt_reduction)
            else:
                transaction.update(
                    reservation_ref,
                    {
                        "billing_authoritative_cost_nanos": cost_nanos,
                        "usage_reconciliation_status": "complete",
                        "usage_reconciliation_next_attempt_at": None,
                        "usage_reconciliation_completed_at": now,
                        "updated_at": now,
                    },
                )
                return True

            transaction.create(
                adjustment_ref,
                {
                    "schema_version": 1,
                    "transaction_id": adjustment_id,
                    "transaction_type": "agent_usage_authoritative_adjustment",
                    "status": "adjusted",
                    "billing_subject_id": subject_id,
                    "owner_uid": event.user_id,
                    "wallet_document_id": customer_wallet_document_id(subject_id),
                    "currency": "USD",
                    "turn_id": event.turn_id,
                    "ledger_document_id": event.turn_id,
                    "reservation_id": reservation_id,
                    "prior_assessed_cost_nanos": assessed_cost,
                    "authoritative_cost_nanos": cost_nanos,
                    "estimated_cost_delta_nanos": delta,
                    "amount_nanos": charged,
                    "shortfall_nanos": debt_added,
                    "debt_reduced_nanos": debt_reduced,
                    "refunded_nanos": refunded,
                    "remaining_usage_debt_nanos": new_debt,
                    "period_id": customer_billing_period_document_id(subject_id, period_key),
                    "created_at": now,
                },
            )
            transaction.update(
                wallet_ref,
                {
                    "available_credit_nanos": new_available,
                    "settled_usage_nanos": new_settled,
                    "usage_debt_nanos": new_debt,
                    "lifetime_usage_debt_added_nanos": new_lifetime_debt,
                    "updated_at": now,
                    "last_settlement_at": now,
                },
            )
            transaction.update(
                reservation_ref,
                {
                    "estimated_cost_nanos": cost_nanos,
                    "billing_assessed_cost_nanos": cost_nanos,
                    "billing_assessed_collected_nanos": new_assessed_collected,
                    "billing_assessed_debt_nanos": new_assessed_debt,
                    "settled_amount_nanos": new_assessed_collected,
                    "shortfall_nanos": new_assessed_debt,
                    "billing_authoritative_cost_nanos": cost_nanos,
                    "usage_reconciliation_status": "complete",
                    "usage_reconciliation_next_attempt_at": None,
                    "usage_reconciliation_completed_at": now,
                    "updated_at": now,
                },
            )
            period_updates = {
                "usage_estimated_nanos": new_period_estimated,
                "collected_usage_nanos": new_period_collected,
                "uncollected_usage_nanos": new_period_shortfall,
                "updated_at": now,
                "last_provider_estimate_status": "authoritative_usage_reconciled",
            }
            if period_snapshot.exists:
                transaction.update(period_ref, period_updates)
            else:
                transaction.create(
                    period_ref,
                    {
                        "schema_version": 1,
                        "billing_subject_id": subject_id,
                        "owner_uid": event.user_id,
                        "currency": "USD",
                        "period_key": period_key,
                        "period_start": period_start,
                        "period_end": period_end,
                        "status": "open",
                        "monthly_service_fee_nanos": self._settings.monthly_service_fee_nanos,
                        "monthly_service_fee_status": "pending_collection",
                        "created_at": now,
                        **period_updates,
                    },
                )
            return True

        return self._transaction_runner(client, operation)

    def reconcile_due_sync(self) -> BillingBatchReconciliationResult:
        client = self._firestore_client_factory()
        now = datetime.now(timezone.utc)
        snapshots = list(self._pending_entries(client))
        groups: dict[tuple[str, str, str, bool], list[tuple[Any, dict[str, Any]]]] = defaultdict(list)
        for snapshot in snapshots:
            data = snapshot.to_dict() or {}
            if data.get("status") not in {"batch_pending", "late_batch_pending"}:
                continue
            subject = str(data.get("billing_subject_id") or "").strip()
            session = str(data.get("session_id") or "").strip()
            period = str(data.get("period_key") or "").strip()
            if subject and period:
                is_late = data.get("status") == "late_batch_pending"
                groups[(subject, session, period, is_late)].append((snapshot, data))

        settled_batches = settled_turns = skipped = 0
        for _, rows in groups.items():
            rows.sort(key=lambda item: (_as_utc(item[1].get("billing_batch_created_at")), str(item[1].get("turn_id"))))
            candidates: list[tuple[Any, dict[str, Any]]] = []
            exposure = 0
            for row in rows:
                candidates.append(row)
                exposure += _nonnegative(row[1].get("estimated_cost_nanos"), "estimated_cost_nanos")
                if (
                    len(candidates) >= self._settings.billing_batch_max_turns
                    or exposure >= self._settings.billing_batch_max_exposure_nanos
                ):
                    break
            if not self._group_is_due(candidates, now):
                skipped += len(rows)
                continue
            result = self._settle_batch(client, candidates)
            if result is None:
                skipped += len(candidates)
                continue
            settled_batches += 1
            settled_turns += result.turn_count
            skipped += max(0, len(rows) - len(candidates))
        return BillingBatchReconciliationResult(
            scanned_entries=len(snapshots),
            settled_batches=settled_batches,
            settled_turns=settled_turns,
            skipped_entries=skipped,
        )

    def _pending_entries(self, client: Any):
        collection = client.collection(self._settings.billing_reservations_collection)
        limit = max(1, self._settings.billing_reconciliation_batch_size // 2)
        try:
            from google.cloud.firestore_v1.base_query import FieldFilter

            snapshots = []
            for status in ("batch_pending", "late_batch_pending"):
                snapshots.extend(
                    collection.where(filter=FieldFilter("status", "==", status))
                    .order_by("billing_batch_created_at")
                    .limit(limit)
                    .stream()
                )
            return snapshots
        except (AttributeError, ModuleNotFoundError, ImportError):
            snapshots = []
            for snapshot in collection.stream():
                if (snapshot.to_dict() or {}).get("status") in {"batch_pending", "late_batch_pending"}:
                    snapshots.append(snapshot)
            snapshots.sort(key=lambda snap: _as_utc((snap.to_dict() or {}).get("billing_batch_created_at")))
            return snapshots[: max(1, self._settings.billing_reconciliation_batch_size)]

    def _group_is_due(self, rows: list[tuple[Any, dict[str, Any]]], now: datetime) -> bool:
        if not rows:
            return False
        if len(rows) >= self._settings.billing_batch_max_turns:
            return True
        exposure = sum(_nonnegative(row.get("estimated_cost_nanos"), "estimated_cost_nanos") for _, row in rows)
        if exposure >= self._settings.billing_batch_max_exposure_nanos:
            return True
        oldest = _as_utc(rows[0][1].get("billing_batch_created_at"))
        newest = _as_utc(rows[-1][1].get("billing_batch_created_at"))
        if (now - oldest).total_seconds() >= self._settings.billing_batch_max_age_seconds:
            return True
        return (now - newest).total_seconds() >= self._settings.billing_batch_session_idle_seconds

    def _settle_batch(self, client: Any, rows: list[tuple[Any, dict[str, Any]]]) -> BillingBatchSettlementResult | None:
        reservation_refs = [snapshot.reference for snapshot, _ in rows]
        entries = [data for _, data in rows]
        turn_ids = sorted(str(entry["turn_id"]) for entry in entries)
        batch_id = "usage_batch_" + hashlib.sha256("|".join(turn_ids).encode("utf-8")).hexdigest()[:40]
        subject_id = str(entries[0]["billing_subject_id"])
        owner_uid = str(entries[0]["owner_uid"])
        if any(entry.get("billing_subject_id") != subject_id or entry.get("owner_uid") != owner_uid for entry in entries):
            raise RetryableWorkerError("A billing batch contains multiple wallet owners.")
        wallet_ref = client.collection(self._settings.wallets_collection).document(customer_wallet_document_id(subject_id))
        ledger_refs = [client.collection(self._settings.billing_ledger_collection).document(str(entry["turn_id"])) for entry in entries]
        transaction_ref = client.collection(self._settings.wallet_transactions_collection).document(batch_id)
        period_id = customer_billing_period_document_id(subject_id, str(entries[0]["period_key"]))
        period_ref = client.collection(self._settings.customer_billing_periods_collection).document(period_id)
        settled_at = datetime.now(timezone.utc)
        late_batch = bool(entries[0].get("late_usage"))

        def operation(transaction: Any) -> BillingBatchSettlementResult | None:
            transaction_snapshot = get_transaction_document_snapshot(transaction, transaction_ref)
            if transaction_snapshot.exists:
                return _existing_batch_result(transaction_snapshot.to_dict() or {}, batch_id)
            reservation_snapshots = [get_transaction_document_snapshot(transaction, ref) for ref in reservation_refs]
            ledger_snapshots = [get_transaction_document_snapshot(transaction, ref) for ref in ledger_refs]
            wallet_snapshot = get_transaction_document_snapshot(transaction, wallet_ref)
            period_snapshot = get_transaction_document_snapshot(transaction, period_ref)
            if not wallet_snapshot.exists or any(not snap.exists for snap in reservation_snapshots + ledger_snapshots):
                raise RetryableWorkerError(f"Wallet, reservation, or ledger is missing for batch {batch_id}.")
            wallet = wallet_snapshot.to_dict() or {}
            reservations = [snap.to_dict() or {} for snap in reservation_snapshots]
            ledgers = [snap.to_dict() or {} for snap in ledger_snapshots]
            if wallet.get("billing_subject_id") != subject_id or wallet.get("owner_uid") != owner_uid or wallet.get("status") != "active":
                raise RetryableWorkerError(f"Wallet ownership/status mismatch for batch {batch_id}.")
            expected_reservation_status = "late_batch_pending" if late_batch else "batch_pending"
            for entry, reservation, ledger in zip(entries, reservations, ledgers):
                if entry.get("owner_uid") != owner_uid or entry.get("billing_subject_id") != subject_id:
                    raise RetryableWorkerError(f"Batch reservation owner mismatch for {batch_id}.")
                if reservation.get("status") != expected_reservation_status or reservation.get("turn_id") != entry.get("turn_id"):
                    raise RetryableWorkerError(f"Reservation state mismatch for batch turn {entry.get('turn_id')}.")
                if (
                    ledger.get("turn_id") != entry.get("turn_id")
                    or ledger.get("uid") != owner_uid
                    or ledger.get("billing_subject_id") != subject_id
                    or ledger.get("estimated_cost_usd_nanos") != entry.get("estimated_cost_nanos")
                    or reservation.get("estimated_cost_nanos") != entry.get("estimated_cost_nanos")
                ):
                    raise RetryableWorkerError(f"Ledger mismatch for batch turn {entry.get('turn_id')}.")
            try:
                available = _nonnegative(wallet.get("available_credit_nanos", 0), "available_credit_nanos")
                pending_total = _nonnegative(wallet.get("pending_batch_usage_nanos", 0), "pending_batch_usage_nanos")
                total_cost = sum(
                    _nonnegative(reservation.get("estimated_cost_nanos"), "estimated_cost_nanos")
                    for reservation in reservations
                )
                # Legacy wallets may still carry reserved_credit_nanos. It is
                # preserved for compatibility, but v4 never creates new holds.
                wallet_reserved = _nonnegative(wallet.get("reserved_credit_nanos", 0), "reserved_credit_nanos")
                settled_usage = _nonnegative(wallet.get("settled_usage_nanos", 0), "settled_usage_nanos")
                pending_count = _nonnegative(wallet.get("pending_usage_reconciliation_count", 0), "pending_usage_reconciliation_count")
            except ValueError as exc:
                raise RetryableWorkerError(f"Invalid wallet or batch amounts for {batch_id}.") from exc
            if pending_total < total_cost:
                raise RetryableWorkerError(f"Pending batch amount is lower than batch cost for {batch_id}.")
            # Pending batch values are estimates, not holds. Charge against the
            # actual available balance; preserve any unfunded confirmed amount
            # as debt instead of leaving it mixed with provisional estimates.
            spendable = available
            charges, shortfalls = _allocate_costs(reservations, spendable)
            charged = sum(charges)
            total_shortfall = sum(shortfalls)
            new_available = available - charged
            new_pending = pending_total - total_cost
            usage_debt = _nonnegative(wallet.get("usage_debt_nanos", 0), "usage_debt_nanos")
            transaction.create(
                transaction_ref,
                {
                    "schema_version": 1,
                    "transaction_id": batch_id,
                    "transaction_type": "agent_usage_late_batch_debit" if late_batch else "agent_usage_batch_debit",
                    "status": "late_settled_shortfall" if late_batch and total_shortfall else "settled_shortfall" if total_shortfall else "late_settled" if late_batch else "settled",
                    "billing_subject_id": subject_id,
                    "owner_uid": owner_uid,
                    "wallet_document_id": customer_wallet_document_id(subject_id),
                    "currency": "USD",
                    "turn_ids": turn_ids,
                    "turn_count": len(entries),
                    "estimated_cost_nanos": total_cost,
                    "amount_nanos": charged,
                    "shortfall_nanos": total_shortfall,
                    "usage_debt_added_nanos": total_shortfall,
                    "remaining_usage_debt_nanos": usage_debt + total_shortfall,
                    "period_id": period_id,
                    "provisional": True,
                    "created_at": settled_at,
                },
            )
            transaction.update(
                wallet_ref,
                {
                    "available_credit_nanos": new_available,
                    "reserved_credit_nanos": wallet_reserved,
                    "pending_batch_usage_nanos": new_pending,
                    "settled_usage_nanos": settled_usage + charged,
                    "usage_debt_nanos": usage_debt + total_shortfall,
                    "lifetime_usage_debt_added_nanos": _nonnegative(wallet.get("lifetime_usage_debt_added_nanos", 0), "lifetime_usage_debt_added_nanos") + total_shortfall,
                    "updated_at": settled_at,
                    "last_settlement_at": settled_at,
                },
            )
            for entry, reservation, reservation_ref, charge, shortfall in zip(entries, reservations, reservation_refs, charges, shortfalls):
                reservation_status = ("late_settled_shortfall" if shortfall else "late_settled") if late_batch else ("settled_shortfall" if shortfall else "settled")
                reserved_amount = 0 if late_batch else _nonnegative(reservation.get("reserved_amount_nanos", 0), "reserved_amount_nanos")
                late_usage_still_pending = (
                    late_batch
                    and reservation.get("usage_reconciliation_status") == "pending"
                )
                usage_reconciliation_status = reservation.get(
                    "usage_reconciliation_status"
                )
                if (
                    late_batch
                    and not late_usage_still_pending
                    and usage_reconciliation_status != "manual_review"
                ):
                    usage_reconciliation_status = "complete"
                transaction.update(
                    reservation_ref,
                    {
                        "status": reservation_status,
                        "settled_at": settled_at,
                        "settled_amount_nanos": charge,
                        "released_amount_nanos": reserved_amount,
                        "estimated_cost_nanos": _nonnegative(reservation.get("estimated_cost_nanos"), "estimated_cost_nanos"),
                        "billing_assessed_cost_nanos": _nonnegative(reservation.get("estimated_cost_nanos"), "estimated_cost_nanos"),
                        "billing_assessed_collected_nanos": charge,
                        "billing_assessed_debt_nanos": shortfall,
                        "late_usage_shortfall_nanos": shortfall if late_batch else reservation.get("late_usage_shortfall_nanos", 0),
                        "shortfall_nanos": shortfall,
                        "billing_batch_id": batch_id,
                        "billing_batch_status": "settled",
                        "usage_reconciliation_status": usage_reconciliation_status,
                        "usage_recovery_hold_registered": False,
                        "usage_reconciliation_next_attempt_at": reservation.get("usage_reconciliation_next_attempt_at") if late_usage_still_pending else None if late_batch else reservation.get("usage_reconciliation_next_attempt_at"),
                        "updated_at": settled_at,
                    },
                )

            self._update_period(
                transaction=transaction,
                period_ref=period_ref,
                period_snapshot=period_snapshot,
                entries=[{**entry, "late_usage": late_batch} for entry in entries],
                charged=charged,
                shortfall=total_shortfall,
                settled_at=settled_at,
                owner_uid=owner_uid,
                subject_id=subject_id,
            )
            status = "late_settled_shortfall" if late_batch and total_shortfall else "settled_shortfall" if total_shortfall else "late_settled" if late_batch else "settled"
            return BillingBatchSettlementResult(batch_id, status, len(entries), total_cost, charged, total_shortfall)

        return self._transaction_runner(client, operation)

    def _update_period(self, *, transaction: Any, period_ref: Any, period_snapshot: Any, entries: list[dict[str, Any]], charged: int, shortfall: int, settled_at: datetime, owner_uid: str, subject_id: str) -> None:
        period = period_snapshot.to_dict() or {}
        late = bool(entries[0].get("late_usage"))
        total_cost = sum(_nonnegative(item.get("estimated_cost_nanos"), "estimated_cost_nanos") for item in entries)
        previous_usage = _nonnegative(period.get("usage_estimated_nanos", 0), "usage_estimated_nanos")
        previous_collected = _nonnegative(period.get("collected_usage_nanos", 0), "collected_usage_nanos")
        previous_shortfall = _nonnegative(period.get("uncollected_usage_nanos", 0), "uncollected_usage_nanos")
        previous_turns = _nonnegative(period.get("usage_turn_count", 0), "usage_turn_count")
        previous_unpriced = _nonnegative(period.get("unpriced_turn_count", 0), "unpriced_turn_count")
        updates = {
            "usage_estimated_nanos": previous_usage + total_cost,
            "collected_usage_nanos": previous_collected + charged,
            "uncollected_usage_nanos": previous_shortfall + shortfall,
            "usage_turn_count": previous_turns + (0 if late else len(entries)),
            "unpriced_turn_count": max(0, previous_unpriced - (len(entries) if late else 0)),
            "last_turn_at": max((_as_utc(item.get("turn_created_at")) for item in entries), default=settled_at),
            "updated_at": settled_at,
            "last_provider_estimate_status": "provisional_pending_daily_provider_reconciliation",
        }
        if period_snapshot.exists:
            transaction.update(period_ref, updates)
            return
        period_key = str(entries[0]["period_key"])
        period_start, period_end = _period_bounds(period_key)
        transaction.create(
            period_ref,
            {
                "schema_version": 1,
                "billing_subject_id": subject_id,
                "owner_uid": owner_uid,
                "currency": "USD",
                "period_key": period_key,
                "period_start": period_start,
                "period_end": period_end,
                "status": "open",
                "monthly_service_fee_nanos": self._settings.monthly_service_fee_nanos,
                "monthly_service_fee_status": "pending_collection",
                "created_at": settled_at,
                **updates,
            },
        )

    @staticmethod
    def _billing_metadata(event: TurnCompletedEvent) -> dict[str, Any] | None:
        metadata = event.metadata if isinstance(event.metadata, dict) else {}
        billing = metadata.get("billing")
        return dict(billing) if isinstance(billing, dict) else None

    @staticmethod
    def _validate_owner(event: TurnCompletedEvent, reservation: dict[str, Any], wallet: dict[str, Any], subject: str) -> None:
        if reservation.get("turn_id") != event.turn_id or reservation.get("owner_uid") != event.user_id or reservation.get("billing_subject_id") != subject or reservation.get("agent_id") != event.agent_id:
            raise RetryableWorkerError(f"Reservation ownership mismatch for turn {event.turn_id}.")
        if wallet.get("owner_uid") != event.user_id or wallet.get("billing_subject_id") != subject or wallet.get("status") != "active":
            raise RetryableWorkerError(f"Wallet ownership/status mismatch for turn {event.turn_id}.")


def _allocate_costs(entries: list[dict[str, Any]], available_credit_nanos: int) -> tuple[list[int], list[int]]:
    remaining = max(0, available_credit_nanos)
    charges: list[int] = []
    shortfalls: list[int] = []
    for entry in entries:
        cost = _nonnegative(entry.get("estimated_cost_nanos"), "estimated_cost_nanos")
        charged = min(cost, remaining)
        charges.append(charged)
        shortfalls.append(cost - charged)
        remaining -= charged
    return charges, shortfalls


def _event_cost_nanos(event: TurnCompletedEvent) -> int | None:
    usage = event.usage if isinstance(event.usage, dict) else {}
    raw = usage.get("estimated_cost_usd")
    if raw is None or isinstance(raw, bool):
        return None
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, ValueError):
        return None
    if not value.is_finite() or value < 0:
        return None
    return int((value * NANOS_PER_USD).to_integral_value(rounding=ROUND_HALF_UP))


def _existing_batch_result(data: dict[str, Any], batch_id: str) -> BillingBatchSettlementResult:
    return BillingBatchSettlementResult(
        batch_id=batch_id,
        status=str(data.get("status", "settled")),
        turn_count=int(data.get("turn_count", len(data.get("turn_ids") or []))),
        estimated_cost_nanos=_nonnegative(data.get("estimated_cost_nanos", 0), "estimated_cost_nanos"),
        charged_amount_nanos=_nonnegative(data.get("amount_nanos", 0), "amount_nanos"),
        shortfall_nanos=_nonnegative(data.get("shortfall_nanos", 0), "shortfall_nanos"),
    )


def _nonnegative(value: Any, name: str) -> int:
    try:
        return nonnegative_int(value, field_name=name)
    except ValueError as exc:
        raise RetryableWorkerError(f"Invalid {name} in billing batch.") from exc


def _required_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RetryableWorkerError(f"Billing field {name} is required.")
    return value.strip()


def _as_utc(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise RetryableWorkerError("Billing batch entry is missing a timestamp.")
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _billing_period(value: datetime) -> tuple[str, datetime, datetime]:
    start, end = _period_bounds(value.strftime("%Y-%m"))
    return value.strftime("%Y-%m"), start, end


def _period_bounds(period_key: str) -> tuple[datetime, datetime]:
    try:
        year_text, month_text = period_key.split("-", 1)
        year, month = int(year_text), int(month_text)
        start = datetime(year, month, 1, tzinfo=timezone.utc)
    except (ValueError, TypeError) as exc:
        raise RetryableWorkerError("Billing batch has an invalid period key.") from exc
    end = datetime(year + 1, 1, 1, tzinfo=timezone.utc) if month == 12 else datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return start, end


def _run_firestore_transaction(client: Any, operation: Callable[[Any], Any]) -> Any:
    from google.cloud import firestore

    @firestore.transactional
    def run(transaction: Any) -> Any:
        return operation(transaction)

    return run(client.transaction())
