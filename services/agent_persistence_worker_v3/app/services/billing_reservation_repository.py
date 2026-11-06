"""Worker-owned, zero-hold lifecycle markers for completed turns."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from common.schemas import TurnCompletedEvent
from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.core.errors import RetryableWorkerError


class BillingReservationRepository:
    """Creates the turn lifecycle marker atomically with its immutable ledger.

    The legacy collection name is retained for schema compatibility, but new
    documents are not wallet holds: the gateway performs a read-only minimum
    balance check, and estimates are not subtracted from wallet balance.
    """

    def __init__(self, collection: str | None = None) -> None:
        self._collection = collection or get_settings().billing_reservations_collection
        self._settings = get_settings()

    def add_create_to_batch(
        self, batch: Any, client: Any, event: TurnCompletedEvent
    ) -> None:
        metadata = event.metadata if isinstance(event.metadata, dict) else {}
        billing = metadata.get("billing")
        if not isinstance(billing, dict):
            return
        turn_id = str(event.turn_id)
        reservation_id = str(
            billing.get("reservation_id") or billing.get("billing_reservation_id") or ""
        ).strip()
        subject_id = str(billing.get("billing_subject_id") or "").strip()
        if reservation_id != turn_id or subject_id != event.user_id:
            raise RetryableWorkerError(
                f"Billing metadata does not match completed turn {turn_id}."
            )

        now = datetime.now(timezone.utc)
        usage = event.usage if isinstance(event.usage, dict) else {}
        provider_pending = _has_pending_provider_usage(usage)
        provisional_estimate = (
            provider_pending
            and self._settings.billing_batch_settlement_enabled
            and usage.get("estimated_cost_usd") is not None
        )
        status = (
            "late_usage_pending"
            if provider_pending and not provisional_estimate
            else "completed_pending"
        )
        data: dict[str, Any] = {
            "schema_version": 1,
            "reservation_id": turn_id,
            "turn_id": turn_id,
            "request_id": billing.get("request_id"),
            "agent_id": event.agent_id,
            "session_id": event.session_id,
            "thread_id": event.thread_id,
            "owner_uid": event.user_id,
            "billing_subject_id": subject_id,
            "currency": "USD",
            "reserved_amount_nanos": 0,
            "minimum_balance_nanos": _nonnegative_int(
                billing.get("minimum_balance_nanos")
            ),
            "authorization_mode": "minimum_balance_check",
            "status": status,
            "billing_settlement_next_attempt_at": now
            if status == "completed_pending"
            else None,
            "created_at": event.created_at or now,
            "turn_created_at": event.created_at or now,
            "updated_at": now,
        }
        if provider_pending:
            data.update(
                {
                    "usage_reconciliation_status": "pending",
                    "usage_reconciliation_attempt_count": 0,
                    "usage_reconciliation_started_at": now,
                    "usage_reconciliation_deadline_at": now
                    + timedelta(
                        seconds=max(
                            1,
                            self._settings.billing_usage_reconciliation_retention_seconds,
                        )
                    ),
                    "usage_reconciliation_next_attempt_at": now
                    + timedelta(
                        seconds=max(
                            1,
                            self._settings.billing_usage_reconciliation_initial_backoff_seconds,
                        )
                    ),
                    "usage_recovery_hold_registered": False,
                }
            )
        batch.create(
            client.collection(self._collection).document(turn_id),
            data,
        )


def _has_pending_provider_usage(usage: dict[str, Any]) -> bool:
    if usage.get("provider") != "openai_agents_api":
        return False
    missing = usage.get("missing_usage_turn_count", 0)
    if isinstance(missing, int) and not isinstance(missing, bool) and missing > 0:
        return True
    if usage.get("usage_reporting_complete") is False:
        return True
    reports = usage.get("raw_turn_usage_reports")
    return isinstance(reports, list) and any(
        isinstance(report, dict)
        and not _has_complete_usage(report.get("usage"))
        for report in reports
    )


def _has_complete_usage(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    input_tokens = value.get("input_tokens")
    output_tokens = value.get("output_tokens")
    total_tokens = value.get("total_tokens")
    return all(
        isinstance(item, int) and not isinstance(item, bool) and item >= 0
        for item in (input_tokens, output_tokens, total_tokens)
    )


def _nonnegative_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return 0
    return value
