"""Pub/Sub event receiver for the persistence worker."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi import HTTPException

from common.constants import EVENT_TYPE_THREAD_DELETE_REQUESTED, EVENT_TYPE_TURN_COMPLETED
from services.agent_persistence_worker_v3.app.core.auth import verify_eventarc_request
from services.agent_persistence_worker_v3.app.core.logging import log_structured
from services.agent_persistence_worker_v3.app.core.errors import RetryableWorkerError
from services.agent_persistence_worker_v3.app.models.events import (
    ThreadDeleteRequestedEvent,
    TurnCompletedEvent,
)
from services.agent_persistence_worker_v3.app.models.pubsub import PubSubPushEnvelope
from services.agent_persistence_worker_v3.app.services.delete_thread import DeleteThreadService
from services.agent_persistence_worker_v3.app.services.billing_reconciliation import (
    BillingReconciliationService,
)
from services.agent_persistence_worker_v3.app.services.persist_turn import PersistTurnService
from services.agent_persistence_worker_v3.app.services.openai_cost_reconciliation import (
    OpenAICostReconciliationService,
)


LOGGER = logging.getLogger(__name__)
router = APIRouter()
PERSIST_SERVICE = PersistTurnService()
DELETE_THREAD_SERVICE = DeleteThreadService()
BILLING_RECONCILIATION_SERVICE = BillingReconciliationService()
OPENAI_COST_RECONCILIATION_SERVICE = OpenAICostReconciliationService()


@router.post("/events/pubsub")
async def receive_pubsub_event(
    request: Request,
    envelope: PubSubPushEnvelope,
) -> dict[str, object]:
    await verify_eventarc_request(request)
    try:
        decoded_payload = envelope.message.decode_json()
        event_type = str(decoded_payload.get("event_type", "")).strip()
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid Pub/Sub event payload: {exc}",
        ) from exc

    if event_type == EVENT_TYPE_TURN_COMPLETED:
        return await _handle_turn_completed(decoded_payload)
    if event_type == EVENT_TYPE_THREAD_DELETE_REQUESTED:
        return await _handle_thread_delete_requested(decoded_payload)
    raise HTTPException(
        status_code=400,
        detail=f"Unsupported event type: {event_type or 'missing'}",
    )


@router.post("/internal/billing/reconcile")
async def reconcile_expired_billing_reservations() -> dict[str, object]:
    """Cloud Scheduler-only endpoint; Cloud Run IAM protects this route."""

    try:
        result = await BILLING_RECONCILIATION_SERVICE.reconcile_expired()
    except RetryableWorkerError as exc:
        log_structured(
            LOGGER,
            logging.ERROR,
            "worker_billing_reconciliation_retryable_failure",
            reason=str(exc),
        )
        raise HTTPException(
            status_code=500,
            detail=f"Retryable billing reconciliation failure: {exc}",
        ) from exc
    # The scheduler invokes this endpoint every five minutes. Emit a structured
    # completion event only when the pass changes billing state; routine polls
    # that merely scan or skip pending records remain visible in access logs.
    if any(
        (
            result.settled_reservations,
            result.released_reservations,
            result.queued_batch_entries,
            result.settled_batches,
            result.settled_batch_turns,
        )
    ):
        log_structured(
            LOGGER,
            logging.INFO,
            "worker_billing_reconciliation_completed",
            scanned_reservations=result.scanned_reservations,
            settled_reservations=result.settled_reservations,
            released_reservations=result.released_reservations,
            skipped_reservations=result.skipped_reservations,
            queued_batch_entries=result.queued_batch_entries,
            settled_batches=result.settled_batches,
            settled_batch_turns=result.settled_batch_turns,
        )
    return {
        "ok": True,
        "scanned_reservations": result.scanned_reservations,
        "settled_reservations": result.settled_reservations,
        "released_reservations": result.released_reservations,
        "skipped_reservations": result.skipped_reservations,
        "queued_batch_entries": result.queued_batch_entries,
        "settled_batches": result.settled_batches,
        "settled_batch_turns": result.settled_batch_turns,
    }


@router.post("/internal/billing/reconcile-openai-costs")
async def reconcile_openai_provider_costs() -> dict[str, object]:
    """Daily Cloud Scheduler endpoint; Cloud Run IAM protects this route."""
    try:
        result = await OPENAI_COST_RECONCILIATION_SERVICE.reconcile()
    except RetryableWorkerError as exc:
        log_structured(
            LOGGER,
            logging.ERROR,
            "worker_openai_cost_reconciliation_retryable_failure",
            reason=str(exc),
        )
        raise HTTPException(
            status_code=500,
            detail=f"Retryable OpenAI cost reconciliation failure: {exc}",
        ) from exc
    log_structured(
        LOGGER,
        logging.INFO,
        "worker_openai_cost_reconciliation_completed",
        status=result.status,
        reconciled_days=result.reconciled_days,
        provider_cost_usd=result.provider_cost_usd,
        agents_api_cost_usd=result.agents_api_cost_usd,
        local_estimate_usd=result.local_estimate_usd,
    )
    return {
        "ok": result.status in {"completed", "disabled"},
        "status": result.status,
        "reconciled_days": result.reconciled_days,
        "provider_cost_usd": result.provider_cost_usd,
        "agents_api_cost_usd": result.agents_api_cost_usd,
        "local_estimate_usd": result.local_estimate_usd,
    }


async def _handle_turn_completed(decoded_payload: dict[str, object]) -> dict[str, object]:
    try:
        event = TurnCompletedEvent(**decoded_payload)
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid turn-completed payload: {exc}",
        ) from exc
    try:
        result = await PERSIST_SERVICE.persist(event)
    except RetryableWorkerError as exc:
        log_structured(
            LOGGER,
            logging.ERROR,
            "worker_event_persist_retryable_failure",
            event_id=event.event_id,
            thread_id=event.thread_id,
            reason=str(exc),
        )
        raise HTTPException(
            status_code=500,
            detail=f"Retryable persistence failure: {exc}",
        ) from exc

    if result.ignored_reason:
        event_name = (
            "worker_event_integrity_rejected"
            if result.ignored_reason
            in {"uid_mismatch", "agent_id_mismatch", "session_id_mismatch"}
            else "worker_event_ignored"
        )
        log_level = logging.ERROR if event_name == "worker_event_integrity_rejected" else logging.WARNING
        log_structured(
            LOGGER,
            log_level,
            event_name,
            event_id=result.event_id,
            thread_id=result.thread_id,
            persisted=result.persisted,
            billing_settlement_status=getattr(result, "billing_settlement_status", None),
            ignored_reason=result.ignored_reason,
        )
    else:
        log_structured(
            LOGGER,
            logging.INFO,
            "worker_event_persisted",
            event_id=result.event_id,
            thread_id=result.thread_id,
            persisted=result.persisted,
            billing_settlement_status=getattr(result, "billing_settlement_status", None),
        )
    return {
        "ok": True,
        "event_id": result.event_id,
        "thread_id": result.thread_id,
        "persisted": result.persisted,
        "ignored_reason": result.ignored_reason,
        "billing_settlement_status": getattr(result, "billing_settlement_status", None),
    }


async def _handle_thread_delete_requested(
    decoded_payload: dict[str, object],
) -> dict[str, object]:
    try:
        event = ThreadDeleteRequestedEvent(**decoded_payload)
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid thread-delete payload: {exc}",
        ) from exc
    try:
        result = await DELETE_THREAD_SERVICE.delete_requested(event)
    except RetryableWorkerError as exc:
        log_structured(
            LOGGER,
            logging.ERROR,
            "worker_thread_delete_retryable_failure",
            event_id=event.event_id,
            thread_id=event.thread_id,
            reason=str(exc),
        )
        raise HTTPException(
            status_code=500,
            detail=f"Retryable delete failure: {exc}",
        ) from exc

    log_structured(
        LOGGER,
        logging.INFO,
        "worker_thread_delete_processed",
        event_id=result.event_id,
        thread_id=result.thread_id,
        runtime_session_status=result.runtime_session_status,
    )
    return {
        "ok": True,
        "event_id": result.event_id,
        "thread_id": result.thread_id,
        "runtime_session_status": result.runtime_session_status,
    }
