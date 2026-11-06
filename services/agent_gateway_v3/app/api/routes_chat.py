"""Buffered chat route implementation for the agent gateway."""

from __future__ import annotations

from datetime import datetime, timezone
import logging

from fastapi import APIRouter, Request

from common.diagnostics import sanitize_for_diagnostics
from common.schemas import ChatRequest, ChatResponse
from services.agent_gateway_v3.app.core.auth import authenticate_request
from services.agent_gateway_v3.app.core.config import get_settings
from services.agent_gateway_v3.app.core.errors import ApiError
from services.agent_gateway_v3.app.core.logging import log_structured
from services.agent_gateway_v3.app.services.agent_registry import get_agent_config
from services.agent_gateway_v3.app.services.chat_backend_resolver import get_chat_backend_client
from services.agent_gateway_v3.app.services.chat_session_service import get_chat_session_service
from services.agent_gateway_v3.app.services.pubsub_publisher import get_pubsub_publisher
from services.agent_gateway_v3.app.services.request_context import build_request_context
from services.agent_gateway_v3.app.services.turn_event_builder import (
    build_turn_completed_event,
)
from services.agent_gateway_v3.app.services.wallet_reservations import (
    get_wallet_balance_gate,
)


LOGGER = logging.getLogger(__name__)
CHAT_ROUTE_DIAGNOSTICS_VERSION = "chat-route-diagnostics-v3"
router = APIRouter()


@router.post("/v1/agents/{agent_id}/chat")
async def chat(request: Request, agent_id: str, payload: ChatRequest) -> ChatResponse:
    agent_config = get_agent_config(agent_id)
    if (
        getattr(get_settings(), "billing_enforcement_enabled", False)
        and not agent_config.persistence_enabled
    ):
        raise ApiError(
            503,
            "billing_persistence_required",
            "This agent cannot be used while prepaid billing is enabled because settlement is unavailable.",
            {"agent_id": agent_config.agent_id},
        )
    user_id = await authenticate_request(request)
    request_context = build_request_context(
        agent_id=agent_config.agent_id,
        client_turn_id=payload.client_turn_id,
        user_id=user_id,
    )
    log_structured(
        LOGGER,
        logging.INFO,
        "gateway_chat_started",
        request_id=request_context.request_id,
        turn_id=request_context.turn_id,
        agent_id=agent_config.agent_id,
        user_id=user_id,
        diagnostics_version=CHAT_ROUTE_DIAGNOSTICS_VERSION,
    )
    backend_started_at = datetime.now(timezone.utc)
    wallet_balance_gate = await get_wallet_balance_gate()
    billing_turn_context = await wallet_balance_gate.authorize(
        user_id=user_id,
        agent_id=agent_config.agent_id,
        request_id=request_context.request_id,
        turn_id=request_context.turn_id,
        minimum_balance_nanos=agent_config.minimum_balance_nanos,
    )
    backend_client = await get_chat_backend_client(agent_config)
    session_service = await get_chat_session_service()
    session_result = await session_service.resolve_existing(
        agent_config=agent_config,
        user_id=user_id,
        request=payload,
    )
    if session_result is None:
        session_result = await session_service.create_new(
            runtime_client=backend_client,
            agent_config=agent_config,
            user_id=user_id,
            request=payload,
        )
    try:
        agent_response = await backend_client.chat_buffered_query(
            agent_config=agent_config,
            user_id=user_id,
            session_id=session_result.session_id,
            message=payload.message,
            idempotency_key=request_context.turn_id,
        )
    except ApiError as exc:
        backend_latency_ms = int(
            (datetime.now(timezone.utc) - backend_started_at).total_seconds() * 1000
        )
        log_structured(
            LOGGER,
            logging.WARNING,
            "gateway_backend_call_failed",
            request_id=request_context.request_id,
            turn_id=request_context.turn_id,
            agent_id=agent_config.agent_id,
            backend=agent_config.backend,
            thread_id=session_result.thread_id,
            session_id=session_result.session_id,
            session_created=session_result.created_new,
            latency_ms=backend_latency_ms,
            diagnostics_version=CHAT_ROUTE_DIAGNOSTICS_VERSION,
            error_code=exc.code,
            error_status_code=exc.status_code,
            error_details=sanitize_for_diagnostics(exc.details),
        )
        raise
    backend_latency_ms = int(
        (datetime.now(timezone.utc) - backend_started_at).total_seconds() * 1000
    )
    log_structured(
        LOGGER,
        logging.INFO,
        "gateway_backend_call_completed",
        request_id=request_context.request_id,
        turn_id=request_context.turn_id,
        agent_id=agent_config.agent_id,
        backend=agent_config.backend,
        thread_id=session_result.thread_id,
        session_id=session_result.session_id,
        session_created=session_result.created_new,
        latency_ms=backend_latency_ms,
        response_event_count=agent_response.event_count,
        reply_text_length=len(agent_response.reply_text),
        diagnostics_version=CHAT_ROUTE_DIAGNOSTICS_VERSION,
    )
    usage = getattr(agent_response, "usage", {}) or {}
    publish_result = None
    publish_latency_ms = 0
    if agent_config.persistence_enabled:
        publisher = await get_pubsub_publisher()
        persistence_event = build_turn_completed_event(
            request_context=request_context,
            agent_config=agent_config,
            user_id=user_id,
            payload=payload,
            thread_id=session_result.thread_id,
            session_id=session_result.session_id,
            assistant_message=agent_response.reply_text,
            usage=usage,
            billing_metadata=(
                billing_turn_context.event_metadata() if billing_turn_context else None
            ),
        )
        publish_started_at = datetime.now(timezone.utc)
        publish_result = await publisher.publish_turn_completed(persistence_event)
        publish_latency_ms = int(
            (datetime.now(timezone.utc) - publish_started_at).total_seconds() * 1000
        )
    latency_ms = int(
        (datetime.now(timezone.utc) - request_context.started_at).total_seconds() * 1000
    )
    log_structured(
        LOGGER,
        logging.INFO,
        "gateway_chat_completed",
        request_id=request_context.request_id,
        turn_id=request_context.turn_id,
        agent_id=agent_config.agent_id,
        thread_id=session_result.thread_id,
        session_id=session_result.session_id,
        session_created=session_result.created_new,
        backend=agent_config.backend,
        backend_latency_ms=backend_latency_ms,
        response_event_count=agent_response.event_count,
        reply_text_length=len(agent_response.reply_text),
        diagnostics_version=CHAT_ROUTE_DIAGNOSTICS_VERSION,
        pubsub_message_id=publish_result.message_id if publish_result else None,
        publish_latency_ms=publish_latency_ms,
        latency_ms=latency_ms,
    )
    return ChatResponse(
        ok=True,
        agent_id=agent_config.agent_id,
        thread_id=session_result.thread_id,
        session_id=session_result.session_id,
        turn_id=request_context.turn_id,
        reply_text=agent_response.reply_text,
        usage=usage,
    )
