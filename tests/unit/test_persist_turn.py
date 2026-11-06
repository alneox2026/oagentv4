import asyncio

from common.schemas import InfrastructureMetering, TurnCompletedEvent
from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.services.persist_turn import (
    PersistTurnService,
)


class Conflict(Exception):
    pass


class FakeBatch:
    def __init__(self, *, raise_conflict: bool = False) -> None:
        self.raise_conflict = raise_conflict
        self.actions = []

    def commit(self) -> None:
        if self.raise_conflict:
            raise Conflict("duplicate")

    def create(self, reference, value) -> None:
        self.actions.append(("create", reference.path, value))


class FakeDocumentReference:
    def __init__(self, collection: str, document_id: str) -> None:
        self.path = f"{collection}/{document_id}"


class FakeCollection:
    def __init__(self, name: str) -> None:
        self.name = name

    def document(self, document_id: str) -> FakeDocumentReference:
        return FakeDocumentReference(self.name, document_id)


class FakeClient:
    def __init__(self, *, raise_conflict: bool = False) -> None:
        self.raise_conflict = raise_conflict
        self.batch_instance = FakeBatch(raise_conflict=raise_conflict)

    def batch(self) -> FakeBatch:
        return self.batch_instance

    def collection(self, name: str) -> FakeCollection:
        return FakeCollection(name)


class FakeUsageHydrator:
    def hydrate(self, *, session_id, reports):
        return reports


class FakeIdempotencyStore:
    def add_create_to_batch(self, batch, client, event) -> None:
        batch.actions.append(("idempotency", event.event_id))


class FakeBillingLedgerRepository:
    def __init__(self) -> None:
        self.events = []

    def add_create_to_batch(self, batch, client, event) -> None:
        batch.actions.append(("billing_ledger", event.turn_id))
        self.events.append(event)


class FakeThreadsRepository:
    def __init__(self, existing_thread=None, ignored_reason=None) -> None:
        self.existing_thread = existing_thread
        self.ignored_reason = ignored_reason

    def load_existing(self, client, thread_id):
        return self.existing_thread

    def validate_existing_for_turn(self, existing_thread, event):
        return self.ignored_reason

    def add_upsert_to_batch(self, batch, client, event, *, existing_thread=None) -> None:
        batch.actions.append(("thread", event.thread_id))


class FakeMessagesRepository:
    def add_turn_messages_to_batch(self, batch, client, event) -> None:
        batch.actions.append(("messages", event.turn_id))


class FakeSettlementService:
    def __init__(self):
        self.events = []

    def settle_if_required_sync(self, event):
        self.events.append(event)
        return None


class FakeBatchSettlementService:
    def __init__(self, result=True):
        self.events = []
        self.result = result

    def enqueue_turn_sync(self, event, *, late_usage=False):
        self.events.append((event, late_usage))
        return self.result


def _build_event() -> TurnCompletedEvent:
    return TurnCompletedEvent(
        event_id="evt-1",
        turn_id="turn-1",
        agent_id="maxima",
        user_id="user-1",
        thread_id="thread-1",
        session_id="session-1",
        user_message="hello",
        assistant_message="hi",
    )


def test_persist_turn_commits_batch_once() -> None:
    client = FakeClient()
    service = PersistTurnService(
        idempotency_store=FakeIdempotencyStore(),
        billing_ledger_repository=FakeBillingLedgerRepository(),
        threads_repository=FakeThreadsRepository(),
        messages_repository=FakeMessagesRepository(),
        firestore_client_factory=lambda: client,
    )

    result = asyncio.run(service.persist(_build_event()))

    assert result.persisted is True
    assert client.batch_instance.actions == [
        ("idempotency", "evt-1"),
        ("billing_ledger", "turn-1"),
        ("thread", "thread-1"),
        ("messages", "turn-1"),
    ]


def test_worker_creates_zero_hold_marker_atomically_when_usage_is_missing(monkeypatch) -> None:
    monkeypatch.setenv("DEPLOYMENT_ENVIRONMENT", "test")
    event = _build_event().model_copy(
        update={
            "metadata": {
                "billing": {
                    "reservation_id": "turn-1",
                    "billing_subject_id": "user-1",
                    "minimum_balance_nanos": 50_000_000,
                    "request_id": "req-1",
                }
            },
            "usage": {
                "provider": "openai_agents_api",
                "pricing_model": "gpt-6-luna",
                "completed_agent_turn_count": 1,
                "missing_usage_turn_count": 1,
                "usage_reporting_complete": False,
                "raw_turn_usage_reports": [],
            },
        }
    )
    client = FakeClient()
    service = PersistTurnService(
        idempotency_store=FakeIdempotencyStore(),
        billing_ledger_repository=FakeBillingLedgerRepository(),
        threads_repository=FakeThreadsRepository(),
        messages_repository=FakeMessagesRepository(),
        firestore_client_factory=lambda: client,
        openai_turn_usage_hydrator=FakeUsageHydrator(),
    )

    result = asyncio.run(service.persist(event))

    assert result.billing_settlement_status == "usage_pending"
    marker = next(
        action[2]
        for action in client.batch_instance.actions
        if action[0] == "create" and action[1].endswith("/turn-1")
    )
    assert marker["reserved_amount_nanos"] == 0
    assert marker["minimum_balance_nanos"] == 50_000_000
    assert marker["authorization_mode"] == "minimum_balance_check"
    assert marker["status"] == "late_usage_pending"
    assert marker["usage_recovery_hold_registered"] is False


def test_persist_turn_treats_conflict_as_duplicate() -> None:
    client = FakeClient(raise_conflict=True)
    service = PersistTurnService(
        idempotency_store=FakeIdempotencyStore(),
        billing_ledger_repository=FakeBillingLedgerRepository(),
        threads_repository=FakeThreadsRepository(),
        messages_repository=FakeMessagesRepository(),
        firestore_client_factory=lambda: client,
    )

    result = asyncio.run(service.persist(_build_event()))

    assert result.persisted is False


def test_persist_turn_records_idempotency_only_for_deleted_thread() -> None:
    client = FakeClient()
    service = PersistTurnService(
        idempotency_store=FakeIdempotencyStore(),
        billing_ledger_repository=FakeBillingLedgerRepository(),
        threads_repository=FakeThreadsRepository(ignored_reason="thread_deleted"),
        messages_repository=FakeMessagesRepository(),
        firestore_client_factory=lambda: client,
    )

    result = asyncio.run(service.persist(_build_event()))

    assert result.persisted is False
    assert result.ignored_reason == "thread_deleted"
    assert client.batch_instance.actions == [("idempotency", "evt-1")]


def test_openai_turn_usage_is_priced_in_persistence_worker(monkeypatch) -> None:
    monkeypatch.setenv("DEPLOYMENT_ENVIRONMENT", "test")
    ledger = FakeBillingLedgerRepository()
    event = _build_event().model_copy(
        update={
            "usage": {
                "provider": "openai_agents_api",
                "billing_status": "pending_worker_processing",
                "pricing_model": "gpt-6-luna",
                "completed_agent_turn_count": 1,
                "missing_usage_turn_count": 0,
                "raw_turn_usage_reports": [
                    {
                        "turn_id": "agent_turn_1",
                        "subagent_id": None,
                        "usage": {
                            "input_tokens": 1000,
                            "input_tokens_details": {"cached_tokens": 200},
                            "output_tokens": 100,
                            "output_tokens_details": {"reasoning_tokens": 20},
                        },
                    }
                ],
            },
            "infrastructure_metering": InfrastructureMetering(
                deployment_environment="development",
                project_id="test-project",
                gateway_region="us-central1",
                billing_subject_id="user-1",
                root_request_id="req-1",
                agent_backend="openai_managed",
                agent_region="global",
                gateway_wall_duration_ms=100,
                assistant_content_bytes=2,
            ),
        }
    )
    client = FakeClient()
    service = PersistTurnService(
        idempotency_store=FakeIdempotencyStore(),
        billing_ledger_repository=ledger,
        threads_repository=FakeThreadsRepository(),
        messages_repository=FakeMessagesRepository(),
        firestore_client_factory=lambda: client,
    )

    asyncio.run(service.persist(event))

    processed_usage = ledger.events[0].usage
    assert processed_usage["billing_status"] == "processed_by_persistence_worker"
    assert processed_usage["token_counts"]["prompt_token_count"] == 1000
    assert processed_usage["token_counts"]["cached_content_token_count"] == 200
    assert processed_usage["estimated_cost_usd"] == 0.000132
    assert processed_usage["raw_turn_usage_reports"][0]["turn_id"] == "agent_turn_1"


def test_missing_openai_usage_does_not_create_a_zero_charge(monkeypatch) -> None:
    monkeypatch.setenv("DEPLOYMENT_ENVIRONMENT", "test")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    event = _build_event().model_copy(
        update={
            "usage": {
                "pricing_model": "gpt-6-luna",
                "completed_agent_turn_count": 1,
                "raw_turn_usage_reports": [
                    {"turn_id": "agent_turn_1", "subagent_id": None, "usage": None}
                ],
            },
            "infrastructure_metering": InfrastructureMetering(
                deployment_environment="development",
                project_id="test-project",
                gateway_region="us-central1",
                billing_subject_id="user-1",
                root_request_id="req-1",
                agent_backend="openai_managed",
                agent_region="global",
                gateway_wall_duration_ms=100,
                assistant_content_bytes=2,
            ),
        }
    )
    service = PersistTurnService(
        idempotency_store=FakeIdempotencyStore(),
        billing_ledger_repository=FakeBillingLedgerRepository(),
        threads_repository=FakeThreadsRepository(),
        messages_repository=FakeMessagesRepository(),
        firestore_client_factory=FakeClient,
    )

    processed = service._normalize_openai_usage_after_response(event)

    assert processed.usage["estimated_cost_usd"] is None
    assert processed.usage["estimated_cost_complete"] is False
    assert "agents_api_turn_usage_unavailable" in processed.usage["pricing_warnings"]


def test_persist_turn_keeps_reservation_pending_until_openai_usage_arrives(monkeypatch) -> None:
    monkeypatch.setenv("DEPLOYMENT_ENVIRONMENT", "test")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    ledger = FakeBillingLedgerRepository()
    settlement = FakeSettlementService()
    event = _build_event().model_copy(
        update={
            "usage": {
                "provider": "openai_agents_api",
                "pricing_model": "gpt-6-luna",
                "completed_agent_turn_count": 1,
                "raw_turn_usage_reports": [
                    {"turn_id": "agent_turn_1", "subagent_id": None, "usage": None}
                ],
            },
            "infrastructure_metering": InfrastructureMetering(
                deployment_environment="development",
                project_id="test-project",
                gateway_region="us-central1",
                billing_subject_id="user-1",
                root_request_id="req-1",
                agent_backend="openai_managed",
                agent_region="global",
                gateway_wall_duration_ms=100,
                assistant_content_bytes=2,
            ),
        }
    )
    service = PersistTurnService(
        idempotency_store=FakeIdempotencyStore(),
        billing_ledger_repository=ledger,
        billing_settlement_service=settlement,
        threads_repository=FakeThreadsRepository(),
        messages_repository=FakeMessagesRepository(),
        firestore_client_factory=FakeClient,
    )

    result = asyncio.run(service.persist(event))

    assert result.billing_settlement_status == "usage_pending"
    assert settlement.events == []
    assert ledger.events[0].usage["billing_status"] == "pending_provider_usage"
    assert ledger.events[0].usage["estimated_cost_usd"] is None


def test_partial_openai_usage_is_batched_as_a_provisional_estimate(monkeypatch) -> None:
    monkeypatch.setenv("DEPLOYMENT_ENVIRONMENT", "test")
    monkeypatch.setenv("BILLING_BATCH_SETTLEMENT_ENABLED", "true")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    get_settings.cache_clear()
    client = FakeClient()
    ledger = FakeBillingLedgerRepository()
    batch_settlement = FakeBatchSettlementService()
    event = _build_event().model_copy(
        update={
            "metadata": {
                "billing": {
                    "reservation_id": "turn-1",
                    "billing_subject_id": "user-1",
                    "minimum_balance_nanos": 50_000_000,
                    "request_id": "req-1",
                }
            },
            "usage": {
                "provider": "openai_agents_api",
                "pricing_model": "gpt-6-luna",
                "completed_agent_turn_count": 2,
                "raw_turn_usage_reports": [
                    {
                        "turn_id": "agent-turn-1",
                        "subagent_id": None,
                        "usage": {"input_tokens": 1000, "output_tokens": 100},
                    },
                    {
                        "turn_id": "agent-turn-2",
                        "subagent_id": "subagent-1",
                        "usage": None,
                    },
                ],
            },
        }
    )
    service = PersistTurnService(
        idempotency_store=FakeIdempotencyStore(),
        billing_ledger_repository=ledger,
        billing_batch_settlement_service=batch_settlement,
        threads_repository=FakeThreadsRepository(),
        messages_repository=FakeMessagesRepository(),
        firestore_client_factory=lambda: client,
        openai_turn_usage_hydrator=FakeUsageHydrator(),
    )

    result = asyncio.run(service.persist(event))

    processed_usage = ledger.events[0].usage
    assert result.billing_settlement_status == "batch_pending_provisional_usage"
    assert processed_usage["missing_usage_turn_count"] == 1
    assert processed_usage["estimated_cost_usd"] > 0
    assert processed_usage["estimated_cost_complete"] is False
    assert processed_usage["cost_status"] == "provisional_estimate"
    assert len(batch_settlement.events) == 1
    marker = next(
        action[2]
        for action in client.batch_instance.actions
        if action[0] == "create" and action[1].endswith("/turn-1")
    )
    assert marker["status"] == "completed_pending"
    assert marker["usage_reconciliation_status"] == "pending"
    assert marker["reserved_amount_nanos"] == 0
    get_settings.cache_clear()
