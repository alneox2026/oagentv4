from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from common.billing import customer_wallet_document_id
from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.services import billing_reconciliation
from services.agent_persistence_worker_v3.app.services.billing_reconciliation import (
    BillingReconciliationService,
)
from tests.unit.test_wallet_reservations import FakeClient


@pytest.fixture(autouse=True)
def _reset_worker_settings_cache(monkeypatch):
    # These tests cover v4 pending-marker recovery plus legacy per-turn hold
    # migration. Batch settlement has dedicated coverage in its own test module.
    monkeypatch.setenv("BILLING_BATCH_SETTLEMENT_ENABLED", "false")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _run_transaction(client, operation):
    return operation(client.transaction())


def test_expired_reservation_releases_held_credit_when_no_ledger_exists(monkeypatch):
    settings = get_settings()
    client = FakeClient()
    user_id = "user-1"
    wallet_id = customer_wallet_document_id(user_id)
    client.documents[(settings.wallets_collection, wallet_id)] = {
        "billing_subject_id": user_id,
        "owner_uid": user_id,
        "currency": "USD",
        "status": "active",
        "available_credit_nanos": 500_000_000,
        "reserved_credit_nanos": 500_000_000,
        "settled_usage_nanos": 0,
    }
    client.documents[(settings.billing_reservations_collection, "turn-1")] = {
        "reservation_id": "turn-1",
        "turn_id": "turn-1",
        "request_id": "req-1",
        "billing_subject_id": user_id,
        "owner_uid": user_id,
        "agent_id": "maxima",
        "currency": "USD",
        "reserved_amount_nanos": 500_000_000,
        "status": "reserved",
    }
    monkeypatch.setattr(
        billing_reconciliation,
        "_run_firestore_transaction",
        _run_transaction,
    )
    service = BillingReconciliationService(
        firestore_client_factory=lambda: client,
    )

    released = service._release_expired_reservation_sync(
        client,
        client.documents[(settings.billing_reservations_collection, "turn-1")],
        datetime(2026, 8, 11, 11, 0, tzinfo=timezone.utc),
    )

    assert released is True
    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    assert wallet["available_credit_nanos"] == 1_000_000_000
    assert wallet["reserved_credit_nanos"] == 0
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    assert reservation["status"] == "expired_released"
    transaction = client.documents[(settings.wallet_transactions_collection, "reservation_expired_turn-1")]
    assert transaction["transaction_type"] == "reservation_expiry_release"
    assert transaction["released_amount_nanos"] == 500_000_000


def test_pending_marker_query_includes_v4_state_and_filters_before_batch_limit(
    monkeypatch,
):
    monkeypatch.setenv("BILLING_RECONCILIATION_BATCH_SIZE", "2")
    get_settings.cache_clear()

    class Snapshot:
        def __init__(self, data):
            self.data = data

        def to_dict(self):
            return dict(self.data)

    class Collection:
        def __init__(self, documents):
            self.documents = documents

        def stream(self):
            return list(self.documents)

    class Client:
        def __init__(self, documents):
            self.collection_ref = Collection(documents)

        def collection(self, name):
            return self.collection_ref

    expired = datetime(2026, 8, 11, 10, 0, tzinfo=timezone.utc)
    documents = [
        Snapshot({"status": "settled", "expires_at": expired})
        for _ in range(5)
    ]
    active = Snapshot({"status": "reserved", "expires_at": expired})
    completed_pending = Snapshot(
        {"status": "completed_pending", "created_at": expired}
    )
    documents.extend([active, completed_pending])
    client = Client(documents)
    service = BillingReconciliationService(firestore_client_factory=lambda: client)

    results = list(service._pending_turn_markers(client))

    assert results == [active, completed_pending]


def test_completed_marker_query_skips_until_settlement_retry_is_due(monkeypatch):
    get_settings.cache_clear()
    settings = get_settings()
    reservation = {
        "status": "completed_pending",
        "turn_id": "turn-not-due",
        "billing_settlement_next_attempt_at": datetime.now(timezone.utc)
        + timedelta(minutes=5),
    }
    client = FakeReconciliationClient(settings, reservation, None)
    service = BillingReconciliationService(firestore_client_factory=lambda: client)

    results = list(service._pending_turn_markers(client, datetime.now(timezone.utc)))

    assert results == []
    get_settings.cache_clear()


class FakeUsageHydrator:
    def __init__(self, reports):
        self.reports = reports
        self.calls = []

    def hydrate(self, *, session_id, reports):
        self.calls.append((session_id, reports))
        return self.reports


class FakeLedgerRepository:
    def __init__(self):
        self.updates = []

    def update_recovered_usage(self, client, turn_id, usage):
        self.updates.append((turn_id, usage))


class FakeMessagesRepository:
    def __init__(self):
        self.updates = []

    def update_assistant_usage(self, client, *, thread_id, turn_id, usage):
        self.updates.append((thread_id, turn_id, usage))
        return True


class FakeSettlementService:
    def __init__(self):
        self.events = []
        self.late_events = []
        self.legacy_registrations = []

    def settle_if_required_sync(self, event):
        self.events.append(event)
        return object()

    def settle_late_usage_sync(self, event):
        self.late_events.append(event)
        return SimpleNamespace(status="late_settled")

    def register_late_usage_recovery_sync(self, reservation, *, now):
        self.legacy_registrations.append((reservation, now))
        return True


class FakeQuery:
    def __init__(self, snapshots):
        self.snapshots = snapshots

    def where(self, **kwargs):
        field_filter = kwargs.get("filter")
        field = getattr(field_filter, "field_path", None)
        operator = getattr(field_filter, "op_string", None)
        expected = getattr(field_filter, "value", None)
        if field:
            def matches(snapshot):
                actual = (snapshot.to_dict() or {}).get(field)
                if operator in ("==", "== "):
                    return actual == expected
                if operator == "in":
                    return actual in expected
                if operator == "<=":
                    return actual is not None and actual <= expected
                return True

            self.snapshots = [snapshot for snapshot in self.snapshots if matches(snapshot)]
        return self

    def order_by(self, *_args):
        return self

    def limit(self, *_args):
        return self

    def stream(self):
        return self.snapshots

    def document(self, _document_id):
        if not self.snapshots:
            return FakeDocumentReference({})
        return FakeDocumentReference(self.snapshots[0].data)


class FakeDocumentSnapshot:
    def __init__(self, data):
        self.data = data
        self.exists = data is not None

    def to_dict(self):
        return dict(self.data) if self.data is not None else None


class FakeDocumentReference:
    def __init__(self, data):
        self.data = data

    def get(self):
        return FakeDocumentSnapshot(self.data)

    def update(self, updates):
        self.data.update(updates)


class FakeReconciliationClient:
    def __init__(self, settings, reservation, ledger):
        self.settings = settings
        self.reservation = reservation
        self.ledger = ledger

    def collection(self, name):
        if name == self.settings.billing_reservations_collection:
            return FakeQuery([FakeDocumentSnapshot(self.reservation)])
        if name == self.settings.billing_ledger_collection:
            return FakeCollection(self.ledger)
        raise AssertionError(f"Unexpected collection: {name}")


class FakeCollection:
    def __init__(self, data):
        self.data = data

    def document(self, _document_id):
        return FakeDocumentReference(self.data)


def test_reconciler_reprices_turn_when_delayed_openai_usage_arrives(monkeypatch):
    monkeypatch.setenv("DEPLOYMENT_ENVIRONMENT", "test")
    get_settings.cache_clear()
    ledger_repository = FakeLedgerRepository()
    messages_repository = FakeMessagesRepository()
    recovered_reports = [
        {
            "turn_id": "provider-turn-1",
            "subagent_id": None,
            "usage": {
                "input_tokens": 1000,
                "input_tokens_details": {"cached_tokens": 200},
                "output_tokens": 100,
                "output_tokens_details": {"reasoning_tokens": 20},
            },
        }
    ]
    hydrator = FakeUsageHydrator(recovered_reports)
    service = BillingReconciliationService(
        firestore_client_factory=lambda: None,
        usage_hydrator=hydrator,
        ledger_repository=ledger_repository,
        messages_repository=messages_repository,
    )
    ledger = {
        "turn_id": "turn-1",
        "session_id": "session-1",
        "thread_id": "thread-1",
        "pricing_model": "gpt-6-luna",
        "usage": {
            "provider": "openai_agents_api",
            "billing_status": "pending_provider_usage",
            "pricing_model": "gpt-6-luna",
            "completed_agent_turn_count": 1,
            "missing_usage_turn_count": 1,
            "usage_reporting_complete": False,
            "estimated_cost_usd": None,
            "raw_turn_usage_reports": [
                {"turn_id": "provider-turn-1", "subagent_id": None, "usage": None}
            ],
        },
    }

    updated = service._recover_openai_usage(object(), ledger)

    assert hydrator.calls[0][0] == "session-1"
    assert updated["usage"]["missing_usage_turn_count"] == 0
    assert updated["usage"]["estimated_cost_usd"] > 0
    assert not service._has_pending_openai_usage(updated)
    assert len(ledger_repository.updates) == 1
    assert messages_repository.updates[0][:2] == ("thread-1", "turn-1")
    get_settings.cache_clear()


def test_reconciler_leaves_missing_usage_pending_before_reservation_expiry(
    monkeypatch,
):
    get_settings.cache_clear()
    settings = get_settings()
    ledger = {
        "event_id": "event-1",
        "turn_id": "turn-1",
        "agent_id": "maxima",
        "uid": "user-1",
        "thread_id": "thread-1",
        "session_id": "session-1",
        "created_at": datetime.now(timezone.utc),
        "estimated_cost_usd": None,
        "usage": {
            "provider": "openai_agents_api",
            "billing_status": "pending_provider_usage",
            "pricing_model": "gpt-6-luna",
            "completed_agent_turn_count": 1,
            "missing_usage_turn_count": 1,
            "usage_reporting_complete": False,
            "estimated_cost_usd": None,
            "raw_turn_usage_reports": [
                {"turn_id": "provider-turn-1", "subagent_id": None, "usage": None}
            ],
        },
    }
    reservation = {
        "status": "reserved",
        "turn_id": "turn-1",
        "expires_at": datetime.now(timezone.utc).replace(year=2027),
        "reservation_id": "turn-1",
        "billing_subject_id": "user-1",
    }
    client = FakeReconciliationClient(settings, reservation, ledger)
    settlement = FakeSettlementService()
    service = BillingReconciliationService(
        firestore_client_factory=lambda: client,
        settlement_service=settlement,
        usage_hydrator=FakeUsageHydrator(ledger["usage"]["raw_turn_usage_reports"]),
        ledger_repository=FakeLedgerRepository(),
        messages_repository=FakeMessagesRepository(),
    )

    result = service.reconcile_expired_sync()

    assert result.scanned_reservations == 1
    assert result.skipped_reservations == 1
    assert settlement.events == []
    assert reservation["usage_reconciliation_status"] == "pending"
    assert reservation["usage_reconciliation_attempt_count"] == 1
    assert reservation["usage_reconciliation_next_attempt_at"] > datetime.now(timezone.utc)
    get_settings.cache_clear()


def test_completed_pending_usage_is_looked_up_once_per_reconciliation_pass(monkeypatch):
    get_settings.cache_clear()
    settings = get_settings()
    now = datetime.now(timezone.utc)
    reports = [
        {"turn_id": "provider-turn-1", "subagent_id": None, "usage": None}
    ]
    ledger = {
        "event_id": "event-1",
        "turn_id": "turn-1",
        "agent_id": "maxima",
        "uid": "user-1",
        "billing_subject_id": "user-1",
        "billing_reservation_id": "turn-1",
        "thread_id": "thread-1",
        "session_id": "session-1",
        "created_at": now,
        "estimated_cost_usd_nanos": None,
        "usage": {
            "provider": "openai_agents_api",
            "billing_status": "pending_provider_usage",
            "pricing_model": "gpt-6-luna",
            "completed_agent_turn_count": 1,
            "missing_usage_turn_count": 1,
            "usage_reporting_complete": False,
            "estimated_cost_usd": None,
            "raw_turn_usage_reports": reports,
        },
    }
    reservation = {
        "status": "completed_pending",
        "usage_reconciliation_status": "pending",
        "turn_id": "turn-1",
        "reservation_id": "turn-1",
        "billing_subject_id": "user-1",
        "owner_uid": "user-1",
        "agent_id": "maxima",
        "session_id": "session-1",
        "thread_id": "thread-1",
        "created_at": now,
        "billing_settlement_next_attempt_at": now - timedelta(seconds=1),
        "usage_reconciliation_next_attempt_at": now - timedelta(seconds=1),
        "usage_reconciliation_deadline_at": now + timedelta(days=7),
    }
    client = FakeReconciliationClient(settings, reservation, ledger)
    hydrator = FakeUsageHydrator(reports)
    service = BillingReconciliationService(
        firestore_client_factory=lambda: client,
        settlement_service=FakeSettlementService(),
        usage_hydrator=hydrator,
        ledger_repository=FakeLedgerRepository(),
        messages_repository=FakeMessagesRepository(),
    )

    service.reconcile_expired_sync()

    assert len(hydrator.calls) == 1
    get_settings.cache_clear()


@pytest.mark.parametrize(
    ("lookup_batch_size", "concurrency", "expected_recovered", "expected_deferred"),
    [
        (1, 1, {"turn-1"}, {"turn-2"}),
        (2, 1, {"turn-1", "turn-2"}, set()),
        (2, 2, {"turn-1", "turn-2"}, set()),
    ],
)
def test_usage_lookup_backlog_is_bounded_without_counting_deferred_work_as_a_retry(
    monkeypatch, lookup_batch_size, concurrency, expected_recovered, expected_deferred
):
    monkeypatch.setenv(
        "BILLING_USAGE_RECONCILIATION_LOOKUP_BATCH_SIZE", str(lookup_batch_size)
    )
    monkeypatch.setenv(
        "BILLING_USAGE_RECONCILIATION_CONCURRENCY", str(concurrency)
    )
    get_settings.cache_clear()
    settings = get_settings()
    now = datetime.now(timezone.utc)
    reservations = {
        f"turn-{index}": {
            "status": "late_usage_pending",
            "usage_reconciliation_status": "pending",
            "turn_id": f"turn-{index}",
            "usage_reconciliation_attempt_count": 0,
            "usage_reconciliation_next_attempt_at": now - timedelta(seconds=1),
            "usage_reconciliation_deadline_at": now + timedelta(days=7),
        }
        for index in (1, 2)
    }
    ledgers = {
        turn_id: {
            "turn_id": turn_id,
            "session_id": f"session-{turn_id}",
            "usage": {
                "provider": "openai_agents_api",
                "billing_status": "pending_provider_usage",
                "missing_usage_turn_count": 1,
                "usage_reporting_complete": False,
                "raw_turn_usage_reports": [
                    {"turn_id": f"provider-{turn_id}", "usage": None}
                ],
            },
        }
        for turn_id in reservations
    }

    class DocumentReference:
        def __init__(self, collection, document_id):
            self.collection = collection
            self.document_id = document_id

        def get(self):
            data = self.collection.documents.get(self.document_id)
            return FakeDocumentSnapshot(data)

        def update(self, updates):
            self.collection.documents[self.document_id].update(updates)

    class Collection:
        def __init__(self, documents):
            self.documents = documents

        def document(self, document_id):
            return DocumentReference(self, document_id)

    class Client:
        def collection(self, name):
            if name == settings.billing_reservations_collection:
                return Collection(reservations)
            if name == settings.billing_ledger_collection:
                return Collection(ledgers)
            raise AssertionError(f"Unexpected collection: {name}")

    snapshots = [FakeDocumentSnapshot(value) for value in reservations.values()]
    hydrator = FakeUsageHydrator([])
    service = BillingReconciliationService(
        firestore_client_factory=Client,
        usage_hydrator=hydrator,
    )

    _, recovered, deferred = service._hydrate_due_usage_reports(
        Client(),
        [],
        snapshots,
        now,
    )

    assert len(hydrator.calls) == len(expected_recovered)
    assert set(recovered) == expected_recovered
    assert deferred == expected_deferred
    assert reservations["turn-2"]["usage_reconciliation_attempt_count"] == 0
    if "turn-2" in expected_deferred:
        assert reservations["turn-2"]["usage_reconciliation_next_attempt_at"] > now
    get_settings.cache_clear()


def test_reconciler_recovers_usage_after_reservation_was_released(monkeypatch):
    get_settings.cache_clear()
    settings = get_settings()
    ledger = {
        "event_id": "event-1",
        "turn_id": "turn-1",
        "agent_id": "maxima",
        "uid": "user-1",
        "billing_subject_id": "user-1",
        "billing_reservation_id": "turn-1",
        "thread_id": "thread-1",
        "session_id": "session-1",
        "created_at": datetime.now(timezone.utc),
        "estimated_cost_usd_nanos": None,
        "usage": {
            "provider": "openai_agents_api",
            "billing_status": "pending_provider_usage",
            "pricing_model": "gpt-6-luna",
            "completed_agent_turn_count": 1,
            "missing_usage_turn_count": 1,
            "usage_reporting_complete": False,
            "estimated_cost_usd": None,
            "raw_turn_usage_reports": [
                {"turn_id": "provider-turn-1", "subagent_id": None, "usage": None}
            ],
        },
    }
    reservation = {
        "status": "late_usage_pending",
        "usage_reconciliation_status": "pending",
        "turn_id": "turn-1",
        "reservation_id": "turn-1",
        "billing_subject_id": "user-1",
        "owner_uid": "user-1",
        "agent_id": "maxima",
        "usage_recovery_hold_registered": True,
        "usage_reconciliation_next_attempt_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "usage_reconciliation_deadline_at": datetime(2027, 1, 1, tzinfo=timezone.utc),
    }
    client = FakeReconciliationClient(settings, reservation, ledger)
    recovered_reports = [
        {
            "turn_id": "provider-turn-1",
            "subagent_id": None,
            "usage": {
                "input_tokens": 1000,
                "input_tokens_details": {"cached_tokens": 200},
                "output_tokens": 100,
                "output_tokens_details": {"reasoning_tokens": 20},
            },
        }
    ]
    settlement = FakeSettlementService()
    ledger_repository = FakeLedgerRepository()
    service = BillingReconciliationService(
        firestore_client_factory=lambda: client,
        settlement_service=settlement,
        usage_hydrator=FakeUsageHydrator(recovered_reports),
        ledger_repository=ledger_repository,
        messages_repository=FakeMessagesRepository(),
    )

    result = service.reconcile_expired_sync()

    assert result.settled_reservations == 1
    assert settlement.late_events[0].turn_id == "turn-1"
    assert ledger_repository.updates[0][1]["missing_usage_turn_count"] == 0
    get_settings.cache_clear()


def test_legacy_unpriced_turn_without_ledger_is_retained_for_recovery(monkeypatch):
    get_settings.cache_clear()
    settings = get_settings()
    reservation = {
        "status": "unpriced_released",
        "turn_id": "turn-legacy",
        "reservation_id": "turn-legacy",
        "billing_subject_id": "user-1",
        "owner_uid": "user-1",
    }
    client = FakeReconciliationClient(settings, reservation, None)
    settlement = FakeSettlementService()
    service = BillingReconciliationService(
        firestore_client_factory=lambda: client,
        settlement_service=settlement,
    )

    result = service.reconcile_expired_sync()

    assert result.scanned_reservations == 1
    assert result.skipped_reservations == 1
    assert len(settlement.legacy_registrations) == 1
    assert reservation["status"] == "unpriced_released"


def test_missing_ledger_is_retried_until_the_usage_retention_deadline(monkeypatch):
    get_settings.cache_clear()
    settings = get_settings()
    now = datetime.now(timezone.utc)
    reservation = {
        "status": "late_usage_pending",
        "usage_reconciliation_status": "pending",
        "turn_id": "turn-missing-ledger",
        "reservation_id": "turn-missing-ledger",
        "billing_subject_id": "user-1",
        "owner_uid": "user-1",
        "agent_id": "maxima",
        "usage_reconciliation_attempt_count": 1,
        "usage_reconciliation_started_at": now - timedelta(hours=1),
        "usage_reconciliation_deadline_at": now + timedelta(days=2),
        "usage_reconciliation_next_attempt_at": now - timedelta(seconds=1),
    }
    client = FakeReconciliationClient(settings, reservation, None)
    service = BillingReconciliationService(
        firestore_client_factory=lambda: client,
        settlement_service=FakeSettlementService(),
        usage_hydrator=FakeUsageHydrator([]),
    )

    result = service.reconcile_expired_sync()

    assert result.scanned_reservations == 1
    assert result.skipped_reservations == 1
    assert reservation["status"] == "late_usage_pending"
    assert reservation["usage_reconciliation_status"] == "pending"
    assert reservation["usage_reconciliation_next_attempt_at"] > now
    assert "usage_reconciliation_manual_review_reason" not in reservation
    get_settings.cache_clear()


def test_usage_retry_stops_scanning_until_its_next_attempt(monkeypatch):
    get_settings.cache_clear()
    settings = get_settings()
    reservation = {
        "status": "settled",
        "usage_reconciliation_status": "pending",
        "turn_id": "turn-waiting",
        "usage_reconciliation_next_attempt_at": datetime.now(timezone.utc)
        + timedelta(hours=1),
        "usage_reconciliation_deadline_at": datetime.now(timezone.utc)
        + timedelta(days=7),
    }
    client = FakeReconciliationClient(settings, reservation, None)
    hydrator = FakeUsageHydrator([])
    service = BillingReconciliationService(
        firestore_client_factory=lambda: client,
        settlement_service=FakeSettlementService(),
        usage_hydrator=hydrator,
    )

    result = service.reconcile_expired_sync()

    assert result.scanned_reservations == 0
    assert hydrator.calls == []
    get_settings.cache_clear()


@pytest.mark.parametrize(
    ("ledger", "expected_reason"),
    [
        (None, "late_ledger_missing_at_retention_expiry"),
        (
            {"turn_id": "turn-expired", "usage": {"provider": "openai_agents_api"}},
            "provider_usage_retention_expired",
        ),
    ],
)
def test_usage_retry_marks_manual_review_at_retention_deadline(
    monkeypatch, ledger, expected_reason
):
    get_settings.cache_clear()
    settings = get_settings()
    now = datetime.now(timezone.utc)
    reservation = {
        "status": "settled",
        "usage_reconciliation_status": "pending",
        "turn_id": "turn-expired",
        "usage_reconciliation_next_attempt_at": now + timedelta(hours=1),
        "usage_reconciliation_deadline_at": now - timedelta(seconds=1),
    }
    client = FakeReconciliationClient(settings, reservation, ledger)
    service = BillingReconciliationService(
        firestore_client_factory=lambda: client,
        settlement_service=FakeSettlementService(),
    )

    result = service.reconcile_expired_sync()

    assert result.scanned_reservations == 1
    assert reservation["status"] == "usage_recovery_manual_review"
    assert reservation["usage_reconciliation_status"] == "manual_review"
    assert reservation["usage_reconciliation_manual_review_reason"] == expected_reason
    assert reservation["usage_reconciliation_next_attempt_at"] is None
    get_settings.cache_clear()


def test_expired_usage_retry_keeps_active_batch_settlement_queued(monkeypatch):
    monkeypatch.setenv("BILLING_BATCH_SETTLEMENT_ENABLED", "true")
    get_settings.cache_clear()
    settings = get_settings()
    now = datetime.now(timezone.utc)
    reservation = {
        "status": "late_batch_pending",
        "billing_batch_status": "pending",
        "usage_reconciliation_status": "pending",
        "turn_id": "turn-expired-batch",
        "usage_reconciliation_next_attempt_at": now + timedelta(hours=1),
        "usage_reconciliation_deadline_at": now - timedelta(seconds=1),
    }
    ledger = {
        "turn_id": "turn-expired-batch",
        "estimated_cost_usd_nanos": 25_000,
        "usage": {"provider": "openai_agents_api"},
    }

    class BatchSettlement:
        def reconcile_due_sync(self):
            return SimpleNamespace(settled_batches=0, settled_turns=0)

    client = FakeReconciliationClient(settings, reservation, ledger)
    service = BillingReconciliationService(
        firestore_client_factory=lambda: client,
        settlement_service=FakeSettlementService(),
        batch_settlement_service=BatchSettlement(),
    )

    service.reconcile_expired_sync()

    assert reservation["status"] == "late_batch_pending"
    assert reservation["billing_batch_status"] == "pending"
    assert reservation["usage_reconciliation_status"] == "manual_review"
    assert reservation["usage_reconciliation_manual_review_reason"] == (
        "provider_usage_retention_expired"
    )
    assert reservation["usage_reconciliation_next_attempt_at"] is None
    get_settings.cache_clear()

