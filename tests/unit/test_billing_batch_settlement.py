from datetime import datetime, timedelta, timezone

from common.billing import customer_billing_period_document_id, customer_wallet_document_id
from common.schemas import TurnCompletedEvent
from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.services.billing_batch_settlement import (
    BillingBatchSettlementService,
)
class Snapshot:
    def __init__(self, data, reference=None):
        self._data = data
        self.reference = reference
        self.exists = data is not None

    def to_dict(self):
        return dict(self._data) if self._data is not None else None


class DocRef:
    def __init__(self, client, collection, document_id):
        self.client = client
        self.collection = collection
        self.document_id = document_id
        self.key = (collection, document_id)

    def get(self):
        return Snapshot(self.client.documents.get(self.key), self)


class Query:
    def __init__(self, collection, filters=()):
        self.collection = collection
        self.filters = list(filters)
        self.limit_count = None

    def where(self, *args, **kwargs):
        if "filter" in kwargs:
            field_filter = kwargs["filter"]
            self.filters.append((field_filter.field_path, field_filter.op_string, field_filter.value))
        elif len(args) == 3:
            self.filters.append(tuple(args))
        return self

    def order_by(self, *_args, **_kwargs):
        return self

    def limit(self, count):
        self.limit_count = count
        return self

    def stream(self):
        snapshots = []
        for (collection, _), value in self.collection.client.documents.items():
            if collection != self.collection.name:
                continue
            matched = True
            for field, op, expected in self.filters:
                actual = value.get(field)
                if op in ("==", "== "):
                    matched = matched and actual == expected
                elif op == ">=":
                    matched = matched and actual is not None and actual >= expected
                elif op == "<":
                    matched = matched and actual is not None and actual < expected
            if matched:
                reference = self.collection.document(value.get("turn_id") or "")
                snapshots.append(Snapshot(value, reference))
        return snapshots[: self.limit_count] if self.limit_count else snapshots


class Collection:
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def document(self, document_id):
        return DocRef(self.client, self.name, document_id)

    def where(self, *args, **kwargs):
        return Query(self, [(args[0], args[1], args[2])] if len(args) == 3 else []).where(**kwargs) if kwargs else Query(self, [(args[0], args[1], args[2])] if len(args) == 3 else [])

    def stream(self):
        return Query(self).stream()


class Transaction:
    def __init__(self, client):
        self.client = client

    def get(self, reference):
        return reference.get()

    def create(self, reference, data):
        if reference.key in self.client.documents:
            raise RuntimeError("document already exists")
        self.client.documents[reference.key] = dict(data)

    def update(self, reference, updates):
        self.client.documents[reference.key].update(updates)


class Client:
    def __init__(self):
        self.documents = {}

    def collection(self, name):
        return Collection(self, name)

    def transaction(self):
        return Transaction(self)


def run_transaction(client, operation):
    return operation(client.transaction())


def event(*, when, cost=0.1):
    return TurnCompletedEvent(
        event_id="evt-turn-1",
        turn_id="turn-1",
        agent_id="agent-a",
        user_id="user-1",
        thread_id="thread-1",
        session_id="session-1",
        user_message="hello",
        assistant_message="hi",
        created_at=when,
        usage={
            "provider": "openai_agents_api",
            "estimated_cost_usd": cost,
            "pricing_model": "gpt-6-luna",
            "pricing_version": "test-v1",
        },
        metadata={
            "billing": {
                "reservation_id": "turn-1",
                "billing_subject_id": "user-1",
                "reserved_amount_nanos": 200_000_000,
            }
        },
    )


def seed_wallet_and_lifecycle_marker(client):
    settings = get_settings()
    wallet_id = customer_wallet_document_id("user-1")
    client.documents[(settings.wallets_collection, wallet_id)] = {
        "owner_uid": "user-1",
        "billing_subject_id": "user-1",
        "status": "active",
        "available_credit_nanos": 1_000_000_000,
        "reserved_credit_nanos": 0,
        "settled_usage_nanos": 0,
        "pending_batch_usage_nanos": 0,
    }
    client.documents[(settings.billing_reservations_collection, "turn-1")] = {
        "turn_id": "turn-1",
        "reservation_id": "turn-1",
        "request_id": "req-1",
        "billing_subject_id": "user-1",
        "owner_uid": "user-1",
        "agent_id": "agent-a",
        "reserved_amount_nanos": 0,
        "minimum_balance_nanos": 50_000_000,
        "status": "completed_pending",
    }
    client.documents[(settings.billing_ledger_collection, "turn-1")] = {
        "turn_id": "turn-1",
        "uid": "user-1",
        "billing_subject_id": "user-1",
        "estimated_cost_usd_nanos": 100_000_000,
    }
    return wallet_id


def test_enqueue_tracks_pending_estimate_without_changing_wallet_credit():
    get_settings.cache_clear()
    settings = get_settings()
    client = Client()
    wallet_id = seed_wallet_and_lifecycle_marker(client)
    service = BillingBatchSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=run_transaction,
    )

    now = datetime.now(timezone.utc) - timedelta(minutes=10)
    assert service.enqueue_turn_sync(event(when=now)) is True
    assert service.enqueue_turn_sync(event(when=now)) is False

    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    assert wallet["available_credit_nanos"] == 1_000_000_000
    assert wallet["reserved_credit_nanos"] == 0
    assert wallet["pending_batch_usage_nanos"] == 100_000_000
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    assert reservation["status"] == "batch_pending"
    assert reservation["released_amount_nanos"] == 0


def test_zero_hold_lifecycle_marker_still_batches_and_charges_usage():
    get_settings.cache_clear()
    settings = get_settings()
    client = Client()
    wallet_id = seed_wallet_and_lifecycle_marker(client)
    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    wallet.update({"available_credit_nanos": 1_000_000_000, "reserved_credit_nanos": 0})
    client.documents[(settings.billing_reservations_collection, "turn-1")][
        "reserved_amount_nanos"
    ] = 0
    turn = event(when=datetime.now(timezone.utc) - timedelta(minutes=10))
    turn.metadata["billing"]["reserved_amount_nanos"] = 0
    service = BillingBatchSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=run_transaction,
    )

    assert service.enqueue_turn_sync(turn) is True
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    reservation["billing_batch_created_at"] = datetime.now(timezone.utc) - timedelta(minutes=10)
    result = service.reconcile_due_sync()

    assert result.settled_batches == 1
    assert wallet["available_credit_nanos"] == 900_000_000
    assert wallet["reserved_credit_nanos"] == 0
    assert wallet["pending_batch_usage_nanos"] == 0
    assert wallet["settled_usage_nanos"] == 100_000_000
    assert reservation["released_amount_nanos"] == 0
    assert reservation["status"] == "settled"


def test_idle_batch_debits_wallet_once_and_preserves_turn_audit():
    get_settings.cache_clear()
    settings = get_settings()
    client = Client()
    wallet_id = seed_wallet_and_lifecycle_marker(client)
    service = BillingBatchSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=run_transaction,
    )
    old_event = event(when=datetime.now(timezone.utc) - timedelta(minutes=10))
    assert service.enqueue_turn_sync(old_event)
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    reservation["billing_batch_created_at"] = datetime.now(timezone.utc) - timedelta(minutes=10)

    result = service.reconcile_due_sync()

    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    assert result.settled_batches == 1
    assert result.settled_turns == 1
    assert wallet["available_credit_nanos"] == 900_000_000
    assert wallet["reserved_credit_nanos"] == 0
    assert wallet["pending_batch_usage_nanos"] == 0
    assert wallet["settled_usage_nanos"] == 100_000_000
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    assert reservation["status"] == "settled"
    transaction = next(
        value
        for (collection, _), value in client.documents.items()
        if collection == settings.wallet_transactions_collection
    )
    assert transaction["transaction_type"] == "agent_usage_batch_debit"
    assert transaction["amount_nanos"] == 100_000_000
    assert transaction["turn_ids"] == ["turn-1"]
    assert reservation["billing_batch_status"] == "settled"
    assert reservation["billing_batch_id"] == transaction["transaction_id"]
    period_id = customer_billing_period_document_id("user-1", old_event.created_at.strftime("%Y-%m"))
    period = client.documents[(settings.customer_billing_periods_collection, period_id)]
    assert period["usage_estimated_nanos"] == 100_000_000


def test_batch_settlement_preserves_estimates_queued_for_other_sessions():
    get_settings.cache_clear()
    settings = get_settings()
    client = Client()
    wallet_id = seed_wallet_and_lifecycle_marker(client)
    client.documents[(settings.wallets_collection, wallet_id)]["pending_batch_usage_nanos"] = 50_000_000
    service = BillingBatchSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=run_transaction,
    )
    old_event = event(when=datetime.now(timezone.utc) - timedelta(minutes=10))
    assert service.enqueue_turn_sync(old_event)
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    reservation["billing_batch_created_at"] = datetime.now(timezone.utc) - timedelta(minutes=10)

    result = service.reconcile_due_sync()

    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    assert result.settled_batches == 1
    assert wallet["available_credit_nanos"] == 900_000_000
    assert wallet["pending_batch_usage_nanos"] == 50_000_000


def test_known_batch_shortfall_moves_from_pending_estimate_to_shared_wallet_debt():
    get_settings.cache_clear()
    settings = get_settings()
    client = Client()
    wallet_id = seed_wallet_and_lifecycle_marker(client)
    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    wallet.update({"available_credit_nanos": 100_000_000, "reserved_credit_nanos": 0})
    client.documents[(settings.billing_ledger_collection, "turn-1")]["estimated_cost_usd_nanos"] = 350_000_000
    turn = event(when=datetime.now(timezone.utc) - timedelta(minutes=10), cost=0.35)
    batch_service = BillingBatchSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=run_transaction,
    )
    assert batch_service.enqueue_turn_sync(turn)
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    reservation["billing_batch_created_at"] = datetime.now(timezone.utc) - timedelta(minutes=10)

    result = batch_service.reconcile_due_sync()

    assert result.settled_batches == 1
    assert wallet["available_credit_nanos"] == 0
    assert wallet["pending_batch_usage_nanos"] == 0
    assert wallet["usage_debt_nanos"] == 250_000_000
    assert wallet["lifetime_usage_debt_added_nanos"] == 250_000_000
    assert reservation["status"] == "settled_shortfall"
    assert wallet["available_credit_nanos"] == 0
    assert wallet["settled_usage_nanos"] == 100_000_000
    period_id = customer_billing_period_document_id("user-1", turn.created_at.strftime("%Y-%m"))
    period = client.documents[(settings.customer_billing_periods_collection, period_id)]
    assert period["usage_estimated_nanos"] == 350_000_000
    assert period["collected_usage_nanos"] == 100_000_000
    assert period["uncollected_usage_nanos"] == 250_000_000


def test_authoritative_usage_replaces_estimate_before_batch_close():
    get_settings.cache_clear()
    settings = get_settings()
    client = Client()
    wallet_id = seed_wallet_and_lifecycle_marker(client)
    service = BillingBatchSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=run_transaction,
    )
    provisional = event(when=datetime.now(timezone.utc) - timedelta(minutes=10))
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    reservation["usage_reconciliation_status"] = "pending"
    assert service.enqueue_turn_sync(provisional)
    reservation["billing_batch_created_at"] = datetime.now(timezone.utc) - timedelta(minutes=10)

    authoritative = event(
        when=provisional.created_at,
        cost=0.15,
    )
    client.documents[(settings.billing_ledger_collection, "turn-1")][
        "estimated_cost_usd_nanos"
    ] = 150_000_000

    assert service.reconcile_authoritative_usage_sync(authoritative) is True
    assert client.documents[(settings.wallets_collection, wallet_id)][
        "pending_batch_usage_nanos"
    ] == 150_000_000
    assert reservation["estimated_cost_nanos"] == 150_000_000
    assert reservation["usage_reconciliation_status"] == "complete"

    settled = service.reconcile_due_sync()

    assert settled.settled_batches == 1
    assert client.documents[(settings.wallets_collection, wallet_id)][
        "available_credit_nanos"
    ] == 850_000_000


def test_partial_usage_updates_a_late_batch_without_stopping_provider_retries():
    get_settings.cache_clear()
    settings = get_settings()
    client = Client()
    wallet_id = seed_wallet_and_lifecycle_marker(client)
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    reservation.update(
        {
            "status": "late_usage_pending",
            "usage_reconciliation_status": "pending",
            "usage_reconciliation_next_attempt_at": datetime.now(timezone.utc),
        }
    )
    service = BillingBatchSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=run_transaction,
    )
    initial = event(when=datetime.now(timezone.utc) - timedelta(minutes=10))
    assert service.enqueue_turn_sync(
        initial,
        late_usage=True,
        preserve_usage_recovery=True,
    )
    reservation["billing_batch_created_at"] = datetime.now(timezone.utc) - timedelta(minutes=10)
    assert reservation["usage_reconciliation_status"] == "pending"

    newer_partial = event(when=initial.created_at, cost=0.12)
    client.documents[(settings.billing_ledger_collection, "turn-1")][
        "estimated_cost_usd_nanos"
    ] = 120_000_000
    assert service.update_pending_estimate_sync(newer_partial) is True
    assert client.documents[(settings.wallets_collection, wallet_id)][
        "pending_batch_usage_nanos"
    ] == 120_000_000

    authoritative = event(when=initial.created_at, cost=0.15)
    client.documents[(settings.billing_ledger_collection, "turn-1")][
        "estimated_cost_usd_nanos"
    ] = 150_000_000
    assert service.reconcile_authoritative_usage_sync(authoritative) is True
    assert reservation["usage_reconciliation_status"] == "complete"
    assert client.documents[(settings.wallets_collection, wallet_id)][
        "pending_batch_usage_nanos"
    ] == 150_000_000

    assert service.reconcile_due_sync().settled_batches == 1
    assert client.documents[(settings.wallets_collection, wallet_id)][
        "available_credit_nanos"
    ] == 850_000_000


def test_late_batch_settlement_preserves_terminal_manual_review_state():
    get_settings.cache_clear()
    settings = get_settings()
    client = Client()
    wallet_id = seed_wallet_and_lifecycle_marker(client)
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    reservation.update(
        {
            "status": "late_usage_pending",
            "usage_reconciliation_status": "pending",
        }
    )
    service = BillingBatchSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=run_transaction,
    )
    provisional = event(when=datetime.now(timezone.utc) - timedelta(minutes=10))
    assert service.enqueue_turn_sync(
        provisional,
        late_usage=True,
        preserve_usage_recovery=True,
    )
    reservation.update(
        {
            "usage_reconciliation_status": "manual_review",
            "usage_reconciliation_manual_review_reason": "provider_usage_retention_expired",
            "billing_batch_created_at": datetime.now(timezone.utc)
            - timedelta(minutes=10),
        }
    )

    result = service.reconcile_due_sync()

    assert result.settled_batches == 1
    assert reservation["status"] == "late_settled"
    assert reservation["billing_batch_status"] == "settled"
    assert reservation["usage_reconciliation_status"] == "manual_review"
    assert reservation["usage_reconciliation_manual_review_reason"] == (
        "provider_usage_retention_expired"
    )


def test_authoritative_positive_difference_is_idempotent_and_adds_shortfall_debt():
    get_settings.cache_clear()
    settings = get_settings()
    client = Client()
    wallet_id = seed_wallet_and_lifecycle_marker(client)
    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    wallet["available_credit_nanos"] = 40_000_000
    service = BillingBatchSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=run_transaction,
    )
    provisional = event(when=datetime.now(timezone.utc) - timedelta(minutes=10))
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    reservation["usage_reconciliation_status"] = "pending"
    assert service.enqueue_turn_sync(provisional)
    reservation["billing_batch_created_at"] = datetime.now(timezone.utc) - timedelta(minutes=10)
    service.reconcile_due_sync()

    authoritative = event(when=provisional.created_at, cost=0.15)
    client.documents[(settings.billing_ledger_collection, "turn-1")][
        "estimated_cost_usd_nanos"
    ] = 150_000_000
    assert service.reconcile_authoritative_usage_sync(authoritative) is True
    assert service.reconcile_authoritative_usage_sync(authoritative) is True

    assert wallet["available_credit_nanos"] == 0
    assert wallet["settled_usage_nanos"] == 40_000_000
    assert wallet["usage_debt_nanos"] == 110_000_000
    assert wallet["lifetime_usage_debt_added_nanos"] == 110_000_000
    assert reservation["billing_assessed_cost_nanos"] == 150_000_000
    adjustments = [
        data
        for (collection, _), data in client.documents.items()
        if collection == settings.wallet_transactions_collection
        and data.get("transaction_type") == "agent_usage_authoritative_adjustment"
    ]
    assert len(adjustments) == 1
    assert adjustments[0]["estimated_cost_delta_nanos"] == 50_000_000


def test_downward_authoritative_correction_refunds_after_debt_was_repaid():
    get_settings.cache_clear()
    settings = get_settings()
    client = Client()
    wallet_id = seed_wallet_and_lifecycle_marker(client)
    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    wallet["available_credit_nanos"] = 20_000_000
    service = BillingBatchSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=run_transaction,
    )
    provisional = event(when=datetime.now(timezone.utc) - timedelta(minutes=10))
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    reservation["usage_reconciliation_status"] = "pending"
    assert service.enqueue_turn_sync(provisional)
    reservation["billing_batch_created_at"] = datetime.now(timezone.utc) - timedelta(minutes=10)
    service.reconcile_due_sync()
    assert wallet["usage_debt_nanos"] == 80_000_000

    # A later top-up can repay this shared debt before provider usage arrives.
    wallet["usage_debt_nanos"] = 0
    authoritative = event(when=provisional.created_at, cost=0)
    client.documents[(settings.billing_ledger_collection, "turn-1")][
        "estimated_cost_usd_nanos"
    ] = 0

    assert service.reconcile_authoritative_usage_sync(authoritative) is True

    assert wallet["available_credit_nanos"] == 100_000_000
    assert wallet["settled_usage_nanos"] == 0
    assert wallet["usage_debt_nanos"] == 0
    assert reservation["billing_assessed_cost_nanos"] == 0
    period_id = customer_billing_period_document_id("user-1", provisional.created_at.strftime("%Y-%m"))
    period = client.documents[(settings.customer_billing_periods_collection, period_id)]
    assert period["usage_estimated_nanos"] == 0
    assert period["collected_usage_nanos"] == 0
    assert period["uncollected_usage_nanos"] == 0


def test_partial_downward_correction_keeps_original_paid_usage_accounted():
    get_settings.cache_clear()
    settings = get_settings()
    client = Client()
    wallet_id = seed_wallet_and_lifecycle_marker(client)
    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    wallet["available_credit_nanos"] = 20_000_000
    service = BillingBatchSettlementService(
        firestore_client_factory=lambda: client,
        transaction_runner=run_transaction,
    )
    provisional = event(when=datetime.now(timezone.utc) - timedelta(minutes=10))
    reservation = client.documents[(settings.billing_reservations_collection, "turn-1")]
    reservation["usage_reconciliation_status"] = "pending"
    assert service.enqueue_turn_sync(provisional)
    reservation["billing_batch_created_at"] = datetime.now(timezone.utc) - timedelta(minutes=10)
    service.reconcile_due_sync()
    wallet["usage_debt_nanos"] = 0

    authoritative = event(when=provisional.created_at, cost=0.06)
    client.documents[(settings.billing_ledger_collection, "turn-1")][
        "estimated_cost_usd_nanos"
    ] = 60_000_000
    assert service.reconcile_authoritative_usage_sync(authoritative) is True

    assert wallet["available_credit_nanos"] == 40_000_000
    assert wallet["settled_usage_nanos"] == 20_000_000
    assert wallet["usage_debt_nanos"] == 0
    assert reservation["billing_assessed_collected_nanos"] == 20_000_000
    assert reservation["billing_assessed_debt_nanos"] == 40_000_000
    period_id = customer_billing_period_document_id("user-1", provisional.created_at.strftime("%Y-%m"))
    period = client.documents[(settings.customer_billing_periods_collection, period_id)]
    assert period["usage_estimated_nanos"] == 60_000_000
    assert period["collected_usage_nanos"] == 20_000_000
    assert period["uncollected_usage_nanos"] == 40_000_000
