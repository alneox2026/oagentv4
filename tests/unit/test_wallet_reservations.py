import asyncio
from datetime import datetime, timezone

import pytest

from common.billing import customer_wallet_document_id
from services.agent_gateway_v3.app.core.config import get_settings
from services.agent_gateway_v3.app.core.errors import ApiError
from services.agent_gateway_v3.app.services.wallet_reservations import WalletBalanceGate


@pytest.fixture(autouse=True)
def _reset_gateway_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class FakeSnapshot:
    def __init__(self, data):
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return dict(self._data) if self._data is not None else None


class FakeDocumentReference:
    def __init__(self, client, collection, document_id):
        self.client = client
        self.collection = collection
        self.document_id = document_id

    @property
    def key(self):
        return self.collection, self.document_id

    def get(self):
        self.client.reads += 1
        return FakeSnapshot(self.client.documents.get(self.key))


class FakeCollectionReference:
    def __init__(self, client, collection):
        self.client = client
        self.collection_name = collection

    def document(self, document_id):
        return FakeDocumentReference(self.client, self.collection_name, document_id)


class FakeTransaction:
    def __init__(self, client):
        self.client = client

    def get(self, document_ref):
        self.client.reads += 1
        return FakeSnapshot(self.client.documents.get(document_ref.key))

    def create(self, document_ref, data):
        if document_ref.key in self.client.documents:
            raise RuntimeError("duplicate create")
        self.client.writes += 1
        self.client.documents[document_ref.key] = dict(data)

    def update(self, document_ref, updates):
        self.client.writes += 1
        self.client.documents[document_ref.key].update(updates)


class FakeClient:
    """Small fake shared by the worker billing unit tests."""

    def __init__(self):
        self.documents = {}
        self.reads = 0
        self.writes = 0

    def collection(self, collection):
        return FakeCollectionReference(self, collection)

    def transaction(self):
        return FakeTransaction(self)


def _run_transaction(client, operation):
    return operation(client.transaction())


def _enable_billing(monkeypatch):
    monkeypatch.setenv("BILLING_ENFORCEMENT_ENABLED", "true")
    monkeypatch.setenv("BILLING_MINIMUM_BALANCE_NANOS", "50000000")
    get_settings.cache_clear()


def _wallet(user_id, available_credit_nanos=1_000_000_000):
    return {
        "schema_version": 1,
        "billing_subject_id": user_id,
        "owner_uid": user_id,
        "currency": "USD",
        "status": "active",
        "available_credit_nanos": available_credit_nanos,
        "reserved_credit_nanos": 0,
        "settled_usage_nanos": 0,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }


def _gate_with_wallet(monkeypatch, *, available_credit_nanos=1_000_000_000):
    _enable_billing(monkeypatch)
    settings = get_settings()
    client = FakeClient()
    user_id = "user-1"
    wallet_id = customer_wallet_document_id(user_id)
    client.documents[(settings.wallets_collection, wallet_id)] = _wallet(
        user_id, available_credit_nanos
    )
    gate = WalletBalanceGate(firestore_client_factory=lambda: client)
    return gate, client, settings, user_id, wallet_id


def _authorize(gate, *, user_id="user-1", turn_id="turn-1", minimum=None):
    return asyncio.run(
        gate.authorize(
            user_id=user_id,
            agent_id="maxima",
            request_id="req-1",
            turn_id=turn_id,
            minimum_balance_nanos=minimum,
        )
    )


def test_gate_checks_five_cent_floor_with_one_read_and_no_financial_writes(monkeypatch):
    gate, client, settings, user_id, wallet_id = _gate_with_wallet(
        monkeypatch, available_credit_nanos=50_000_000
    )
    wallet_before = dict(client.documents[(settings.wallets_collection, wallet_id)])

    context = _authorize(gate)

    assert context is not None
    assert context.minimum_balance_nanos == 50_000_000
    assert context.event_metadata()["reserved_amount_nanos"] == 0
    assert context.event_metadata()["request_id"] == "req-1"
    assert client.reads == 1
    assert client.writes == 0
    assert client.documents[(settings.wallets_collection, wallet_id)] == wallet_before
    assert len(client.documents) == 1


def test_gate_rejects_below_floor_without_creating_session_marker_or_wallet_write(monkeypatch):
    gate, client, settings, _, wallet_id = _gate_with_wallet(
        monkeypatch, available_credit_nanos=49_999_999
    )

    with pytest.raises(ApiError) as exc_info:
        _authorize(gate)

    assert exc_info.value.status_code == 402
    assert exc_info.value.code == "insufficient_credit"
    assert client.reads == 1
    assert client.writes == 0
    assert len(client.documents) == 1
    assert client.documents[(settings.wallets_collection, wallet_id)]["available_credit_nanos"] == 49_999_999


def test_gate_ignores_pending_estimates_and_existing_debt_when_balance_meets_floor(monkeypatch):
    gate, client, settings, _, wallet_id = _gate_with_wallet(
        monkeypatch, available_credit_nanos=50_000_000
    )
    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    wallet["pending_batch_usage_nanos"] = 900_000_000
    wallet["usage_debt_nanos"] = 2_000_000_000
    wallet["pending_usage_reconciliation_count"] = 5

    assert _authorize(gate) is not None
    assert client.writes == 0
    assert wallet["available_credit_nanos"] == 50_000_000


def test_gate_does_not_block_later_turns_for_unresolved_usage(monkeypatch):
    gate, client, settings, _, wallet_id = _gate_with_wallet(monkeypatch)
    wallet = client.documents[(settings.wallets_collection, wallet_id)]
    wallet["pending_usage_reconciliation_count"] = 1
    wallet["usage_debt_nanos"] = 250_000_000

    first = _authorize(gate, turn_id="turn-1")
    second = _authorize(gate, turn_id="turn-2")

    assert first is not None and second is not None
    assert client.reads == 2
    assert client.writes == 0
    assert len(client.documents) == 1
    assert wallet["available_credit_nanos"] == 1_000_000_000


def test_gate_enforces_agent_specific_minimum_when_configured(monkeypatch):
    gate, client, _, _, _ = _gate_with_wallet(
        monkeypatch, available_credit_nanos=100_000_000
    )

    with pytest.raises(ApiError) as exc_info:
        _authorize(gate, minimum=200_000_000)

    assert exc_info.value.status_code == 402
    assert exc_info.value.code == "insufficient_credit"
    assert client.writes == 0


def test_gate_rejects_missing_or_mismatched_wallet(monkeypatch):
    _enable_billing(monkeypatch)
    settings = get_settings()
    client = FakeClient()
    gate = WalletBalanceGate(firestore_client_factory=lambda: client)
    with pytest.raises(ApiError) as missing:
        _authorize(gate)
    assert missing.value.code == "wallet_not_ready"

    wallet_id = customer_wallet_document_id("user-1")
    client.documents[(settings.wallets_collection, wallet_id)] = _wallet("someone-else")
    with pytest.raises(ApiError) as mismatch:
        _authorize(gate)
    assert mismatch.value.code == "wallet_ownership_mismatch"
    assert client.writes == 0


def test_gate_skips_firestore_when_enforcement_is_disabled(monkeypatch):
    monkeypatch.setenv("BILLING_ENFORCEMENT_ENABLED", "false")
    get_settings.cache_clear()
    client = FakeClient()
    gate = WalletBalanceGate(firestore_client_factory=lambda: client)

    context = _authorize(gate)

    assert context is None
    assert client.reads == 0
    assert client.writes == 0
