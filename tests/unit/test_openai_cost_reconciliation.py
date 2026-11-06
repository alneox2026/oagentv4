from datetime import datetime, timezone

import httpx

from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.core.errors import RetryableWorkerError
from services.agent_persistence_worker_v3.app.services.openai_cost_reconciliation import (
    OpenAICostReconciliationService,
)


class Snapshot:
    def __init__(self, data):
        self.data = data

    def to_dict(self):
        return dict(self.data)


class Query:
    def __init__(self, collection):
        self.collection = collection
        self.filters = []

    def where(self, *args):
        self.filters.append(args)
        return self

    def stream(self):
        result = []
        for data in self.collection.client.entries:
            created_at = data["created_at"]
            if all(
                (op == ">=" and created_at >= value)
                or (op == "<" and created_at < value)
                for field, op, value in self.filters
                if field == "created_at"
            ):
                result.append(Snapshot(data))
        return result


class Document:
    def __init__(self, collection, document_id):
        self.collection = collection
        self.document_id = document_id

    def set(self, data, merge=False):
        if merge and self.document_id in self.collection.client.documents:
            self.collection.client.documents[self.document_id].update(data)
        else:
            self.collection.client.documents[self.document_id] = dict(data)


class Collection:
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def where(self, *args):
        return Query(self).where(*args)

    def stream(self):
        return Query(self).stream()

    def document(self, document_id):
        return Document(self, document_id)


class Firestore:
    def __init__(self):
        self.entries = []
        self.documents = {}

    def collection(self, name):
        return Collection(self, name)


def test_daily_import_records_actuals_and_variance_without_user_wallet_allocation(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("OPENAI_COST_RECONCILIATION_ENABLED", "true")
    monkeypatch.setenv("OPENAI_COST_RECONCILIATION_LOOKBACK_DAYS", "7")
    monkeypatch.setenv("OPENAI_PROJECT_ID", "proj_dedicated")
    monkeypatch.setenv("OPENAI_ADMIN_API_KEY", "admin-secret")
    get_settings.cache_clear()
    expected_start = int(datetime(2026, 10, 22, tzinfo=timezone.utc).timestamp())
    day_start = int(datetime(2026, 10, 28, tzinfo=timezone.utc).timestamp())
    requests = []

    def handler(request):
        requests.append(request)
        assert request.headers["Authorization"] == "Bearer admin-secret"
        assert request.url.params.get("project_ids") == "proj_dedicated"
        assert request.url.params.get_list("group_by") == ["project_id", "api_source", "line_item"]
        assert int(request.url.params["start_time"]) == expected_start
        return httpx.Response(
            200,
            json={
                "object": "page",
                "has_more": False,
                "data": [
                    {
                        "object": "bucket",
                        "start_time": day_start,
                        "end_time": day_start + 86400,
                        "results": [
                            {
                                "amount": {"value": 0.07, "currency": "usd"},
                                "project_id": "proj_dedicated",
                                "api_source": "agents_api",
                                "line_item": "gpt-6-luna input tokens",
                                "quantity": 10000,
                                "quantity_unit": "tokens",
                            },
                            {
                                "amount": {"value": 0.02, "currency": "usd"},
                                "project_id": "proj_dedicated",
                                "api_source": "agents_api",
                                "line_item": "OpenAI hosted containers",
                                "quantity": 30,
                                "quantity_unit": "duration_minutes",
                            },
                            {
                                "amount": {"value": 0.01, "currency": "usd"},
                                "project_id": "proj_dedicated",
                                "api_source": "unlabeled",
                                "line_item": "unknown",
                            },
                        ],
                    }
                ],
            },
        )

    firestore = Firestore()
    firestore.entries.append(
        {
            "turn_id": "turn-1",
            "created_at": datetime(2026, 10, 28, 12, tzinfo=timezone.utc),
            "estimated_cost_usd_nanos": 50_000_000,
        }
    )
    service = OpenAICostReconciliationService(
        firestore_client_factory=lambda: firestore,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        now=lambda: datetime(2026, 10, 29, 4, tzinfo=timezone.utc),
    )

    result = service.reconcile_sync()

    assert len(requests) == 1
    assert result.status == "completed"
    assert result.reconciled_days == 7
    assert result.provider_cost_usd == 0.10
    assert result.agents_api_cost_usd == 0.09
    assert result.local_estimate_usd == 0.05
    stored = next(iter(firestore.documents.values()))
    assert stored["provider_actual_cost_usd"] == 0.10
    assert stored["agents_api_actual_cost_usd"] == 0.09
    assert stored["agents_api_minus_local_model_estimate_usd"] == 0.04
    assert stored["customer_attribution_status"] == "aggregate_only_not_mapped_to_firebase_users"
    assert stored["wallet_adjustment_status"] == "not_applied_requires_explicit_allocation_policy"
    get_settings.cache_clear()


def test_daily_import_fails_loudly_without_admin_key(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("OPENAI_COST_RECONCILIATION_ENABLED", "true")
    monkeypatch.delenv("OPENAI_ADMIN_API_KEY", raising=False)
    get_settings.cache_clear()

    try:
        OpenAICostReconciliationService().reconcile_sync()
    except RetryableWorkerError as exc:
        assert "OPENAI_ADMIN_API_KEY" in str(exc)
    else:
        raise AssertionError("Missing admin key should fail the enabled scheduled reconciliation.")
    get_settings.cache_clear()
