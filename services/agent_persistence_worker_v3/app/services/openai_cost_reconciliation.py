"""Import daily organization-level OpenAI cost data for reconciliation evidence.

The Costs API reports aggregates; it does not make an OpenAI project/API user
equivalent to a Firebase billing subject. This module deliberately records the
provider actuals and variance without silently allocating them to wallets.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.core.errors import RetryableWorkerError
from services.agent_persistence_worker_v3.app.services.firestore_client import get_firestore_client

OPENAI_COSTS_URL = "https://api.openai.com/v1/organization/costs"


@dataclass(frozen=True)
class OpenAICostReconciliationResult:
    status: str
    reconciled_days: int = 0
    provider_cost_usd: float = 0.0
    agents_api_cost_usd: float = 0.0
    local_estimate_usd: float = 0.0


class OpenAICostReconciliationService:
    """Fetch and persist daily provider actuals alongside local model estimates."""

    def __init__(
        self,
        *,
        firestore_client_factory: Callable[[], Any] | None = None,
        http_client: httpx.Client | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._settings = get_settings()
        self._firestore_client_factory = firestore_client_factory or get_firestore_client
        self._http_client = http_client
        self._now = now or (lambda: datetime.now(timezone.utc))

    async def reconcile(self) -> OpenAICostReconciliationResult:
        return await asyncio.to_thread(self.reconcile_sync)

    def reconcile_sync(self) -> OpenAICostReconciliationResult:
        if not self._settings.openai_cost_reconciliation_enabled:
            return OpenAICostReconciliationResult("disabled")
        api_key = os.getenv("OPENAI_ADMIN_API_KEY", "").strip()
        if not api_key:
            raise RetryableWorkerError(
                "OpenAI daily cost reconciliation is enabled but OPENAI_ADMIN_API_KEY is not configured."
            )

        now = self._now().astimezone(timezone.utc)
        end = datetime.combine(now.date(), datetime_time.min, tzinfo=timezone.utc)
        lookback_days = max(1, min(180, self._settings.openai_cost_reconciliation_lookback_days))
        start = end - timedelta(days=lookback_days)
        client = self._http_client or httpx.Client(timeout=httpx.Timeout(30.0, connect=5.0))
        owns_client = self._http_client is None
        try:
            buckets = self._fetch_buckets(client, api_key, start, end)
        finally:
            if owns_client:
                client.close()
        # Persist a record for every completed day in the lookback, including
        # zero-cost days, so a provider correction can replace an older value.
        for day_offset in range(lookback_days):
            bucket_start = int((start + timedelta(days=day_offset)).timestamp())
            buckets.setdefault(bucket_start, [])

        firestore = self._firestore_client_factory()
        provider_totals = Decimal("0")
        agents_api_totals = Decimal("0")
        local_totals = Decimal("0")
        days_written = 0
        for bucket_start, rows in buckets.items():
            day_start = datetime.fromtimestamp(bucket_start, tz=timezone.utc)
            day_end = day_start + timedelta(days=1)
            local_nanos = self._local_estimate_for_day(firestore, day_start, day_end)
            local_usd = Decimal(local_nanos) / Decimal("1000000000")
            total_usd = sum((row["amount_usd"] for row in rows), Decimal("0"))
            agents_usd = sum(
                (row["amount_usd"] for row in rows if row.get("api_source") == "agents_api"),
                Decimal("0"),
            )
            provider_totals += total_usd
            agents_api_totals += agents_usd
            local_totals += local_usd
            project_scope = self._settings.openai_project_id or "organization"
            project_hash = hashlib.sha256(project_scope.encode("utf-8")).hexdigest()[:12]
            document_id = f"{day_start:%Y-%m-%d}_{project_hash}"
            payload = {
                "schema_version": 1,
                "document_id": document_id,
                "provider": "openai",
                "source": "organization_costs_api",
                "bucket_width": "1d",
                "bucket_start": day_start,
                "bucket_end": day_end,
                "project_scope": project_scope,
                "project_filter_applied": bool(self._settings.openai_project_id),
                "currency": "USD",
                "provider_actual_cost_usd": float(total_usd),
                "agents_api_actual_cost_usd": float(agents_usd),
                "local_model_estimate_usd": float(local_usd),
                "agents_api_minus_local_model_estimate_usd": float(agents_usd - local_usd),
                "local_estimate_basis": "per_turn_token_rate_card; excludes non-model OpenAI resources",
                "customer_attribution_status": "aggregate_only_not_mapped_to_firebase_users",
                "wallet_adjustment_status": "not_applied_requires_explicit_allocation_policy",
                "line_items": [
                    {
                        "project_id": row.get("project_id"),
                        "api_source": row.get("api_source"),
                        "line_item": row.get("line_item"),
                        "amount_usd": float(row["amount_usd"]),
                        "quantity": row.get("quantity"),
                        "quantity_unit": row.get("quantity_unit"),
                    }
                    for row in rows
                ],
                "reconciled_at": now,
                "reconciliation_window_days": lookback_days,
            }
            firestore.collection(self._settings.openai_costs_collection).document(document_id).set(
                payload,
                merge=True,
            )
            days_written += 1
        return OpenAICostReconciliationResult(
            status="completed",
            reconciled_days=days_written,
            provider_cost_usd=float(provider_totals),
            agents_api_cost_usd=float(agents_api_totals),
            local_estimate_usd=float(local_totals),
        )

    def _fetch_buckets(self, client: httpx.Client, api_key: str, start: datetime, end: datetime) -> dict[int, list[dict[str, Any]]]:
        headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}
        params: dict[str, Any] = {
            "start_time": int(start.timestamp()),
            "end_time": int(end.timestamp()),
            "bucket_width": "1d",
            "limit": 180,
            "group_by": ["project_id", "api_source", "line_item"],
        }
        if self._settings.openai_project_id:
            params["project_ids"] = [self._settings.openai_project_id]
        results: dict[int, list[dict[str, Any]]] = defaultdict(list)
        while True:
            try:
                response = client.get(OPENAI_COSTS_URL, headers=headers, params=params)
            except httpx.RequestError as exc:
                raise RetryableWorkerError(f"OpenAI Costs API request failed: {type(exc).__name__}.") from exc
            if response.status_code >= 500 or response.status_code == 429:
                raise RetryableWorkerError(f"OpenAI Costs API returned retryable HTTP {response.status_code}.")
            if response.status_code != 200:
                raise RetryableWorkerError(f"OpenAI Costs API returned HTTP {response.status_code}.")
            try:
                payload = response.json()
            except ValueError as exc:
                raise RetryableWorkerError("OpenAI Costs API returned invalid JSON.") from exc
            if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
                raise RetryableWorkerError("OpenAI Costs API response is missing its bucket data.")
            for bucket in payload["data"]:
                if not isinstance(bucket, dict) or not isinstance(bucket.get("results"), list):
                    continue
                bucket_start = _as_int(bucket.get("start_time"))
                if bucket_start is None:
                    continue
                results.setdefault(bucket_start, [])
                for row in bucket["results"]:
                    normalized = _normalize_cost_row(row)
                    if normalized is not None:
                        results[bucket_start].append(normalized)
            if payload.get("has_more") is not True:
                break
            page = payload.get("next_page")
            if not page:
                raise RetryableWorkerError("OpenAI Costs API indicated another page but omitted next_page.")
            params["page"] = str(page)
        return dict(results)

    def _local_estimate_for_day(self, client: Any, start: datetime, end: datetime) -> int:
        collection = client.collection(self._settings.billing_ledger_collection)
        try:
            from google.cloud.firestore_v1.base_query import FieldFilter

            snapshots = (
                collection.where(filter=FieldFilter("created_at", ">=", start))
                .where(filter=FieldFilter("created_at", "<", end))
                .stream()
            )
        except (AttributeError, ImportError, ModuleNotFoundError, TypeError):
            # Lightweight fakes and older Firestore clients may not accept the
            # keyword filter API. Stream and apply the same bounded date filter
            # below instead of falling back to deprecated positional filters.
            snapshots = collection.stream()
        total = 0
        for snapshot in snapshots:
            data = snapshot.to_dict() or {}
            created_at = data.get("created_at")
            if not isinstance(created_at, datetime):
                continue
            created_at = created_at.replace(tzinfo=timezone.utc) if created_at.tzinfo is None else created_at.astimezone(timezone.utc)
            if start <= created_at < end:
                amount = data.get("estimated_cost_usd_nanos")
                if isinstance(amount, int) and not isinstance(amount, bool) and amount >= 0:
                    total += amount
        return total


def _normalize_cost_row(row: Any) -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    amount = row.get("amount")
    raw_value = amount.get("value") if isinstance(amount, dict) else None
    currency = str(amount.get("currency") or "usd").lower() if isinstance(amount, dict) else "usd"
    if currency != "usd":
        raise RetryableWorkerError(f"OpenAI Costs API returned unsupported currency {currency!r}.")
    try:
        amount_usd = Decimal(str(raw_value))
    except (InvalidOperation, ValueError):
        return None
    if not amount_usd.is_finite() or amount_usd < 0:
        return None
    return {
        "amount_usd": amount_usd,
        "project_id": row.get("project_id"),
        "api_source": row.get("api_source"),
        "line_item": row.get("line_item"),
        "quantity": row.get("quantity"),
        "quantity_unit": row.get("quantity_unit"),
    }


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
