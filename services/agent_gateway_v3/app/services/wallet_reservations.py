"""Fast minimum-balance authorization for prepaid agent requests."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from common.billing import customer_wallet_document_id, nonnegative_int
from services.agent_gateway_v3.app.core.config import get_settings
from services.agent_gateway_v3.app.core.errors import ApiError
from services.agent_gateway_v3.app.services.firestore_client import get_firestore_client


@dataclass(frozen=True)
class BillingTurnContext:
    """Non-financial billing metadata passed to the asynchronous worker."""

    reservation_id: str
    billing_subject_id: str
    user_id: str
    agent_id: str
    request_id: str
    minimum_balance_nanos: int
    currency: str

    def event_metadata(self) -> dict[str, Any]:
        return {
            "reservation_id": self.reservation_id,
            "billing_subject_id": self.billing_subject_id,
            "request_id": self.request_id,
            "agent_id": self.agent_id,
            "reserved_amount_nanos": 0,
            "minimum_balance_nanos": self.minimum_balance_nanos,
            "billing_currency": self.currency,
        }


class WalletBalanceGate:
    """Read the wallet once and enforce only its configured minimum balance.

    The gateway intentionally performs no billing writes, price calculation,
    reservation, or release. Confirmed usage is charged asynchronously by the
    persistence worker; any unfunded portion is recorded as wallet usage debt.
    """

    def __init__(
        self,
        *,
        firestore_client_factory: Callable[[], Any] | None = None,
    ) -> None:
        self._settings = get_settings()
        self._firestore_client_factory = firestore_client_factory or get_firestore_client

    async def authorize(
        self,
        *,
        user_id: str,
        agent_id: str,
        request_id: str,
        turn_id: str,
        minimum_balance_nanos: int | None = None,
    ) -> BillingTurnContext | None:
        """Authorize from the current available balance, or skip if disabled."""

        if not self._settings.billing_enforcement_enabled:
            return None
        return await asyncio.to_thread(
            self._authorize_sync,
            user_id=user_id,
            agent_id=agent_id,
            request_id=request_id,
            turn_id=turn_id,
            minimum_balance_nanos=minimum_balance_nanos,
        )

    def _authorize_sync(
        self,
        *,
        user_id: str,
        agent_id: str,
        request_id: str,
        turn_id: str,
        minimum_balance_nanos: int | None = None,
    ) -> BillingTurnContext:
        billing_subject_id = user_id
        required_balance_nanos = (
            minimum_balance_nanos
            if minimum_balance_nanos is not None and minimum_balance_nanos > 0
            else self._settings.billing_minimum_balance_nanos
        )
        if required_balance_nanos <= 0:
            raise RuntimeError("BILLING_MINIMUM_BALANCE_NANOS must be greater than zero.")

        client = self._firestore_client_factory()
        wallet_ref = client.collection(self._settings.wallets_collection).document(
            customer_wallet_document_id(billing_subject_id)
        )
        wallet_snapshot = wallet_ref.get()
        if not wallet_snapshot.exists:
            raise ApiError(
                402,
                "wallet_not_ready",
                "A prepaid usage balance is required before using this agent.",
            )
        wallet = wallet_snapshot.to_dict() or {}
        if wallet.get("owner_uid") != user_id or wallet.get("billing_subject_id") != billing_subject_id:
            raise ApiError(
                403,
                "wallet_ownership_mismatch",
                "The prepaid usage balance is not available for this account.",
            )
        if wallet.get("status") != "active":
            raise ApiError(
                403,
                "wallet_inactive",
                "The prepaid usage balance is not active.",
            )

        try:
            available_credit_nanos = nonnegative_int(
                wallet.get("available_credit_nanos", 0),
                field_name="available_credit_nanos",
            )
        except ValueError as exc:
            raise ApiError(
                503,
                "wallet_invalid",
                "The prepaid usage balance is temporarily unavailable.",
            ) from exc

        if available_credit_nanos < required_balance_nanos:
            raise ApiError(
                402,
                "insufficient_credit",
                "Your available prepaid balance is below the minimum required to start this agent request.",
                {
                    "currency": "USD",
                    "required_minimum_balance_nanos": required_balance_nanos,
                    "available_credit_nanos": available_credit_nanos,
                    "wallet_available_credit_nanos": available_credit_nanos,
                },
            )

        return BillingTurnContext(
            reservation_id=turn_id,
            billing_subject_id=billing_subject_id,
            user_id=user_id,
            agent_id=agent_id,
            request_id=request_id,
            minimum_balance_nanos=required_balance_nanos,
            currency="USD",
        )


async def get_wallet_balance_gate() -> WalletBalanceGate:
    return WalletBalanceGate()
