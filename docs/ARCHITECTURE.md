# OpenAI Managed Agents middleware architecture

```text
FlutterFlow client
  ├─ Firebase ID token
  ├─ message + optional middleware thread_id/client_turn_id
  └─ SSE response
        │
        ▼
Cloud Run gateway
  ├─ verifies Firebase identity and resolves server-owned billing subject
  ├─ checks the configured available-balance floor (read-only)
  ├─ creates/loads OpenAI-hosted session from saved remote_agent_id
  ├─ subscribes to session events, then submits an idempotent input event
  ├─ streams output and retains raw root/subagent turn usage reports
  └─ publishes completed turn and raw reports to Pub/Sub
        │
        ▼
Persistence worker
  ├─ recovers missing usage from the session with bounded read-only lookups
  ├─ aggregates tokens and applies the versioned per-turn rate card
  ├─ persists transcript and idempotency marker
  ├─ creates the immutable per-turn billing ledger and lifecycle marker atomically
  ├─ moves priced usage into the session batch and tracks its provisional estimate separately
  └─ runs one wallet debit for batches; continues usage recovery independently of wallet authorization
        │
        └─ Daily provider-cost reconciler (admin key; worker only)
             ├─ imports OpenAI organization Costs API daily buckets and line items
             ├─ re-fetches a bounded lookback so revised provider totals are captured
             └─ records aggregate variance; does not silently allocate aggregate costs to users
```

## OpenAI API contract

- `POST /v1/agents/sessions` uses the configured saved `agent_id` and `{type: "openai_hosted"}` environment. The saved session ID is stored in the server-owned thread record.
- `GET /v1/agents/sessions/{session_id}/events?stream=true` is opened before `POST /v1/agents/sessions/{session_id}/events` submits `agent.session.input.message`.
- The message submission carries the middleware's stable turn ID as `Idempotency-Key`.
- The stream ends only on root `agent.session.turn.completed`, `turn.failed`, or `turn.cancelled`; subagent terminal events are retained for usage aggregation.
- The gateway sends `Authorization: Bearer ...` and `OpenAI-Beta: agents=v1`. It does not call the Agents SDK.

## Billing and state boundaries

- Authentication, billing identity, turn identity, Agent ID selection, and usage fields are server-owned. Clients cannot authoritatively set them.
- Firestore collection names are resolved through Terraform configuration. Runtime transcript collections may be stack-scoped; shared wallet, transaction, and billing-period collections retain the configured central names.
- The gateway streams; the worker owns durable transcript and financial mutations.
- Token aggregation and pricing happen in the persistence worker after the model response, not in the gateway stream. The customer response is not held open for usage recovery or pricing.
- Every turn keeps a versioned per-turn token-rate estimate in its immutable ledger. Customer wallet debits are grouped by authenticated owner, OpenAI session, and billing month, closing on session inactivity, a maximum age, turn count, or provisional exposure threshold. The worker-owned lifecycle document carries pending batch state (the legacy collection name is retained for compatibility; no separate per-turn batch-entry document is created); the batch transaction is idempotent and updates the wallet once, while individual ledgers remain available for audit.
- The gateway only compares current `available_credit_nanos` with the configured minimum balance (default 50,000,000 nanos / $0.05). It performs no billing writes, usage pricing, reservation, hold, or debit. After the turn, the worker atomically creates the immutable ledger and its lifecycle marker. The threshold is an eligibility floor, not a spend cap; concurrent requests and unsettled batch usage can exceed the visible balance. `pending_batch_usage_nanos` contains provisional estimates only. Known turn-level usage is batched even when another root or subagent report is missing; the batch closes within five minutes by default while provider lookup continues independently. At close, the worker debits the estimate against available funds and moves any shortfall to shared wallet `usage_debt_nanos`; the central billing service's idempotent top-up flow repays that debt before crediting the remaining top-up amount. Missing usage is never treated as zero and missing usage alone does not block subsequent requests.
- When complete provider usage arrives, an open batch's estimate is replaced in place. If its estimate was already settled, an idempotent per-turn adjustment applies only the difference: positive amounts debit available funds and add any shortfall to shared debt; downward corrections reduce debt and refund paid amounts. The ledger and reservation marker remain available through the seven-day lookup window, after which unresolved usage enters manual review. Due-time indexes keep unchanged retries out of each scheduled scan.
- Direct settlement (when batching is disabled) and turns with no usable estimate follow the same available-balance-first rule and move any remaining assessed charge to `usage_debt_nanos`; provisional batch estimates are not separately recorded as debt. The central billing service's shared v4 credit operation repays wallet debt before crediting top-up remainder.
- The daily OpenAI Costs API import is aggregate provider evidence and a variance check, not customer-level source-of-truth. Provider `project_id`/`user_id` dimensions are not Firebase `billing_subject_id`; no wallet adjustment is made without an explicit, validated allocation method. See the root README's billing limitations.
- The worker retries read-only usage lookups when stream reports omit usage. If counts remain unavailable, the ledger records unavailable/unpriced rather than inventing token counts.

## Operations

- Store `OPENAI_API_KEY` in Secret Manager; never embed it in the registry or client. Terraform grants the gateway and worker access; the worker uses the key only for read-only missing-usage recovery.
- If daily provider-cost import is enabled, store a separate organization admin key as `OPENAI_ADMIN_API_KEY`; Terraform exposes it only to the worker and grants it no gateway access.
- The registry model must match the saved Agent. Unknown models or missing price-card entries fail closed.
- This worker currently marks local thread deletion complete without deleting the OpenAI-hosted session. Do not promise remote session deletion until that lifecycle is implemented and tested.
