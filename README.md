# OpenAI Managed Agents Billing Middleware Template

This template exposes an OpenAI **Agents API** saved agent through a Firebase-authenticated Cloud Run gateway, streams its output to the app, and charges reported model-token usage through the existing central billing flow. It uses raw HTTP against the Agents API; it does not use the Agents SDK.

The template is a new sibling of the existing middleware templates. It follows the managed-agent gateway/persistence-worker boundary and retains Terraform-configured Firestore collection names so the same wallet and billing records can be shared.

> **V4 wallet contract:** requests use the $0.05 minimum-balance gate without holds; the worker batches turn-level usage estimates and records any unfunded amount as shared wallet `usage_debt_nanos`. When final provider usage arrives, it applies only the difference from the amount already assessed. The matching v4 centralized billing service must repay shared debt before adding top-up remainder to available balance. This template change does not deploy either service.

## Request flow

1. The gateway authenticates the Firebase user and derives the billing subject, request ID, and idempotent turn ID on the server.
2. For a follow-up, it validates ownership of the existing thread and resolves the provider session from server-owned storage; this step must not create a provider session.
3. The gateway checks that the active wallet's current `available_credit_nanos` is at least the configured minimum balance (default $0.05). If the check fails, it returns the billing error before creating a provider session or submitting input to the Agents API. This is an eligibility check only: the gateway does not calculate token costs, place a monetary hold, or debit the wallet.
4. Only after the balance check succeeds, a new conversation creates its OpenAI-hosted session from the saved Agent ID. The OpenAI session ID is held in the server-owned thread record; clients must not choose it. The gateway then subscribes to the session event stream and submits the message with an `Idempotency-Key`.
5. The gateway streams text to the client and waits for the root turn to complete, fail, or cancel. Session-idle or stream-close alone is not considered success.
6. After the response completes, the gateway publishes raw per-turn usage reports to Pub/Sub. The persistence worker aggregates and prices available turn reports, writes the immutable per-turn ledger and queues a batch entry. A separate worker reconciliation debits grouped turns once per batch; pricing and settlement stay outside the response-stream path. Missing reports remain on an independent provider-usage retry schedule.

## Billing preflight invariant — keep this order

Treat this ordering as a hard billing and side-effect boundary:

- Authenticate and validate existing thread/session ownership using server-owned state only.
- Check that the current available balance is at least the configured floor (default $0.05).
  If the check fails (including HTTP 402), return without creating a provider-side session
  or sending agent/model input.
- Only after the balance check succeeds may the gateway create a provider-side session or submit
  agent/model input. The gateway writes no billing marker; the worker creates the lifecycle record
  atomically with the durable turn ledger after completion.
- Keep buffered and streaming paths aligned; test that zero-credit requests return 402 with no
  provider-session or agent calls.

Purely local ID generation may happen before the balance check only when it has no provider-side side effects.

Use the streaming route:

```http
POST /v1/agents/openai_managed_agent/chat/stream
Authorization: Bearer <Firebase ID token>
Content-Type: application/json

{"message":"Hello","client_turn_id":"a-unique-client-turn-id"}
```

For a follow-up, send the returned `thread_id` and (optionally) the returned `session_id`. The gateway checks thread ownership and resolves the provider session from Firestore. Never accept a client-provided billing subject, wallet amount, provider Agent ID, or usage report as authoritative.

## Agent and model configuration

The provided OpenAI Agent ID is configured in both `config/agents.dev.yaml` and `config/agents.prod.yaml`:

```yaml
remote_agent_id: agent_bc86941978354b21a682cde68398daca57998fc854d74ce3a1
model: gpt-6-luna
```

`openai_managed_agent` is this middleware's local route name, while `MaxBaris` is the saved Agent's dashboard display name. They do not need to match: the middleware addresses the saved Agent by `remote_agent_id`. The Agent's tools, instructions, reasoning, and multi-agent settings remain managed in the OpenAI dashboard and are not duplicated here.

The registry's `model` must match the model on the saved Agent and must have a current explicit rate card in `config/billing.prod.yaml`. The gateway checks the model returned while creating a session and fails closed on a mismatch. Update the model and catalog together if the saved Agent uses a different model. Agent ID is configuration, not a secret.

## Batched customer debits and usage estimates

The customer-facing wallet debit is asynchronous and batched. Before each provider call, the gateway performs a bounded read of the active wallet and checks only that `available_credit_nanos` meets the configured minimum (default $0.05). It does not write a marker, calculate usage, reserve funds, or debit the wallet. After the turn, the worker atomically writes the immutable ledger and a per-turn lifecycle marker; the legacy `billing_reservations_*` collection name is retained for compatibility, but new documents are not holds. Each priced turn is added to the session batch. A batch becomes due on the first of: five minutes of session inactivity, five-minute maximum age, 20 priced turns, or USD 1.00 of provisional estimated usage. These are configurable Terraform values (`billing_batch_session_idle_seconds`, `billing_batch_max_age_seconds`, `billing_batch_max_turns`, and `billing_batch_max_exposure_nanos`). The five-minute worker job processes due batches as well as provider-usage retries.

Keep `billing_reconciliation_enabled = true` whenever `billing_enforcement_enabled = true`. Terraform enables the five-minute worker job by default and rejects a billing-enforced deployment with reconciliation disabled; without that job, batches would remain pending and delayed-usage recovery would stop.

When a priced turn joins a batch, its token-rate estimate is added to `pending_batch_usage_nanos`; this is a provisional estimate, not a hold or confirmed debt. The gateway does not calculate usage costs, subtract pending estimates, write billing state, or debit the wallet: it performs only the configured floor comparison against the wallet's current `available_credit_nanos`. If OpenAI reports some root or subagent turn usage while another turn is missing, the worker queues the known turn-level estimate as provisional usage and keeps retrying the missing report independently. With no usable turn-level usage, it keeps the marker pending without inventing a zero charge. The $0.05 floor is an eligibility threshold, not a spend cap: concurrent requests and usage accumulated before batch close can exceed the visible balance. Unresolved usage by itself does not block a later question.

Batch settlement uses one idempotent wallet transaction per group, updates the customer billing-period aggregate once, and marks each lifecycle record. At close, the worker clears that batch's provisional amount from `pending_batch_usage_nanos`, debits as much estimated usage as the wallet can cover, and adds any unpaid remainder to the shared `usage_debt_nanos` ledger (also recording `lifetime_usage_debt_added_nanos`). The worker retains the per-turn ledger and reservation marker while either settlement or usage recovery is active. The centralized billing service's atomic top-up flow repays known debt first and credits only the remainder; this middleware does not retry per-turn shortfall collection. The per-turn ledgers and wallet transaction retain the audit trail. Batching reduces wallet/transaction writes; it intentionally does not eliminate per-turn transcript, idempotency, or audit writes.

If batching is disabled, direct settlement applies the same available-balance-first, debt-for-the-remainder rule. Turns without a usable estimate wait for provider usage and then use the same debt ledger whether they are placed in a late batch or settled directly.

Debt repayment depends on the shared billing API using the matching v4 debt-aware credit operation: wallet credits must reduce `usage_debt_nanos` first and add only the net remainder to `available_credit_nanos`. Deploy and verify that central billing contract before relying on top-ups to recover middleware usage debt.

The local model charge is still a **versioned token-rate estimate**, not exact provider pass-through. The worker prices reported `input_tokens` (splitting cached input where reported) and `output_tokens` (including reasoning) using the versioned local catalog. It aggregates root-agent and observed subagent turns. The response stream is not held open for usage recovery, token aggregation, or pricing.

The Agents API reports best-effort usage; it can be null or revised after a turn, and does not expose a separate cache-write-token count. After the response, the worker makes up to three bounded, read-only lookup attempts when stream events omitted usage, with each attempt limited to three pages of recent session turns. The middleware therefore:

- Records a per-turn token-rate estimate and `pricing_warnings` in the ledger rather than calling it an exact invoice total.
- Prices unreported cache writes as zero (not as a guessed surcharge), so estimates can understate model cost when a model charges for cache writes.
- Uses the short-context rate card as an explicit estimate because Agents API reports turn totals rather than per-model-call context tiers; a warning is added when aggregate input crosses the catalog threshold.
- Keeps batch/session wallet totals provisional. The optional daily OpenAI Costs API job re-fetches the last seven completed UTC days and stores provider aggregates, line items, local model-estimate comparison, and variance in `openai_provider_costs_*`. It can include OpenAI-hosted container/sandbox and other provider line items that are present in the Costs API. Use a dedicated OpenAI project per middleware stack for a meaningful comparison.
- Does **not** automatically allocate organization/project-level actuals to Firebase users or adjust wallets from them. The Costs API reports aggregate groupings, and an OpenAI project/user ID is not the authenticated Firebase billing subject. The daily records explicitly state that user attribution and wallet adjustment were not applied. A customer-level allocation/true-up policy must be chosen and validated before claiming provider-exact per-user charges.
- Missing Agents API usage stays pending without delaying the streamed response. When some per-turn token reports are available, the worker batches their cost as a provisional lower-bound estimate. It retries missing reports independently of batch settlement using exponential backoff (defaults: 5 minutes initially, capped at 6 hours) for up to 7 days. When complete provider usage arrives, an open batch is updated in place; a settled turn receives only the idempotent cost difference. Positive differences use available credit first and add any remainder to shared wallet debt. Downward corrections reduce outstanding debt and refund any already-paid portion to the wallet. If no usable estimate exists, the worker waits for provider usage rather than guessing zero. After the retry window, unresolved usage moves to `usage_recovery_manual_review`; the account can still make requests whenever its available balance meets the minimum. Configure the policy with `billing_usage_reconciliation_initial_backoff_seconds`, `billing_usage_reconciliation_max_backoff_seconds`, and `billing_usage_reconciliation_retention_seconds`.
- Provider-usage retries query only due lifecycle markers and run with bounded concurrency (defaults: up to 32 lookups, 8 at once). Work beyond the per-pass limit is deferred without incrementing its retry count. Completed-but-not-enqueued settlement markers also have a next-attempt timestamp, so unchanged pending records are not repeatedly scanned on each worker run. Terraform provisions the Firestore indexes for these due-time queries.

For a new middleware with an empty reservation collection, no document migration is required; Terraform creates the new due-time indexes before enabling the scheduled reconciler. If the new worker will reuse a populated `billing_reservations_*` collection, backfill `billing_settlement_next_attempt_at` to the current UTC time for existing `completed_pending` documents that lack it before enabling reconciliation. Keep the existing ledger and reservation documents; they are needed to recover pending usage and settlement.

The Cloud Scheduler missing-usage/batch-settlement job defaults to every 5 minutes (`*/5 * * * *`) in this OpenAI-managed template; its schedule is configurable with `billing_reconciliation_schedule`. The optional provider-cost job runs daily at `17 2 * * *` UTC and rechecks the configured lookback window. Do not change the daily job to a five-minute schedule: the provider Costs API currently returns daily buckets.

Check the current [Agents API usage documentation](https://developers.openai.com/api/docs/guides/agents-api/observability) and [OpenAI API pricing](https://developers.openai.com/api/docs/pricing) before production pricing changes. The included `gpt-6-luna` catalog matches the existing OpenAI middleware catalog in this workspace; it is not a substitute for reviewing current rates.

## Credentials

The runtime provider credential is an OpenAI API key authorized for Agents API read/write operations, Responses inference, and the saved Agent. Store it in Google Secret Manager; do not put it in YAML, Terraform variables, command-line arguments, source control, or FlutterFlow. Terraform grants the gateway and persistence-worker service accounts access to the runtime secret. The worker uses the key only for bounded, read-only recovery of missing turn usage; it does not submit agent input.

Daily cost import requires a separate OpenAI organization admin API key. It is injected into the worker only; the public gateway never receives it. The example enables daily import and names `openai-admin-api-key`, so create that Secret Manager secret and add an organization admin key before applying the example Terraform settings. Do not reuse a normal project API key for this endpoint.

The Cloud Shell deploy script reads these OpenAI cost-reconciliation settings from `infra/terraform/terraform.tfvars`, or from matching environment variables: `openai_cost_reconciliation_enabled`, `openai_admin_api_key_secret_id`, `openai_admin_api_key_secret_version`, `openai_cost_reconciliation_schedule`, `openai_cost_reconciliation_lookback_days`, and `openai_project_id`. The lookback setting must be an unquoted whole number from 1 through 180 (for example, `openai_cost_reconciliation_lookback_days = 7`); invalid values stop deployment before Terraform initialization. When enabled, the script verifies the admin-key secret exists before continuing. The admin key value itself must remain in Secret Manager.

`openai_session_spend_control_limit_cents` optionally sets a best-effort OpenAI session limit in USD cents. It is disabled at zero by default, applies to a provider session (not a Firebase user wallet), and is supplemental protection—not a final usage or billing source.

The Terraform example references an existing secret:

```hcl
openai_api_key_secret_id      = "openai-api-key"
openai_api_key_secret_version = "1"
```

If the secret does not already exist, create it, then add a version without putting the key in shell history or a command argument:

```bash
gcloud secrets create openai-api-key --replication-policy=automatic
read -s -p "OpenAI API key: " OPENAI_API_KEY
printf '%s' "$OPENAI_API_KEY" | gcloud secrets versions add openai-api-key --data-file=-
unset OPENAI_API_KEY
```

If the secret already exists, skip the create command and add a new version. No API key is needed for unit tests or template construction; a live OpenAI smoke test and deployment require the key.

For local live testing, provide the key to the gateway process as the `OPENAI_API_KEY` environment variable. This template does not load secrets from a `.env` file. Do not paste the key into the agent YAML, Terraform variables, or source code.

## Build, deploy, and test

Use the existing Cloud Shell scripts, after reviewing `config/agents.prod.yaml`, `config/billing.prod.yaml`, Terraform project/region values, and the selected shared billing collection names:

```bash
bash scripts/cloudshell_build_middleware.sh
bash scripts/cloudshell_deploy_middleware.sh
```

The sample Terraform settings and Cloud Shell deployment script enable prepaid enforcement and batch settlement for testing. Confirm that the test wallet exists, is server-owned, has enough credit, and that shared collection names point to the intended billing service before running a test. The daily OpenAI cost job additionally needs its admin-key secret. These template edits do not modify or deploy any already-running Cloud Run service.

Run the test suite from this directory:

```bash
python -m pytest -q
```

The tests mock upstream HTTP and Firestore; passing them does not validate the supplied Agent ID, API key permissions, saved Agent's actual model, current OpenAI rates, organization Costs API permissions, or production wallet integration. Daily aggregate reconciliation records actual provider totals but does not adjust customer wallets until a defensible per-user allocation policy exists.

## Important lifecycle note

Archiving/deleting a local thread updates the middleware's thread lifecycle. The current persistence worker does not delete the corresponding OpenAI-hosted session. OpenAI session deletion/retention should be decided and implemented separately before promising remote data deletion to end users.
