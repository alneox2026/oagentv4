# For a normal deployment, use scripts/cloudshell_build_middleware.sh and
# scripts/cloudshell_deploy_middleware.sh instead of this manual example.
# Those scripts derive stack-specific images, resource names, state, and
# runtime collections. Shared billing collection names come from Terraform
# configuration (variables.tf defaults or terraform.tfvars).
project_id      = "ceo-dev123"
region          = "us-central1"
gateway_image   = "us-central1-docker.pkg.dev/YOUR_PROJECT_ID/ceosystem/YOUR_STACK-gateway:YOUR_TAG"
worker_image    = "us-central1-docker.pkg.dev/YOUR_PROJECT_ID/ceosystem/YOUR_STACK-persistence-worker:YOUR_TAG"

gateway_service_name                    = "oagentv4-gateway"
worker_service_name                     = "oagentv4-worker"
gateway_service_account_name            = "oagentv4gatewaysa"
worker_service_account_name             = "oagentv4workersa"
eventarc_service_account_name           = "oagentv4eventarcsa"
billing_reconciler_service_account_name = "oagentv4reconcilersa"
pubsub_topic_name                       = "agent-turn-events-oagentv4"
allowed_origins                         = ["https://ceoappdev.flutterflow.app"]
gateway_stream_debug                    = false

# OpenAI API key for saved Agents API agents and post-response usage recovery.
# Grant the key Agents API read/write and Responses inference permissions.
# Create the secret if needed, then enter the key at a hidden prompt (do not put
# it in terraform.tfvars or as a command argument):
#   gcloud secrets create openai-api-key --replication-policy=automatic
#   read -s -p "OpenAI API key: " OPENAI_API_KEY
#   printf '%s' "$OPENAI_API_KEY" | gcloud secrets versions add openai-api-key --data-file=-
#   unset OPENAI_API_KEY
openai_api_key_secret_id      = "openai-api-key"
openai_api_key_secret_version = "1"

# Optional supplemental provider-side per-session spend cap (USD cents). This
# is best-effort and is not the per-customer billing source of truth.
openai_session_spend_control_limit_cents = 0

# Daily OpenAI actual-cost import requires a separate organization admin key.
# Create an existing Secret Manager secret named below and add the admin key as
# a secret version before enabling the job. Keep this key worker-only.
#   gcloud secrets create openai-admin-api-key --replication-policy=automatic
#   read -s -p "OpenAI admin API key: " OPENAI_ADMIN_API_KEY
#   printf '%s' "$OPENAI_ADMIN_API_KEY" | gcloud secrets versions add openai-admin-api-key --data-file=-
#   unset OPENAI_ADMIN_API_KEY
openai_admin_api_key_secret_id      = "openai-admin-api-key"
openai_admin_api_key_secret_version = "1"
openai_cost_reconciliation_enabled  = true
openai_cost_reconciliation_schedule = "17 2 * * *"
openai_cost_reconciliation_lookback_days = 7
# Prefer a dedicated OpenAI project for this middleware. Leave blank only if
# you intentionally want organization-wide aggregate data (not user-attributed).
openai_project_id = ""

# Prepaid wallet minimum-balance authorization and billing reconciliation.
# The gateway requires at least $0.05 available before each turn. This is an
# eligibility check only: the gateway does not place a monetary hold or debit.
# The worker prices usage and applies wallet debits asynchronously in batches.
# Provider-usage lookup has its own exponential-backoff lifecycle, independent
# of normal settlement or expiry of the worker lifecycle marker, and runs for up to
# seven days.
# Unresolved usage requires manual billing review but does not block new turns
# while the current available balance remains at or above the minimum; known
# late usage is collected when wallet funds are available.
billing_enforcement_enabled    = true
billing_minimum_balance_nanos  = 50000000
billing_reconciliation_enabled = true
billing_reconciliation_schedule = "*/5 * * * *"
billing_usage_reconciliation_lookup_batch_size = 32
billing_usage_reconciliation_concurrency = 8
billing_usage_reconciliation_initial_backoff_seconds = 300
billing_usage_reconciliation_max_backoff_seconds = 21600
billing_usage_reconciliation_retention_seconds = 604800
billing_batch_settlement_enabled = true
billing_batch_max_age_seconds = 300
billing_batch_session_idle_seconds = 300
billing_batch_max_turns = 20
billing_batch_max_exposure_nanos = 1000000000
deployment_environment         = "development"

# Optional: add Cloud Monitoring notification channel resource names for launch alerts.
alert_notification_channels = []
