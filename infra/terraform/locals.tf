locals {
  enabled_apis = toset([
    "artifactregistry.googleapis.com",
    "cloudscheduler.googleapis.com",
    "eventarc.googleapis.com",
    "firestore.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "pubsub.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
  ])

  gateway_env = {
    GOOGLE_CLOUD_PROJECT             = var.project_id
    GOOGLE_CLOUD_REGION              = var.region
    DEPLOYMENT_ENVIRONMENT            = var.deployment_environment
    AGENT_REGISTRY_PATH              = var.agent_registry_path
    AGENT_TURN_EVENTS_TOPIC          = var.pubsub_topic_name
    FIRESTORE_THREADS_COLLECTION     = var.firestore_threads_collection
    FIRESTORE_CUSTOMER_WALLETS_COLLECTION = var.firestore_customer_wallets_collection
    BILLING_ENFORCEMENT_ENABLED      = tostring(var.billing_enforcement_enabled)
    BILLING_MINIMUM_BALANCE_NANOS    = tostring(var.billing_minimum_balance_nanos)
    OPENAI_SESSION_SPEND_CONTROL_LIMIT_CENTS = tostring(var.openai_session_spend_control_limit_cents)
    PUBSUB_PUBLISH_TIMEOUT_SECONDS   = tostring(var.pubsub_publish_timeout_seconds)
    REQUIRE_FIREBASE_AUTH            = tostring(var.require_firebase_auth)
    ALLOWED_ORIGINS                  = join(",", var.allowed_origins)
    GATEWAY_LOG_LEVEL                = var.gateway_log_level
    GATEWAY_STREAM_DEBUG             = tostring(var.gateway_stream_debug)
    UPSTREAM_CONNECT_TIMEOUT_SECONDS = tostring(var.upstream_connect_timeout_seconds)
    UPSTREAM_READ_TIMEOUT_SECONDS    = tostring(var.upstream_read_timeout_seconds)
    UPSTREAM_TOTAL_TIMEOUT_SECONDS   = tostring(var.upstream_total_timeout_seconds)
  }

  worker_env = {
    GOOGLE_CLOUD_PROJECT                    = var.project_id
    DEPLOYMENT_ENVIRONMENT                   = var.deployment_environment
    WORKER_LOG_LEVEL                        = var.worker_log_level
    FIRESTORE_THREADS_COLLECTION            = var.firestore_threads_collection
    FIRESTORE_MESSAGES_SUBCOLLECTION        = var.firestore_messages_subcollection
    FIRESTORE_IDEMPOTENCY_COLLECTION        = var.firestore_idempotency_collection
    FIRESTORE_BILLING_LEDGER_COLLECTION      = var.firestore_billing_ledger_collection
    FIRESTORE_CUSTOMER_WALLETS_COLLECTION    = var.firestore_customer_wallets_collection
    FIRESTORE_BILLING_RESERVATIONS_COLLECTION = var.firestore_billing_reservations_collection
    FIRESTORE_WALLET_TRANSACTIONS_COLLECTION = var.firestore_wallet_transactions_collection
    FIRESTORE_CUSTOMER_BILLING_PERIODS_COLLECTION = var.firestore_customer_billing_periods_collection
    FIRESTORE_OPENAI_COSTS_COLLECTION = var.firestore_openai_costs_collection
    BILLING_BATCH_SETTLEMENT_ENABLED = tostring(var.billing_batch_settlement_enabled)
    BILLING_BATCH_MAX_AGE_SECONDS = tostring(var.billing_batch_max_age_seconds)
    BILLING_BATCH_SESSION_IDLE_SECONDS = tostring(var.billing_batch_session_idle_seconds)
    BILLING_BATCH_MAX_TURNS = tostring(var.billing_batch_max_turns)
    BILLING_BATCH_MAX_EXPOSURE_NANOS = tostring(var.billing_batch_max_exposure_nanos)
    OPENAI_COST_RECONCILIATION_ENABLED = tostring(var.openai_cost_reconciliation_enabled)
    OPENAI_COST_RECONCILIATION_LOOKBACK_DAYS = tostring(var.openai_cost_reconciliation_lookback_days)
    OPENAI_PROJECT_ID = var.openai_project_id
    MONTHLY_SERVICE_FEE_NANOS                = tostring(var.monthly_service_fee_nanos)
    BILLING_RECONCILIATION_BATCH_SIZE        = tostring(var.billing_reconciliation_batch_size)
    BILLING_USAGE_RECONCILIATION_LOOKUP_BATCH_SIZE = tostring(var.billing_usage_reconciliation_lookup_batch_size)
    BILLING_USAGE_RECONCILIATION_CONCURRENCY = tostring(var.billing_usage_reconciliation_concurrency)
    BILLING_USAGE_RECONCILIATION_INITIAL_BACKOFF_SECONDS = tostring(var.billing_usage_reconciliation_initial_backoff_seconds)
    BILLING_USAGE_RECONCILIATION_MAX_BACKOFF_SECONDS = tostring(var.billing_usage_reconciliation_max_backoff_seconds)
    BILLING_USAGE_RECONCILIATION_RETENTION_SECONDS = tostring(var.billing_usage_reconciliation_retention_seconds)
    WORKER_REQUIRE_EVENTARC_AUTH            = tostring(var.worker_require_eventarc_auth)
    WORKER_EVENTARC_ALLOWED_SERVICE_ACCOUNT = var.worker_eventarc_allowed_service_account != "" ? var.worker_eventarc_allowed_service_account : google_service_account.eventarc.email
    WORKER_EVENTARC_AUDIENCE                = var.worker_eventarc_audience
  }

  gateway_secret_env = var.openai_api_key_secret_id == "" ? {} : {
    OPENAI_API_KEY = {
      secret  = var.openai_api_key_secret_id
      version = var.openai_api_key_secret_version
    }
  }

  worker_secret_env = merge(
    var.openai_api_key_secret_id == "" ? {} : {
      OPENAI_API_KEY = {
        secret  = var.openai_api_key_secret_id
        version = var.openai_api_key_secret_version
      }
    },
    !var.openai_cost_reconciliation_enabled || var.openai_admin_api_key_secret_id == "" ? {} : {
      OPENAI_ADMIN_API_KEY = {
        secret  = var.openai_admin_api_key_secret_id
        version = var.openai_admin_api_key_secret_version
      }
    }
  )
}
