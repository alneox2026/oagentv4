variable "project_id" {
  description = "Google Cloud project ID."
  type        = string
  default     = "ceo-dev123"
}

variable "region" {
  description = "Primary deployment region."
  type        = string
  default     = "us-central1"
}

variable "gateway_service_name" {
  description = "Cloud Run service name for the public agent gateway."
  type        = string
  default     = "oagentv4-gateway"
}

variable "worker_service_name" {
  description = "Cloud Run service name for the persistence worker."
  type        = string
  default     = "oagentv4-worker"
}

variable "gateway_image" {
  description = "Container image URI for the gateway service."
  type        = string
}

variable "worker_image" {
  description = "Container image URI for the persistence worker."
  type        = string
}

variable "gateway_service_account_name" {
  description = "Service account name for the gateway service."
  type        = string
  default     = "oagentv4gatewaysa"
}

variable "worker_service_account_name" {
  description = "Service account name for the worker service."
  type        = string
  default     = "oagentv4workersa"
}

variable "eventarc_service_account_name" {
  description = "Service account name for the Eventarc trigger."
  type        = string
  default     = "oagentv4eventarcsa"
}

variable "billing_reconciler_service_account_name" {
  description = "Service account name for the Cloud Scheduler billing reconciler."
  type        = string
  default     = "oagentv4reconcilersa"
}


variable "pubsub_topic_name" {
  description = "Topic for completed turn events."
  type        = string
  default     = "agent-turn-events-oagentv4"
}

variable "pubsub_publish_timeout_seconds" {
  description = "Pub/Sub publish timeout for the gateway."
  type        = number
  default     = 30
}

variable "require_firebase_auth" {
  description = "Whether the gateway requires Firebase bearer tokens."
  type        = bool
  default     = true
}

variable "allowed_origins" {
  description = "Allowed CORS origins for the public gateway."
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for origin in var.allowed_origins : trimspace(origin) != "*"
    ])
    error_message = "Wildcard CORS origins are not allowed. Provide explicit web origins instead."
  }
}

variable "agent_registry_path" {
  description = "Absolute path inside the container to the production agent registry."
  type        = string
  default     = "/app/config/agents.prod.yaml"
}

variable "gateway_log_level" {
  description = "Gateway application log level."
  type        = string
  default     = "INFO"
}

variable "gateway_stream_debug" {
  description = "Whether to emit one-shot upstream stream diagnostics from the gateway."
  type        = bool
  default     = false
}

variable "worker_log_level" {
  description = "Worker application log level."
  type        = string
  default     = "INFO"
}

variable "upstream_connect_timeout_seconds" {
  description = "Gateway connect timeout to the OpenAI Agents API."
  type        = number
  default     = 10
}

variable "upstream_read_timeout_seconds" {
  description = "Maximum idle time between upstream stream chunks. This is not a total-turn deadline; use upstream_total_timeout_seconds for the absolute wall-clock limit."
  type        = number
  default     = 570
}

variable "upstream_total_timeout_seconds" {
  description = "Absolute wall-clock limit for an upstream agent turn, measured from gateway request start. The 570s default is 30s below the 600s Cloud Run request deadline; post-acceptance timeouts publish partial usage for worker reconciliation and retain the billing marker."
  type        = number
  default     = 570
}

# The Cloud Shell deployment script namespaces middleware collections by
# FIRESTORE_NAMESPACE. Shared billing collections below keep their configured
# values so every middleware can point at the Central Billing records.
variable "firestore_threads_collection" {
  description = "Top-level Firestore collection for chat threads. Cloud Shell honors this custom value; template *_v3 defaults become stack-scoped."
  type        = string
  default     = "agent_threads_oagentv4"
}

variable "firestore_messages_subcollection" {
  description = "Subcollection name for messages under each thread document."
  type        = string
  default     = "messages_oagentv4"
}


variable "firestore_idempotency_collection" {
  description = "Top-level Firestore collection for processed event ids."
  type        = string
  default     = "processed_events_oagentv4"
}

variable "firestore_billing_ledger_collection" {
  description = "Top-level immutable Firestore collection for completed-turn billing ledgers."
  type        = string
  default     = "agent_billing_ledger_oagentv4"
}

variable "firestore_customer_wallets_collection" {
  description = "Top-level Firestore collection for server-owned prepaid customer wallets."
  type        = string
  default     = "customer_wallets_billingv4"
}

variable "firestore_billing_reservations_collection" {
  description = "Top-level Firestore collection for per-turn billing authorization and reconciliation markers."
  type        = string
  default     = "billing_reservations_oagentv4"
}

variable "firestore_openai_costs_collection" {
  description = "Stack-scoped collection for daily OpenAI Costs API aggregates and variance evidence."
  type        = string
  default     = "openai_provider_costs_oagentv4"
}

variable "firestore_wallet_transactions_collection" {
  description = "Top-level immutable Firestore collection for customer wallet transactions."
  type        = string
  default     = "wallet_transactions_billingv4"
}

variable "firestore_customer_billing_periods_collection" {
  description = "Top-level Firestore collection for customer monthly billing aggregates."
  type        = string
  default     = "customer_billing_periods_billingv4"
}

variable "billing_enforcement_enabled" {
  description = "Whether the gateway must verify a minimum prepaid wallet balance before every agent request."
  type        = bool
  default     = true
}

variable "billing_minimum_balance_nanos" {
  description = "Minimum available USD wallet balance required before a request; 50000000 is USD 0.05. This is an eligibility threshold only and does not reserve or deduct funds."
  type        = number
  default     = 50000000

  validation {
    condition     = var.billing_minimum_balance_nanos > 0
    error_message = "billing_minimum_balance_nanos must be greater than zero."
  }
}

variable "monthly_service_fee_nanos" {
  description = "Monthly service fee recorded for each customer billing period; 5000000000 is USD 5.00."
  type        = number
  default     = 5000000000
}

variable "billing_reconciliation_batch_size" {
  description = "Maximum billing marker entries processed by one scheduled reconciliation run."
  type        = number
  default     = 100
}

variable "billing_usage_reconciliation_lookup_batch_size" {
  description = "Maximum provider-usage lookups submitted by one reconciliation run."
  type        = number
  default     = 32

  validation {
    condition     = var.billing_usage_reconciliation_lookup_batch_size > 0 && var.billing_usage_reconciliation_lookup_batch_size <= 100
    error_message = "billing_usage_reconciliation_lookup_batch_size must be between 1 and 100."
  }
}

variable "billing_usage_reconciliation_concurrency" {
  description = "Maximum concurrent read-only provider-usage lookups per reconciliation run."
  type        = number
  default     = 8

  validation {
    condition     = var.billing_usage_reconciliation_concurrency > 0 && var.billing_usage_reconciliation_concurrency <= 32
    error_message = "billing_usage_reconciliation_concurrency must be between 1 and 32."
  }
}

variable "billing_usage_reconciliation_initial_backoff_seconds" {
  description = "Initial delay between post-response OpenAI usage recovery attempts."
  type        = number
  default     = 300

  validation {
    condition     = var.billing_usage_reconciliation_initial_backoff_seconds > 0
    error_message = "The usage reconciliation initial backoff must be greater than zero."
  }
}

variable "billing_usage_reconciliation_max_backoff_seconds" {
  description = "Maximum delay between post-response OpenAI usage recovery attempts."
  type        = number
  default     = 21600

  validation {
    condition     = var.billing_usage_reconciliation_max_backoff_seconds >= var.billing_usage_reconciliation_initial_backoff_seconds
    error_message = "The maximum usage reconciliation backoff must be at least the initial backoff."
  }
}

variable "billing_usage_reconciliation_retention_seconds" {
  description = "Maximum time to automatically look up missing OpenAI usage before requiring manual billing review."
  type        = number
  default     = 604800

  validation {
    condition     = var.billing_usage_reconciliation_retention_seconds > 0
    error_message = "The usage reconciliation retention window must be greater than zero."
  }
}

variable "billing_reconciliation_schedule" {
  description = "Five-field UTC cron schedule for retrying missing provider usage and reconciling billing markers."
  type        = string
  default     = "*/5 * * * *"

  validation {
    condition = can(regex(
      "^[-A-Za-z0-9*/?,]+([[:space:]]+[-A-Za-z0-9*/?,]+){4}$",
      trimspace(var.billing_reconciliation_schedule)
    ))
    error_message = "billing_reconciliation_schedule must be a five-field Cloud Scheduler cron expression, for example \"*/5 * * * *\"."
  }
}

variable "billing_reconciliation_enabled" {
  description = "Whether Terraform should schedule usage recovery and billing reconciliation on the worker service. Required when prepaid billing enforcement is enabled."
  type        = bool
  default     = true
}

variable "billing_batch_settlement_enabled" {
  description = "Whether priced turns are grouped into wallet debits instead of being debited individually."
  type        = bool
  default     = true
}

variable "billing_batch_max_age_seconds" {
  description = "Maximum age of a pending priced-turn batch before it is considered due; the default is five minutes."
  type        = number
  default     = 300
  validation {
    condition     = var.billing_batch_max_age_seconds > 0
    error_message = "billing_batch_max_age_seconds must be greater than zero."
  }
}

variable "billing_batch_session_idle_seconds" {
  description = "Idle time after the latest completed session turn before closing its pending usage batch."
  type        = number
  default     = 300
  validation {
    condition     = var.billing_batch_session_idle_seconds > 0
    error_message = "billing_batch_session_idle_seconds must be greater than zero."
  }
}

variable "billing_batch_max_turns" {
  description = "Close a session batch after this many priced turns, even before its age limit."
  type        = number
  default     = 20
  validation {
    condition     = var.billing_batch_max_turns > 0 && var.billing_batch_max_turns <= 100
    error_message = "billing_batch_max_turns must be between 1 and 100."
  }
}

variable "billing_batch_max_exposure_nanos" {
  description = "Close a session batch when provisional token-rate charges reach this USD-nanos exposure; 1000000000 is USD 1.00."
  type        = number
  default     = 1000000000
  validation {
    condition     = var.billing_batch_max_exposure_nanos > 0
    error_message = "billing_batch_max_exposure_nanos must be greater than zero."
  }
}

variable "openai_cost_reconciliation_enabled" {
  description = "Whether to run a daily import of OpenAI organization Costs API aggregates. This records provider totals and variances; it does not allocate aggregate costs to Firebase users."
  type        = bool
  default     = false
}

variable "openai_cost_reconciliation_schedule" {
  description = "Five-field UTC cron schedule for daily OpenAI provider cost import."
  type        = string
  default     = "17 2 * * *"
  validation {
    condition = can(regex(
      "^[-A-Za-z0-9*/?,]+([[:space:]]+[-A-Za-z0-9*/?,]+){4}$",
      trimspace(var.openai_cost_reconciliation_schedule)
    ))
    error_message = "openai_cost_reconciliation_schedule must be a five-field Cloud Scheduler cron expression, for example \"17 2 * * *\"."
  }
}

variable "openai_cost_reconciliation_lookback_days" {
  description = "Number of completed UTC daily buckets to re-fetch on each provider-cost reconciliation."
  type        = number
  default     = 7
  validation {
    condition     = var.openai_cost_reconciliation_lookback_days >= 1 && var.openai_cost_reconciliation_lookback_days <= 180
    error_message = "openai_cost_reconciliation_lookback_days must be between 1 and 180."
  }
}

variable "openai_project_id" {
  description = "Optional dedicated OpenAI project ID to filter Costs API reconciliation. Use a project dedicated to this middleware stack for meaningful variance analysis."
  type        = string
  default     = ""
}

variable "gateway_min_instances" {
  description = "Minimum number of gateway instances."
  type        = number
  default     = 0
}


variable "gateway_max_instances" {
  description = "Maximum number of gateway instances."
  type        = number
  default     = 20
}

variable "gateway_concurrency" {
  description = "Maximum concurrent requests per gateway instance."
  type        = number
  default     = 16
}

variable "gateway_cpu" {
  description = "Gateway CPU limit."
  type        = string
  default     = "1"
}

variable "gateway_memory" {
  description = "Gateway memory limit."
  type        = string
  default     = "1Gi"
}

variable "gateway_timeout" {
  description = "Gateway request timeout."
  type        = string
  default     = "600s"
}

variable "openai_api_key_secret_id" {
  description = "Existing Secret Manager secret ID containing the OpenAI API key used by the gateway and post-response turn-usage recovery in the worker."
  type        = string
  default     = "openai-api-key"
}

variable "openai_api_key_secret_version" {
  description = "Pinned numeric version of the OpenAI API key Secret Manager secret."
  type        = string
  default     = "1"
}

variable "openai_admin_api_key_secret_id" {
  description = "Optional existing Secret Manager secret containing an OpenAI organization admin key for the Costs API. Worker-only; never grant this key to the gateway."
  type        = string
  default     = "openai-admin-api-key"
}

variable "openai_admin_api_key_secret_version" {
  description = "Pinned numeric version of the OpenAI organization admin API key secret."
  type        = string
  default     = "1"
}

variable "openai_session_spend_control_limit_cents" {
  description = "Optional best-effort OpenAI session spend_control limit in USD cents; zero leaves provider session limits disabled. This is not the customer billing ledger."
  type        = number
  default     = 0
  validation {
    condition     = var.openai_session_spend_control_limit_cents >= 0
    error_message = "openai_session_spend_control_limit_cents cannot be negative."
  }
}

variable "cloud_run_execution_environment" {
  description = "Pinned Cloud Run execution environment for both services."
  type        = string
  default     = "EXECUTION_ENVIRONMENT_GEN2"
}

variable "cloud_run_request_based_billing" {
  description = "Whether Cloud Run CPU should be allocated only during requests."
  type        = bool
  default     = true
}

variable "worker_min_instances" {
  description = "Minimum number of worker instances."
  type        = number
  default     = 0
}

variable "worker_max_instances" {
  description = "Maximum number of worker instances."
  type        = number
  default     = 20
}

variable "worker_concurrency" {
  description = "Maximum concurrent requests per worker instance."
  type        = number
  default     = 8
}

variable "worker_cpu" {
  description = "Worker CPU limit."
  type        = string
  default     = "1"
}

variable "worker_memory" {
  description = "Worker memory limit."
  type        = string
  default     = "512Mi"
}

variable "worker_timeout" {
  description = "Worker request timeout."
  type        = string
  default     = "120s"
}

variable "deployment_environment" {
  description = "Deployment stage used for production-only safety checks in the Cloud Shell deploy script."
  type        = string
  default     = "development"

  validation {
    condition     = contains(["development", "staging", "production"], var.deployment_environment)
    error_message = "deployment_environment must be development, staging, or production."
  }
}

variable "worker_require_eventarc_auth" {
  description = "Whether the worker verifies Eventarc OIDC tokens in application code."
  type        = bool
  default     = false
}

variable "worker_eventarc_allowed_service_account" {
  description = "Expected Eventarc service account email for worker push requests. Defaults to the Terraform-managed Eventarc service account when empty."
  type        = string
  default     = ""
}

variable "worker_eventarc_audience" {
  description = "Expected OIDC audience for worker push requests when application-level Eventarc auth is enabled."
  type        = string
  default     = ""
}

variable "alert_notification_channels" {
  description = "Cloud Monitoring notification channel resource names for alert policies."
  type        = list(string)
  default     = []
}

variable "gateway_p95_latency_threshold_ms" {
  description = "Gateway p95 latency threshold in milliseconds for launch alerting."
  type        = number
  default     = 8000
}
