resource "google_cloud_scheduler_job" "billing_reconciliation" {
  count = var.billing_reconciliation_enabled ? 1 : 0

  name             = "${var.worker_service_name}-billing-reconciliation"
  description      = "Settles due usage batches, retries provider usage independently with backoff, and marks expired recovery for manual review."
  region           = var.region
  schedule         = var.billing_reconciliation_schedule
  time_zone        = "UTC"
  attempt_deadline = "120s"

  retry_config {
    retry_count = 3
  }

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.worker.uri}/internal/billing/reconcile"
    body        = base64encode("{}")

    headers = {
      "Content-Type" = "application/json"
    }

    oidc_token {
      service_account_email = google_service_account.billing_reconciler.email
      audience              = google_cloud_run_v2_service.worker.uri
    }
  }

  depends_on = [
    google_project_service.apis,
    google_cloud_run_v2_service.worker,
    google_cloud_run_v2_service_iam_member.worker_billing_reconciler_invoker,
    google_firestore_index.billing_reservations_status_expires_at,
    google_firestore_index.billing_reservations_late_reconciliation,
    google_firestore_index.billing_reservations_usage_deadline,
    google_firestore_index.billing_reservations_completed_settlement_retry,
  ]
}

resource "google_cloud_scheduler_job" "openai_cost_reconciliation" {
  count = var.openai_cost_reconciliation_enabled ? 1 : 0

  name             = "${var.worker_service_name}-openai-cost-reconciliation"
  description      = "Imports daily OpenAI organization cost aggregates and compares them with local token-rate estimates; does not allocate aggregate provider costs to customer wallets."
  region           = var.region
  schedule         = var.openai_cost_reconciliation_schedule
  time_zone        = "UTC"
  attempt_deadline = "300s"

  lifecycle {
    precondition {
      condition     = trimspace(var.openai_admin_api_key_secret_id) != ""
      error_message = "Set openai_admin_api_key_secret_id to an existing organization admin key secret before enabling daily cost reconciliation."
    }
  }

  retry_config {
    retry_count = 3
  }

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.worker.uri}/internal/billing/reconcile-openai-costs"
    body        = base64encode("{}")

    headers = {
      "Content-Type" = "application/json"
    }

    oidc_token {
      service_account_email = google_service_account.billing_reconciler.email
      audience              = google_cloud_run_v2_service.worker.uri
    }
  }

  depends_on = [
    google_project_service.apis,
    google_cloud_run_v2_service.worker,
    google_cloud_run_v2_service_iam_member.worker_billing_reconciler_invoker,
    google_secret_manager_secret_iam_member.worker_openai_admin_api_key_accessor,
  ]
}

resource "google_firestore_index" "billing_reservations_status_expires_at" {
  project    = var.project_id
  database   = "(default)"
  collection = var.firestore_billing_reservations_collection

  fields {
    field_path = "status"
    order      = "ASCENDING"
  }

  fields {
    field_path = "expires_at"
    order      = "ASCENDING"
  }
}

resource "google_firestore_index" "billing_reservations_late_reconciliation" {
  project    = var.project_id
  database   = "(default)"
  collection = var.firestore_billing_reservations_collection

  fields {
    field_path = "usage_reconciliation_status"
    order      = "ASCENDING"
  }

  fields {
    field_path = "usage_reconciliation_next_attempt_at"
    order      = "ASCENDING"
  }
}

resource "google_firestore_index" "billing_reservations_usage_deadline" {
  project    = var.project_id
  database   = "(default)"
  collection = var.firestore_billing_reservations_collection

  fields {
    field_path = "usage_reconciliation_status"
    order      = "ASCENDING"
  }

  fields {
    field_path = "usage_reconciliation_deadline_at"
    order      = "ASCENDING"
  }
}

resource "google_firestore_index" "billing_reservations_completed_settlement_retry" {
  project    = var.project_id
  database   = "(default)"
  collection = var.firestore_billing_reservations_collection

  fields {
    field_path = "status"
    order      = "ASCENDING"
  }

  fields {
    field_path = "billing_settlement_next_attempt_at"
    order      = "ASCENDING"
  }
}

resource "google_firestore_index" "billing_reservations_status_batch_created_at" {
  project    = var.project_id
  database   = "(default)"
  collection = var.firestore_billing_reservations_collection

  fields {
    field_path = "status"
    order      = "ASCENDING"
  }

  fields {
    field_path = "billing_batch_created_at"
    order      = "ASCENDING"
  }
}
