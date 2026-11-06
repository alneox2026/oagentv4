"""Regression checks for deployment-safe Cloud Monitoring alert definitions."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_worker_retry_alert_matches_logs_without_custom_metric_race() -> None:
    monitoring = (ROOT / "infra" / "terraform" / "monitoring.tf").read_text(
        encoding="utf-8"
    )
    import_script = (ROOT / "scripts" / "import_existing_resources.sh").read_text(
        encoding="utf-8"
    )

    worker_alert = monitoring.split(
        'resource "google_monitoring_alert_policy" "worker_retryable_failures" {',
        maxsplit=1,
    )[1]

    assert "condition_matched_log" in worker_alert
    assert 'resource.labels.service_name="${var.worker_service_name}"' in worker_alert
    assert 'jsonPayload.event="worker_event_persist_retryable_failure"' in worker_alert
    assert 'jsonPayload.event="worker_thread_delete_retryable_failure"' in worker_alert
    assert 'period = "300s"' in worker_alert
    assert "condition_threshold" not in worker_alert
    assert 'resource "google_logging_metric" "worker_retryable_failures"' in monitoring
    assert 'google_logging_metric.worker_retryable_failures' not in worker_alert
    assert "logging.googleapis.com/user/" not in worker_alert
    assert 'google_logging_metric.worker_retryable_failures' in import_script
