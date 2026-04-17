"""Example DAG: DWH Backfill with Async Monitoring.

Based on Scenario 2: creates a backfill for a DWH transfer, monitors it
asynchronously using a deferrable operator, and cancels on failure.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.supermetrics.operators.backfill import (
    SupermetricsCancelBackfillOperator,
    SupermetricsCreateBackfillOperator,
)


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["backfill", "supermetrics"],
)
def example_supermetrics_backfill():
    """Backfill historical data into the DWH with async monitoring."""

    create_backfill = SupermetricsCreateBackfillOperator(
        task_id="create_backfill",
        team_id="{{ var.value.supermetrics_team_id }}",
        transfer_id="{{ var.value.linkedin_transfer_id }}",
        range_start="{{ macros.ds_add(ds, -90) }}",
        range_end="{{ ds }}",
        deferrable=True,
        poll_interval=60,
    )

    @task
    def notify_success(**context):
        event = context["ti"].xcom_pull(task_ids="create_backfill")
        print(f"Backfill completed successfully: {event}")

    cancel_backfill = SupermetricsCancelBackfillOperator(
        task_id="cancel_backfill",
        team_id="{{ var.value.supermetrics_team_id }}",
        transfer_id="{{ var.value.linkedin_transfer_id }}",
        trigger_rule="one_failed",
    )

    success = notify_success()
    create_backfill >> [success, cancel_backfill]


example_supermetrics_backfill()
