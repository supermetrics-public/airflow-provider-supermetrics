"""Example DAG: Query Supermetrics data to S3.

Pulls analytics data and streams results to Amazon S3 as NDJSON.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag
from airflow.providers.supermetrics.operators.transfer.s3 import (
    SupermetricsQueryToS3Operator,
)


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["analytics", "supermetrics"],
)
def example_supermetrics_to_s3():
    """Pull Google Analytics data to S3 daily."""

    SupermetricsQueryToS3Operator(
        task_id="ga4_to_s3",
        ds_id="GAWA",
        ds_accounts=["{{ var.value.ga4_property_id }}"],
        fields=["Date", "Sessions", "Users", "Conversions"],
        query_start_date="{{ ds }}",
        query_end_date="{{ ds }}",
        s3_bucket="analytics-data-lake",
        s3_key="raw/ga4/{{ ds }}/sessions.ndjson",
        file_format="ndjson",
    )


example_supermetrics_to_s3()
