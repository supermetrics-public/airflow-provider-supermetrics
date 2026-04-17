"""Example DAG: Daily Cross-Channel Marketing Report to GCS.

Based on Scenario 1: pulls GA4 and Facebook Ads data in parallel,
streams results directly to GCS as NDJSON files.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag
from airflow.providers.supermetrics.operators.transfer.gcs import (
    SupermetricsQueryToGCSOperator,
)


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["marketing", "supermetrics"],
)
def example_supermetrics_to_gcs():
    """Pull GA4 + Facebook Ads data to GCS daily."""

    ga4_to_gcs = SupermetricsQueryToGCSOperator(
        task_id="ga4_to_gcs",
        ds_id="GAWA",
        ds_accounts=["{{ var.value.ga4_property_id }}"],
        fields=["Date", "Sessions", "Users", "Conversions", "BounceRate"],
        query_start_date="{{ ds }}",
        query_end_date="{{ ds }}",
        gcs_bucket="marketing-data-lake",
        gcs_object="raw/ga4/{{ ds }}/sessions.ndjson",
        file_format="ndjson",
    )

    fb_ads_to_gcs = SupermetricsQueryToGCSOperator(
        task_id="fb_ads_to_gcs",
        ds_id="FA",
        ds_accounts=["{{ var.value.fb_ad_account_id }}"],
        fields=["Date", "CampaignName", "Impressions", "Clicks", "Spend", "Conversions"],
        query_start_date="{{ ds }}",
        query_end_date="{{ ds }}",
        query_params={"filter": "Spend > 0"},
        gcs_bucket="marketing-data-lake",
        gcs_object="raw/facebook_ads/{{ ds }}/campaigns.ndjson",
        file_format="ndjson",
    )

    # Both tasks run in parallel
    [ga4_to_gcs, fb_ads_to_gcs]


example_supermetrics_to_gcs()
