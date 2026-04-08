# Scenario 1: Daily Cross-Channel Marketing Report

## Problem Statement

A marketing team needs a unified daily report combining Google Analytics 4 and Facebook Ads data,
loaded into their data warehouse via GCS. The DAG should:

- Pull from two data sources in parallel (GA4 + Facebook Ads)
- Write each result to a date-partitioned GCS path as NDJSON
- Run on a daily schedule with idempotent, backfill-friendly date parameters
- Avoid materializing large result sets in Airflow worker memory

## Provider Components Used

| Component | Architecture Ref | Purpose |
|-----------|-----------------|---------|
| `SupermetricsHook` | [arch:4.3] | Authenticates via API key stored in Airflow Connection |
| `SupermetricsQueryToGCSOperator` | [arch:4.7] | Executes query and streams results directly to GCS |

## Airflow Features Leveraged

- **Parallel task execution**: Two independent GCS operators run simultaneously
- **Jinja2 templating**: `start_date` and `end_date` use `{{ ds }}` for the logical execution date
- **`template_fields`**: `ds_id`, `ds_accounts`, `fields`, `start_date`, `end_date`, `gcs_object`
- **Optional extras**: Requires `airflow-provider-supermetrics[gcs]` for `GCSHook` dependency
- **Catchup & backfill**: `catchup=True` replays historical dates; each run is idempotent

## DAG Code

```python
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.supermetrics.operators.transfer.gcs import (
    SupermetricsQueryToGCSOperator,
)


@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["marketing", "supermetrics"],
)
def daily_marketing_report():
    """Pull GA4 + Facebook Ads data to GCS daily."""

    ga4_to_gcs = SupermetricsQueryToGCSOperator(
        task_id="ga4_to_gcs",
        ds_id="GAWA",
        ds_accounts=["{{ var.value.ga4_property_id }}"],
        fields=["Date", "Sessions", "Users", "Conversions", "BounceRate"],
        start_date="{{ ds }}",
        end_date="{{ ds }}",
        gcs_bucket="marketing-data-lake",
        gcs_object="raw/ga4/{{ ds }}/sessions.ndjson",
        file_format="ndjson",
    )

    fb_ads_to_gcs = SupermetricsQueryToGCSOperator(
        task_id="fb_ads_to_gcs",
        ds_id="FA",
        ds_accounts=["{{ var.value.fb_ad_account_id }}"],
        fields=[
            "Date",
            "CampaignName",
            "Impressions",
            "Clicks",
            "Spend",
            "Conversions",
        ],
        start_date="{{ ds }}",
        end_date="{{ ds }}",
        query_params={"filter": "Spend > 0"},
        gcs_bucket="marketing-data-lake",
        gcs_object="raw/facebook_ads/{{ ds }}/campaigns.ndjson",
        file_format="ndjson",
    )

    # Both tasks run in parallel — no dependency between them
    [ga4_to_gcs, fb_ads_to_gcs]


daily_marketing_report()
```

## Key Design Points

- **No worker memory pressure**: data streams directly from the Supermetrics API to GCS via
  `SupermetricsQueryToGCSOperator`, never materializing the full result set in memory.
- **Independent sources**: if Facebook Ads fails, GA4 data still lands. Each task retries
  independently.
- **Date partitioning**: GCS object path includes `{{ ds }}`, making each run idempotent.
  Re-running a past date overwrites only that partition.
- **Downstream loading**: A separate DAG (or downstream tasks) can use `GCSToBigQueryOperator`
  to load the NDJSON files into BigQuery.
- **Connection setup**: requires two Airflow connections — `supermetrics_default` (API key) and
  `google_cloud_default` (GCP service account).
