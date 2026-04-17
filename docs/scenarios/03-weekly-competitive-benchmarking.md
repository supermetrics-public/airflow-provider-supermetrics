# Scenario 3: Weekly Competitive Benchmarking

## Problem Statement

A growth team runs a weekly analysis comparing ad performance across Google Ads and LinkedIn Ads.
The pipeline must:

- Query last week's performance data from multiple ad platforms via Supermetrics
- Enrich the data with Python logic (calculate derived KPIs like CPA, CTR)
- Post a formatted summary to a Slack channel every Monday morning

## Provider Components Used

| Component | Architecture Ref | Purpose |
|-----------|-----------------|---------|
| `SupermetricsHook` | [arch:4.3] | Connection management |
| `SupermetricsQueryOperator` | [arch:4.4] | Pulls query results into XCom as `list[dict]` |

## Airflow Features Leveraged

- **XCom integration**: query results pushed as `list[dict]` (Pydantic `model_dump()`),
  consumed by downstream `@task` functions
- **TaskFlow API** (`@task` decorator): Python function enriches data without a custom operator
- **Jinja2 templating**: `start_date` uses `{{ macros.ds_add(ds, -7) }}` for rolling 7-day window
- **Weekly schedule**: `schedule="0 8 * * 1"` runs every Monday at 8am
- **`template_fields`**: all query parameters are templatable

## DAG Code

```python
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.supermetrics.operators.query import SupermetricsQueryOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


@dag(
    schedule="0 8 * * 1",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["analytics", "supermetrics", "slack"],
)
def weekly_ad_benchmark():
    """Weekly cross-platform ad performance benchmarking."""

    # Pull last week's Google Ads data
    google_ads = SupermetricsQueryOperator(
        task_id="google_ads_weekly",
        ds_id="AW",
        ds_accounts=["{{ var.value.google_ads_customer_id }}"],
        fields=["CampaignName", "Impressions", "Clicks", "Cost", "Conversions"],
        query_start_date="{{ macros.ds_add(ds, -7) }}",
        query_end_date="{{ ds }}",
        query_params={"max_rows": 10000},
    )

    # Pull last week's LinkedIn Ads data
    linkedin_ads = SupermetricsQueryOperator(
        task_id="linkedin_ads_weekly",
        ds_id="LI",
        ds_accounts=["{{ var.value.linkedin_ad_account_id }}"],
        fields=["CampaignName", "Impressions", "Clicks", "Spend", "Conversions"],
        query_start_date="{{ macros.ds_add(ds, -7) }}",
        query_end_date="{{ ds }}",
    )

    # Enrich data with derived KPIs
    @task
    def compute_benchmarks(google_data: list[dict], linkedin_data: list[dict]) -> dict:
        def avg_cost_per_conversion(rows: list[dict]) -> float:
            total_cost = sum(
                float(r.get("Cost", r.get("Spend", 0))) for r in rows
            )
            total_conv = sum(float(r.get("Conversions", 0)) for r in rows) or 1
            return round(total_cost / total_conv, 2)

        def click_through_rate(rows: list[dict]) -> float:
            total_clicks = sum(float(r.get("Clicks", 0)) for r in rows)
            total_impressions = sum(float(r.get("Impressions", 0)) for r in rows) or 1
            return round(total_clicks / total_impressions * 100, 2)

        return {
            "google_ads_cpa": avg_cost_per_conversion(google_data),
            "google_ads_ctr": click_through_rate(google_data),
            "linkedin_ads_cpa": avg_cost_per_conversion(linkedin_data),
            "linkedin_ads_ctr": click_through_rate(linkedin_data),
            "google_campaigns": len(google_data),
            "linkedin_campaigns": len(linkedin_data),
        }

    benchmarks = compute_benchmarks(google_ads.output, linkedin_ads.output)

    # Post summary to Slack
    SlackWebhookOperator(
        task_id="post_to_slack",
        slack_webhook_conn_id="slack_marketing",
        message=(
            "*Weekly Ad Benchmark Report*\n"
            "---\n"
            "*Google Ads*: CPA ${{ ti.xcom_pull(task_ids='compute_benchmarks')['google_ads_cpa'] }}"
            " | CTR {{ ti.xcom_pull(task_ids='compute_benchmarks')['google_ads_ctr'] }}%\n"
            "*LinkedIn Ads*: CPA ${{ ti.xcom_pull(task_ids='compute_benchmarks')['linkedin_ads_cpa'] }}"
            " | CTR {{ ti.xcom_pull(task_ids='compute_benchmarks')['linkedin_ads_ctr'] }}%\n"
            "---\n"
            "Campaigns analyzed: "
            "{{ ti.xcom_pull(task_ids='compute_benchmarks')['google_campaigns'] }} Google, "
            "{{ ti.xcom_pull(task_ids='compute_benchmarks')['linkedin_campaigns'] }} LinkedIn"
        ),
    )


weekly_ad_benchmark()
```

## Key Design Points

- **XCom is appropriate here**: weekly aggregates produce small result sets (hundreds of rows,
  not millions). For large datasets, use Scenario 1's `SupermetricsQueryToGCSOperator` instead.
- **TaskFlow `@task`**: the idiomatic Airflow 2.x way to do Python processing. The operator
  handles all SDK interaction; downstream tasks see plain `list[dict]`.
- **Pydantic serialization**: `SupermetricsQueryOperator` calls `result.model_dump()` on the
  SDK's Pydantic response, producing JSON-safe dicts that flow through XCom without issues.
- **Parallel queries**: Google Ads and LinkedIn Ads queries execute simultaneously, both feeding
  into `compute_benchmarks` via TaskFlow's automatic XCom resolution.
- **Extensible**: adding a third data source (e.g., Facebook Ads) is just another
  `SupermetricsQueryOperator` task and one more parameter to `compute_benchmarks`.
