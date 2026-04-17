"""Example DAG: Weekly Competitive Benchmarking with SupermetricsQueryOperator.

Based on Scenario 3: pulls ad performance data from Google Ads and LinkedIn Ads,
computes derived KPIs, and could push to Slack or another downstream system.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.supermetrics.operators.query import SupermetricsQueryOperator


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["analytics", "supermetrics"],
)
def example_supermetrics_query():
    """Weekly cross-platform ad performance benchmarking."""

    google_ads = SupermetricsQueryOperator(
        task_id="google_ads_weekly",
        ds_id="AW",
        ds_accounts=["{{ var.value.google_ads_customer_id }}"],
        fields=["CampaignName", "Impressions", "Clicks", "Cost", "Conversions"],
        query_start_date="{{ macros.ds_add(ds, -7) }}",
        query_end_date="{{ ds }}",
        query_params={"max_rows": 10000},
    )

    linkedin_ads = SupermetricsQueryOperator(
        task_id="linkedin_ads_weekly",
        ds_id="LI",
        ds_accounts=["{{ var.value.linkedin_ad_account_id }}"],
        fields=["CampaignName", "Impressions", "Clicks", "Spend", "Conversions"],
        query_start_date="{{ macros.ds_add(ds, -7) }}",
        query_end_date="{{ ds }}",
    )

    @task
    def compute_benchmarks(google_data: list[dict], linkedin_data: list[dict]) -> dict:
        def avg_cost_per_conversion(rows: list[dict]) -> float:
            total_cost = sum(float(r.get("Cost", r.get("Spend", 0))) for r in rows)
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

    compute_benchmarks(google_ads.output, linkedin_ads.output)


example_supermetrics_query()
