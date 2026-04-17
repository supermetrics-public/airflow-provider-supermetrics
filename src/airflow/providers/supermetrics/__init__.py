"""Apache Airflow provider for Supermetrics."""

from __future__ import annotations


def get_provider_info() -> dict:
    return {
        "package-name": "airflow-provider-supermetrics",
        "name": "Supermetrics",
        "description": "Apache Airflow provider for Supermetrics analytics platform.",
        "versions": ["0.1.0"],
        "connection-types": [
            {
                "connection-type": "supermetrics",
                "hook-class-name": (
                    "airflow.providers.supermetrics.hooks.supermetrics.SupermetricsHook"
                ),
            }
        ],
    }
