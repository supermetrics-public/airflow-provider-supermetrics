"""Supermetrics hook for Apache Airflow."""

from __future__ import annotations

from typing import Any

from airflow.hooks.base import BaseHook

from supermetrics import SupermetricsAsyncClient, SupermetricsClient


class SupermetricsHook(BaseHook):
    """Hook for the Supermetrics API.

    Authenticates using an API key stored in the **Password** field
    of the Airflow connection.
    """

    conn_name_attr = "supermetrics_conn_id"
    default_conn_name = "supermetrics_default"
    conn_type = "supermetrics"
    hook_name = "Supermetrics"

    def __init__(self, supermetrics_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.supermetrics_conn_id = supermetrics_conn_id
        self._client: SupermetricsClient | None = None

    def _get_api_key(self) -> str:
        conn = self.get_connection(self.supermetrics_conn_id)
        if not conn.password:
            raise ValueError(
                f"No API key found in connection '{self.supermetrics_conn_id}'. "
                "Set the API key in the Password field."
            )
        return conn.password

    def get_conn(self) -> SupermetricsClient:
        """Return a cached synchronous Supermetrics client."""
        if self._client is None:
            self._client = SupermetricsClient(api_key=self._get_api_key())
        return self._client

    def get_async_client(self) -> SupermetricsAsyncClient:
        """Return a new async Supermetrics client.

        Caller must manage the lifecycle with ``async with``.
        """
        return SupermetricsAsyncClient(api_key=self._get_api_key())

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection form widgets for the Airflow UI."""
        return {}

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Customize connection form: hide irrelevant fields, relabel password."""
        return {
            "hidden_fields": ["schema", "host", "login", "port", "extra"],
            "relabeling": {"password": "API Key"},
            "placeholders": {"password": "Enter your Supermetrics API key"},
        }
