"""Supermetrics query operator for Apache Airflow."""

from __future__ import annotations

from typing import Any, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from airflow.providers.supermetrics.hooks.supermetrics import SupermetricsHook
from supermetrics import AuthenticationError, NetworkError, ValidationError


class SupermetricsQueryOperator(BaseOperator):
    """Execute a Supermetrics query and push results to XCom.

    Queries the Supermetrics API for analytics data and returns
    the result as a ``list[dict]`` to XCom.
    """

    template_fields: Sequence[str] = (
        "ds_id",
        "ds_accounts",
        "fields",
        "query_start_date",
        "query_end_date",
        "query_params",
    )
    ui_color = "#00b4d8"

    def __init__(
        self,
        *,
        ds_id: str,
        ds_accounts: list[str],
        fields: list[str],
        query_start_date: str,
        query_end_date: str,
        query_params: dict[str, Any] | None = None,
        supermetrics_conn_id: str = SupermetricsHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.ds_id = ds_id
        self.ds_accounts = ds_accounts
        self.fields = fields
        self.query_start_date = query_start_date
        self.query_end_date = query_end_date
        self.query_params = query_params or {}
        self.supermetrics_conn_id = supermetrics_conn_id

    def execute(self, context: Any) -> list[dict[str, str]]:
        hook = SupermetricsHook(supermetrics_conn_id=self.supermetrics_conn_id)
        client = hook.get_conn()

        try:
            result = client.queries.execute(
                ds_id=self.ds_id,
                ds_accounts=self.ds_accounts,
                fields=self.fields,
                start_date=self.query_start_date,
                end_date=self.query_end_date,
                **self.query_params,
            )
        except AuthenticationError as e:
            raise AirflowException(f"Supermetrics authentication failed: {e}") from e
        except ValidationError as e:
            raise AirflowException(f"Supermetrics validation error: {e}") from e
        except NetworkError as e:
            raise AirflowException(f"Supermetrics network error: {e}") from e

        if result is None:
            self.log.warning("Query returned no result")
            return []

        # Convert DataResponse to list[dict] using field names from metadata
        from supermetrics._generated.supermetrics_api_client.types import UNSET

        data = result.data if result.data is not UNSET else []
        if not data:
            return []

        # Extract field names from response metadata
        field_names: list[str] = []
        if (
            result.meta is not UNSET
            and result.meta is not None
            and result.meta.query is not UNSET
            and result.meta.query is not None
            and result.meta.query.fields is not UNSET
            and result.meta.query.fields is not None
        ):
            field_names = [f.field_id for f in result.meta.query.fields if f.field_id is not UNSET]

        if not field_names:
            # Fallback: use the requested field names
            field_names = list(self.fields)

        rows: list[dict[str, str]] = [dict(zip(field_names, row)) for row in data]
        self.log.info("Query returned %d rows", len(rows))
        return rows
