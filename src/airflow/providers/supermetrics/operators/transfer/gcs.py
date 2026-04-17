"""Supermetrics query to GCS transfer operator."""

from __future__ import annotations

import csv
import io
import json
from typing import Any, Literal, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from airflow.providers.supermetrics.hooks.supermetrics import SupermetricsHook
from supermetrics import AuthenticationError, NetworkError, ValidationError
from supermetrics._generated.supermetrics_api_client.types import UNSET


class SupermetricsQueryToGCSOperator(BaseOperator):
    """Execute a Supermetrics query and stream results to Google Cloud Storage."""

    template_fields: Sequence[str] = (
        "ds_id",
        "ds_accounts",
        "fields",
        "query_start_date",
        "query_end_date",
        "query_params",
        "gcs_bucket",
        "gcs_object",
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
        gcs_bucket: str,
        gcs_object: str,
        gcp_conn_id: str = "google_cloud_default",
        file_format: Literal["ndjson", "csv"] = "ndjson",
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
        self.gcs_bucket = gcs_bucket
        self.gcs_object = gcs_object
        self.gcp_conn_id = gcp_conn_id
        self.file_format = file_format
        self.query_params = query_params or {}
        self.supermetrics_conn_id = supermetrics_conn_id

    def execute(self, context: Any) -> str:
        try:
            from airflow.providers.google.cloud.hooks.gcs import GCSHook
        except ImportError:
            raise AirflowException(
                "The 'apache-airflow-providers-google' package is required for "
                "SupermetricsQueryToGCSOperator. Install it with: "
                "pip install 'airflow-provider-supermetrics[gcs]'"
            )

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
            self.log.warning("Query returned no result, writing empty file")
            data: list[list[str]] = []
            field_names: list[str] = list(self.fields)
        else:
            data = result.data if result.data is not UNSET else []
            # Extract field names from response metadata
            field_names = []
            if (
                result.meta is not UNSET
                and result.meta is not None
                and result.meta.query is not UNSET
                and result.meta.query is not None
                and result.meta.query.fields is not UNSET
                and result.meta.query.fields is not None
            ):
                field_names = [
                    f.field_id for f in result.meta.query.fields if f.field_id is not UNSET
                ]
            if not field_names:
                field_names = list(self.fields)

        # Serialize to buffer
        if self.file_format == "ndjson":
            buf = io.StringIO()
            for row in data:
                row_dict = dict(zip(field_names, row))
                buf.write(json.dumps(row_dict) + "\n")
        else:  # csv
            buf = io.StringIO(newline="")
            writer = csv.writer(buf)
            writer.writerow(field_names)
            writer.writerows(data)

        # Upload to GCS
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        gcs_hook.upload(
            bucket_name=self.gcs_bucket,
            object_name=self.gcs_object,
            data=buf.getvalue(),
        )

        uri = f"gs://{self.gcs_bucket}/{self.gcs_object}"
        self.log.info("Uploaded %d rows to %s", len(data), uri)
        return uri
