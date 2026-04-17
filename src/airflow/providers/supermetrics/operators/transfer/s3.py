"""Supermetrics query to S3 transfer operator."""

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


class SupermetricsQueryToS3Operator(BaseOperator):
    """Execute a Supermetrics query and stream results to Amazon S3."""

    template_fields: Sequence[str] = (
        "ds_id",
        "ds_accounts",
        "fields",
        "query_start_date",
        "query_end_date",
        "query_params",
        "s3_bucket",
        "s3_key",
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
        s3_bucket: str,
        s3_key: str,
        aws_conn_id: str = "aws_default",
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
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_id = aws_conn_id
        self.file_format = file_format
        self.query_params = query_params or {}
        self.supermetrics_conn_id = supermetrics_conn_id

    def execute(self, context: Any) -> str:
        try:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        except ImportError:
            raise AirflowException(
                "The 'apache-airflow-providers-amazon' package is required for "
                "SupermetricsQueryToS3Operator. Install it with: "
                "pip install 'airflow-provider-supermetrics[s3]'"
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

        # Upload to S3
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        s3_hook.load_string(
            string_data=buf.getvalue(),
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True,
        )

        uri = f"s3://{self.s3_bucket}/{self.s3_key}"
        self.log.info("Uploaded %d rows to %s", len(data), uri)
        return uri
