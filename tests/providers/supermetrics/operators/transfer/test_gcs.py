"""Tests for SupermetricsQueryToGCSOperator."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException


def _make_response(field_ids: list[str], data: list[list[str]]):
    """Build a mock DataResponse with fields metadata and data rows."""
    fields_items = []
    for fid in field_ids:
        item = MagicMock()
        item.field_id = fid
        fields_items.append(item)

    query = MagicMock()
    query.fields = fields_items

    meta = MagicMock()
    meta.query = query

    response = MagicMock()
    response.meta = meta
    response.data = data
    return response


def _make_gcs_operator(**kwargs):
    from airflow.providers.supermetrics.operators.transfer.gcs import (
        SupermetricsQueryToGCSOperator,
    )

    defaults = {
        "task_id": "test_gcs_transfer",
        "ds_id": "GAWA",
        "ds_accounts": ["123"],
        "fields": ["Date", "Sessions"],
        "query_start_date": "2024-01-01",
        "query_end_date": "2024-01-31",
        "gcs_bucket": "my-bucket",
        "gcs_object": "data/output.ndjson",
    }
    defaults.update(kwargs)
    return SupermetricsQueryToGCSOperator(**defaults)


@pytest.fixture(autouse=True)
def mock_gcs_module():
    """Make GCSHook importable in tests without installing google provider."""
    mock_gcs_hook_cls = MagicMock()
    mock_module = MagicMock()
    mock_module.GCSHook = mock_gcs_hook_cls
    with patch.dict(
        "sys.modules",
        {
            "airflow.providers.google": MagicMock(),
            "airflow.providers.google.cloud": MagicMock(),
            "airflow.providers.google.cloud.hooks": MagicMock(),
            "airflow.providers.google.cloud.hooks.gcs": mock_module,
        },
    ):
        yield mock_gcs_hook_cls


class TestSupermetricsQueryToGCSOperator:
    @patch("airflow.providers.supermetrics.operators.transfer.gcs.SupermetricsHook")
    def test_writes_ndjson_to_gcs(self, mock_hook_cls, mock_gcs_module):
        """GCS operator should write NDJSON to mocked GCS."""
        mock_hook = MagicMock()
        mock_hook_cls.return_value = mock_hook
        response = _make_response(
            ["Date", "Sessions"],
            [["2024-01-01", "100"], ["2024-01-02", "200"]],
        )
        mock_hook.get_conn().queries.execute.return_value = response

        op = _make_gcs_operator()
        result = op.execute(context=MagicMock())

        assert result == "gs://my-bucket/data/output.ndjson"

        # Verify GCSHook was instantiated and upload called
        mock_gcs_instance = mock_gcs_module.return_value
        mock_gcs_instance.upload.assert_called_once()
        call_kwargs = mock_gcs_instance.upload.call_args
        assert call_kwargs.kwargs["bucket_name"] == "my-bucket"
        assert call_kwargs.kwargs["object_name"] == "data/output.ndjson"

        # Verify NDJSON content
        uploaded_data = call_kwargs.kwargs["data"]
        lines = uploaded_data.strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0]) == {"Date": "2024-01-01", "Sessions": "100"}
        assert json.loads(lines[1]) == {"Date": "2024-01-02", "Sessions": "200"}

    @patch("airflow.providers.supermetrics.operators.transfer.gcs.SupermetricsHook")
    def test_writes_csv_to_gcs(self, mock_hook_cls, mock_gcs_module):
        """GCS operator should write CSV to mocked GCS."""
        mock_hook = MagicMock()
        mock_hook_cls.return_value = mock_hook
        response = _make_response(
            ["Date", "Sessions"],
            [["2024-01-01", "100"], ["2024-01-02", "200"]],
        )
        mock_hook.get_conn().queries.execute.return_value = response

        op = _make_gcs_operator(
            file_format="csv",
            gcs_object="data/output.csv",
        )
        result = op.execute(context=MagicMock())

        assert result == "gs://my-bucket/data/output.csv"

        mock_gcs_instance = mock_gcs_module.return_value
        mock_gcs_instance.upload.assert_called_once()
        call_kwargs = mock_gcs_instance.upload.call_args
        uploaded_data = call_kwargs.kwargs["data"]

        lines = uploaded_data.strip().splitlines()
        assert len(lines) == 3  # header + 2 data rows
        assert lines[0] == "Date,Sessions"
        assert lines[1] == "2024-01-01,100"
        assert lines[2] == "2024-01-02,200"

    @patch("airflow.providers.supermetrics.operators.transfer.gcs.SupermetricsHook")
    def test_raises_without_google_provider(self, mock_hook_cls):
        """Should raise helpful error when google provider is not installed."""
        op = _make_gcs_operator()
        with patch.dict("sys.modules", {"airflow.providers.google.cloud.hooks.gcs": None}):
            with patch.dict("sys.modules", {"airflow.providers.google.cloud.hooks": None}):
                with patch.dict("sys.modules", {"airflow.providers.google.cloud": None}):
                    with patch.dict("sys.modules", {"airflow.providers.google": None}):
                        with pytest.raises(
                            AirflowException,
                            match="apache-airflow-providers-google",
                        ):
                            op.execute(context=MagicMock())

    def test_template_fields_declared(self):
        """All required template fields must be declared."""
        from airflow.providers.supermetrics.operators.transfer.gcs import (
            SupermetricsQueryToGCSOperator,
        )

        expected = {
            "ds_id",
            "ds_accounts",
            "fields",
            "query_start_date",
            "query_end_date",
            "query_params",
            "gcs_bucket",
            "gcs_object",
        }
        assert set(SupermetricsQueryToGCSOperator.template_fields) == expected

    @patch("airflow.providers.supermetrics.operators.transfer.gcs.SupermetricsHook")
    def test_none_result_writes_empty_file(self, mock_hook_cls, mock_gcs_module):
        """Query returning None should write an empty file."""
        mock_hook = MagicMock()
        mock_hook_cls.return_value = mock_hook
        mock_hook.get_conn().queries.execute.return_value = None

        op = _make_gcs_operator()
        result = op.execute(context=MagicMock())

        assert result == "gs://my-bucket/data/output.ndjson"
        mock_gcs_instance = mock_gcs_module.return_value
        call_kwargs = mock_gcs_instance.upload.call_args
        assert call_kwargs.kwargs["data"] == ""
