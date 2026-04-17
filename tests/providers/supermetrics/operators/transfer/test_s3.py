"""Tests for SupermetricsQueryToS3Operator."""

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


def _make_s3_operator(**kwargs):
    from airflow.providers.supermetrics.operators.transfer.s3 import (
        SupermetricsQueryToS3Operator,
    )

    defaults = {
        "task_id": "test_s3_transfer",
        "ds_id": "GAWA",
        "ds_accounts": ["123"],
        "fields": ["Date", "Sessions"],
        "query_start_date": "2024-01-01",
        "query_end_date": "2024-01-31",
        "s3_bucket": "my-bucket",
        "s3_key": "data/output.ndjson",
    }
    defaults.update(kwargs)
    return SupermetricsQueryToS3Operator(**defaults)


@pytest.fixture(autouse=True)
def mock_s3_module():
    """Make S3Hook importable in tests without installing amazon provider."""
    mock_s3_hook_cls = MagicMock()
    mock_module = MagicMock()
    mock_module.S3Hook = mock_s3_hook_cls
    with patch.dict(
        "sys.modules",
        {
            "airflow.providers.amazon": MagicMock(),
            "airflow.providers.amazon.aws": MagicMock(),
            "airflow.providers.amazon.aws.hooks": MagicMock(),
            "airflow.providers.amazon.aws.hooks.s3": mock_module,
        },
    ):
        yield mock_s3_hook_cls


class TestSupermetricsQueryToS3Operator:
    @patch("airflow.providers.supermetrics.operators.transfer.s3.SupermetricsHook")
    def test_writes_ndjson_to_s3(self, mock_hook_cls, mock_s3_module):
        """S3 operator should write NDJSON to mocked S3."""
        mock_hook = MagicMock()
        mock_hook_cls.return_value = mock_hook
        response = _make_response(
            ["Date", "Sessions"],
            [["2024-01-01", "100"], ["2024-01-02", "200"]],
        )
        mock_hook.get_conn().queries.execute.return_value = response

        op = _make_s3_operator()
        result = op.execute(context=MagicMock())

        assert result == "s3://my-bucket/data/output.ndjson"

        # Verify S3Hook was instantiated and load_string called
        mock_s3_instance = mock_s3_module.return_value
        mock_s3_instance.load_string.assert_called_once()
        call_kwargs = mock_s3_instance.load_string.call_args
        assert call_kwargs.kwargs["bucket_name"] == "my-bucket"
        assert call_kwargs.kwargs["key"] == "data/output.ndjson"
        assert call_kwargs.kwargs["replace"] is True

        # Verify NDJSON content
        uploaded_data = call_kwargs.kwargs["string_data"]
        lines = uploaded_data.strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0]) == {"Date": "2024-01-01", "Sessions": "100"}
        assert json.loads(lines[1]) == {"Date": "2024-01-02", "Sessions": "200"}

    @patch("airflow.providers.supermetrics.operators.transfer.s3.SupermetricsHook")
    def test_raises_without_amazon_provider(self, mock_hook_cls):
        """Should raise helpful error when amazon provider is not installed."""
        op = _make_s3_operator()
        with patch.dict("sys.modules", {"airflow.providers.amazon.aws.hooks.s3": None}):
            with patch.dict("sys.modules", {"airflow.providers.amazon.aws.hooks": None}):
                with patch.dict("sys.modules", {"airflow.providers.amazon.aws": None}):
                    with patch.dict("sys.modules", {"airflow.providers.amazon": None}):
                        with pytest.raises(
                            AirflowException,
                            match="apache-airflow-providers-amazon",
                        ):
                            op.execute(context=MagicMock())

    def test_template_fields_declared(self):
        """All required template fields must be declared."""
        from airflow.providers.supermetrics.operators.transfer.s3 import (
            SupermetricsQueryToS3Operator,
        )

        expected = {
            "ds_id",
            "ds_accounts",
            "fields",
            "query_start_date",
            "query_end_date",
            "query_params",
            "s3_bucket",
            "s3_key",
        }
        assert set(SupermetricsQueryToS3Operator.template_fields) == expected

    @patch("airflow.providers.supermetrics.operators.transfer.s3.SupermetricsHook")
    def test_none_result_writes_empty_file(self, mock_hook_cls, mock_s3_module):
        """Query returning None should write an empty file."""
        mock_hook = MagicMock()
        mock_hook_cls.return_value = mock_hook
        mock_hook.get_conn().queries.execute.return_value = None

        op = _make_s3_operator()
        result = op.execute(context=MagicMock())

        assert result == "s3://my-bucket/data/output.ndjson"
        mock_s3_instance = mock_s3_module.return_value
        call_kwargs = mock_s3_instance.load_string.call_args
        assert call_kwargs.kwargs["string_data"] == ""
