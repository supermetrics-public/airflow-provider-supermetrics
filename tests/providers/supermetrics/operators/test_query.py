"""Tests for SupermetricsQueryOperator."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from supermetrics import AuthenticationError, NetworkError, ValidationError
from supermetrics._generated.supermetrics_api_client.types import UNSET

from airflow.providers.supermetrics.operators.query import SupermetricsQueryOperator


@pytest.fixture()
def mock_hook():
    with patch("airflow.providers.supermetrics.operators.query.SupermetricsHook") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_instance


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


def _make_operator(**kwargs):
    defaults = {
        "task_id": "test_query",
        "ds_id": "GAWA",
        "ds_accounts": ["123"],
        "fields": ["Date", "Sessions"],
        "query_start_date": "2024-01-01",
        "query_end_date": "2024-01-31",
    }
    defaults.update(kwargs)
    return SupermetricsQueryOperator(**defaults)


class TestSupermetricsQueryOperator:
    def test_successful_query_returns_list_of_dicts(self, mock_hook):
        """Query should return list[dict] from DataResponse rows."""
        response = _make_response(
            ["Date", "Sessions"],
            [["2024-01-01", "100"], ["2024-01-02", "200"]],
        )
        mock_hook.get_conn().queries.execute.return_value = response

        op = _make_operator()
        result = op.execute(context=MagicMock())

        assert result == [
            {"Date": "2024-01-01", "Sessions": "100"},
            {"Date": "2024-01-02", "Sessions": "200"},
        ]

    def test_none_result_returns_empty_list(self, mock_hook):
        """Query returning None should produce an empty list."""
        mock_hook.get_conn().queries.execute.return_value = None

        op = _make_operator()
        result = op.execute(context=MagicMock())

        assert result == []

    def test_empty_data_returns_empty_list(self, mock_hook):
        """Query with empty data should produce an empty list."""
        response = _make_response(["Date", "Sessions"], [])
        mock_hook.get_conn().queries.execute.return_value = response

        op = _make_operator()
        result = op.execute(context=MagicMock())

        assert result == []

    def test_authentication_error_raises_airflow_exception(self, mock_hook):
        """AuthenticationError should be wrapped in AirflowException."""
        mock_hook.get_conn().queries.execute.side_effect = AuthenticationError("invalid key")

        op = _make_operator()
        with pytest.raises(AirflowException, match="authentication failed"):
            op.execute(context=MagicMock())

    def test_validation_error_raises_airflow_exception(self, mock_hook):
        """ValidationError should be wrapped in AirflowException."""
        mock_hook.get_conn().queries.execute.side_effect = ValidationError("bad params")

        op = _make_operator()
        with pytest.raises(AirflowException, match="validation error"):
            op.execute(context=MagicMock())

    def test_network_error_raises_airflow_exception(self, mock_hook):
        """NetworkError should be wrapped in AirflowException."""
        mock_hook.get_conn().queries.execute.side_effect = NetworkError("connection timeout")

        op = _make_operator()
        with pytest.raises(AirflowException, match="network error"):
            op.execute(context=MagicMock())

    def test_template_fields_declared(self):
        """All required template fields must be declared."""
        expected = {
            "ds_id",
            "ds_accounts",
            "fields",
            "query_start_date",
            "query_end_date",
            "query_params",
        }
        assert set(SupermetricsQueryOperator.template_fields) == expected

    def test_query_params_passed_to_sdk(self, mock_hook):
        """Extra query_params should be forwarded to the SDK."""
        response = _make_response(["Date"], [["2024-01-01"]])
        mock_hook.get_conn().queries.execute.return_value = response

        op = _make_operator(query_params={"max_rows": 100})
        op.execute(context=MagicMock())

        mock_hook.get_conn().queries.execute.assert_called_once_with(
            ds_id="GAWA",
            ds_accounts=["123"],
            fields=["Date", "Sessions"],
            start_date="2024-01-01",  # SDK param name
            end_date="2024-01-31",  # SDK param name
            max_rows=100,
        )

    def test_fallback_to_requested_field_names(self, mock_hook):
        """When response metadata has no fields, use requested field names."""
        response = MagicMock()
        response.meta = UNSET
        response.data = [["2024-01-01", "100"]]

        mock_hook.get_conn().queries.execute.return_value = response

        op = _make_operator()
        result = op.execute(context=MagicMock())

        assert result == [{"Date": "2024-01-01", "Sessions": "100"}]
