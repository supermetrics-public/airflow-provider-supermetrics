"""Tests for SupermetricsHook."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.supermetrics.hooks.supermetrics import SupermetricsHook


@pytest.fixture()
def mock_connection():
    conn = MagicMock()
    conn.password = "api-key"
    return conn


@pytest.fixture()
def hook(mock_connection):
    with patch.object(SupermetricsHook, "get_connection", return_value=mock_connection):
        yield SupermetricsHook(supermetrics_conn_id="supermetrics_default")


class TestSupermetricsHook:
    @patch("airflow.providers.supermetrics.hooks.supermetrics.SupermetricsClient")
    def test_get_conn_caches_client(self, mock_client_cls, hook):
        """get_conn() called twice should construct the client only once."""
        mock_client_cls.return_value = MagicMock()

        client1 = hook.get_conn()
        client2 = hook.get_conn()

        assert client1 is client2
        mock_client_cls.assert_called_once_with(api_key="test-api-key")

    @patch("airflow.providers.supermetrics.hooks.supermetrics.SupermetricsAsyncClient")
    def test_get_async_client_returns_new_instance(self, mock_async_cls, hook):
        """get_async_client() should return a new instance each call."""
        mock_async_cls.side_effect = [MagicMock(), MagicMock()]

        client1 = hook.get_async_client()
        client2 = hook.get_async_client()

        assert client1 is not client2
        assert mock_async_cls.call_count == 2

    @patch("airflow.providers.supermetrics.hooks.supermetrics.SupermetricsClient")
    def test_api_key_extracted_from_password(self, mock_client_cls, hook):
        """API key should come from the connection's password field."""
        hook.get_conn()
        mock_client_cls.assert_called_once_with(api_key="test-api-key")

    def test_missing_api_key_raises_error(self):
        """Hook should raise ValueError when password field is empty."""
        conn = MagicMock()
        conn.password = None
        with patch.object(SupermetricsHook, "get_connection", return_value=conn):
            hook = SupermetricsHook()
            with pytest.raises(ValueError, match="No API key found"):
                hook.get_conn()

    def test_get_ui_field_behaviour(self):
        """Verify UI field behaviour configuration."""
        behaviour = SupermetricsHook.get_ui_field_behaviour()
        assert "schema" in behaviour["hidden_fields"]
        assert "host" in behaviour["hidden_fields"]
        assert "login" in behaviour["hidden_fields"]
        assert "port" in behaviour["hidden_fields"]
        assert "extra" in behaviour["hidden_fields"]
        assert behaviour["relabeling"]["password"] == "API Key"

    def test_get_connection_form_widgets(self):
        """Verify connection form widgets returns a dict."""
        widgets = SupermetricsHook.get_connection_form_widgets()
        assert isinstance(widgets, dict)

    def test_default_conn_name(self):
        """Verify the default connection name."""
        assert SupermetricsHook.default_conn_name == "supermetrics_default"

    def test_conn_type(self):
        """Verify the connection type."""
        assert SupermetricsHook.conn_type == "supermetrics"
