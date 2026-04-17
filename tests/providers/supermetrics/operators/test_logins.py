"""Tests for Supermetrics login operators."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from supermetrics import AuthenticationError, NetworkError, ValidationError

from airflow.providers.supermetrics.operators.logins import (
    SupermetricsCreateLoginLinkOperator,
    SupermetricsGetLoginOperator,
    SupermetricsListLoginsOperator,
)


@pytest.fixture()
def mock_hook():
    with patch("airflow.providers.supermetrics.operators.logins.SupermetricsHook") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_instance


@pytest.fixture()
def mock_context():
    """Return a mock Airflow context with a task instance."""
    ti = MagicMock()
    return {"ti": ti}


class TestSupermetricsCreateLoginLinkOperator:
    def test_returns_expected_xcom_dict(self, mock_hook, mock_context):
        """Execute should return a dict with login_id and link_url."""
        login_link = MagicMock()
        login_link.login_id = "lg_abc123"
        login_link.login_url = "https://example.com/login/lg_abc123"
        mock_hook.get_conn().login_links.create.return_value = login_link

        op = SupermetricsCreateLoginLinkOperator(
            task_id="create_login_link",
            ds_id="GA4",
            description="Test login",
        )
        result = op.execute(context=mock_context)

        assert result == {
            "login_id": "lg_abc123",
            "link_url": "https://example.com/login/lg_abc123",
        }

    def test_pushes_separate_xcom_values(self, mock_hook, mock_context):
        """Execute should push login_id and link_url as separate XCom values."""
        login_link = MagicMock()
        login_link.login_id = "lg_abc123"
        login_link.login_url = "https://example.com/login/lg_abc123"
        mock_hook.get_conn().login_links.create.return_value = login_link

        op = SupermetricsCreateLoginLinkOperator(
            task_id="create_login_link",
            ds_id="GA4",
        )
        op.execute(context=mock_context)

        ti = mock_context["ti"]
        ti.xcom_push.assert_any_call(key="login_id", value="lg_abc123")
        ti.xcom_push.assert_any_call(key="link_url", value="https://example.com/login/lg_abc123")

    def test_calls_sdk_with_correct_args(self, mock_hook, mock_context):
        """Create should forward ds_id and description to the SDK."""
        login_link = MagicMock()
        login_link.login_id = "lg_x"
        login_link.login_url = "https://example.com/login/lg_x"
        mock_hook.get_conn().login_links.create.return_value = login_link

        op = SupermetricsCreateLoginLinkOperator(
            task_id="create_login_link",
            ds_id="GA4",
            description="My description",
        )
        op.execute(context=mock_context)

        mock_hook.get_conn().login_links.create.assert_called_once_with(
            ds_id="GA4",
            description="My description",
        )

    def test_template_fields_declared(self):
        """Template fields must include ds_id and description."""
        assert set(SupermetricsCreateLoginLinkOperator.template_fields) == {
            "ds_id",
            "description",
        }

    def test_authentication_error_raises_airflow_exception(self, mock_hook, mock_context):
        """AuthenticationError should be wrapped in AirflowException."""
        mock_hook.get_conn().login_links.create.side_effect = AuthenticationError("bad key")

        op = SupermetricsCreateLoginLinkOperator(
            task_id="create_login_link",
            ds_id="GA4",
        )
        with pytest.raises(AirflowException, match="authentication failed"):
            op.execute(context=mock_context)

    def test_validation_error_raises_airflow_exception(self, mock_hook, mock_context):
        """ValidationError should be wrapped in AirflowException."""
        mock_hook.get_conn().login_links.create.side_effect = ValidationError("invalid ds_id")

        op = SupermetricsCreateLoginLinkOperator(
            task_id="create_login_link",
            ds_id="INVALID",
        )
        with pytest.raises(AirflowException, match="validation error"):
            op.execute(context=mock_context)

    def test_network_error_raises_airflow_exception(self, mock_hook, mock_context):
        """NetworkError should be wrapped in AirflowException."""
        mock_hook.get_conn().login_links.create.side_effect = NetworkError("timeout")

        op = SupermetricsCreateLoginLinkOperator(
            task_id="create_login_link",
            ds_id="GA4",
        )
        with pytest.raises(AirflowException, match="network error"):
            op.execute(context=mock_context)


class TestSupermetricsListLoginsOperator:
    def test_serializes_logins_with_to_dict(self, mock_hook):
        """List should call to_dict() on each login and return list of dicts."""
        login1 = MagicMock()
        login1.to_dict.return_value = {"login_id": "lg_1", "ds_id": "GA4"}
        login2 = MagicMock()
        login2.to_dict.return_value = {"login_id": "lg_2", "ds_id": "FB"}
        mock_hook.get_conn().logins.list.return_value = [login1, login2]

        op = SupermetricsListLoginsOperator(task_id="list_logins")
        result = op.execute(context=MagicMock())

        assert result == [
            {"login_id": "lg_1", "ds_id": "GA4"},
            {"login_id": "lg_2", "ds_id": "FB"},
        ]
        login1.to_dict.assert_called_once()
        login2.to_dict.assert_called_once()

    def test_empty_list_returns_empty(self, mock_hook):
        """List with no logins should return an empty list."""
        mock_hook.get_conn().logins.list.return_value = []

        op = SupermetricsListLoginsOperator(task_id="list_logins")
        result = op.execute(context=MagicMock())

        assert result == []

    def test_template_fields_empty(self):
        """List operator has no template fields."""
        assert SupermetricsListLoginsOperator.template_fields == ()

    def test_authentication_error_raises_airflow_exception(self, mock_hook):
        """AuthenticationError should be wrapped in AirflowException."""
        mock_hook.get_conn().logins.list.side_effect = AuthenticationError("bad key")

        op = SupermetricsListLoginsOperator(task_id="list_logins")
        with pytest.raises(AirflowException, match="authentication failed"):
            op.execute(context=MagicMock())


class TestSupermetricsGetLoginOperator:
    def test_returns_login_dict(self, mock_hook):
        """Get should call to_dict() on the result and return a dict."""
        login = MagicMock()
        login.to_dict.return_value = {
            "login_id": "lg_abc",
            "ds_id": "GA4",
            "status": "active",
        }
        mock_hook.get_conn().logins.get.return_value = login

        op = SupermetricsGetLoginOperator(
            task_id="get_login",
            login_id="lg_abc",
        )
        result = op.execute(context=MagicMock())

        assert result == {
            "login_id": "lg_abc",
            "ds_id": "GA4",
            "status": "active",
        }
        login.to_dict.assert_called_once()

    def test_calls_sdk_with_login_id(self, mock_hook):
        """Get should pass login_id to the SDK."""
        login = MagicMock()
        login.to_dict.return_value = {"login_id": "lg_xyz"}
        mock_hook.get_conn().logins.get.return_value = login

        op = SupermetricsGetLoginOperator(
            task_id="get_login",
            login_id="lg_xyz",
        )
        op.execute(context=MagicMock())

        mock_hook.get_conn().logins.get.assert_called_once_with(login_id="lg_xyz")

    def test_template_fields_declared(self):
        """Template fields must include login_id."""
        assert set(SupermetricsGetLoginOperator.template_fields) == {"login_id"}

    def test_authentication_error_raises_airflow_exception(self, mock_hook):
        """AuthenticationError should be wrapped in AirflowException."""
        mock_hook.get_conn().logins.get.side_effect = AuthenticationError("bad key")

        op = SupermetricsGetLoginOperator(
            task_id="get_login",
            login_id="lg_abc",
        )
        with pytest.raises(AirflowException, match="authentication failed"):
            op.execute(context=MagicMock())
