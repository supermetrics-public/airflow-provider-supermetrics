"""Tests for Supermetrics backfill operators."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from supermetrics import AuthenticationError, NetworkError, ValidationError

from airflow.providers.supermetrics.operators.backfill import (
    SupermetricsCancelBackfillOperator,
    SupermetricsCreateBackfillOperator,
)


@pytest.fixture()
def mock_hook():
    with patch("airflow.providers.supermetrics.operators.backfill.SupermetricsHook") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_instance


def _make_context() -> dict:
    """Build a minimal Airflow context with a mock TaskInstance."""
    ti = MagicMock()
    return {"ti": ti}


class TestSupermetricsCreateBackfillOperator:
    def test_create_backfill_pushes_id_to_xcom(self, mock_hook):
        """Create operator should push backfill_id to XCom."""
        mock_backfill = MagicMock()
        mock_backfill.transfer_backfill_id = 42
        mock_hook.get_conn().backfills.create.return_value = mock_backfill

        op = SupermetricsCreateBackfillOperator(
            task_id="create_backfill",
            team_id=1,
            transfer_id=10,
            range_start="2024-01-01",
            range_end="2024-01-31",
        )
        context = _make_context()
        result = op.execute(context)

        assert result == 42
        context["ti"].xcom_push.assert_called_once_with(key="backfill_id", value=42)
        mock_hook.get_conn().backfills.create.assert_called_once_with(
            team_id=1,
            transfer_id=10,
            range_start=date(2024, 1, 1),
            range_end=date(2024, 1, 31),
        )

    def test_create_backfill_authentication_error(self, mock_hook):
        """AuthenticationError should be wrapped in AirflowException."""
        mock_hook.get_conn().backfills.create.side_effect = AuthenticationError("invalid key")

        op = SupermetricsCreateBackfillOperator(
            task_id="create_backfill",
            team_id=1,
            transfer_id=10,
            range_start="2024-01-01",
            range_end="2024-01-31",
        )
        with pytest.raises(AirflowException, match="authentication failed"):
            op.execute(_make_context())

    def test_create_backfill_validation_error(self, mock_hook):
        """ValidationError should be wrapped in AirflowException."""
        mock_hook.get_conn().backfills.create.side_effect = ValidationError("bad params")

        op = SupermetricsCreateBackfillOperator(
            task_id="create_backfill",
            team_id=1,
            transfer_id=10,
            range_start="2024-01-01",
            range_end="2024-01-31",
        )
        with pytest.raises(AirflowException, match="validation error"):
            op.execute(_make_context())

    def test_create_backfill_network_error(self, mock_hook):
        """NetworkError should be wrapped in AirflowException."""
        mock_hook.get_conn().backfills.create.side_effect = NetworkError("connection timeout")

        op = SupermetricsCreateBackfillOperator(
            task_id="create_backfill",
            team_id=1,
            transfer_id=10,
            range_start="2024-01-01",
            range_end="2024-01-31",
        )
        with pytest.raises(AirflowException, match="network error"):
            op.execute(_make_context())

    def test_template_fields_declared(self):
        """All required template fields must be declared."""
        expected = {"team_id", "transfer_id", "range_start", "range_end"}
        assert set(SupermetricsCreateBackfillOperator.template_fields) == expected

    def test_create_backfill_defers_when_deferrable(self, mock_hook):
        """When deferrable=True, operator should call self.defer."""
        mock_backfill = MagicMock()
        mock_backfill.transfer_backfill_id = 999
        mock_hook.get_conn().backfills.create.return_value = mock_backfill
        mock_hook._get_api_key.return_value = "test-key"

        op = SupermetricsCreateBackfillOperator(
            task_id="test",
            team_id=1,
            transfer_id=2,
            range_start="2024-01-01",
            range_end="2024-03-31",
            deferrable=True,
        )
        with pytest.raises(TaskDeferred):
            op.execute(context=_make_context())

    def test_execute_complete_raises_on_failure(self, mock_hook):
        """execute_complete should raise AirflowException when event is not success."""
        op = SupermetricsCreateBackfillOperator(
            task_id="test",
            team_id=1,
            transfer_id=2,
            range_start="2024-01-01",
            range_end="2024-03-31",
            deferrable=True,
        )
        with pytest.raises(AirflowException, match="Backfill failed with status: FAILED"):
            op.execute_complete(context=MagicMock(), event={"success": False, "status": "FAILED"})

    def test_execute_complete_succeeds(self, mock_hook):
        """execute_complete should succeed when event indicates success."""
        op = SupermetricsCreateBackfillOperator(
            task_id="test",
            team_id=1,
            transfer_id=2,
            range_start="2024-01-01",
            range_end="2024-03-31",
            deferrable=True,
        )
        # Should not raise
        op.execute_complete(context=MagicMock(), event={"success": True, "status": "COMPLETED"})


class TestSupermetricsCancelBackfillOperator:
    def test_cancel_backfill_calls_get_latest_then_cancel(self, mock_hook):
        """Cancel operator should get latest backfill then cancel it."""
        mock_backfill = MagicMock()
        mock_backfill.transfer_backfill_id = 42
        mock_hook.get_conn().backfills.get_latest.return_value = mock_backfill

        op = SupermetricsCancelBackfillOperator(
            task_id="cancel_backfill",
            team_id=1,
            transfer_id=10,
        )
        op.execute(_make_context())

        mock_hook.get_conn().backfills.get_latest.assert_called_once_with(
            team_id=1,
            transfer_id=10,
        )
        mock_hook.get_conn().backfills.cancel.assert_called_once_with(
            team_id=1,
            backfill_id=42,
        )

    def test_cancel_backfill_authentication_error(self, mock_hook):
        """AuthenticationError should be wrapped in AirflowException."""
        mock_hook.get_conn().backfills.get_latest.side_effect = AuthenticationError("invalid key")

        op = SupermetricsCancelBackfillOperator(
            task_id="cancel_backfill",
            team_id=1,
            transfer_id=10,
        )
        with pytest.raises(AirflowException, match="authentication failed"):
            op.execute(_make_context())

    def test_cancel_backfill_network_error(self, mock_hook):
        """NetworkError should be wrapped in AirflowException."""
        mock_hook.get_conn().backfills.get_latest.side_effect = NetworkError("connection timeout")

        op = SupermetricsCancelBackfillOperator(
            task_id="cancel_backfill",
            team_id=1,
            transfer_id=10,
        )
        with pytest.raises(AirflowException, match="network error"):
            op.execute(_make_context())

    def test_template_fields_declared(self):
        """All required template fields must be declared."""
        expected = {"team_id", "transfer_id"}
        assert set(SupermetricsCancelBackfillOperator.template_fields) == expected
