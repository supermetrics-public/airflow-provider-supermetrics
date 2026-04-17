"""Tests for SupermetricsBackfillSensor."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from airflow.providers.supermetrics.sensors.backfill import SupermetricsBackfillSensor


@pytest.fixture()
def mock_hook():
    with patch("airflow.providers.supermetrics.sensors.backfill.SupermetricsHook") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_instance


def _make_sensor(**kwargs) -> SupermetricsBackfillSensor:
    defaults = {
        "task_id": "wait_for_backfill",
        "team_id": 1,
        "transfer_id": 10,
    }
    defaults.update(kwargs)
    return SupermetricsBackfillSensor(**defaults)


class TestSupermetricsBackfillSensor:
    def test_returns_true_on_completed(self, mock_hook):
        """Sensor should return True when backfill status is COMPLETED."""
        mock_backfill = MagicMock()
        mock_backfill.status = "COMPLETED"
        mock_hook.get_conn().backfills.get_latest.return_value = mock_backfill

        sensor = _make_sensor()
        result = sensor.poke(context=MagicMock())

        assert result is True
        mock_hook.get_conn().backfills.get_latest.assert_called_once_with(
            team_id=1,
            transfer_id=10,
        )

    def test_raises_on_failed_status(self, mock_hook):
        """Sensor should raise AirflowException when backfill status is FAILED."""
        mock_backfill = MagicMock()
        mock_backfill.status = "FAILED"
        mock_hook.get_conn().backfills.get_latest.return_value = mock_backfill

        sensor = _make_sensor()
        with pytest.raises(AirflowException, match="terminal failure status"):
            sensor.poke(context=MagicMock())

    def test_raises_on_cancelled_status(self, mock_hook):
        """Sensor should raise AirflowException when backfill status is CANCELLED."""
        mock_backfill = MagicMock()
        mock_backfill.status = "CANCELLED"
        mock_hook.get_conn().backfills.get_latest.return_value = mock_backfill

        sensor = _make_sensor()
        with pytest.raises(AirflowException, match="terminal failure status"):
            sensor.poke(context=MagicMock())

    def test_returns_false_on_running_status(self, mock_hook):
        """Sensor should return False when backfill is still in progress."""
        mock_backfill = MagicMock()
        mock_backfill.status = "RUNNING"
        mock_hook.get_conn().backfills.get_latest.return_value = mock_backfill

        sensor = _make_sensor()
        result = sensor.poke(context=MagicMock())

        assert result is False

    def test_returns_false_on_pending_status(self, mock_hook):
        """Sensor should return False when backfill is pending."""
        mock_backfill = MagicMock()
        mock_backfill.status = "PENDING"
        mock_hook.get_conn().backfills.get_latest.return_value = mock_backfill

        sensor = _make_sensor()
        result = sensor.poke(context=MagicMock())

        assert result is False

    def test_template_fields_declared(self):
        """All required template fields must be declared."""
        expected = {"team_id", "transfer_id"}
        assert set(SupermetricsBackfillSensor.template_fields) == expected
