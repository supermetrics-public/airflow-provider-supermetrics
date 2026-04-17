"""Supermetrics backfill sensor for Apache Airflow."""

from __future__ import annotations

from typing import Any, Sequence

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator

from airflow.providers.supermetrics.constants import (
    TERMINAL_FAILURE_STATUSES,
    TERMINAL_SUCCESS_STATUSES,
)
from airflow.providers.supermetrics.hooks.supermetrics import SupermetricsHook


class SupermetricsBackfillSensor(BaseSensorOperator):
    """Wait for a Supermetrics backfill to reach a terminal status.

    Polls the latest backfill for the given transfer and succeeds when
    the status is ``COMPLETED``. Raises ``AirflowException`` on
    ``FAILED`` or ``CANCELLED``.
    """

    template_fields: Sequence[str] = ("team_id", "transfer_id")

    def __init__(
        self,
        *,
        team_id: int,
        transfer_id: int,
        supermetrics_conn_id: str = SupermetricsHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.team_id = team_id
        self.transfer_id = transfer_id
        self.supermetrics_conn_id = supermetrics_conn_id

    def poke(self, context: Any) -> bool:
        """Check the latest backfill status."""
        hook = SupermetricsHook(supermetrics_conn_id=self.supermetrics_conn_id)
        client = hook.get_conn()

        backfill = client.backfills.get_latest(
            team_id=int(self.team_id),
            transfer_id=int(self.transfer_id),
        )

        status: str = backfill.status
        self.log.info("Backfill status: %s", status)

        if status in TERMINAL_FAILURE_STATUSES:
            raise AirflowException(f"Backfill reached terminal failure status: {status}")

        return status in TERMINAL_SUCCESS_STATUSES
