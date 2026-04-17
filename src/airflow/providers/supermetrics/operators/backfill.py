"""Supermetrics backfill operators for Apache Airflow."""

from __future__ import annotations

from datetime import date
from typing import Any, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from airflow.providers.supermetrics.hooks.supermetrics import SupermetricsHook
from airflow.providers.supermetrics.triggers.backfill import SupermetricsBackfillTrigger
from supermetrics import AuthenticationError, NetworkError, ValidationError


class SupermetricsCreateBackfillOperator(BaseOperator):
    """Create a Supermetrics backfill and push the backfill ID to XCom.

    Calls the Supermetrics API to create a backfill for the specified
    transfer and date range.
    """

    template_fields: Sequence[str] = ("team_id", "transfer_id", "range_start", "range_end")
    ui_color = "#00b4d8"

    def __init__(
        self,
        *,
        team_id: int,
        transfer_id: int,
        range_start: str,
        range_end: str,
        supermetrics_conn_id: str = SupermetricsHook.default_conn_name,
        deferrable: bool = False,
        poll_interval: int = 30,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.team_id = team_id
        self.transfer_id = transfer_id
        self.range_start = range_start
        self.range_end = range_end
        self.supermetrics_conn_id = supermetrics_conn_id
        self.deferrable = deferrable
        self.poll_interval = poll_interval

    def execute(self, context: Any) -> int:
        """Create a backfill and push the backfill ID to XCom."""
        hook = SupermetricsHook(supermetrics_conn_id=self.supermetrics_conn_id)
        client = hook.get_conn()

        try:
            backfill = client.backfills.create(
                team_id=int(self.team_id),
                transfer_id=int(self.transfer_id),
                range_start=date.fromisoformat(self.range_start),
                range_end=date.fromisoformat(self.range_end),
            )
        except AuthenticationError as e:
            raise AirflowException(f"Supermetrics authentication failed: {e}") from e
        except ValidationError as e:
            raise AirflowException(f"Supermetrics validation error: {e}") from e
        except NetworkError as e:
            raise AirflowException(f"Supermetrics network error: {e}") from e

        backfill_id: int = backfill.transfer_backfill_id
        context["ti"].xcom_push(key="backfill_id", value=backfill_id)
        self.log.info("Created backfill with ID %s", backfill_id)

        if self.deferrable:
            api_key = hook._get_api_key()  # Extract before deferral
            self.defer(
                trigger=SupermetricsBackfillTrigger(
                    team_id=int(self.team_id),
                    transfer_id=int(self.transfer_id),
                    api_key=api_key,
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )

        return backfill_id

    def execute_complete(self, context: Any, event: dict[str, Any]) -> None:
        """Handle the trigger event after deferral."""
        if not event.get("success"):
            raise AirflowException(f"Backfill failed with status: {event.get('status')}")
        self.log.info("Backfill completed successfully: %s", event.get("status"))


class SupermetricsCancelBackfillOperator(BaseOperator):
    """Cancel the latest Supermetrics backfill for a transfer.

    Retrieves the latest backfill for the given transfer, then cancels it.
    """

    template_fields: Sequence[str] = ("team_id", "transfer_id")
    ui_color = "#00b4d8"

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

    def execute(self, context: Any) -> None:
        """Cancel the latest backfill for the configured transfer."""
        hook = SupermetricsHook(supermetrics_conn_id=self.supermetrics_conn_id)
        client = hook.get_conn()

        try:
            backfill = client.backfills.get_latest(
                team_id=int(self.team_id),
                transfer_id=int(self.transfer_id),
            )
            client.backfills.cancel(
                team_id=int(self.team_id),
                backfill_id=backfill.transfer_backfill_id,
            )
        except AuthenticationError as e:
            raise AirflowException(f"Supermetrics authentication failed: {e}") from e
        except ValidationError as e:
            raise AirflowException(f"Supermetrics validation error: {e}") from e
        except NetworkError as e:
            raise AirflowException(f"Supermetrics network error: {e}") from e

        self.log.info(
            "Cancelled backfill %s for transfer %s",
            backfill.transfer_backfill_id,
            self.transfer_id,
        )
