"""Supermetrics backfill trigger for async monitoring."""

from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent

from airflow.providers.supermetrics.constants import (
    TERMINAL_STATUSES,
    TERMINAL_SUCCESS_STATUSES,
)


class SupermetricsBackfillTrigger(BaseTrigger):
    """Polls backfill status asynchronously in the Airflow Triggerer."""

    def __init__(
        self,
        team_id: int,
        transfer_id: int,
        api_key: str,
        poll_interval: int = 30,
    ) -> None:
        super().__init__()
        self.team_id = team_id
        self.transfer_id = transfer_id
        self.api_key = api_key
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.supermetrics.triggers.backfill.SupermetricsBackfillTrigger",
            {
                "team_id": self.team_id,
                "transfer_id": self.transfer_id,
                "api_key": self.api_key,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        from supermetrics import SupermetricsAsyncClient

        async with SupermetricsAsyncClient(api_key=self.api_key) as client:
            while True:
                backfill = await client.backfills.get_latest(
                    team_id=self.team_id, transfer_id=self.transfer_id
                )
                if backfill.status in TERMINAL_STATUSES:
                    yield TriggerEvent(
                        {
                            "status": backfill.status,
                            "success": backfill.status in TERMINAL_SUCCESS_STATUSES,
                        }
                    )
                    return
                await asyncio.sleep(self.poll_interval)
