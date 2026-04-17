"""Tests for SupermetricsBackfillTrigger."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airflow.providers.supermetrics.triggers.backfill import SupermetricsBackfillTrigger


class TestSupermetricsBackfillTrigger:
    def test_serialize_round_trip(self):
        """serialize() should return class path and all init params."""
        trigger = SupermetricsBackfillTrigger(
            team_id=1, transfer_id=2, api_key="key123", poll_interval=60
        )
        cls_path, kwargs = trigger.serialize()
        assert (
            cls_path
            == "airflow.providers.supermetrics.triggers.backfill.SupermetricsBackfillTrigger"
        )
        assert kwargs == {
            "team_id": 1,
            "transfer_id": 2,
            "api_key": "key123",
            "poll_interval": 60,
        }
        # Verify round-trip reconstruction works
        reconstructed = SupermetricsBackfillTrigger(**kwargs)
        assert reconstructed.serialize() == trigger.serialize()

    @pytest.mark.asyncio
    async def test_run_yields_success_on_completed(self):
        """run() should yield success event when backfill completes."""
        trigger = SupermetricsBackfillTrigger(team_id=1, transfer_id=2, api_key="key123")
        mock_backfill = MagicMock()
        mock_backfill.status = "COMPLETED"

        with patch("supermetrics.SupermetricsAsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.backfills.get_latest.return_value = mock_backfill
            mock_cls.return_value = mock_client

            events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["success"] is True
        assert events[0].payload["status"] == "COMPLETED"

    @pytest.mark.asyncio
    async def test_run_yields_failure_on_failed(self):
        """run() should yield failure event when backfill fails."""
        trigger = SupermetricsBackfillTrigger(team_id=1, transfer_id=2, api_key="key123")
        mock_backfill = MagicMock()
        mock_backfill.status = "FAILED"

        with patch("supermetrics.SupermetricsAsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.backfills.get_latest.return_value = mock_backfill
            mock_cls.return_value = mock_client

            events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["success"] is False
        assert events[0].payload["status"] == "FAILED"

    @pytest.mark.asyncio
    async def test_run_polls_while_in_progress(self):
        """run() should keep polling until terminal status."""
        trigger = SupermetricsBackfillTrigger(
            team_id=1, transfer_id=2, api_key="key123", poll_interval=0
        )

        running = MagicMock()
        running.status = "RUNNING"
        completed = MagicMock()
        completed.status = "COMPLETED"

        with patch("supermetrics.SupermetricsAsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.backfills.get_latest.side_effect = [running, running, completed]
            mock_cls.return_value = mock_client

            events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["success"] is True
        assert mock_client.backfills.get_latest.call_count == 3
