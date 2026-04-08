# Scenario 2: DWH Backfill with Async Monitoring

## Problem Statement

After connecting a new data source (LinkedIn Ads), the data engineering team needs to backfill
90 days of historical data into their Supermetrics-managed data warehouse. Backfills can take
30+ minutes. The DAG must:

- Create a backfill for a specific DWH transfer configuration
- Monitor completion **without blocking an Airflow worker slot** for the entire duration
- Cancel the backfill automatically on DAG failure (cleanup)
- Support a non-deferrable fallback for environments without a running Triggerer

## Provider Components Used

| Component | Architecture Ref | Purpose |
|-----------|-----------------|---------|
| `SupermetricsHook` | [arch:4.3] | Connection and client management |
| `SupermetricsCreateBackfillOperator` | [arch:4.5] | Creates backfill, defers to trigger when `deferrable=True` |
| `SupermetricsBackfillTrigger` | [arch:4.10] | Async polling in the Triggerer process |
| `SupermetricsCancelBackfillOperator` | [arch:4.5] | Cleanup on failure |
| `SupermetricsBackfillSensor` | [arch:4.9] | Non-deferrable fallback (poke mode) |

## Airflow Features Leveraged

- **Deferrable operators**: `deferrable=True` offloads polling to the Triggerer process,
  freeing the worker slot immediately after backfill creation
- **Triggerer process**: `SupermetricsBackfillTrigger` runs in the async event loop,
  polling every `poll_interval` seconds without occupying a worker
- **XCom**: backfill creation pushes `backfill_id` for downstream reference
- **Trigger rules**: cancel operator uses `trigger_rule="one_failed"` for automatic cleanup
- **`template_fields`**: `team_id`, `transfer_id`, `range_start`, `range_end` are all templatable

## DAG Code

```python
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.supermetrics.operators.backfill import (
    SupermetricsCreateBackfillOperator,
    SupermetricsCancelBackfillOperator,
)


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["backfill", "supermetrics"],
)
def backfill_historical_data():
    """Backfill 90 days of LinkedIn Ads data into the DWH."""

    # Create the backfill — worker slot freed immediately via deferrable
    create_backfill = SupermetricsCreateBackfillOperator(
        task_id="create_backfill",
        team_id="{{ var.value.supermetrics_team_id }}",
        transfer_id="{{ var.value.linkedin_transfer_id }}",
        range_start="{{ macros.ds_add(ds, -90) }}",
        range_end="{{ ds }}",
        deferrable=True,
        poll_interval=60,
    )

    # On success: notify the team
    @task
    def notify_success(**context):
        event = context["ti"].xcom_pull(task_ids="create_backfill")
        print(f"Backfill completed successfully: {event}")

    # On failure: cancel the backfill to prevent orphaned long-running jobs
    cancel_backfill = SupermetricsCancelBackfillOperator(
        task_id="cancel_backfill",
        team_id="{{ var.value.supermetrics_team_id }}",
        transfer_id="{{ var.value.linkedin_transfer_id }}",
        trigger_rule="one_failed",
    )

    success = notify_success()
    create_backfill >> [success, cancel_backfill]


backfill_historical_data()
```

## Non-Deferrable Fallback

For environments without a Triggerer process, replace `deferrable=True` with a separate
`SupermetricsBackfillSensor`:

```python
from airflow.providers.supermetrics.sensors.backfill import SupermetricsBackfillSensor

# Create backfill (non-deferrable)
create_backfill = SupermetricsCreateBackfillOperator(
    task_id="create_backfill",
    team_id=12345,
    transfer_id=67890,
    range_start="2024-01-01",
    range_end="2024-03-31",
    deferrable=False,  # just create and exit
)

# Sensor polls in poke mode (blocks a worker slot)
wait_for_backfill = SupermetricsBackfillSensor(
    task_id="wait_for_backfill",
    team_id=12345,
    transfer_id=67890,
    poke_interval=60,
    timeout=7200,
    mode="poke",
)

create_backfill >> wait_for_backfill
```

## Key Design Points

- **Deferrable mode**: the worker slot is freed immediately after `client.backfills.create()`
  returns. The Triggerer process handles the 30+ minute polling cycle.
- **API key extraction**: the operator extracts the API key from the Airflow Connection
  *before* deferral, because triggers run in a separate process without access to the Airflow DB.
- **Cancel as safety net**: `trigger_rule="one_failed"` ensures the cancel operator runs only
  when the backfill task fails, preventing orphaned long-running backfills.
- **Terminal statuses**: both the sensor and trigger share `TERMINAL_SUCCESS_STATUSES` and
  `TERMINAL_FAILURE_STATUSES` from `constants.py` — single source of truth.
- **Sensor fallback**: useful for local development or Airflow deployments without a Triggerer.
