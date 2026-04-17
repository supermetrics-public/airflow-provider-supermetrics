# airflow-provider-supermetrics

Apache Airflow provider package for [Supermetrics](https://supermetrics.com), enabling DAG authors to query analytics data from 100+ marketing platforms directly from Airflow.

## Requirements

- Python >= 3.11
- Apache Airflow >= 2.7.0
- Supermetrics Python SDK >= 0.2.0b2

## Installation

```bash
pip install airflow-provider-supermetrics
```

For cloud storage transfer operators, install with extras:

```bash
# Google Cloud Storage support
pip install "airflow-provider-supermetrics[gcs]"

# Amazon S3 support
pip install "airflow-provider-supermetrics[s3]"
```

## Connection Setup

1. In the Airflow UI, go to **Admin > Connections**
2. Create a new connection:
   - **Connection Id**: `supermetrics_default`
   - **Connection Type**: `Supermetrics`
   - **API Key**: Your Supermetrics API key

## Operators

### SupermetricsQueryOperator

Execute a Supermetrics query and push results to XCom as `list[dict]`.

```python
from airflow.providers.supermetrics.operators.query import SupermetricsQueryOperator

query = SupermetricsQueryOperator(
    task_id="query_ga4",
    ds_id="GAWA",
    ds_accounts=["123456"],
    fields=["Date", "Sessions", "Users"],
    query_start_date="{{ ds }}",
    query_end_date="{{ ds }}",
)
```

### SupermetricsQueryToGCSOperator

Execute a query and stream results directly to Google Cloud Storage.

```python
from airflow.providers.supermetrics.operators.transfer.gcs import SupermetricsQueryToGCSOperator

to_gcs = SupermetricsQueryToGCSOperator(
    task_id="to_gcs",
    ds_id="GAWA",
    ds_accounts=["123456"],
    fields=["Date", "Sessions"],
    query_start_date="{{ ds }}",
    query_end_date="{{ ds }}",
    gcs_bucket="my-bucket",
    gcs_object="data/{{ ds }}/sessions.ndjson",
    file_format="ndjson",  # or "csv"
)
```

### SupermetricsQueryToS3Operator

Same as GCS but for Amazon S3.

```python
from airflow.providers.supermetrics.operators.transfer.s3 import SupermetricsQueryToS3Operator

to_s3 = SupermetricsQueryToS3Operator(
    task_id="to_s3",
    ds_id="GAWA",
    ds_accounts=["123456"],
    fields=["Date", "Sessions"],
    query_start_date="{{ ds }}",
    query_end_date="{{ ds }}",
    s3_bucket="my-bucket",
    s3_key="data/{{ ds }}/sessions.ndjson",
)
```

### SupermetricsCreateBackfillOperator

Create a DWH backfill. Supports deferrable mode for async monitoring.

```python
from airflow.providers.supermetrics.operators.backfill import SupermetricsCreateBackfillOperator

backfill = SupermetricsCreateBackfillOperator(
    task_id="create_backfill",
    team_id=12345,
    transfer_id=67890,
    range_start="{{ macros.ds_add(ds, -90) }}",
    range_end="{{ ds }}",
    deferrable=True,  # non-blocking via Triggerer
    poll_interval=60,
)
```

### SupermetricsCancelBackfillOperator

Cancel the latest backfill for a transfer.

```python
from airflow.providers.supermetrics.operators.backfill import SupermetricsCancelBackfillOperator

cancel = SupermetricsCancelBackfillOperator(
    task_id="cancel_backfill",
    team_id=12345,
    transfer_id=67890,
    trigger_rule="one_failed",
)
```

### SupermetricsBackfillSensor

Poll for backfill completion (non-deferrable alternative).

```python
from airflow.providers.supermetrics.sensors.backfill import SupermetricsBackfillSensor

sensor = SupermetricsBackfillSensor(
    task_id="wait_for_backfill",
    team_id=12345,
    transfer_id=67890,
    poke_interval=60,
    timeout=7200,
)
```

### Login Management Operators

```python
from airflow.providers.supermetrics.operators.logins import (
    SupermetricsCreateLoginLinkOperator,
    SupermetricsListLoginsOperator,
    SupermetricsGetLoginOperator,
)

# Create an OAuth login link
create_link = SupermetricsCreateLoginLinkOperator(
    task_id="create_login_link",
    ds_id="GAWA",
    description="GA4 login for pipeline",
)

# List all logins
list_logins = SupermetricsListLoginsOperator(task_id="list_logins")

# Get a specific login
get_login = SupermetricsGetLoginOperator(task_id="get_login", login_id="login-123")
```

## Template Fields

All operators support Jinja2 templating on key fields. Use `{{ ds }}`, `{{ macros.ds_add(ds, -7) }}`, and other Airflow macros for dynamic date parameters.

## License

Apache-2.0
