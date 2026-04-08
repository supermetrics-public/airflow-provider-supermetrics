# Architecture & Component Specifications

**Date**: 2026-04-08
**SDK**: `supermetrics>=0.2.0b2` (Python >= 3.11)
**Target Airflow**: 2.7+
**Package**: `airflow-provider-supermetrics`
**Namespace**: `airflow.providers.supermetrics`

---

## 1. Overview & Goals

Build a production-grade Apache Airflow provider that wraps the official `supermetrics` Python SDK, enabling DAG authors to:

- Execute analytics queries from 100+ data sources (Google Analytics, Google Ads, Facebook Ads, LinkedIn Ads, Twitter Ads, etc.) directly from Airflow tasks.
- Manage Data Warehouse (DWH) backfills declaratively in DAGs.
- Transfer query results directly to cloud storage (GCS, S3) without materializing data in Airflow workers.
- Use deferrable (async) operators to avoid blocking Airflow worker slots during long-running backfill operations.

---

## 2. Requirements

### 2.1 Functional Requirements

| # | Requirement |
|---|-------------|
| F1 | Store Supermetrics API key securely in Airflow Connections with a custom connection type UI |
| F2 | Execute ad-hoc queries against any Supermetrics-supported data source |
| F3 | Support all query parameters: `ds_id`, `ds_accounts`, `fields`, `start_date`, `end_date`, plus arbitrary extras |
| F4 | Create, monitor, and cancel DWH backfills |
| F5 | Stream query results to Google Cloud Storage (NDJSON or CSV) |
| F6 | Stream query results to Amazon S3 (NDJSON or CSV) |
| F7 | Deferrable operators for backfill monitoring (async, no worker-slot blocking) |
| F8 | Poke-based sensor for backfill completion as a fallback for non-deferrable environments |
| F9 | Manage logins and login links for OAuth-based data source authentication |
| F10 | All operator fields support Jinja2 templating |
| F11 | Full XCom integration with JSON-serializable result payloads |

### 2.2 Non-Functional Requirements

| # | Requirement |
|---|-------------|
| N1 | Python >= 3.11, Airflow >= 2.7.0 |
| N2 | Zero mandatory dependencies beyond `apache-airflow` and `supermetrics`; cloud transfer operators use optional extras |
| N3 | Follows official Airflow provider conventions (`get_provider_info()`, `conn_name_attr`, `hook_name`, etc.) |
| N4 | All public APIs have type annotations |
| N5 | Unit test coverage >= 90% on hooks, operators, sensors, triggers |
| N6 | Linted with `ruff`, formatted with `black`, type-checked with `mypy` |

### 2.3 Dependencies

```toml
# Core (always required)
apache-airflow = ">=2.7.0"
supermetrics = ">=0.2.0b2"

# Optional extras
[project.optional-dependencies]
gcs = ["apache-airflow-providers-google>=10.0.0"]
s3  = ["apache-airflow-providers-amazon>=8.0.0"]
dev = ["pytest", "pytest-asyncio", "unittest-mock", "ruff", "black", "mypy", "pre-commit"]
```

---

## 3. Package Architecture

### 3.1 Directory Structure

```
airflow-provider-supermetrics/
├── airflow/
│   └── providers/
│       └── supermetrics/
│           ├── __init__.py                     # get_provider_info()
│           ├── hooks/
│           │   ├── __init__.py
│           │   └── supermetrics.py             # SupermetricsHook
│           ├── operators/
│           │   ├── __init__.py
│           │   ├── query.py                    # SupermetricsQueryOperator
│           │   ├── backfill.py                 # Backfill create/cancel operators
│           │   ├── logins.py                   # Login link / login management operators
│           │   └── transfer/
│           │       ├── __init__.py
│           │       ├── gcs.py                  # SupermetricsQueryToGCSOperator
│           │       └── s3.py                   # SupermetricsQueryToS3Operator
│           ├── sensors/
│           │   ├── __init__.py
│           │   └── backfill.py                 # SupermetricsBackfillSensor
│           └── triggers/
│               ├── __init__.py
│               └── backfill.py                 # SupermetricsBackfillTrigger
├── tests/
│   └── providers/
│       └── supermetrics/
│           ├── __init__.py
│           ├── hooks/
│           │   └── test_supermetrics_hook.py
│           ├── operators/
│           │   ├── test_query.py
│           │   ├── test_backfill.py
│           │   ├── test_logins.py
│           │   └── transfer/
│           │       ├── test_gcs.py
│           │       └── test_s3.py
│           ├── sensors/
│           │   └── test_backfill_sensor.py
│           └── triggers/
│               └── test_backfill_trigger.py
├── example_dags/
│   ├── example_supermetrics_query.py
│   ├── example_supermetrics_backfill.py
│   ├── example_supermetrics_to_gcs.py
│   └── example_supermetrics_to_s3.py
├── pyproject.toml
├── CHANGELOG.md
└── README.md
```

### 3.2 Component Dependency Graph

```
SupermetricsHook
    ↑ used by
    ├── SupermetricsQueryOperator
    ├── SupermetricsCreateBackfillOperator  ──defers to──► SupermetricsBackfillTrigger
    ├── SupermetricsCancelBackfillOperator
    ├── SupermetricsCreateLoginLinkOperator
    ├── SupermetricsListLoginsOperator
    ├── SupermetricsQueryToGCSOperator
    ├── SupermetricsQueryToS3Operator
    └── SupermetricsBackfillSensor          (poke-mode alternative to trigger)
```

---

## 4. Component Specifications

### 4.1 `pyproject.toml`

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "airflow-provider-supermetrics"
version = "0.1.0"
description = "Apache Airflow provider package for Supermetrics"
readme = "README.md"
license = { text = "Apache-2.0" }
requires-python = ">=3.11"
keywords = ["airflow", "provider", "supermetrics", "analytics"]
classifiers = [
    "Framework :: Apache Airflow",
    "Framework :: Apache Airflow :: Provider",
    "Development Status :: 3 - Alpha",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
]
dependencies = [
    "apache-airflow>=2.7.0",
    "supermetrics>=0.2.0b2",
]

[project.optional-dependencies]
gcs = ["apache-airflow-providers-google>=10.0.0"]
s3  = ["apache-airflow-providers-amazon>=8.0.0"]
dev = ["pytest>=8.0", "pytest-asyncio>=0.23", "ruff", "black", "mypy", "pre-commit"]

[project.entry-points."apache_airflow_provider"]
provider_info = "airflow.providers.supermetrics:get_provider_info"

[tool.hatch.build.targets.wheel]
packages = ["airflow"]

[tool.ruff]
target-version = "py311"
line-length = 100

[tool.mypy]
python_version = "3.11"
strict = true
```

### 4.2 `airflow/providers/supermetrics/__init__.py` — Provider Registration

```python
"""Apache Airflow provider for Supermetrics."""
from __future__ import annotations


def get_provider_info() -> dict:
    return {
        "package-name": "airflow-provider-supermetrics",
        "name": "Supermetrics",
        "description": "Apache Airflow provider for Supermetrics analytics platform.",
        "versions": ["0.1.0"],
        "connection-types": [
            {
                "connection-type": "supermetrics",
                "hook-class-name": (
                    "airflow.providers.supermetrics.hooks.supermetrics.SupermetricsHook"
                ),
            }
        ],
    }
```

### 4.3 `SupermetricsHook`

**Design decisions:**
- API key stored in the **Password** field of the Airflow connection.
- Connection type `supermetrics` registers a clean UI form (hides irrelevant fields).
- Caches the sync client instance per hook instance.
- Async client is returned as a context manager (caller manages lifecycle) because Airflow triggers manage their own async context.

```python
class SupermetricsHook(BaseHook):
    conn_name_attr = "supermetrics_conn_id"
    default_conn_name = "supermetrics_default"
    conn_type = "supermetrics"
    hook_name = "Supermetrics"

    def __init__(self, supermetrics_conn_id: str = default_conn_name) -> None: ...

    def get_conn(self) -> SupermetricsClient:
        """Return cached sync client. Initializes on first call."""
        ...

    def get_async_client(self) -> SupermetricsAsyncClient:
        """Return a new async client. Caller must manage lifecycle with `async with`."""
        ...

    @classmethod
    def get_connection_form_widgets(cls) -> dict: ...
    # Returns Flask-AppBuilder widgets for API Key field

    @classmethod
    def get_ui_field_behaviour(cls) -> dict: ...
    # Hides: schema, host, login, port, extra
    # Relabels: password -> "API Key"
    # Placeholder: "Enter your Supermetrics API key"
```

### 4.4 `SupermetricsQueryOperator`

Executes a query and pushes the full result (rows as list-of-dicts) to XCom.

| Attribute | Detail |
|-----------|--------|
| `template_fields` | `ds_id`, `ds_accounts`, `fields`, `start_date`, `end_date`, `query_params` |
| `ui_color` | `#00b4d8` |
| Returns | `list[dict]` serialized from `result.rows` |

**Constructor parameters:**

```python
def __init__(
    self,
    *,
    ds_id: str,                          # e.g. "GAWA", "FA", "AW"
    ds_accounts: list[str],              # account IDs within the data source
    fields: list[str],                   # dimensions + metrics to retrieve
    start_date: str,                     # YYYY-MM-DD
    end_date: str,                       # YYYY-MM-DD
    query_params: dict | None = None,    # extra SDK query kwargs
    supermetrics_conn_id: str = SupermetricsHook.default_conn_name,
    deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
    **kwargs: Any,
) -> None: ...
```

**Validated by**: [Scenario 3](scenarios/03-weekly-competitive-benchmarking.md) — XCom + TaskFlow integration.

### 4.5 Backfill Operators

**`SupermetricsCreateBackfillOperator`**
- Calls `client.backfills.create(team_id, transfer_id, range_start, range_end)`.
- Pushes created backfill ID to XCom key `backfill_id`.
- When `deferrable=True`: immediately defers to `SupermetricsBackfillTrigger` after creating the backfill.
- When `deferrable=False`: creates backfill and exits (use sensor to wait separately).

**`SupermetricsCancelBackfillOperator`**
- Calls `client.backfills.cancel(team_id, transfer_id)`.
- No wait logic — cancellation is synchronous.

| Parameter | Both operators |
|-----------|---------------|
| `team_id` | `int` — Supermetrics team ID |
| `transfer_id` | `int` — DWH transfer configuration ID |
| `supermetrics_conn_id` | Connection ID |

**`SupermetricsCreateBackfillOperator` extra parameters:**

```python
range_start: str           # YYYY-MM-DD
range_end: str             # YYYY-MM-DD
deferrable: bool           # default: conf.getboolean("operators", "default_deferrable")
poll_interval: int = 30    # seconds between trigger polls (deferrable mode only)
```

**Validated by**: [Scenario 2](scenarios/02-dwh-backfill-with-async-monitoring.md) — deferrable backfill lifecycle.

### 4.6 Login Operators

**`SupermetricsCreateLoginLinkOperator`**
- Calls `client.login_links.create(ds_id, description)`.
- Pushes `login_id` and `link_url` to XCom.

**`SupermetricsListLoginsOperator`**
- Calls `client.logins.list()`.
- Pushes serialized list of logins to XCom.

**`SupermetricsGetLoginOperator`**
- Calls `client.logins.get(login_id=...)`.
- Pushes login details to XCom.

### 4.7 `SupermetricsQueryToGCSOperator`

Executes query via `SupermetricsHook`, streams rows to GCS as NDJSON or CSV.

```python
def __init__(
    self,
    *,
    # All SupermetricsQueryOperator params, plus:
    gcs_bucket: str,
    gcs_object: str,              # supports Jinja2 templating in path
    gcp_conn_id: str = "google_cloud_default",
    file_format: Literal["ndjson", "csv"] = "ndjson",
    **kwargs: Any,
) -> None: ...
```

- Guard import: `from airflow.providers.google.cloud.hooks.gcs import GCSHook` inside `execute()` with helpful `ImportError`.
- Never materializes the entire result set in memory — writes rows chunk by chunk.
- Pushes GCS URI to XCom for downstream tasks.

**Validated by**: [Scenario 1](scenarios/01-daily-cross-channel-marketing-report.md) — parallel GCS loading.

### 4.8 `SupermetricsQueryToS3Operator`

Same as GCS variant but targets S3.

```python
def __init__(
    self,
    *,
    # All SupermetricsQueryOperator params, plus:
    s3_bucket: str,
    s3_key: str,
    aws_conn_id: str = "aws_default",
    file_format: Literal["ndjson", "csv"] = "ndjson",
    **kwargs: Any,
) -> None: ...
```

### 4.9 `SupermetricsBackfillSensor`

Polls `client.backfills.get_latest(team_id, transfer_id)` in poke mode.

```python
class SupermetricsBackfillSensor(BaseSensorOperator):
    template_fields = ("team_id", "transfer_id")
    poke_interval: int = 60

    def poke(self, context: Context) -> bool:
        hook = SupermetricsHook(self.supermetrics_conn_id)
        backfill = hook.get_conn().backfills.get_latest(
            team_id=self.team_id, transfer_id=self.transfer_id
        )
        if backfill.status in TERMINAL_FAILURE_STATUSES:
            raise AirflowException(f"Backfill failed: {backfill.status}")
        return backfill.status in TERMINAL_SUCCESS_STATUSES
```

**Validated by**: [Scenario 2](scenarios/02-dwh-backfill-with-async-monitoring.md) — non-deferrable fallback.

### 4.10 `SupermetricsBackfillTrigger`

Runs in the Airflow Triggerer process (lightweight async loop).

```python
class SupermetricsBackfillTrigger(BaseTrigger):
    def __init__(
        self,
        team_id: int,
        transfer_id: int,
        api_key: str,            # extracted from connection before deferral
        poll_interval: int = 30,
    ) -> None: ...

    def serialize(self) -> tuple[str, dict]:
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
        async with SupermetricsAsyncClient(api_key=self.api_key) as client:
            while True:
                backfill = await client.backfills.get_latest(
                    team_id=self.team_id, transfer_id=self.transfer_id
                )
                if backfill.status in TERMINAL_STATUSES:
                    yield TriggerEvent({
                        "status": backfill.status,
                        "success": backfill.status in TERMINAL_SUCCESS_STATUSES,
                    })
                    return
                await asyncio.sleep(self.poll_interval)
```

**Validated by**: [Scenario 2](scenarios/02-dwh-backfill-with-async-monitoring.md) — async monitoring.

---

## 5. Testing Strategy

### Unit Tests (all phases)

- Mock all SDK client calls — tests must not make real HTTP requests.
- Mock `SupermetricsHook.get_connection()` to return a fixture `Connection` object.
- Use `unittest.mock.patch` for sync mocks and `unittest.mock.AsyncMock` for async.

### Async Trigger Tests

```python
import pytest
from unittest.mock import AsyncMock, patch

@pytest.mark.asyncio
async def test_backfill_trigger_success():
    trigger = SupermetricsBackfillTrigger(team_id=1, transfer_id=2, api_key="test_key")
    mock_backfill = AsyncMock()
    mock_backfill.status = "completed"

    with patch("supermetrics.SupermetricsAsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.backfills.get_latest.return_value = mock_backfill
        mock_client_cls.return_value = mock_client

        events = [event async for event in trigger.run()]

    assert len(events) == 1
    assert events[0].payload["success"] is True
```

### Integration Smoke Test (optional, CI-gated)

- Marked `@pytest.mark.integration`, requires `SUPERMETRICS_API_KEY` env var.
- Runs only in CI on merge to main with secrets configured.

### Coverage Target

```ini
[tool.pytest.ini_options]
addopts = "--cov=airflow/providers/supermetrics --cov-report=term-missing --cov-fail-under=90"
```

---

## 6. Airflow Connection Setup Reference

| Field | Value |
|-------|-------|
| **Conn Id** | `supermetrics_default` (or custom) |
| **Conn Type** | `Supermetrics` |
| **API Key** | `<your Supermetrics API key>` |

All other fields (Host, Schema, Login, Port, Extra) are hidden by the hook's `get_ui_field_behaviour()`.

---

## 7. Known Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| SDK is alpha (`0.2.0b2`) — API may change | Pin `supermetrics>=0.2.0b2,<0.3.0` in production; monitor HISTORY.md |
| Async client lifecycle unclear | Wrap in `async with` + test; if no context manager, use `try/finally client.close()` |
| Terminal backfill status strings not documented | Confirm against real API or SDK source; define as configurable `frozenset` constant |
| XCom size limits for large query results | Document that `SupermetricsQueryOperator` is for small/medium result sets; recommend transfer operators for large data |
| Airflow 3.x compatibility | Avoid deprecated APIs; track Airflow changelog for breaking changes |
