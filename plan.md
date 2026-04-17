# Apache Airflow Provider for Supermetrics — Implementation Checklist

**Date**: 2026-04-08
**SDK**: `supermetrics>=0.2.0b2` (Python >= 3.11)
**Target Airflow**: 2.7+
**Package**: `airflow-provider-supermetrics`
**Namespace**: `airflow.providers.supermetrics`

---

## How to Use This Document

- Each phase contains atomic checkboxes — one deliverable per line
- Cross-references point to context files using relative paths:
  - `[arch:X.Y]` = see `docs/architecture.md` section X.Y
  - `[scenario:N]` = see `docs/scenarios/0N-*.md`
- Check off items as you complete them: `- [ ]` becomes `- [x]`
- Phases 1-4 = **MVP**. Phases 5-9 = production hardening.

---

## Context Map

Where to find implementation context for each component:

| Component | Spec | Scenario | Implementation Path |
|-----------|------|----------|---------------------|
| `SupermetricsHook` | [arch:4.3] | All scenarios | `airflow/providers/supermetrics/hooks/supermetrics.py` |
| `SupermetricsQueryOperator` | [arch:4.4] | [scenario:3] | `airflow/providers/supermetrics/operators/query.py` |
| `SupermetricsCreateBackfillOperator` | [arch:4.5] | [scenario:2] | `airflow/providers/supermetrics/operators/backfill.py` |
| `SupermetricsCancelBackfillOperator` | [arch:4.5] | [scenario:2] | `airflow/providers/supermetrics/operators/backfill.py` |
| `SupermetricsCreateLoginLinkOperator` | [arch:4.6] | — | `airflow/providers/supermetrics/operators/logins.py` |
| `SupermetricsListLoginsOperator` | [arch:4.6] | — | `airflow/providers/supermetrics/operators/logins.py` |
| `SupermetricsGetLoginOperator` | [arch:4.6] | — | `airflow/providers/supermetrics/operators/logins.py` |
| `SupermetricsQueryToGCSOperator` | [arch:4.7] | [scenario:1] | `airflow/providers/supermetrics/operators/transfer/gcs.py` |
| `SupermetricsQueryToS3Operator` | [arch:4.8] | — | `airflow/providers/supermetrics/operators/transfer/s3.py` |
| `SupermetricsBackfillSensor` | [arch:4.9] | [scenario:2] | `airflow/providers/supermetrics/sensors/backfill.py` |
| `SupermetricsBackfillTrigger` | [arch:4.10] | [scenario:2] | `airflow/providers/supermetrics/triggers/backfill.py` |

---

## Scenario Validation Matrix

Verifies that every feature exercised in the 3 use case scenarios is fully covered by the plan.

| Scenario | Required Components | Phases | Validated |
|----------|---------------------|--------|-----------|
| **1**: Daily Cross-Channel Report | `SupermetricsQueryToGCSOperator`, `SupermetricsHook`, Jinja2 templating, parallel tasks | 2, 7 | [x] |
| **2**: DWH Backfill Async | `SupermetricsCreateBackfillOperator` (deferrable), `SupermetricsBackfillTrigger`, `SupermetricsCancelBackfillOperator`, `SupermetricsBackfillSensor` (fallback) | 2, 4, 6 | [x] |
| **3**: Weekly Benchmarking | `SupermetricsQueryOperator`, XCom `list[dict]`, TaskFlow `@task` compatibility | 2, 3 | [x] |

---

## Phase 0 — Project Bootstrap

> **Goal**: Git repo initialized, planning artifacts in place, dev tools ready.

- [x] Initialize git repository
- [x] Create `.gitignore` (Python, mypy, ruff, pytest, dist, venv, .claude/)
- [x] Install dev tools: `ruff`, `black`, `mypy` via `uv tool install`
- [x] Create `CLAUDE.md` with project instructions
- [x] Write Scenario 1: Daily Cross-Channel Marketing Report → [`docs/scenarios/01-daily-cross-channel-marketing-report.md`](docs/scenarios/01-daily-cross-channel-marketing-report.md)
- [x] Write Scenario 2: DWH Backfill with Async Monitoring → [`docs/scenarios/02-dwh-backfill-with-async-monitoring.md`](docs/scenarios/02-dwh-backfill-with-async-monitoring.md)
- [x] Write Scenario 3: Weekly Competitive Benchmarking → [`docs/scenarios/03-weekly-competitive-benchmarking.md`](docs/scenarios/03-weekly-competitive-benchmarking.md)
- [x] Extract component specs to [`docs/architecture.md`](docs/architecture.md)
- [x] Rewrite `plan.md` as task checklist (this file)
- [x] Initial git commit with all planning artifacts

---

## Phase 1 — Project Scaffold

> **Goal**: Importable package with empty stubs; CI passes.
> **Context**: [arch:3.1] (directory structure), [arch:4.1] (pyproject.toml), [arch:4.2] (provider registration)

- [x] Create full directory tree with all `__init__.py` files → [arch:3.1]
- [x] Write `pyproject.toml` with metadata, entry point, and extras → [arch:4.1]
- [x] Write `airflow/providers/supermetrics/__init__.py` with `get_provider_info()` → [arch:4.2]
- [x] Configure `ruff` settings in `pyproject.toml`
- [x] Configure `black` settings in `pyproject.toml`
- [x] Configure `mypy` settings in `pyproject.toml`
- [x] Write `.pre-commit-config.yaml` (ruff, black, mypy hooks)
- [x] Add `[tool.pytest.ini_options]` to `pyproject.toml` → [arch:5]
- [x] Verify `pip install -e ".[dev]"` succeeds
- [x] Verify `airflow providers list` shows the provider

---

## Phase 2 — Core Hook

> **Goal**: `SupermetricsHook` fully functional with connection UI.
> **Context**: [arch:4.3]. Used by **all scenarios**.

- [x] Implement `SupermetricsHook.__init__` accepting `supermetrics_conn_id`
- [x] Implement `get_conn()` with cached sync client instantiation
- [x] Implement `get_async_client()` returning new async client per call
- [x] Implement `get_connection_form_widgets()` with API Key widget
- [x] Implement `get_ui_field_behaviour()` — hide host/schema/login/port/extra, relabel password
- [x] Write test: `get_conn()` caches client (called twice, constructed once)
- [x] Write test: `get_async_client()` returns new instance each call
- [x] Write test: API key correctly extracted from `conn.password`
- [x] Write test: `AuthenticationError` propagation

---

## Phase 3 — Query Operator

> **Goal**: Execute queries and push results to XCom.
> **Context**: [arch:4.4]. Validates [scenario:3] (weekly benchmarking).

- [x] Implement `SupermetricsQueryOperator` with all constructor params → [arch:4.4]
- [x] Declare `template_fields`: `ds_id`, `ds_accounts`, `fields`, `start_date`, `end_date`, `query_params`
- [x] Implement `execute()` — call SDK, serialize Pydantic result via `model_dump()`
- [x] Map `supermetrics.AuthenticationError` → `AirflowException`
- [x] Map `supermetrics.ValidationError` → `AirflowException`
- [x] Handle `supermetrics.NetworkError` (wrap or re-raise)
- [x] Write test: successful query returns `list[dict]` to XCom
- [x] Write test: Pydantic model serialization round-trip
- [x] Write test: each exception type mapping
- [x] Write test: template fields declared correctly
- [x] Validate: [scenario:3] DAG works with this operator

---

## Phase 4 — Backfill Operators & Sensor

> **Goal**: Full DWH backfill lifecycle management (sync path only).
> **Context**: [arch:4.5], [arch:4.9]. Validates [scenario:2] sync path.

- [x] Create `constants.py` with `TERMINAL_SUCCESS_STATUSES` and `TERMINAL_FAILURE_STATUSES` frozensets
- [x] Implement `SupermetricsCreateBackfillOperator` (sync, `deferrable=False` only) → [arch:4.5]
- [x] Implement `SupermetricsCancelBackfillOperator` → [arch:4.5]
- [x] Implement `SupermetricsBackfillSensor` with configurable `poke_interval` → [arch:4.9]
- [x] Write test: create backfill pushes `backfill_id` to XCom
- [x] Write test: cancel backfill calls correct SDK method
- [x] Write test: sensor returns `True` on success status
- [x] Write test: sensor raises `AirflowException` on failure status
- [x] Write test: sensor returns `False` on in-progress status

---

## Phase 5 — Login Management Operators

> **Goal**: OAuth login link workflow from DAGs.
> **Context**: [arch:4.6]

- [x] Implement `SupermetricsCreateLoginLinkOperator` — push `login_id` + `link_url` to XCom
- [x] Implement `SupermetricsListLoginsOperator` — push serialized list to XCom
- [x] Implement `SupermetricsGetLoginOperator` — push login details to XCom
- [x] Write test: create login link returns expected XCom keys
- [x] Write test: list logins serialization
- [x] Write test: get login by ID

---

## Phase 6 — Deferrable Operators & Trigger

> **Goal**: Non-blocking backfill monitoring using Airflow Triggerer.
> **Context**: [arch:4.10], [arch:4.5] deferrable path. Validates [scenario:2] async path.

- [x] Implement `SupermetricsBackfillTrigger.serialize()` — JSON-primitives only → [arch:4.10]
- [x] Implement `SupermetricsBackfillTrigger.run()` — async poll loop → [arch:4.10]
- [x] Verify `SupermetricsAsyncClient` context manager protocol (`async with`)
- [x] Add deferrable path to `SupermetricsCreateBackfillOperator`:
  - [x] Extract API key from connection before deferral
  - [x] Call `self.defer(trigger=..., method_name="execute_complete")`
  - [x] Implement `execute_complete(context, event)` resume method
- [x] Write async test: trigger `serialize()` round-trip
- [x] Write async test: trigger `run()` yields success event
- [x] Write async test: trigger `run()` yields failure event
- [x] Write async test: trigger `run()` polls while in-progress
- [x] Write test: operator deferral path (mock `self.defer`)
- [x] Validate: [scenario:2] fully works end-to-end with deferrable=True

---

## Phase 7 — Transfer Operators

> **Goal**: Direct query-to-cloud-storage pipelines.
> **Context**: [arch:4.7], [arch:4.8]. Validates [scenario:1] (daily marketing report).

- [x] Implement `SupermetricsQueryToGCSOperator` → [arch:4.7]
  - [x] Guard import for `GCSHook` with helpful `ImportError` message
  - [x] Serialize rows to NDJSON (default) or CSV
  - [x] Stream rows chunk-by-chunk (avoid full memory materialization)
  - [x] Push GCS URI to XCom
- [x] Implement `SupermetricsQueryToS3Operator` → [arch:4.8]
  - [x] Guard import for `S3Hook` with helpful `ImportError` message
  - [x] Same serialization and streaming pattern as GCS
  - [x] Push S3 URI to XCom
- [x] Write test: GCS operator writes NDJSON to mocked GCS
- [x] Write test: GCS operator writes CSV to mocked GCS
- [x] Write test: GCS operator raises helpful error without google provider
- [x] Write test: S3 operator writes NDJSON to mocked S3
- [x] Write test: S3 operator raises helpful error without amazon provider
- [x] Validate: [scenario:1] fully works with GCS operator

---

## Phase 8 — Example DAGs

> **Goal**: Working reference DAGs for users.
> **Context**: Scenario files serve as design specs for these examples.

- [x] Write `example_dags/example_supermetrics_query.py` — mirrors [scenario:3] pattern
- [x] Write `example_dags/example_supermetrics_backfill.py` — mirrors [scenario:2] pattern
- [x] Write `example_dags/example_supermetrics_to_gcs.py` — mirrors [scenario:1] pattern
- [x] Write `example_dags/example_supermetrics_to_s3.py`
- [x] All DAGs: `schedule=None`, `start_date=datetime(2024,1,1)`, `supermetrics_default` conn
- [x] Verify `airflow dags list` parses all example DAGs without error

---

## Phase 9 — Documentation & Release

> **Goal**: README, CHANGELOG, build verification.

- [x] Write `README.md` — installation, connection setup, operator reference, examples
- [x] Write `CHANGELOG.md` with `0.1.0` section
- [x] Finalize `pyproject.toml` classifiers
- [x] Verify `python -m build` produces valid wheel and sdist
- [x] Verify `pip install dist/*.whl` in clean venv shows provider in `airflow providers list`
- [x] Update scenario files if any API surface changed during implementation
- [x] Final review: all checklist items in this file marked `[x]`

---

## Context Storage Reference

Where implementation context lives during development:

| Location | Purpose | When to Read | When to Update |
|----------|---------|--------------|----------------|
| `plan.md` (this file) | Task checklist | Before starting any phase | Check off completed items |
| [`docs/architecture.md`](docs/architecture.md) | Component specs, code examples, design decisions | When implementing a component (`[arch:X.Y]`) | When a design decision changes |
| [`docs/scenarios/01-*.md`](docs/scenarios/01-daily-cross-channel-marketing-report.md) | GCS transfer, parallel tasks, Jinja2 templating | Implementing Phase 7 | If API surface changes |
| [`docs/scenarios/02-*.md`](docs/scenarios/02-dwh-backfill-with-async-monitoring.md) | Deferrable backfills, triggers, sensors | Implementing Phases 4 & 6 | If API surface changes |
| [`docs/scenarios/03-*.md`](docs/scenarios/03-weekly-competitive-benchmarking.md) | Query operator, XCom, TaskFlow | Implementing Phase 3 | If API surface changes |
| [`CLAUDE.md`](CLAUDE.md) | Project instructions (auto-loaded by Claude Code) | Automatic | When conventions change |
