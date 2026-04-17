# CLAUDE.md — Project Instructions for Claude Code

## Project Overview
Apache Airflow provider wrapping the Supermetrics Python SDK (`supermetrics>=0.2.0b2`).
Package name: `airflow-provider-supermetrics`. Namespace: `airflow.providers.supermetrics`.

## Key Constraints
- Python >= 3.11 (target 3.12)
- Airflow >= 2.7.0
- SDK is beta (`0.2.0b2`) — pin `>=0.2.0b2,<0.3.0`
- Follow official Airflow provider conventions (entry points, hook registration, connection UI)

## Project Layout
Uses `src/` layout to avoid namespace conflicts with installed Airflow:
- Source code: `src/airflow/providers/supermetrics/`
- Tests: `tests/providers/supermetrics/`
- Virtual environment: `.venv/` (run tests with `.venv/bin/python -m pytest`)
- `src/airflow/__init__.py` and `src/airflow/providers/__init__.py` must NOT exist (implicit namespace packages)

## Tooling
- Formatter: `black` (line-length 100)
- Linter: `ruff` (target-version py311, line-length 100)
- Type checker: `mypy` (strict mode)
- Testing: `pytest` + `pytest-asyncio` for trigger tests
- Package manager: `uv`

## Code Conventions
- All public APIs must have type annotations
- All operators must declare `template_fields` for Jinja2 support
- XCom payloads must be JSON-serializable (use `model.model_dump()` for Pydantic objects)
- Mock all SDK client calls in tests — never make real HTTP requests
- Transfer operators guard-import cloud provider dependencies inside `execute()`
- Terminal backfill statuses defined in `constants.py` as frozensets — single source of truth

## Context Files
- `plan.md` — master task checklist (check off items as you complete them)
- `docs/architecture.md` — component specifications, design decisions, code examples
- `docs/scenarios/` — 3 real-world DAG scenarios validating provider design

## Implementation Order
Phases 1-4 = MVP (scaffold, hook, query operator, backfill lifecycle).
Phases 5-9 = hardening (logins, deferrables, transfers, example DAGs, docs).
Each phase produces testable, shippable code. See `plan.md` for the full checklist.
