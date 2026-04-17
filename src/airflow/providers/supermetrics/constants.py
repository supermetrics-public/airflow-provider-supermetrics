"""Constants for Supermetrics provider."""

from __future__ import annotations

TERMINAL_SUCCESS_STATUSES: frozenset[str] = frozenset({"COMPLETED"})
TERMINAL_FAILURE_STATUSES: frozenset[str] = frozenset({"FAILED", "CANCELLED"})
TERMINAL_STATUSES: frozenset[str] = TERMINAL_SUCCESS_STATUSES | TERMINAL_FAILURE_STATUSES
