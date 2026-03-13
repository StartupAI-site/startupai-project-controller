"""Shared machine-readable payload types used across controller layers."""

from __future__ import annotations

from typing import TypeAlias, TypedDict

ObjectPayload: TypeAlias = dict[str, object]
AdmissionSummaryPayload: TypeAlias = ObjectPayload
MetricPayload: TypeAlias = ObjectPayload
StatusPayload: TypeAlias = ObjectPayload
WorkflowStatusPayload: TypeAlias = ObjectPayload


class ControlPlaneHealthPayload(TypedDict):
    """Stable machine-readable control-plane health summary."""

    health: str
    reason_code: str


class SessionRetryStatePayload(TypedDict):
    """Retry metadata emitted for recent-session status surfaces."""

    failure_reason: str | None
    retry_count: int
    retryable: bool
    retry_backoff_base_seconds: int
    retry_backoff_max_seconds: int
    retry_delay_seconds: int | None
    next_retry_at: str | None
    retry_remaining_seconds: int | None
