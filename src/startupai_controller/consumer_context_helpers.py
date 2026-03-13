"""Issue-context helper cluster extracted from board_consumer."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable, Protocol, Sequence

from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_runtime_support_wiring import MetricRecorderPort
from startupai_controller.consumer_types import IssueContextPayload
from startupai_controller.domain.models import CycleBoardSnapshot, ProjectItemSnapshot
from startupai_controller.ports.issue_context import IssueContextPort
from startupai_controller.runtime.wiring import (
    GitHubPortBundle,
    build_github_port_bundle as _build_github_port_bundle,
)
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig

MetricPayload = dict[str, object]


class SnapshotToIssueRefFn(Protocol):
    """Resolve a board snapshot issue ref into the canonical issue ref."""

    def __call__(
        self,
        issue_ref: str,
        issue_prefixes: dict[str, str],
    ) -> str | None: ...


class IssueContextCacheEntryPort(Protocol):
    """Cached issue-context fields consumed by the launch helpers."""

    title: str
    body: str
    issue_updated_at: str
    expires_at: str
    labels: Sequence[str]


class IssueContextCacheStorePort(MetricRecorderPort, Protocol):
    """Persistence surface used by issue-context hydration."""

    def get_issue_context(
        self,
        issue_ref: str,
    ) -> IssueContextCacheEntryPort | None: ...

    def set_issue_context(
        self,
        issue_ref: str,
        *,
        owner: str,
        repo: str,
        number: int,
        title: str,
        body: str,
        labels: list[str],
        issue_updated_at: str,
        fetched_at: str,
        expires_at: str,
    ) -> None: ...


class RecordIssueContextMetricFn(Protocol):
    """Metric recorder callback used during cache hydration."""

    def __call__(
        self,
        db: IssueContextCacheStorePort,
        config: ConsumerConfig,
        metric_name: str,
        *,
        issue_ref: str | None = None,
        payload: MetricPayload | None = None,
        now: datetime | None = None,
    ) -> None: ...


def fetch_issue_context(
    owner: str,
    repo: str,
    number: int,
    *,
    build_github_port_bundle: Callable[
        ..., GitHubPortBundle
    ] = _build_github_port_bundle,
    issue_context_port: IssueContextPort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> IssueContextPayload:
    """Read issue title, body, and labels via the issue-context boundary."""
    port = (
        issue_context_port
        or build_github_port_bundle(
            "",
            0,
            gh_runner=gh_runner,
        ).issue_context
    )
    context = port.get_issue_context(owner, repo, number)
    return {
        "title": context.title,
        "body": context.body,
        "labels": list(context.labels),
        "updated_at": context.updated_at,
    }


def snapshot_for_issue(
    board_snapshot: CycleBoardSnapshot,
    issue_ref: str,
    config: CriticalPathConfig,
    *,
    snapshot_to_issue_ref: SnapshotToIssueRefFn,
) -> ProjectItemSnapshot | None:
    """Return the thin board snapshot row for an issue ref."""
    for snapshot in board_snapshot.items:
        if (
            snapshot_to_issue_ref(snapshot.issue_ref, config.issue_prefixes)
            == issue_ref
        ):
            return snapshot
    return None


def issue_context_cache_is_fresh(
    cached: IssueContextCacheEntryPort | None,
    *,
    snapshot_updated_at: str,
    now: datetime,
    parse_iso8601_timestamp: Callable[[str | None], datetime | None],
) -> bool:
    """Return True when cached issue context is safe to reuse."""
    if cached is None:
        return False
    expires_at = parse_iso8601_timestamp(cached.expires_at)
    if expires_at is None or expires_at <= now:
        return False
    if snapshot_updated_at and cached.issue_updated_at != snapshot_updated_at:
        return False
    return True


def hydrate_issue_context(
    issue_ref: str,
    *,
    owner: str,
    repo: str,
    number: int,
    snapshot: ProjectItemSnapshot | None,
    config: ConsumerConfig,
    db: IssueContextCacheStorePort,
    fetch_issue_context: Callable[..., IssueContextPayload],
    issue_context_cache_is_fresh: Callable[..., bool],
    record_metric: RecordIssueContextMetricFn,
    issue_context_port: IssueContextPort | None = None,
    gh_runner: Callable[..., str] | None = None,
    now: datetime | None = None,
) -> IssueContextPayload:
    """Return locally ready issue context, refreshing the cache when needed."""
    current = now or datetime.now(timezone.utc)
    cached = (
        db.get_issue_context(issue_ref) if config.issue_context_cache_enabled else None
    )
    snapshot_updated_at = snapshot.issue_updated_at if snapshot is not None else ""
    if issue_context_cache_is_fresh(
        cached,
        snapshot_updated_at=snapshot_updated_at,
        now=current,
    ):
        assert cached is not None
        record_metric(
            db,
            config,
            "context_cache_hit",
            issue_ref=issue_ref,
            now=current,
        )
        return {
            "title": cached.title,
            "body": cached.body,
            "labels": [str(label) for label in cached.labels if str(label)],
            "updated_at": cached.issue_updated_at,
        }

    record_metric(
        db,
        config,
        "context_cache_miss",
        issue_ref=issue_ref,
        payload={"stale": cached is not None},
        now=current,
    )
    record_metric(
        db,
        config,
        "context_hydration_started",
        issue_ref=issue_ref,
        now=current,
    )
    fetched_context = fetch_issue_context(
        owner,
        repo,
        number,
        issue_context_port=issue_context_port,
        gh_runner=gh_runner,
    )
    issue_updated_at = (
        fetched_context["updated_at"] or snapshot_updated_at or current.isoformat()
    )
    context: IssueContextPayload = {
        "title": fetched_context["title"]
        or (snapshot.title if snapshot is not None else f"issue-{number}"),
        "body": fetched_context["body"] or "",
        "labels": [str(label) for label in fetched_context["labels"] if str(label)],
        "updated_at": issue_updated_at,
    }
    fetched_at = current.isoformat()
    expires_at = (
        current + timedelta(seconds=config.issue_context_cache_ttl_seconds)
    ).isoformat()
    if config.issue_context_cache_enabled:
        db.set_issue_context(
            issue_ref,
            owner=owner,
            repo=repo,
            number=number,
            title=context["title"],
            body=context["body"],
            labels=list(context["labels"]),
            issue_updated_at=issue_updated_at,
            fetched_at=fetched_at,
            expires_at=expires_at,
        )
    record_metric(
        db,
        config,
        "context_hydration_succeeded",
        issue_ref=issue_ref,
        payload={"cached_until": expires_at},
        now=current,
    )
    return context
