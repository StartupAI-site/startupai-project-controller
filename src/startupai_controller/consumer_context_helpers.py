"""Issue-context helper cluster extracted from board_consumer."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from startupai_controller.runtime.wiring import (
    build_github_port_bundle as _build_github_port_bundle,
)


def fetch_issue_context(
    owner: str,
    repo: str,
    number: int,
    *,
    build_github_port_bundle: Callable[..., Any] = _build_github_port_bundle,
    issue_context_port: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> dict[str, Any]:
    """Read issue title, body, and labels via the issue-context boundary."""
    port = issue_context_port or build_github_port_bundle(
        "",
        0,
        gh_runner=gh_runner,
    ).issue_context
    context = port.get_issue_context(owner, repo, number)
    return {
        "title": context.title,
        "body": context.body,
        "labels": list(context.labels),
        "updated_at": context.updated_at,
    }


def snapshot_for_issue(
    board_snapshot: Any,
    issue_ref: str,
    config: Any,
    *,
    snapshot_to_issue_ref: Callable[..., str | None],
) -> Any | None:
    """Return the thin board snapshot row for an issue ref."""
    for snapshot in board_snapshot.items:
        if snapshot_to_issue_ref(snapshot.issue_ref, config.issue_prefixes) == issue_ref:
            return snapshot
    return None


def issue_context_cache_is_fresh(
    cached: Any,
    *,
    snapshot_updated_at: str,
    now: datetime,
    parse_iso8601_timestamp: Callable[[str | None], datetime | None],
) -> bool:
    """Return True when cached issue context is safe to reuse."""
    if cached is None:
        return False
    expires_at = parse_iso8601_timestamp(getattr(cached, "expires_at", None))
    if expires_at is None or expires_at <= now:
        return False
    if snapshot_updated_at and getattr(cached, "issue_updated_at", "") != snapshot_updated_at:
        return False
    return True


def hydrate_issue_context(
    issue_ref: str,
    *,
    owner: str,
    repo: str,
    number: int,
    snapshot: Any | None,
    config: Any,
    db: Any,
    fetch_issue_context: Callable[..., dict[str, Any]],
    issue_context_cache_is_fresh: Callable[..., bool],
    record_metric: Callable[..., None],
    issue_context_port: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return locally ready issue context, refreshing the cache when needed."""
    current = now or datetime.now(timezone.utc)
    cached = db.get_issue_context(issue_ref) if config.issue_context_cache_enabled else None
    snapshot_updated_at = snapshot.issue_updated_at if snapshot is not None else ""
    if issue_context_cache_is_fresh(
        cached,
        snapshot_updated_at=snapshot_updated_at,
        now=current,
    ):
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
            "labels": cached.labels,
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
    context = fetch_issue_context(
        owner,
        repo,
        number,
        issue_context_port=issue_context_port,
        gh_runner=gh_runner,
    )
    context.setdefault("title", snapshot.title if snapshot is not None else f"issue-{number}")
    context.setdefault("body", "")
    labels = context.get("labels")
    if not isinstance(labels, list):
        labels = []
    context["labels"] = [str(label) for label in labels if str(label)]
    issue_updated_at = str(context.get("updated_at") or snapshot_updated_at or current.isoformat())
    context["updated_at"] = issue_updated_at
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
            title=str(context.get("title") or ""),
            body=str(context.get("body") or ""),
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
