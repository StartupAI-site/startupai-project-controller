"""Selection and retry wiring extracted from board_consumer."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

import startupai_controller.consumer_claim_helpers as _claim_helpers
from startupai_controller import consumer_selection_helpers as _selection_helpers
from startupai_controller.board_graph import _ready_snapshot_rank
from startupai_controller.domain.review_queue_policy import (
    effective_retry_backoff as _effective_retry_backoff_primitives,
    is_retryable_failure_reason as _is_retryable_failure_reason,
    session_retry_due_at as _session_retry_due_at,
)
from startupai_controller.runtime.wiring import build_github_port_bundle
from startupai_controller.domain.scheduling_policy import (
    snapshot_to_issue_ref as _snapshot_to_issue_ref,
)
from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    evaluate_ready_promotion,
    in_any_critical_path,
    parse_issue_ref,
)


class _CompatReviewStatePort:
    """Delegate snapshot reads while overriding status lookup for compatibility."""

    def __init__(
        self,
        base: Any,
        *,
        config: Any,
        project_owner: str,
        project_number: int,
        status_resolver: Callable[..., str],
    ) -> None:
        self._base = base
        self._config = config
        self._project_owner = project_owner
        self._project_number = project_number
        self._status_resolver = status_resolver

    def __getattr__(self, name: str) -> Any:
        return getattr(self._base, name)

    def get_issue_status(self, issue_ref: str) -> str:
        return self._status_resolver(
            issue_ref,
            self._config,
            self._project_owner,
            self._project_number,
        )


def effective_retry_backoff(
    config: Any,
    workflow: Any | None,
    *,
    effective_retry_backoff_primitives: Callable[
        ..., tuple[int, int]
    ] = _effective_retry_backoff_primitives,
) -> tuple[int, int]:
    """Return effective retry backoff (base, max) in seconds."""
    runtime = workflow.runtime if workflow is not None else None
    return effective_retry_backoff_primitives(
        base_seconds=(
            runtime.retry_backoff_base_seconds
            if runtime is not None and runtime.retry_backoff_base_seconds is not None
            else None
        ),
        max_seconds=(
            runtime.retry_backoff_seconds
            if runtime is not None and runtime.retry_backoff_seconds is not None
            else None
        ),
        config_base=config.retry_backoff_base_seconds,
        config_max=config.retry_backoff_seconds,
    )


def retry_backoff_active(
    db: Any,
    issue_ref: str,
    *,
    base_seconds: int,
    max_seconds: int,
    session_retry_due_at: Callable[..., Any] = _session_retry_due_at,
) -> bool:
    """Return True when a recent failed attempt is still cooling down."""
    latest = db.latest_session_for_issue(issue_ref)
    if latest is None:
        return False
    due_at = session_retry_due_at(
        latest,
        base_seconds=base_seconds,
        max_seconds=max_seconds,
    )
    if due_at is None:
        return False
    return datetime.now(timezone.utc) < due_at


def locally_review_owned_issue(
    db: Any,
    issue_ref: str,
) -> bool:
    """Return True when local session/review state still owns the issue."""
    get_review_queue_item = getattr(db, "get_review_queue_item", None)
    get_requeue_state = getattr(db, "get_requeue_state", None)
    latest_session_for_issue = getattr(db, "latest_session_for_issue", None)

    if get_review_queue_item is not None and get_requeue_state is not None:
        entry = get_review_queue_item(issue_ref)
        if entry is not None:
            count, stored_pr_url = get_requeue_state(issue_ref)
            if not (count > 0 and stored_pr_url == entry.pr_url):
                return True

    if latest_session_for_issue is None or get_requeue_state is None:
        return False
    latest = latest_session_for_issue(issue_ref)
    if latest is None or latest.status != "success" or not latest.pr_url:
        return False
    count, stored_pr_url = get_requeue_state(issue_ref)
    return not (count > 0 and stored_pr_url == latest.pr_url)


def next_retry_count(
    db: Any,
    issue_ref: str,
    *,
    current_session_id: str,
    failure_reason: str | None,
    is_retryable_failure_reason: Callable[[str | None], bool],
) -> int:
    """Return the next retry attempt count for a terminal session."""
    if not is_retryable_failure_reason(failure_reason):
        return 0
    previous = db.latest_session_for_issue(
        issue_ref,
        exclude_session_id=current_session_id,
    )
    if previous is None:
        return 1
    if previous.failure_reason is None:
        if previous.status not in {"failed", "timeout"}:
            return 1
        return max(previous.retry_count, 1) + 1
    if not is_retryable_failure_reason(previous.failure_reason):
        return 1
    return max(previous.retry_count, 1) + 1


def session_retry_state(
    session: Any,
    *,
    config: Any,
    workflows: dict[str, Any],
    parse_issue_ref: Callable[[str], Any],
    effective_retry_backoff: Callable[..., tuple[int, int]],
    session_retry_due_at: Callable[..., Any],
    retry_delay_seconds: Callable[..., int],
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return retry metadata for a session."""
    current = now or datetime.now(timezone.utc)
    repo_prefix = session.repo_prefix
    if repo_prefix is None:
        try:
            repo_prefix = parse_issue_ref(session.issue_ref).prefix
        except ValueError:
            repo_prefix = None
    workflow = workflows.get(repo_prefix) if repo_prefix is not None else None
    base_seconds, max_seconds = effective_retry_backoff(config, workflow)
    due_at = session_retry_due_at(
        session,
        base_seconds=base_seconds,
        max_seconds=max_seconds,
    )
    retry_delay_value: int | None = None
    retry_remaining_seconds: int | None = None
    if due_at is not None:
        retry_count = session.retry_count or 1
        retry_delay_value = retry_delay_seconds(
            retry_count,
            base_seconds=base_seconds,
            max_seconds=max_seconds,
        )
        retry_remaining_seconds = max(0, int((due_at - current).total_seconds()))
    return {
        "failure_reason": session.failure_reason,
        "retry_count": session.retry_count,
        "retryable": due_at is not None,
        "retry_backoff_base_seconds": base_seconds,
        "retry_backoff_max_seconds": max_seconds,
        "retry_delay_seconds": retry_delay_value,
        "next_retry_at": due_at.isoformat() if due_at is not None else None,
        "retry_remaining_seconds": retry_remaining_seconds,
    }


def select_best_candidate(
    config: Any,
    project_owner: str,
    project_number: int,
    *,
    executor: str = "codex",
    this_repo_prefix: str | None = None,
    repo_prefixes: tuple[str, ...] = ("crew",),
    review_state_port: Any | None = None,
    status_resolver: Callable[..., str] | None = None,
    ready_items: tuple[Any, ...] | None = None,
    github_memo: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
    issue_filter: Callable[[str], bool] | None = None,
    build_github_port_bundle: Callable[..., Any],
    parse_issue_ref: Callable[[str], Any],
    config_error_type: type[Exception],
    snapshot_to_issue_ref: Callable[[str, Any], str | None],
    in_any_critical_path: Callable[[Any, str], bool],
    evaluate_ready_promotion: Callable[..., tuple[int, str | None]],
    ready_snapshot_rank: Callable[[Any, Any], Any],
) -> str | None:
    """Select the highest-ranked ready issue for the executor."""
    if status_resolver is not None:
        base_review_state_port = (
            review_state_port
            or build_github_port_bundle(
                project_owner,
                project_number,
                config=config,
                github_memo=github_memo,
                gh_runner=gh_runner,
            ).review_state
        )
        review_state_port = _CompatReviewStatePort(
            base_review_state_port,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            status_resolver=status_resolver,
        )
    return _selection_helpers.select_best_candidate(
        config,
        project_owner,
        project_number,
        executor=executor,
        this_repo_prefix=this_repo_prefix,
        repo_prefixes=repo_prefixes,
        review_state_port=review_state_port,
        ready_items=ready_items,
        github_memo=github_memo,
        gh_runner=gh_runner,
        issue_filter=issue_filter,
        build_github_port_bundle=build_github_port_bundle,
        parse_issue_ref=parse_issue_ref,
        config_error_type=config_error_type,
        snapshot_to_issue_ref=snapshot_to_issue_ref,
        in_any_critical_path=in_any_critical_path,
        evaluate_ready_promotion=evaluate_ready_promotion,
        ready_snapshot_rank=ready_snapshot_rank,
    )


def list_project_items_by_status(
    status: str,
    project_owner: str,
    project_number: int,
    *,
    config: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
    build_github_port_bundle: Callable[..., Any],
) -> list[Any]:
    """Read board items for one status through the review-state port."""
    return _selection_helpers.list_project_items_by_status(
        status,
        project_owner,
        project_number,
        config=config,
        gh_runner=gh_runner,
        build_github_port_bundle=build_github_port_bundle,
    )


def select_candidate_for_cycle(
    config: Any,
    db: Any,
    prepared: Any,
    *,
    target_issue: str | None = None,
    review_state_port: Any | None = None,
    status_resolver: Callable[..., str] | None = None,
    gh_runner: Callable[..., str] | None = None,
    excluded_issue_refs: set[str] | None = None,
    parse_issue_ref: Callable[[str], Any],
    effective_retry_backoff: Callable[..., tuple[int, int]],
    retry_backoff_active: Callable[..., bool],
    locally_review_owned_issue: Callable[[Any, str], bool],
    select_best_candidate: Callable[..., str | None],
) -> str | None:
    """Select the next eligible issue for one slot in this cycle."""
    return _claim_helpers.select_candidate_for_cycle(
        config,
        db,
        prepared,
        target_issue=target_issue,
        review_state_port=review_state_port,
        status_resolver=status_resolver,
        gh_runner=gh_runner,
        excluded_issue_refs=excluded_issue_refs,
        parse_issue_ref=parse_issue_ref,
        effective_retry_backoff=effective_retry_backoff,
        retry_backoff_active=retry_backoff_active,
        locally_review_owned_issue=locally_review_owned_issue,
        select_best_candidate=select_best_candidate,
    )


def select_launch_candidate_for_cycle(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    target_issue: str | None,
    review_state_port: Any | None,
    status_resolver: Callable[..., str] | None,
    gh_runner: Callable[..., str] | None,
    cycle_result_factory: Callable[..., Any],
    selected_launch_candidate_factory: Callable[..., Any],
    select_candidate_for_cycle: Callable[..., str | None],
    parse_issue_ref: Callable[[str], Any],
    effective_retry_backoff: Callable[..., tuple[int, int]],
    retry_backoff_active: Callable[..., bool],
    maybe_activate_claim_suppression: Callable[..., bool],
    mark_degraded: Callable[..., None],
    gh_reason_code: Callable[..., str],
    gh_query_error_type: type[Exception],
    logger: Any,
) -> tuple[Any | None, Any | None]:
    """Select a launch candidate and validate its immediate launchability."""
    return _claim_helpers.select_launch_candidate_for_cycle(
        config=config,
        db=db,
        prepared=prepared,
        target_issue=target_issue,
        review_state_port=review_state_port,
        status_resolver=status_resolver,
        gh_runner=gh_runner,
        cycle_result_factory=cycle_result_factory,
        selected_launch_candidate_factory=selected_launch_candidate_factory,
        select_candidate_for_cycle=select_candidate_for_cycle,
        parse_issue_ref=parse_issue_ref,
        effective_retry_backoff=effective_retry_backoff,
        retry_backoff_active=retry_backoff_active,
        maybe_activate_claim_suppression=maybe_activate_claim_suppression,
        mark_degraded=mark_degraded,
        gh_reason_code=gh_reason_code,
        gh_query_error_type=gh_query_error_type,
        logger=logger,
    )


def select_best_candidate_from_shell(
    config: Any,
    project_owner: str,
    project_number: int,
    *,
    executor: str = "codex",
    this_repo_prefix: str | None = None,
    repo_prefixes: tuple[str, ...] = ("crew",),
    automation_config: Any | None = None,
    review_state_port: Any | None = None,
    status_resolver: Callable[..., str] | None = None,
    ready_items: tuple[Any, ...] | None = None,
    github_memo: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
    issue_filter: Callable[[str], bool] | None = None,
) -> str | None:
    """Select the highest-ranked ready issue for the consumer shell."""
    del automation_config
    return select_best_candidate(
        config,
        project_owner,
        project_number,
        executor=executor,
        this_repo_prefix=this_repo_prefix,
        repo_prefixes=repo_prefixes,
        review_state_port=review_state_port,
        status_resolver=status_resolver,
        ready_items=ready_items,
        github_memo=github_memo,
        gh_runner=gh_runner,
        issue_filter=issue_filter,
        build_github_port_bundle=build_github_port_bundle,
        parse_issue_ref=parse_issue_ref,
        config_error_type=ConfigError,
        snapshot_to_issue_ref=_snapshot_to_issue_ref,
        in_any_critical_path=in_any_critical_path,
        evaluate_ready_promotion=evaluate_ready_promotion,
        ready_snapshot_rank=_ready_snapshot_rank,
    )


def select_candidate_for_cycle_from_shell(
    config: Any,
    db: Any,
    prepared: Any,
    *,
    target_issue: str | None = None,
    review_state_port: Any | None = None,
    status_resolver: Callable[..., str] | None = None,
    gh_runner: Callable[..., str] | None = None,
    excluded_issue_refs: set[str] | None = None,
) -> str | None:
    """Select the next eligible issue for one consumer cycle slot."""
    return select_candidate_for_cycle(
        config,
        db,
        prepared,
        target_issue=target_issue,
        review_state_port=review_state_port,
        status_resolver=status_resolver,
        gh_runner=gh_runner,
        excluded_issue_refs=excluded_issue_refs,
        parse_issue_ref=parse_issue_ref,
        effective_retry_backoff=effective_retry_backoff,
        retry_backoff_active=retry_backoff_active,
        locally_review_owned_issue=locally_review_owned_issue,
        select_best_candidate=select_best_candidate_from_shell,
    )
