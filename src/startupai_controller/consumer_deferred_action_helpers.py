"""Deferred-action replay helpers extracted from board_consumer."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Callable, Protocol, TypeAlias

from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.domain.repair_policy import parse_pr_url
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.control_plane_state import ControlValueStorePort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
)

GhRunner = Callable[..., str]
BoardInfoResolver = Callable[..., object]
BoardMutator = Callable[..., None]
CommentChecker = Callable[..., bool]
CommentPoster = Callable[..., None]
DeferredActionPayload: TypeAlias = Mapping[str, object]


class DeferredActionView(Protocol):
    """Minimal deferred-action row surface needed for replay."""

    id: int
    action_type: str

    @property
    def payload(self) -> DeferredActionPayload:
        """Return the stored JSON payload decoded into Python values."""
        ...


class DeferredActionStorePort(ControlValueStorePort, Protocol):
    """Persistence surface needed to replay and delete deferred actions."""

    def enqueue_review_item(
        self,
        issue_ref: str,
        *,
        pr_url: str,
        pr_repo: str,
        pr_number: int,
        source_session_id: str | None = None,
        next_attempt_at: str | None = None,
        now: datetime | None = None,
    ) -> None:
        """Persist one review-queue entry for later rescue processing."""
        ...

    def list_deferred_actions(self) -> list[DeferredActionView]:
        """Return deferred actions in replay order."""
        ...

    def delete_deferred_action(self, action_id: int) -> None:
        """Delete one deferred action after a successful replay."""
        ...


def _payload_string(payload: DeferredActionPayload, key: str) -> str:
    """Return one payload value coerced to string."""
    return str(payload[key])


def _payload_string_set(payload: DeferredActionPayload, key: str) -> set[str]:
    """Return a set of stringified payload values for one sequence field."""
    raw = payload.get(key)
    if isinstance(raw, (list, tuple, set, frozenset)):
        return {str(value) for value in raw}
    return set()


def _payload_int(payload: DeferredActionPayload, key: str) -> int:
    """Return one payload value coerced to int."""
    value = payload[key]
    if isinstance(value, int):
        return int(value)
    return int(str(value))


def replay_deferred_status_action(
    *,
    payload: DeferredActionPayload,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    review_state_port: ReviewStatePort | None,
    board_port: BoardMutationPort | None,
    board_info_resolver: BoardInfoResolver | None,
    board_mutator: BoardMutator | None,
    gh_runner: GhRunner | None,
    set_blocked_with_reason: Callable[..., None],
    transition_issue_to_review: Callable[..., None],
    transition_issue_to_in_progress: Callable[..., None],
    return_issue_to_ready: Callable[..., None],
) -> None:
    """Replay a deferred issue status transition."""
    issue_ref = _payload_string(payload, "issue_ref")
    to_status = _payload_string(payload, "to_status")
    from_statuses = _payload_string_set(payload, "from_statuses")
    blocked_reason = payload.get("blocked_reason")
    if to_status == "Blocked":
        set_blocked_with_reason(
            issue_ref,
            str(blocked_reason or "deferred-control-plane"),
            critical_path_config,
            config.project_owner,
            config.project_number,
            gh_runner=gh_runner,
        )
        return
    if to_status == "Review":
        transition_issue_to_review(
            issue_ref,
            critical_path_config,
            config.project_owner,
            config.project_number,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        return
    if to_status == "In Progress":
        transition_issue_to_in_progress(
            issue_ref,
            critical_path_config,
            config.project_owner,
            config.project_number,
            from_statuses=from_statuses,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        return
    if to_status == "Ready":
        return_issue_to_ready(
            issue_ref,
            critical_path_config,
            config.project_owner,
            config.project_number,
            from_statuses=from_statuses,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        return
    raise GhQueryError(f"Unsupported deferred status target: {to_status}")


def replay_deferred_verdict_marker(
    *,
    payload: DeferredActionPayload,
    comment_checker: CommentChecker | None,
    comment_poster: CommentPoster | None,
    gh_runner: GhRunner | None,
    post_pr_codex_verdict: Callable[..., bool],
) -> None:
    """Replay a deferred PR verdict marker post."""
    post_pr_codex_verdict(
        _payload_string(payload, "pr_url"),
        _payload_string(payload, "session_id"),
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def replay_deferred_issue_comment(
    *,
    payload: DeferredActionPayload,
    critical_path_config: CriticalPathConfig,
    board_port: BoardMutationPort | None,
    comment_poster: CommentPoster | None,
    gh_runner: GhRunner | None,
    resolve_issue_coordinates: Callable[
        [str, CriticalPathConfig], tuple[str, str, int]
    ],
    runtime_comment_poster: Callable[..., None],
) -> None:
    """Replay a deferred issue comment post."""
    issue_ref = _payload_string(payload, "issue_ref")
    owner, repo, number = resolve_issue_coordinates(issue_ref, critical_path_config)
    body = _payload_string(payload, "body")
    if board_port is not None:
        board_port.post_issue_comment(f"{owner}/{repo}", number, body)
        return
    poster = comment_poster or runtime_comment_poster
    poster(owner, repo, number, body, gh_runner=gh_runner)


def replay_deferred_issue_close(
    *,
    payload: DeferredActionPayload,
    critical_path_config: CriticalPathConfig,
    board_port: BoardMutationPort | None,
    gh_runner: GhRunner | None,
    resolve_issue_coordinates: Callable[
        [str, CriticalPathConfig], tuple[str, str, int]
    ],
    runtime_issue_closer: Callable[..., None],
) -> None:
    """Replay a deferred issue close."""
    issue_ref = _payload_string(payload, "issue_ref")
    owner, repo, number = resolve_issue_coordinates(issue_ref, critical_path_config)
    if board_port is not None:
        board_port.close_issue(f"{owner}/{repo}", number)
        return
    runtime_issue_closer(owner, repo, number, gh_runner=gh_runner)


def replay_deferred_check_rerun(
    *,
    payload: DeferredActionPayload,
    pr_port: PullRequestPort | None,
    gh_runner: GhRunner | None,
    runtime_failed_check_rerun: Callable[..., None],
) -> None:
    """Replay a deferred failed-check rerun request."""
    pr_repo = _payload_string(payload, "pr_repo")
    check_name = str(payload.get("check_name") or "")
    run_id = _payload_int(payload, "run_id")
    if pr_port is not None:
        if not pr_port.rerun_failed_check(pr_repo, check_name, run_id):
            raise GhQueryError(f"Failed rerunning check for {pr_repo} run {run_id}")
        return
    runtime_failed_check_rerun(pr_repo, run_id, gh_runner=gh_runner)


def replay_deferred_automerge_enable(
    *,
    payload: DeferredActionPayload,
    pr_port: PullRequestPort | None,
    gh_runner: GhRunner | None,
    runtime_automerge_enabler: Callable[..., None],
) -> None:
    """Replay a deferred auto-merge enablement."""
    pr_repo = _payload_string(payload, "pr_repo")
    pr_number = _payload_int(payload, "pr_number")
    if pr_port is not None:
        pr_port.enable_automerge(pr_repo, pr_number)
        return
    runtime_automerge_enabler(pr_repo, pr_number, gh_runner=gh_runner)


def replay_deferred_review_enqueue(
    *,
    payload: DeferredActionPayload,
    db: DeferredActionStorePort,
) -> None:
    """Replay a deferred review-queue persistence request."""
    issue_ref = _payload_string(payload, "issue_ref")
    pr_url = _payload_string(payload, "pr_url")
    session_id = payload.get("session_id")
    parsed = parse_pr_url(pr_url)
    if parsed is None:
        raise GhQueryError(f"Invalid deferred review PR URL: {pr_url}")
    owner, repo, pr_number = parsed
    now = datetime.now(timezone.utc)
    db.enqueue_review_item(
        issue_ref,
        pr_url=pr_url,
        pr_repo=f"{owner}/{repo}",
        pr_number=pr_number,
        source_session_id=(str(session_id) if session_id is not None else None),
        next_attempt_at=now.isoformat(),
        now=now,
    )


def replay_deferred_action(
    *,
    action: DeferredActionView,
    db: DeferredActionStorePort,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    pr_port: PullRequestPort | None,
    review_state_port: ReviewStatePort | None,
    board_port: BoardMutationPort | None,
    board_info_resolver: BoardInfoResolver | None,
    board_mutator: BoardMutator | None,
    comment_checker: CommentChecker | None,
    comment_poster: CommentPoster | None,
    gh_runner: GhRunner | None,
    set_blocked_with_reason: Callable[..., None],
    transition_issue_to_review: Callable[..., None],
    transition_issue_to_in_progress: Callable[..., None],
    return_issue_to_ready: Callable[..., None],
    post_pr_codex_verdict: Callable[..., bool],
    resolve_issue_coordinates: Callable[
        [str, CriticalPathConfig], tuple[str, str, int]
    ],
    runtime_comment_poster: Callable[..., None],
    runtime_issue_closer: Callable[..., None],
    runtime_failed_check_rerun: Callable[..., None],
    runtime_automerge_enabler: Callable[..., None],
) -> None:
    """Execute one deferred control-plane action."""
    payload = action.payload
    if action.action_type == "set_status":
        replay_deferred_status_action(
            payload=payload,
            config=config,
            critical_path_config=critical_path_config,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
            set_blocked_with_reason=set_blocked_with_reason,
            transition_issue_to_review=transition_issue_to_review,
            transition_issue_to_in_progress=transition_issue_to_in_progress,
            return_issue_to_ready=return_issue_to_ready,
        )
        return
    if action.action_type == "post_verdict_marker":
        replay_deferred_verdict_marker(
            payload=payload,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
            post_pr_codex_verdict=post_pr_codex_verdict,
        )
        return
    if action.action_type == "post_issue_comment":
        replay_deferred_issue_comment(
            payload=payload,
            critical_path_config=critical_path_config,
            board_port=board_port,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
            resolve_issue_coordinates=resolve_issue_coordinates,
            runtime_comment_poster=runtime_comment_poster,
        )
        return
    if action.action_type == "close_issue":
        replay_deferred_issue_close(
            payload=payload,
            critical_path_config=critical_path_config,
            board_port=board_port,
            gh_runner=gh_runner,
            resolve_issue_coordinates=resolve_issue_coordinates,
            runtime_issue_closer=runtime_issue_closer,
        )
        return
    if action.action_type == "rerun_check":
        replay_deferred_check_rerun(
            payload=payload,
            pr_port=pr_port,
            gh_runner=gh_runner,
            runtime_failed_check_rerun=runtime_failed_check_rerun,
        )
        return
    if action.action_type == "enable_automerge":
        replay_deferred_automerge_enable(
            payload=payload,
            pr_port=pr_port,
            gh_runner=gh_runner,
            runtime_automerge_enabler=runtime_automerge_enabler,
        )
        return
    if action.action_type == "enqueue_review_item":
        replay_deferred_review_enqueue(payload=payload, db=db)
        return
    raise GhQueryError(f"Unsupported deferred action type: {action.action_type}")


def replay_deferred_actions(
    db: DeferredActionStorePort,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    *,
    pr_port: PullRequestPort | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    board_info_resolver: BoardInfoResolver | None = None,
    board_mutator: BoardMutator | None = None,
    comment_checker: CommentChecker | None = None,
    comment_poster: CommentPoster | None = None,
    gh_runner: GhRunner | None = None,
    replay_deferred_action: Callable[..., None],
    record_successful_github_mutation: Callable[[ControlValueStorePort], None],
    clear_degraded: Callable[[ControlValueStorePort], None],
) -> tuple[int, ...]:
    """Replay queued control-plane actions after GitHub recovery."""
    replayed: list[int] = []
    for action in db.list_deferred_actions():
        try:
            replay_deferred_action(
                action=action,
                db=db,
                config=config,
                critical_path_config=critical_path_config,
                pr_port=pr_port,
                review_state_port=review_state_port,
                board_port=board_port,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                comment_checker=comment_checker,
                comment_poster=comment_poster,
                gh_runner=gh_runner,
            )
        except GhQueryError:
            raise
        except Exception as error:
            raise GhQueryError(
                f"Deferred action {action.id} failed: {error}"
            ) from error

        db.delete_deferred_action(action.id)
        record_successful_github_mutation(db)
        replayed.append(action.id)

    if replayed:
        clear_degraded(db)
    return tuple(replayed)
