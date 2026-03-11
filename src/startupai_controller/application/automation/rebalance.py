"""In-progress rebalance use case."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from startupai_controller.board_automation_config import (
    BoardAutomationConfig,
    DEFAULT_REBALANCE_CYCLE_MINUTES,
)
from startupai_controller.board_graph import _issue_sort_key, _resolve_issue_coordinates
from startupai_controller.domain.models import IssueSnapshot, RebalanceDecision
from startupai_controller.domain.repair_policy import (
    MARKER_PREFIX,
    marker_for as _marker_for,
    parse_pr_url as _parse_pr_url,
)
from startupai_controller.domain.scheduling_policy import VALID_EXECUTORS, wip_limit_for_lane
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    direct_predecessors,
    parse_issue_ref,
)


@dataclass(frozen=True)
class _WipCandidate:
    ref: str
    owner: str
    repo: str
    number: int
    lane: str
    executor: str
    activity_at: datetime
    has_open_pr: bool


def _handle_dependency_blocked_rebalance_item(
    ref: str,
    *,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    dry_run: bool,
    decision: RebalanceDecision,
    review_state_port: ReviewStatePort | None,
    board_port: BoardMutationPort | None,
    board_info_resolver,
    board_mutator,
    gh_runner,
    set_blocked_with_reason: Callable[..., None],
    set_handoff_target: Callable[..., None],
) -> BoardMutationPort | None:
    """Route a dependency-blocked WIP item to Blocked with claude handoff."""
    preds = ",".join(sorted(direct_predecessors(config, ref)))
    reason = f"dependency-unmet:{preds}"
    if not dry_run:
        set_blocked_with_reason(
            ref,
            reason,
            config,
            project_owner,
            project_number,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        set_handoff_target(
            ref,
            "claude",
            config,
            project_owner,
            project_number,
            review_state_port=review_state_port,
            board_port=board_port,
            gh_runner=gh_runner,
        )
    decision.moved_blocked.append(ref)
    return board_port


def _handle_stale_rebalance_item(
    ref: str,
    *,
    owner: str,
    repo: str,
    number: int,
    now: datetime,
    activity_at: datetime | None,
    confirm_delay: timedelta,
    project_owner: str,
    project_number: int,
    config: CriticalPathConfig,
    dry_run: bool,
    decision: RebalanceDecision,
    review_state_port: ReviewStatePort | None,
    board_port: BoardMutationPort | None,
    board_info_resolver,
    board_mutator,
    gh_runner,
    query_latest_marker_timestamp: Callable[..., datetime | None],
    comment_exists: Callable[..., bool],
    ensure_board_port: Callable[[BoardMutationPort | None], BoardMutationPort],
    transition_issue_status: Callable[..., tuple[bool, str]],
) -> tuple[bool, BoardMutationPort | None]:
    """Mark a stale item or demote it back to Ready after confirmation."""
    stale_prefix = f"<!-- {MARKER_PREFIX}:stale-candidate:{ref}"
    stale_ts = query_latest_marker_timestamp(
        owner,
        repo,
        number,
        stale_prefix,
        gh_runner=gh_runner,
    )
    if (
        stale_ts is None
        or (activity_at is not None and activity_at > stale_ts)
        or (now - stale_ts) < confirm_delay
    ):
        decision.marked_stale.append(ref)
        if not dry_run and stale_ts is None:
            stale_marker = (
                f"<!-- {MARKER_PREFIX}:stale-candidate:{ref}:"
                f"{now.strftime('%Y-%m-%dT%H:%M:%SZ')} -->"
            )
            if not comment_exists(owner, repo, number, stale_marker, gh_runner=gh_runner):
                board_port = ensure_board_port(board_port)
                board_port.post_issue_comment(
                    f"{owner}/{repo}",
                    number,
                    (
                        f"{stale_marker}\n"
                        "Stale candidate detected; will demote on next cycle if still inactive."
                    ),
                )
        return True, board_port

    changed, _old = transition_issue_status(
        ref,
        {"In Progress"},
        "Ready",
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    if changed and not dry_run:
        demote_marker = _marker_for("stale-demote", ref)
        if not comment_exists(owner, repo, number, demote_marker, gh_runner=gh_runner):
            board_port = ensure_board_port(board_port)
            board_port.post_issue_comment(
                f"{owner}/{repo}",
                number,
                (
                    f"{demote_marker}\n"
                    "Moved back to `Ready` after consecutive stale cycles "
                    "with no PR and no fresh activity."
                ),
            )
    decision.moved_ready.append(ref)
    return True, board_port


def _evaluate_rebalance_snapshot(
    snapshot: IssueSnapshot,
    *,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    this_repo_prefix: str | None,
    all_prefixes: bool,
    freshness_cutoff: datetime,
    confirm_delay: timedelta,
    now: datetime,
    dry_run: bool,
    decision: RebalanceDecision,
    review_state_port: ReviewStatePort | None,
    board_port: BoardMutationPort | None,
    board_info_resolver,
    board_mutator,
    status_resolver,
    gh_runner,
    is_graph_member: Callable[..., bool],
    ready_promotion_evaluator: Callable[..., tuple[int, str]],
    set_blocked_with_reason: Callable[..., None],
    set_handoff_target: Callable[..., None],
    query_project_item_field: Callable[..., str],
    query_open_pr_updated_at: Callable[..., datetime | None],
    query_latest_wip_activity_timestamp: Callable[..., datetime | None],
    query_latest_marker_timestamp: Callable[..., datetime | None],
    comment_exists: Callable[..., bool],
    ensure_board_port: Callable[[BoardMutationPort | None], BoardMutationPort],
    transition_issue_status: Callable[..., tuple[bool, str]],
) -> tuple[_WipCandidate | None, BoardMutationPort | None]:
    """Evaluate one in-progress item for dependency, staleness, and lane placement."""
    ref = snapshot.issue_ref
    parsed = parse_issue_ref(ref)
    if not all_prefixes and this_repo_prefix and parsed.prefix != this_repo_prefix:
        return None, board_port

    owner, repo, number = _resolve_issue_coordinates(ref, config)
    executor = snapshot.executor.strip().lower()
    if executor not in VALID_EXECUTORS:
        decision.skipped.append((ref, "invalid-executor"))
        return None, board_port

    if is_graph_member(config, ref):
        code, _msg = ready_promotion_evaluator(
            issue_ref=ref,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            status_resolver=status_resolver,
            require_in_graph=True,
        )
        if code != 0:
            board_port = _handle_dependency_blocked_rebalance_item(
                ref,
                config=config,
                project_owner=project_owner,
                project_number=project_number,
                dry_run=dry_run,
                decision=decision,
                review_state_port=review_state_port,
                board_port=board_port,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                gh_runner=gh_runner,
                set_blocked_with_reason=set_blocked_with_reason,
                set_handoff_target=set_handoff_target,
            )
            return None, board_port

    pr_field = query_project_item_field(
        ref,
        "PR",
        config,
        project_owner,
        project_number,
        gh_runner=gh_runner,
    )
    has_open_pr = False
    parsed_pr = _parse_pr_url(pr_field)
    if parsed_pr is not None:
        pr_owner, pr_repo, pr_number = parsed_pr
        has_open_pr = query_open_pr_updated_at(
            pr_owner,
            pr_repo,
            pr_number,
            gh_runner=gh_runner,
        ) is not None
    activity_at = query_latest_wip_activity_timestamp(
        ref,
        owner,
        repo,
        number,
        pr_field,
        gh_runner=gh_runner,
    )

    is_fresh = activity_at is not None and activity_at >= freshness_cutoff
    if not has_open_pr and not is_fresh:
        handled, board_port = _handle_stale_rebalance_item(
            ref,
            owner=owner,
            repo=repo,
            number=number,
            now=now,
            activity_at=activity_at,
            confirm_delay=confirm_delay,
            project_owner=project_owner,
            project_number=project_number,
            config=config,
            dry_run=dry_run,
            decision=decision,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
            query_latest_marker_timestamp=query_latest_marker_timestamp,
            comment_exists=comment_exists,
            ensure_board_port=ensure_board_port,
            transition_issue_status=transition_issue_status,
        )
        if handled:
            return None, board_port

    return (
        _WipCandidate(
            ref=ref,
            owner=owner,
            repo=repo,
            number=number,
            lane=parsed.prefix,
            executor=executor,
            activity_at=activity_at or datetime.min.replace(tzinfo=timezone.utc),
            has_open_pr=has_open_pr,
        ),
        board_port,
    )


def _apply_rebalance_lane_overflow(
    candidates: list[_WipCandidate],
    *,
    executor: str,
    lane: str,
    automation_config: BoardAutomationConfig,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    dry_run: bool,
    decision: RebalanceDecision,
    review_state_port: ReviewStatePort | None,
    board_port: BoardMutationPort | None,
    board_info_resolver,
    board_mutator,
    gh_runner,
    comment_exists: Callable[..., bool],
    ensure_board_port: Callable[[BoardMutationPort | None], BoardMutationPort],
    transition_issue_status: Callable[..., tuple[bool, str]],
) -> BoardMutationPort | None:
    """Keep the freshest lane items and demote overflow back to Ready."""
    limit = wip_limit_for_lane(
        automation_config.wip_limits,
        executor,
        lane,
        fallback=3,
    )
    ranked = sorted(
        candidates,
        key=lambda item: (
            0 if item.has_open_pr else 1,
            -int(item.activity_at.timestamp()),
            _issue_sort_key(item.ref),
        ),
    )
    keep = ranked[:limit]
    overflow = ranked[limit:]
    decision.kept.extend(item.ref for item in keep)
    for item in overflow:
        changed, _old = transition_issue_status(
            item.ref,
            {"In Progress"},
            "Ready",
            config,
            project_owner,
            project_number,
            dry_run=dry_run,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        if changed and not dry_run:
            marker = _marker_for("wip-rebalance", item.ref)
            if not comment_exists(
                item.owner,
                item.repo,
                item.number,
                marker,
                gh_runner=gh_runner,
            ):
                board_port = ensure_board_port(board_port)
                board_port.post_issue_comment(
                    f"{item.owner}/{item.repo}",
                    item.number,
                    f"{marker}\nMoved back to `Ready` by lane WIP rebalance (limit={limit}).",
                )
        decision.moved_ready.append(item.ref)
    return board_port


def rebalance_wip(
    *,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    in_progress_items: list[IssueSnapshot],
    use_ports: bool,
    review_state_port: ReviewStatePort | None,
    board_port: BoardMutationPort | None,
    this_repo_prefix: str | None = None,
    all_prefixes: bool = False,
    cycle_minutes: int = DEFAULT_REBALANCE_CYCLE_MINUTES,
    dry_run: bool = False,
    board_info_resolver=None,
    board_mutator=None,
    status_resolver=None,
    gh_runner=None,
    is_graph_member: Callable[..., bool],
    ready_promotion_evaluator: Callable[..., tuple[int, str]],
    set_blocked_with_reason: Callable[..., None],
    set_handoff_target: Callable[..., None],
    query_project_item_field: Callable[..., str],
    query_open_pr_updated_at: Callable[..., datetime | None],
    query_latest_wip_activity_timestamp: Callable[..., datetime | None],
    query_latest_marker_timestamp: Callable[..., datetime | None],
    comment_exists: Callable[..., bool],
    ensure_board_port: Callable[[BoardMutationPort | None], BoardMutationPort],
    transition_issue_status: Callable[..., tuple[bool, str]],
    snapshot_to_issue_ref: Callable[[str, dict[str, str]], str | None],
) -> RebalanceDecision:
    """Rebalance In Progress lanes with stale demotion and dependency blocking."""
    now = datetime.now(timezone.utc)
    freshness_cutoff = now - timedelta(hours=automation_config.freshness_hours)
    confirm_delay = timedelta(
        minutes=cycle_minutes * max(1, automation_config.stale_confirmation_cycles - 1)
    )
    decision = RebalanceDecision()
    lane_buckets: dict[tuple[str, str], list[_WipCandidate]] = {}

    for snapshot in in_progress_items:
        if use_ports:
            normalized_snapshot = snapshot
        else:
            normalized_ref = snapshot_to_issue_ref(
                snapshot.issue_ref,
                config.issue_prefixes,
            )
            if normalized_ref is None:
                decision.skipped.append((snapshot.issue_ref, "invalid-ref"))
                continue
            normalized_snapshot = IssueSnapshot(
                issue_ref=normalized_ref,
                status=snapshot.status,
                executor=snapshot.executor,
                priority=snapshot.priority,
                title=snapshot.title,
                item_id=snapshot.item_id,
                project_id=snapshot.project_id,
            )
        candidate, board_port = _evaluate_rebalance_snapshot(
            normalized_snapshot,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            this_repo_prefix=this_repo_prefix,
            all_prefixes=all_prefixes,
            freshness_cutoff=freshness_cutoff,
            confirm_delay=confirm_delay,
            now=now,
            dry_run=dry_run,
            decision=decision,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            status_resolver=status_resolver,
            gh_runner=gh_runner,
            is_graph_member=is_graph_member,
            ready_promotion_evaluator=ready_promotion_evaluator,
            set_blocked_with_reason=set_blocked_with_reason,
            set_handoff_target=set_handoff_target,
            query_project_item_field=query_project_item_field,
            query_open_pr_updated_at=query_open_pr_updated_at,
            query_latest_wip_activity_timestamp=query_latest_wip_activity_timestamp,
            query_latest_marker_timestamp=query_latest_marker_timestamp,
            comment_exists=comment_exists,
            ensure_board_port=ensure_board_port,
            transition_issue_status=transition_issue_status,
        )
        if candidate is not None:
            lane_buckets.setdefault((candidate.executor, candidate.lane), []).append(
                candidate
            )

    for (executor, lane), candidates in lane_buckets.items():
        board_port = _apply_rebalance_lane_overflow(
            candidates,
            executor=executor,
            lane=lane,
            automation_config=automation_config,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            dry_run=dry_run,
            decision=decision,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
            comment_exists=comment_exists,
            ensure_board_port=ensure_board_port,
            transition_issue_status=transition_issue_status,
        )

    return decision
