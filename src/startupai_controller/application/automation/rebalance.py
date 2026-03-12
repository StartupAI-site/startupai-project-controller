"""In-progress rebalance use case."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Callable

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
from startupai_controller.application.automation.ready_claim import _transition_issue_status
from startupai_controller.application.automation.ready_wiring import (
    _wrap_board_port,
    _wrap_review_state_port,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    direct_predecessors,
    evaluate_ready_promotion,
    in_any_critical_path,
    parse_issue_ref,
)

if TYPE_CHECKING:
    from startupai_controller.ports.board_mutations import BoardMutationPort as _BoardMutationPort
    from startupai_controller.ports.pull_requests import PullRequestPort as _PullRequestPort
    from startupai_controller.ports.review_state import ReviewStatePort as _ReviewStatePort
else:
    _BoardMutationPort = None
    _PullRequestPort = None
    _ReviewStatePort = None
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


def _parse_github_timestamp(raw: str | None) -> datetime | None:
    """Parse one GitHub timestamp string into an aware datetime."""
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None


def _latest_wip_activity_timestamp(
    issue_ref: str,
    *,
    review_state_port: ReviewStatePort,
    pr_port: PullRequestPort,
    owner: str,
    repo: str,
    number: int,
    pr_field: str,
) -> datetime | None:
    """Return the latest execution-significant activity timestamp for one WIP issue."""
    latest_comment = review_state_port.latest_non_automation_comment_timestamp(
        f"{owner}/{repo}",
        number,
    )
    latest_pr = None
    parsed_pr = _parse_pr_url(pr_field)
    if parsed_pr is not None:
        pr_owner, pr_repo, pr_number = parsed_pr
        latest_pr = _parse_github_timestamp(
            pr_port.pull_request_updated_at(f"{pr_owner}/{pr_repo}", pr_number)
        )
    values = [ts for ts in (latest_comment, latest_pr) if ts is not None]
    return max(values) if values else None


def _handle_dependency_blocked_rebalance_item(
    ref: str,
    *,
    config: CriticalPathConfig,
    dry_run: bool,
    decision: RebalanceDecision,
    board_port: BoardMutationPort,
) -> None:
    """Route a dependency-blocked WIP item to Blocked with claude handoff."""
    preds = ",".join(sorted(direct_predecessors(config, ref)))
    reason = f"dependency-unmet:{preds}"
    if not dry_run:
        board_port.set_issue_status(ref, "Blocked")
        board_port.set_issue_field(ref, "Blocked Reason", reason)
        board_port.set_issue_field(ref, "Handoff To", "claude")
    decision.moved_blocked.append(ref)


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
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
) -> bool:
    """Mark a stale item or demote it back to Ready after confirmation."""
    stale_prefix = f"<!-- {MARKER_PREFIX}:stale-candidate:{ref}"
    stale_ts = review_state_port.latest_matching_comment_timestamp(
        f"{owner}/{repo}",
        number,
        (stale_prefix,),
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
            if not review_state_port.comment_exists(f"{owner}/{repo}", number, stale_marker):
                board_port.post_issue_comment(
                    f"{owner}/{repo}",
                    number,
                    (
                        f"{stale_marker}\n"
                        "Stale candidate detected; will demote on next cycle if still inactive."
                    ),
                )
        return True

    changed, _old = _transition_issue_status(
        ref,
        {"In Progress"},
        "Ready",
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
    )
    if changed and not dry_run:
        demote_marker = _marker_for("stale-demote", ref)
        if not review_state_port.comment_exists(f"{owner}/{repo}", number, demote_marker):
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
    return True


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
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    pr_port: PullRequestPort,
    is_graph_member: Callable[[CriticalPathConfig, str], bool],
    ready_promotion_evaluator: Callable[..., tuple[int, str]],
) -> _WipCandidate | None:
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
            status_resolver=lambda issue_ref, *_args, **_kwargs: review_state_port.get_issue_status(
                issue_ref
            ),
            require_in_graph=True,
        )
        if code != 0:
            _handle_dependency_blocked_rebalance_item(
                ref,
                config=config,
                dry_run=dry_run,
                decision=decision,
                board_port=board_port,
            )
            return None

    pr_field = review_state_port.project_field_value(ref, "PR")
    has_open_pr = False
    parsed_pr = _parse_pr_url(pr_field)
    if parsed_pr is not None:
        pr_owner, pr_repo, pr_number = parsed_pr
        has_open_pr = (
            pr_port.pull_request_updated_at(f"{pr_owner}/{pr_repo}", pr_number) is not None
        )
    activity_at = _latest_wip_activity_timestamp(
        ref,
        review_state_port=review_state_port,
        pr_port=pr_port,
        owner=owner,
        repo=repo,
        number=number,
        pr_field=pr_field,
    )

    is_fresh = activity_at is not None and activity_at >= freshness_cutoff
    if not has_open_pr and not is_fresh:
        handled = _handle_stale_rebalance_item(
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
        )
        if handled:
            return None

    return _WipCandidate(
        ref=ref,
        owner=owner,
        repo=repo,
        number=number,
        lane=parsed.prefix,
        executor=executor,
        activity_at=activity_at or datetime.min.replace(tzinfo=timezone.utc),
        has_open_pr=has_open_pr,
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
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
) -> None:
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
        changed, _old = _transition_issue_status(
            item.ref,
            {"In Progress"},
            "Ready",
            config,
            project_owner,
            project_number,
            dry_run=dry_run,
            review_state_port=review_state_port,
            board_port=board_port,
        )
        if changed and not dry_run:
            marker = _marker_for("wip-rebalance", item.ref)
            if not review_state_port.comment_exists(
                f"{item.owner}/{item.repo}",
                item.number,
                marker,
            ):
                board_port.post_issue_comment(
                    f"{item.owner}/{item.repo}",
                    item.number,
                    f"{marker}\nMoved back to `Ready` by lane WIP rebalance (limit={limit}).",
                )
        decision.moved_ready.append(item.ref)


def rebalance_wip(
    *,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    in_progress_items: list[IssueSnapshot],
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    pr_port: PullRequestPort,
    is_graph_member: Callable[[CriticalPathConfig, str], bool],
    ready_promotion_evaluator: Callable[..., tuple[int, str]],
    this_repo_prefix: str | None = None,
    all_prefixes: bool = False,
    cycle_minutes: int = DEFAULT_REBALANCE_CYCLE_MINUTES,
    dry_run: bool = False,
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
        candidate = _evaluate_rebalance_snapshot(
            snapshot,
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
            pr_port=pr_port,
            is_graph_member=is_graph_member,
            ready_promotion_evaluator=ready_promotion_evaluator,
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
        )

    return decision


# ---------------------------------------------------------------------------
# Wiring entry-points (port materialisation + delegation)
# ---------------------------------------------------------------------------


def load_rebalance_in_progress_items(
    *,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    review_state_port: _ReviewStatePort | None,
    board_port: _BoardMutationPort | None,
    pr_port: _PullRequestPort | None,
    gh_runner: Callable[..., str] | None,
    # Injected board-automation helpers
    default_review_state_port_fn: Callable[..., object] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
    default_pr_port_fn: Callable[..., object] | None = None,
) -> tuple[_ReviewStatePort, _BoardMutationPort, _PullRequestPort, list[IssueSnapshot]]:
    """Load the current In Progress set for WIP rebalance."""
    review_state_port = review_state_port or default_review_state_port_fn(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    board_port = board_port or default_board_mutation_port_fn(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    pr_port = pr_port or default_pr_port_fn(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    return review_state_port, board_port, pr_port, review_state_port.list_issues_by_status(
        "In Progress"
    )


def wire_rebalance_wip(
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    *,
    this_repo_prefix: str | None = None,
    all_prefixes: bool = False,
    cycle_minutes: int = DEFAULT_REBALANCE_CYCLE_MINUTES,
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    pr_port: _PullRequestPort | None = None,
    board_info_resolver: Callable[..., object] | None = None,
    board_mutator: Callable[..., None] | None = None,
    status_resolver: Callable[..., str] | None = None,
    gh_runner: Callable[..., str] | None = None,
    # Injected board-automation helpers
    default_review_state_port_fn: Callable[..., object] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
    default_pr_port_fn: Callable[..., object] | None = None,
    is_graph_member_fn: Callable[[CriticalPathConfig, str], bool] | None = None,
    ready_promotion_evaluator_fn: Callable[..., tuple[int, str]] | None = None,
    comment_exists_fn: Callable[..., bool] | None = None,
) -> RebalanceDecision:
    """Wire port materialization, then delegate to core rebalance."""
    review_state_port, board_port, pr_port, in_progress = load_rebalance_in_progress_items(
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        review_state_port=review_state_port,
        board_port=board_port,
        pr_port=pr_port,
        gh_runner=gh_runner,
        default_review_state_port_fn=default_review_state_port_fn,
        default_board_mutation_port_fn=default_board_mutation_port_fn,
        default_pr_port_fn=default_pr_port_fn,
    )
    review_state_port = _wrap_review_state_port(
        review_state_port,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        status_resolver=status_resolver,
        comment_exists_fn=comment_exists_fn,
        gh_runner=gh_runner,
    )
    board_port = _wrap_board_port(
        board_port,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    return rebalance_wip(
        config=config,
        automation_config=automation_config,
        project_owner=project_owner,
        project_number=project_number,
        in_progress_items=in_progress,
        review_state_port=review_state_port,
        board_port=board_port,
        pr_port=pr_port,
        is_graph_member=is_graph_member_fn or in_any_critical_path,
        ready_promotion_evaluator=ready_promotion_evaluator_fn or evaluate_ready_promotion,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        cycle_minutes=cycle_minutes,
        dry_run=dry_run,
    )
