"""Ready-queue scheduling and claim use cases."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from startupai_controller.board_automation_config import (
    BoardAutomationConfig,
    DEFAULT_MISSING_EXECUTOR_BLOCK_CAP,
)
from startupai_controller.board_graph import (
    _count_wip_by_executor,
    _count_wip_by_executor_lane,
    _issue_sort_key,
    _ready_snapshot_rank,
    _resolve_issue_coordinates,
)
from startupai_controller.domain.models import ClaimReadyResult, SchedulingDecision
from startupai_controller.domain.repair_policy import marker_for as _marker_for
from startupai_controller.domain.scheduling_policy import (
    VALID_EXECUTORS,
    snapshot_to_issue_ref as _snapshot_to_issue_ref,
    wip_limit_for_lane as _domain_wip_limit_for_lane,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    CriticalPathConfig,
    GhQueryError,
    direct_predecessors,
    evaluate_ready_promotion,
    in_any_critical_path,
    parse_issue_ref,
)


@dataclass(frozen=True)
class BoardInfo:
    """Minimal board identity/status needed for local compatibility helpers."""

    status: str
    item_id: str
    project_id: str


def _query_issue_board_info(
    issue_ref: str,
    config: CriticalPathConfig,
    review_state_port: ReviewStatePort,
) -> BoardInfo:
    """Resolve board item identity through ReviewStatePort."""
    del config  # canonical issue refs are already encoded in the snapshot
    snapshot = next(
        (item for item in review_state_port.build_board_snapshot().items if item.issue_ref == issue_ref),
        None,
    )
    if snapshot is None:
        return BoardInfo(status="NOT_ON_BOARD", item_id="", project_id="")
    return BoardInfo(
        status=snapshot.status or "UNKNOWN",
        item_id=snapshot.item_id,
        project_id=snapshot.project_id,
    )


def _comment_exists(
    owner: str,
    repo: str,
    number: int,
    marker: str,
    *,
    review_state_port: ReviewStatePort,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Check marker presence through ReviewStatePort."""
    del gh_runner
    return review_state_port.comment_exists(f"{owner}/{repo}", number, marker)


def _set_blocked_with_reason(
    issue_ref: str,
    reason: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set Status=Blocked and Blocked Reason on a board item."""
    if board_info_resolver is None and board_mutator is None:
        current_status = review_state_port.get_issue_status(issue_ref)
        if current_status in {None, "NOT_ON_BOARD"}:
            raise GhQueryError(f"{issue_ref} is not on the project board.")
        if dry_run:
            return
        if current_status != "Blocked":
            board_port.set_issue_status(issue_ref, "Blocked")
        board_port.set_issue_field(issue_ref, "Blocked Reason", reason)
        return

    resolve_info = board_info_resolver or (
        lambda ref, cfg, _owner, _number: _query_issue_board_info(
            ref,
            cfg,
            review_state_port,
        )
    )
    info = resolve_info(issue_ref, config, project_owner, project_number)

    if info.status == "NOT_ON_BOARD":
        raise GhQueryError(f"{issue_ref} is not on the project board.")

    if dry_run:
        return

    if board_mutator is not None:
        board_mutator(info.project_id, info.item_id)
    elif info.status != "Blocked":
        board_port.set_issue_status(issue_ref, "Blocked")
    board_port.set_issue_field(issue_ref, "Blocked Reason", reason)


def _transition_issue_status(
    issue_ref: str,
    from_statuses: set[str],
    to_status: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[bool, str]:
    """Transition issue status through ports, with legacy fallback for tests."""
    del gh_runner
    resolve_info = board_info_resolver or (
        lambda ref, cfg, _owner, _number: _query_issue_board_info(
            ref,
            cfg,
            review_state_port,
        )
    )

    if board_info_resolver is None and board_mutator is None:
        old_status = review_state_port.get_issue_status(issue_ref) or "NOT_ON_BOARD"
        if old_status not in from_statuses:
            return False, old_status
        if not dry_run:
            board_port.set_issue_status(issue_ref, to_status)
        return True, old_status

    info = resolve_info(issue_ref, config, project_owner, project_number)
    current_status = info.status
    if current_status not in from_statuses:
        return False, current_status
    if not dry_run:
        if board_mutator is not None:
            board_mutator(info.project_id, info.item_id, to_status)
        else:
            board_port.set_issue_status(issue_ref, to_status)
    return True, current_status


def _wip_limit_for_lane(
    automation_config: BoardAutomationConfig | None,
    executor: str,
    lane: str,
    fallback: int,
) -> int:
    """Resolve WIP limit for an executor/lane pair."""
    wip_limits = automation_config.wip_limits if automation_config else None
    return _domain_wip_limit_for_lane(wip_limits, executor, lane, fallback)


def _post_claim_comment(
    issue_ref: str,
    executor: str,
    config: CriticalPathConfig,
    *,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
) -> None:
    """Post deterministic kickoff comment on successful claim."""
    marker = _marker_for("claim-ready", issue_ref)
    owner, repo, number = _resolve_issue_coordinates(issue_ref, config)

    if _comment_exists(
        owner,
        repo,
        number,
        marker,
        review_state_port=review_state_port,
    ):
        return

    executor_label = f"Executor: `{executor}`" if executor else ""
    body = (
        f"{marker}\n"
        f"**Claimed for execution** by `{executor}`.\n\n"
        "Board transition: `Ready -> In Progress`.\n"
        f"{executor_label}".strip()
    )
    board_port.post_issue_comment(f"{owner}/{repo}", number, body)


def _load_schedule_ready_state(
    *,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    review_state_port: ReviewStatePort,
) -> tuple[
    bool,
    list[Any],
    dict[str, int],
    dict[tuple[str, str], int],
]:
    """Load Ready items and WIP counts for one scheduling pass."""
    use_ports = True
    wip_items = review_state_port.list_issues_by_status("In Progress")
    wip_counts = _count_wip_by_executor(wip_items)
    lane_wip_counts: dict[tuple[str, str], int] = {}
    if automation_config is not None:
        lane_wip_counts = _count_wip_by_executor_lane(config, wip_items)
    ready_items = sorted(
        review_state_port.list_issues_by_status("Ready"),
        key=lambda snapshot: _ready_snapshot_rank(snapshot, config),
    )
    return use_ports, ready_items, wip_counts, lane_wip_counts


def _record_missing_executor_ready_item(
    ref: str,
    reason: str,
    *,
    decision: SchedulingDecision,
    missing_executor_block_cap: int,
    dry_run: bool,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_info_resolver: Callable[..., BoardInfo] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Block or skip one Ready item with a missing/invalid executor."""
    if len(decision.blocked_missing_executor) < missing_executor_block_cap:
        if not dry_run:
            try:
                _set_blocked_with_reason(
                    ref,
                    reason,
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
            except GhQueryError:
                pass
        decision.blocked_missing_executor.append(ref)
        return
    decision.skipped_missing_executor.append(ref)


def _process_schedule_ready_snapshot(
    snapshot: object,
    *,
    use_ports: bool,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    this_repo_prefix: str | None,
    all_prefixes: bool,
    mode: str,
    per_executor_wip_limit: int,
    automation_config: BoardAutomationConfig | None,
    missing_executor_block_cap: int,
    dry_run: bool,
    decision: SchedulingDecision,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    status_resolver: Callable[..., str] | None,
    board_info_resolver: Callable[..., BoardInfo] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    wip_counts: dict[str, int],
    lane_wip_counts: dict[tuple[str, str], int],
) -> None:
    """Apply scheduling policy to one Ready snapshot."""
    del use_ports
    ref = getattr(snapshot, "issue_ref", None)
    if ref is None:
        ref = _snapshot_to_issue_ref(snapshot.issue_ref, config.issue_prefixes)
        if ref is None:
            return

    if not all_prefixes and this_repo_prefix:
        parsed = parse_issue_ref(ref)
        if parsed.prefix != this_repo_prefix:
            return

    is_graph_member = in_any_critical_path(config, ref)
    executor = snapshot.executor.strip().lower()
    if executor not in VALID_EXECUTORS:
        reason = "missing-executor" if not executor else f"invalid-executor:{executor}"
        _record_missing_executor_ready_item(
            ref,
            reason,
            decision=decision,
            missing_executor_block_cap=missing_executor_block_cap,
            dry_run=dry_run,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        return

    if not is_graph_member:
        decision.skipped_non_graph.append(ref)
    else:
        val_code, _val_output = evaluate_ready_promotion(
            issue_ref=ref,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            status_resolver=status_resolver,
            require_in_graph=True,
        )
        if val_code != 0:
            preds = direct_predecessors(config, ref)
            reason = f"dependency-unmet:{','.join(sorted(preds))}"
            if not dry_run:
                try:
                    _set_blocked_with_reason(
                        ref,
                        reason,
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
                except GhQueryError:
                    pass
            decision.blocked_invalid_ready.append(ref)
            return

    lane = parse_issue_ref(ref).prefix
    if automation_config is not None:
        current_wip = lane_wip_counts.get((executor, lane), 0)
        lane_limit = _wip_limit_for_lane(
            automation_config,
            executor,
            lane,
            fallback=per_executor_wip_limit,
        )
    else:
        current_wip = wip_counts.get(executor, 0)
        lane_limit = per_executor_wip_limit
    if current_wip >= lane_limit:
        decision.deferred_wip.append(ref)
        return

    if mode == "claim":
        changed, _old = _transition_issue_status(
            ref,
            {"Ready"},
            "In Progress",
            config,
            project_owner,
            project_number,
            dry_run=dry_run,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
        )
        if not changed:
            return
        decision.claimed.append(ref)
    else:
        decision.claimable.append(ref)

    if automation_config is not None:
        lane_wip_counts[(executor, lane)] = current_wip + 1
    else:
        wip_counts[executor] = current_wip + 1


def schedule_ready_items(
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    this_repo_prefix: str | None = None,
    all_prefixes: bool = False,
    mode: str = "advisory",
    per_executor_wip_limit: int = 3,
    automation_config: BoardAutomationConfig | None = None,
    missing_executor_block_cap: int = DEFAULT_MISSING_EXECUTOR_BLOCK_CAP,
    dry_run: bool = False,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> SchedulingDecision:
    """Classify and optionally claim Ready issues."""
    if mode not in {"advisory", "claim"}:
        raise ConfigError(
            f"Invalid schedule mode '{mode}'. Use advisory or claim."
        )

    decision = SchedulingDecision()
    (
        use_ports,
        ready_items,
        wip_counts,
        lane_wip_counts,
    ) = _load_schedule_ready_state(
        config=config,
        automation_config=automation_config,
        review_state_port=review_state_port,
    )

    for snapshot in ready_items:
        _process_schedule_ready_snapshot(
            snapshot,
            use_ports=use_ports,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            this_repo_prefix=this_repo_prefix,
            all_prefixes=all_prefixes,
            mode=mode,
            per_executor_wip_limit=per_executor_wip_limit,
            automation_config=automation_config,
            missing_executor_block_cap=missing_executor_block_cap,
            dry_run=dry_run,
            decision=decision,
            review_state_port=review_state_port,
            board_port=board_port,
            status_resolver=status_resolver,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
            wip_counts=wip_counts,
            lane_wip_counts=lane_wip_counts,
        )

    return decision


def _load_claim_ready_state(
    *,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    this_repo_prefix: str | None,
    all_prefixes: bool,
    review_state_port: ReviewStatePort,
) -> tuple[
    bool,
    dict[str, object],
    dict[str, int],
    dict[tuple[str, str], int],
]:
    """Load Ready items and current WIP state for one claim attempt."""
    use_ports = True
    ready_items = review_state_port.list_issues_by_status("Ready")
    wip_items = review_state_port.list_issues_by_status("In Progress")
    wip_counts = _count_wip_by_executor(wip_items)
    lane_wip_counts: dict[tuple[str, str], int] = {}
    if automation_config is not None:
        lane_wip_counts = _count_wip_by_executor_lane(
            config,
            wip_items,
        )

    ready_by_ref: dict[str, object] = {}
    for snapshot in ready_items:
        ref = snapshot.issue_ref
        if not all_prefixes and this_repo_prefix:
            parsed = parse_issue_ref(ref)
            if parsed.prefix != this_repo_prefix:
                continue
        ready_by_ref[ref] = snapshot
    return use_ports, ready_by_ref, wip_counts, lane_wip_counts


def _claim_ready_candidates(
    ready_by_ref: dict[str, object],
    *,
    norm_executor: str,
    issue_ref: str | None,
    next_issue: bool,
) -> tuple[list[str], ClaimReadyResult | None]:
    """Resolve the candidate issue refs for one claim request."""
    if issue_ref:
        return [issue_ref], None
    if not next_issue:
        return [], ClaimReadyResult(reason="missing-target")
    candidates = sorted(
        [
            ref
            for ref, snapshot in ready_by_ref.items()
            if snapshot.executor.strip().lower() == norm_executor
        ],
        key=_issue_sort_key,
    )
    if not candidates:
        return [], ClaimReadyResult(reason="no-ready-for-executor")
    return candidates, None


def _attempt_claim_ready_candidate(
    ref: str,
    snapshot: object,
    *,
    issue_ref: str | None,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    status_resolver: Callable[..., str] | None,
    per_executor_wip_limit: int,
    automation_config: BoardAutomationConfig | None,
    dry_run: bool,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_info_resolver: Callable[..., BoardInfo] | None,
    board_mutator: Callable[..., None] | None,
    norm_executor: str,
    lane_wip_counts: dict[tuple[str, str], int],
    wip_counts: dict[str, int],
) -> ClaimReadyResult | None:
    """Attempt to claim one Ready candidate for the executor."""
    item_executor = snapshot.executor.strip().lower()
    if item_executor != norm_executor:
        if issue_ref:
            return ClaimReadyResult(
                reason=f"executor-mismatch:{item_executor or 'unset'}"
            )
        return None

    if in_any_critical_path(config, ref):
        val_code, _val_output = evaluate_ready_promotion(
            issue_ref=ref,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            status_resolver=status_resolver,
            require_in_graph=True,
        )
        if val_code != 0:
            if issue_ref:
                return ClaimReadyResult(reason="dependency-unmet")
            return None

    lane = parse_issue_ref(ref).prefix
    if automation_config is not None:
        current_wip = lane_wip_counts.get((norm_executor, lane), 0)
        lane_limit = _wip_limit_for_lane(
            automation_config,
            norm_executor,
            lane,
            fallback=per_executor_wip_limit,
        )
    else:
        current_wip = wip_counts.get(norm_executor, 0)
        lane_limit = per_executor_wip_limit

    if current_wip >= lane_limit:
        return ClaimReadyResult(reason="wip-limit")

    changed, old_status = _transition_issue_status(
        ref,
        {"Ready"},
        "In Progress",
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
    )
    if not changed:
        return ClaimReadyResult(reason=f"status-not-ready:{old_status}")

    if not dry_run:
        try:
            _post_claim_comment(
                ref,
                norm_executor,
                config,
                review_state_port=review_state_port,
                board_port=board_port,
            )
        except GhQueryError:
            pass

    return ClaimReadyResult(claimed=ref)


def claim_ready_issue(
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    executor: str,
    issue_ref: str | None = None,
    next_issue: bool = False,
    this_repo_prefix: str | None = None,
    all_prefixes: bool = False,
    per_executor_wip_limit: int = 3,
    automation_config: BoardAutomationConfig | None = None,
    dry_run: bool = False,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
) -> ClaimReadyResult:
    """Claim one Ready issue for a specific executor."""
    norm_executor = executor.strip().lower()
    if norm_executor not in VALID_EXECUTORS:
        return ClaimReadyResult(reason=f"invalid-executor:{executor}")

    use_ports, ready_by_ref, wip_counts, lane_wip_counts = _load_claim_ready_state(
        config=config,
        automation_config=automation_config,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        review_state_port=review_state_port,
    )
    del use_ports

    candidates, early_result = _claim_ready_candidates(
        ready_by_ref,
        norm_executor=norm_executor,
        issue_ref=issue_ref,
        next_issue=next_issue,
    )
    if early_result is not None:
        return early_result

    for ref in candidates:
        snapshot = ready_by_ref.get(ref)
        if snapshot is None:
            if issue_ref:
                return ClaimReadyResult(reason="issue-not-ready")
            continue

        result = _attempt_claim_ready_candidate(
            ref,
            snapshot,
            issue_ref=issue_ref,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            status_resolver=status_resolver,
            per_executor_wip_limit=per_executor_wip_limit,
            automation_config=automation_config,
            dry_run=dry_run,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            norm_executor=norm_executor,
            lane_wip_counts=lane_wip_counts,
            wip_counts=wip_counts,
        )
        if result is None:
            continue
        return result

    return ClaimReadyResult(reason="no-eligible-ready")
