"""Review-state synchronization use case."""

from __future__ import annotations

from typing import Callable

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.application.automation.ready_claim import (
    BoardInfo,
    _transition_issue_status,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
    parse_issue_ref,
)


def _sync_review_transition(
    issue_ref: str,
    from_statuses: set[str],
    target_status: str,
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
) -> tuple[int, str]:
    """Apply one review-state transition and format the result."""
    changed, old = _transition_issue_status(
        issue_ref,
        from_statuses,
        target_status,
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
    if changed:
        return 0, f"{issue_ref}: {old} -> {target_status} ({reason})"
    return 2, f"{issue_ref}: no change (Status={old})"


def _required_review_check_contexts(
    issue_ref: str,
    config: CriticalPathConfig,
    *,
    pr_port: PullRequestPort,
) -> set[str] | tuple[int, str]:
    """Return required status-check contexts for the issue repo."""
    parsed = parse_issue_ref(issue_ref)
    repo_slug = config.issue_prefixes[parsed.prefix]
    try:
        return pr_port.required_status_checks(repo_slug, "main")
    except GhQueryError as error:
        return 4, f"Cannot read branch protection for {repo_slug}: {error}"


def _handle_checks_failed_sync_event(
    issue_ref: str,
    *,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    governed_single_machine_issue: bool,
    failed_checks: list[str] | None,
    dry_run: bool,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_info_resolver: Callable[..., BoardInfo] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[int, str]:
    """Handle checks_failed state sync for one issue."""
    if governed_single_machine_issue:
        return 2, f"{issue_ref}: governed local repair transition deferred to consumer"

    required_contexts = _required_review_check_contexts(
        issue_ref,
        config,
        pr_port=pr_port,
    )
    if isinstance(required_contexts, tuple):
        return required_contexts

    actual_failed = set(failed_checks) if failed_checks else set()
    if required_contexts and actual_failed:
        required_failures = actual_failed & required_contexts
        if not required_failures:
            return 2, (
                f"{issue_ref}: check failures are non-required, "
                f"no board change (failed: {sorted(actual_failed)})"
            )
    elif required_contexts and not actual_failed:
        return 2, (
            f"{issue_ref}: checks_failed but no failed check names "
            f"provided; cannot filter by required checks "
            f"({sorted(required_contexts)}) — no board change"
        )

    return _sync_review_transition(
        issue_ref,
        {"Review"},
        "In Progress",
        "required check failed",
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


def sync_review_state(
    event_kind: str,
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    automation_config: BoardAutomationConfig | None = None,
    pr_state: str | None = None,
    review_state: str | None = None,
    checks_state: str | None = None,
    failed_checks: list[str] | None = None,
    dry_run: bool = False,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[int, str]:
    """Sync board state based on PR/review/check events."""
    del pr_state, review_state
    governed_single_machine_issue = (
        automation_config is not None
        and automation_config.execution_authority_mode == "single_machine"
        and parse_issue_ref(issue_ref).prefix in automation_config.execution_authority_repos
    )

    if event_kind == "pr_open":
        return 2, (
            f"{issue_ref}: no board change on PR open — "
            "waiting for review signal"
        )

    if event_kind in {"pr_ready_for_review", "review_submitted"}:
        return _sync_review_transition(
            issue_ref,
            {"In Progress"},
            "Review",
            "PR ready for review" if event_kind == "pr_ready_for_review" else "review submitted",
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

    if event_kind == "changes_requested":
        if governed_single_machine_issue:
            return 2, f"{issue_ref}: governed local repair transition deferred to consumer"
        return _sync_review_transition(
            issue_ref,
            {"Review"},
            "In Progress",
            "changes requested",
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

    if event_kind == "checks_failed":
        return _handle_checks_failed_sync_event(
            issue_ref,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            governed_single_machine_issue=governed_single_machine_issue,
            failed_checks=failed_checks,
            dry_run=dry_run,
            pr_port=pr_port,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )

    if event_kind == "pr_close_merged":
        if checks_state and checks_state != "passed":
            return 2, f"{issue_ref}: PR merged but checks_state={checks_state}"

        return _sync_review_transition(
            issue_ref,
            {"Review", "In Progress"},
            "Done",
            "PR merged",
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

    if event_kind == "checks_passed":
        return 2, f"{issue_ref}: checks_passed (no state change without merge)"

    return 2, f"{issue_ref}: unknown event_kind '{event_kind}'"
