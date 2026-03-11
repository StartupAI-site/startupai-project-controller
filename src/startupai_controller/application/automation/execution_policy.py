"""Execution-policy use case for protected coding lanes."""

from __future__ import annotations

from typing import Callable, Sequence

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.domain.models import ExecutionPolicyDecision, LinkedIssue
from startupai_controller.domain.repair_policy import (
    MARKER_PREFIX,
    marker_for as _marker_for,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    ConfigError,
    parse_issue_ref,
)


def _select_execution_policy_issues(
    *,
    linked: Sequence[LinkedIssue],
    automation_config: BoardAutomationConfig,
    legacy_copilot_only_mode: bool,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    gh_runner,
    query_issue_executor: Callable[..., str],
) -> list[LinkedIssue]:
    """Filter linked issues to the protected execution-policy scope."""
    protected_linked: list[LinkedIssue] = []
    for issue in linked:
        prefix = parse_issue_ref(issue.ref).prefix
        if prefix not in automation_config.execution_authority_repos:
            continue
        if legacy_copilot_only_mode:
            protected_linked.append(issue)
            continue
        executor = query_issue_executor(
            issue.ref,
            "Executor",
            config,
            project_owner,
            project_number,
            gh_runner=gh_runner,
        ).strip().lower()
        if executor in automation_config.execution_authority_executors:
            protected_linked.append(issue)
    return protected_linked


def _apply_execution_policy_to_issue(
    *,
    issue: LinkedIssue,
    policy: str,
    decision: ExecutionPolicyDecision,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    dry_run: bool,
    pr_url: str,
    board_info_resolver,
    board_mutator,
    gh_runner,
    set_status_if_changed: Callable[..., tuple[bool, str]],
    query_issue_assignees: Callable[..., list[str]],
    is_copilot_actor: Callable[[str], bool],
    set_issue_assignees: Callable[..., None],
    comment_exists: Callable[..., bool],
    post_comment: Callable[..., None],
) -> None:
    """Apply the execution policy to one protected linked issue."""
    changed, old_status = set_status_if_changed(
        issue.ref,
        {"In Progress", "Review", "Ready"},
        "Blocked" if policy == "block" else "Ready",
        config,
        project_owner,
        project_number,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    if changed and policy == "block":
        decision.blocked.append(issue.ref)
    elif changed:
        decision.requeued.append(issue.ref)

    assignees = query_issue_assignees(
        issue.owner,
        issue.repo,
        issue.number,
        gh_runner=gh_runner,
    )
    filtered_assignees = [
        login for login in assignees if not is_copilot_actor(login)
    ]
    if filtered_assignees != assignees:
        if not dry_run:
            set_issue_assignees(
                issue.owner,
                issue.repo,
                issue.number,
                filtered_assignees,
                gh_runner=gh_runner,
            )
        decision.copilot_unassigned.append(issue.ref)

    marker = _marker_for("execution-policy", issue.ref)
    if comment_exists(issue.owner, issue.repo, issue.number, marker, gh_runner=gh_runner):
        return
    policy_message = (
        "moved this issue to `Blocked`"
        if policy == "block"
        else "re-queued this issue to `Ready`"
    )
    body = (
        f"{marker}\n"
        "Execution policy found a non-local coding PR without valid "
        f"consumer provenance and {policy_message} "
        f"(previous status: `{old_status}`).\n"
        f"PR: {pr_url}\n"
        "Consumer provenance is required for protected coding lanes."
    )
    if not dry_run:
        post_comment(
            issue.owner,
            issue.repo,
            issue.number,
            body,
            gh_runner=gh_runner,
        )


def _post_execution_policy_pr_comment(
    *,
    pr_repo: str,
    pr_number: int,
    owner: str,
    repo: str,
    policy: str,
    dry_run: bool,
    gh_runner,
    comment_exists: Callable[..., bool],
    post_comment: Callable[..., None],
) -> None:
    """Post the top-level execution policy PR comment if absent."""
    pr_marker = f"<!-- {MARKER_PREFIX}:execution-policy-pr:{pr_repo}#{pr_number} -->"
    if comment_exists(owner, repo, pr_number, pr_marker, gh_runner=gh_runner):
        return
    action_text = (
        "The linked issue has been moved to `Blocked` for operator review."
        if policy == "block"
        else "The linked issue has been re-queued for local execution."
    )
    pr_body = (
        f"{pr_marker}\n"
        "Execution policy rejected this coding PR because it lacks valid local "
        "consumer provenance for a protected execution lane.\n"
        f"{action_text}"
    )
    if not dry_run:
        post_comment(owner, repo, pr_number, pr_body, gh_runner=gh_runner)


def enforce_execution_policy(
    *,
    pr_repo: str,
    pr_number: int,
    actor: str,
    state: str,
    pr_url: str,
    provenance: dict[str, str] | None,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None = None,
    project_owner: str,
    project_number: int,
    allow_copilot_coding_agent: bool = False,
    dry_run: bool = False,
    board_info_resolver=None,
    board_mutator=None,
    gh_runner=None,
    is_copilot_actor: Callable[[str], bool],
    query_issue_executor: Callable[..., str],
    set_status_if_changed: Callable[..., tuple[bool, str]],
    query_issue_assignees: Callable[..., list[str]],
    set_issue_assignees: Callable[..., None],
    comment_exists: Callable[..., bool],
    post_comment: Callable[..., None],
    close_pull_request: Callable[..., None],
    load_linked_issues: Callable[[], Sequence[LinkedIssue]],
) -> ExecutionPolicyDecision:
    """Enforce local execution authority for protected coding PRs."""
    if "/" not in pr_repo:
        raise ConfigError(f"pr_repo must be owner/repo, got '{pr_repo}'.")

    decision = ExecutionPolicyDecision()
    owner, repo = pr_repo.split("/", maxsplit=1)
    legacy_copilot_only_mode = automation_config is None

    if allow_copilot_coding_agent:
        decision.skipped_reason = "policy-disabled"
        return decision
    if automation_config is None:
        if not is_copilot_actor(actor):
            decision.skipped_reason = f"actor={actor or 'unknown'}"
            return decision
        automation_config = BoardAutomationConfig(
            wip_limits={},
            freshness_hours=24,
            stale_confirmation_cycles=2,
            trusted_codex_actors=set(),
            trusted_local_authors=set(),
            execution_authority_mode="single_machine",
            execution_authority_repos=tuple(sorted(set(config.issue_prefixes))),
            execution_authority_executors=("codex",),
            global_concurrency=1,
            non_local_pr_policy="close",
        )

    linked = load_linked_issues()
    protected_linked = _select_execution_policy_issues(
        linked=linked,
        automation_config=automation_config,
        legacy_copilot_only_mode=legacy_copilot_only_mode,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        gh_runner=gh_runner,
        query_issue_executor=query_issue_executor,
    )

    if not protected_linked:
        decision.skipped_reason = "no-protected-linked-issues"
        return decision

    valid_provenance_issue = provenance.get("issue_ref") if provenance else None
    if (
        provenance is not None
        and actor in automation_config.trusted_local_authors
        and any(issue.ref == valid_provenance_issue for issue in protected_linked)
    ):
        decision.skipped_reason = "valid-local-pr"
        return decision

    policy = automation_config.non_local_pr_policy
    for issue in linked:
        if issue not in protected_linked:
            continue
        _apply_execution_policy_to_issue(
            issue=issue,
            policy=policy,
            decision=decision,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            dry_run=dry_run,
            pr_url=pr_url,
            board_info_resolver=board_info_resolver,
            board_mutator=(lambda *a: None) if dry_run else board_mutator,
            gh_runner=gh_runner,
            set_status_if_changed=set_status_if_changed,
            query_issue_assignees=query_issue_assignees,
            is_copilot_actor=is_copilot_actor,
            set_issue_assignees=set_issue_assignees,
            comment_exists=comment_exists,
            post_comment=post_comment,
        )

    decision.enforced_pr = True
    _post_execution_policy_pr_comment(
        pr_repo=pr_repo,
        pr_number=pr_number,
        owner=owner,
        repo=repo,
        policy=policy,
        dry_run=dry_run,
        gh_runner=gh_runner,
        comment_exists=comment_exists,
        post_comment=post_comment,
    )

    if state == "OPEN" and policy == "close":
        decision.pr_closed = True
        if not dry_run:
            close_pull_request(
                pr_repo,
                pr_number,
                comment=(
                    "Closed by execution policy: Copilot coding-agent PRs are "
                    "disabled in the strict execution lane."
                ),
                gh_runner=gh_runner,
            )

    return decision
