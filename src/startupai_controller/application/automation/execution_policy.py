"""Execution-policy use case for protected coding lanes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.board_graph import _resolve_issue_coordinates
from startupai_controller.domain.models import ExecutionPolicyDecision, LinkedIssue
from startupai_controller.domain.repair_policy import (
    MARKER_PREFIX,
    marker_for as _marker_for,
    parse_consumer_provenance as _parse_consumer_provenance,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    ConfigError,
    GhQueryError,
    parse_issue_ref,
)


def _select_execution_policy_issues(
    *,
    linked: list[LinkedIssue],
    automation_config: BoardAutomationConfig,
    legacy_copilot_only_mode: bool,
    review_state_port: ReviewStatePort,
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
        executor = (
            review_state_port.project_field_value(issue.ref, "Executor").strip().lower()
        )
        if executor in automation_config.execution_authority_executors:
            protected_linked.append(issue)
    return protected_linked


def _apply_execution_policy_to_issue(
    *,
    issue: LinkedIssue,
    policy: str,
    decision: ExecutionPolicyDecision,
    dry_run: bool,
    pr_url: str,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    is_copilot_actor: Callable[[str], bool],
) -> None:
    """Apply the execution policy to one protected linked issue."""
    old_status = review_state_port.get_issue_status(issue.ref) or "NOT_ON_BOARD"
    changed = old_status in {"In Progress", "Review", "Ready"}
    if changed and not dry_run:
        board_port.set_issue_status(
            issue.ref, "Blocked" if policy == "block" else "Ready"
        )
    if changed and policy == "block":
        decision.blocked.append(issue.ref)
    elif changed:
        decision.requeued.append(issue.ref)

    repo_slug = f"{issue.owner}/{issue.repo}"
    assignees = list(review_state_port.issue_assignees(repo_slug, issue.number))
    filtered_assignees = [login for login in assignees if not is_copilot_actor(login)]
    if filtered_assignees != assignees:
        if not dry_run:
            board_port.set_issue_assignees(repo_slug, issue.number, filtered_assignees)
        decision.copilot_unassigned.append(issue.ref)

    marker = _marker_for("execution-policy", issue.ref)
    if review_state_port.comment_exists(repo_slug, issue.number, marker):
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
        board_port.post_issue_comment(repo_slug, issue.number, body)


def _post_execution_policy_pr_comment(
    *,
    pr_repo: str,
    pr_number: int,
    owner: str,
    repo: str,
    policy: str,
    dry_run: bool,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
) -> None:
    """Post the top-level execution policy PR comment if absent."""
    pr_marker = f"<!-- {MARKER_PREFIX}:execution-policy-pr:{pr_repo}#{pr_number} -->"
    repo_slug = f"{owner}/{repo}"
    if review_state_port.comment_exists(repo_slug, pr_number, pr_marker):
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
        board_port.post_issue_comment(repo_slug, pr_number, pr_body)


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
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    is_copilot_actor: Callable[[str], bool],
) -> ExecutionPolicyDecision:
    """Enforce local execution authority for protected coding PRs."""
    del project_owner, project_number
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

    linked = [
        LinkedIssue(
            owner=issue_owner,
            repo=issue_repo,
            number=issue_number,
            ref=issue_ref,
        )
        for issue_ref in pr_port.linked_issue_refs(pr_repo, pr_number)
        for issue_owner, issue_repo, issue_number in [
            _resolve_issue_coordinates(issue_ref, config)
        ]
    ]
    protected_linked = _select_execution_policy_issues(
        linked=linked,
        automation_config=automation_config,
        legacy_copilot_only_mode=legacy_copilot_only_mode,
        review_state_port=review_state_port,
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
            dry_run=dry_run,
            pr_url=pr_url,
            review_state_port=review_state_port,
            board_port=board_port,
            is_copilot_actor=is_copilot_actor,
        )

    decision.enforced_pr = True
    _post_execution_policy_pr_comment(
        pr_repo=pr_repo,
        pr_number=pr_number,
        owner=owner,
        repo=repo,
        policy=policy,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
    )

    if state == "OPEN" and policy == "close":
        decision.pr_closed = True
        if not dry_run:
            pr_port.close_pull_request(
                pr_repo,
                pr_number,
                comment=(
                    "Closed by execution policy: Copilot coding-agent PRs are "
                    "disabled in the strict execution lane."
                ),
            )

    return decision


@dataclass(frozen=True)
class ExecutionPolicyPrContext:
    actor: str
    state: str
    url: str
    provenance: dict[str, str] | None


def load_execution_policy_pr_context(
    *,
    pr_repo: str,
    pr_number: int,
    pr_port: PullRequestPort,
) -> ExecutionPolicyPrContext:
    """Load the PR context needed for execution policy decisions."""
    pr_data = pr_port.get_pull_request(pr_repo, pr_number)
    if pr_data is None:
        raise GhQueryError(f"Failed loading PR payload for {pr_repo}#{pr_number}.")
    actor = (pr_data.author or "").strip().lower()
    state = (getattr(pr_data, "state", "") or "OPEN").strip().upper()
    url = pr_data.url
    body = pr_data.body or ""
    return ExecutionPolicyPrContext(
        actor=actor,
        state=state,
        url=url,
        provenance=_parse_consumer_provenance(body),
    )
