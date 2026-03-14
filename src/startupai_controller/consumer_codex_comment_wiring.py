"""Shell-facing codex/comment/PR wiring extracted from board_consumer."""

from __future__ import annotations

import logging
from pathlib import Path
import subprocess
from collections.abc import Callable

import startupai_controller.consumer_comment_pr_shell_wiring as _comment_pr_shell_wiring
import startupai_controller.consumer_codex_runtime_wiring as _codex_runtime_wiring
from startupai_controller.board_graph import _resolve_issue_coordinates
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import CodexSessionResult, IssueContextPayload
from startupai_controller.consumer_workflow import render_workflow_prompt
from startupai_controller.consumer_workflow import WorkflowDefinition
from startupai_controller.domain.models import (
    OpenPullRequestMatch,
    ReviewQueueEntry,
    ReviewSnapshot,
)
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.ready_flow import GitHubRunnerFn
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.ports.status_store import StatusStorePort
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    parse_issue_ref,
)

logger = logging.getLogger("board-consumer")


def assemble_codex_prompt(
    issue_context: IssueContextPayload,
    issue_ref: str,
    config: CriticalPathConfig,
    consumer_config: ConsumerConfig,
    worktree_path: str,
    branch_name: str,
    *,
    dependency_summary: str = "",
    workflow_definition: WorkflowDefinition | None = None,
    session_kind: str = "new_work",
    repair_pr_url: str | None = None,
    branch_reconcile_state: str | None = None,
    branch_reconcile_error: str | None = None,
) -> str:
    """Build the codex execution prompt from the current shell seams."""
    return _codex_runtime_wiring.assemble_codex_prompt(
        issue_context,
        issue_ref,
        config,
        consumer_config,
        worktree_path,
        branch_name,
        dependency_summary=dependency_summary,
        workflow_definition=workflow_definition,
        session_kind=session_kind,
        repair_pr_url=repair_pr_url,
        branch_reconcile_state=branch_reconcile_state,
        branch_reconcile_error=branch_reconcile_error,
        parse_issue_ref=parse_issue_ref,
        resolve_issue_coordinates=_resolve_issue_coordinates,
        extract_acceptance_criteria=extract_acceptance_criteria,
        render_workflow_prompt=render_workflow_prompt,
    )


def run_codex_session(
    worktree_path: str,
    prompt: str,
    schema_path: Path,
    output_path: Path,
    timeout_seconds: int,
    *,
    heartbeat_fn: Callable[[], None] | None = None,
    progress_fn: Callable[[], None] | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> int:
    """Run codex exec with the current shell seams."""
    return _codex_runtime_wiring.run_codex_session(
        worktree_path,
        prompt,
        schema_path,
        output_path,
        timeout_seconds,
        heartbeat_fn=heartbeat_fn,
        progress_fn=progress_fn,
        subprocess_runner=subprocess_runner,
        resolve_cli_command_fn=_codex_runtime_wiring.resolve_cli_command,
        logger=logger,
    )


def parse_codex_result(
    output_path: Path,
    *,
    file_reader: Callable[[Path], str] | None = None,
) -> CodexSessionResult | None:
    """Parse CodexSessionResult JSON from output file."""
    return _codex_runtime_wiring.parse_codex_result(
        output_path,
        file_reader=file_reader,
    )


def create_or_update_pr(
    worktree_path: str,
    branch: str,
    issue_number: int,
    owner: str,
    repo: str,
    title: str,
    config: object | None = None,
    issue_ref: str | None = None,
    session_id: str = "legacy-session",
    *,
    gh_runner: GitHubRunnerFn | None = None,
) -> str:
    """Ensure a PR exists for the branch."""
    return _comment_pr_shell_wiring.create_or_update_pr(
        worktree_path,
        branch,
        issue_number,
        owner,
        repo,
        title,
        config,
        issue_ref,
        session_id,
        gh_runner=gh_runner,
        build_pr_body_fn=build_pr_body,
        repo_to_prefix_for_repo_fn=repo_to_prefix_for_repo,
    )


def build_pr_body(
    title: str,
    issue_number: int,
    *,
    issue_ref: str = "crew#0",
    session_id: str = "legacy-session",
    repo_prefix: str = "crew",
    branch_name: str = "feat/0-legacy",
) -> str:
    """Build the required PR body contract."""
    return _comment_pr_shell_wiring.build_pr_body(
        title,
        issue_number,
        issue_ref=issue_ref,
        session_id=session_id,
        repo_prefix=repo_prefix,
        branch_name=branch_name,
        consumer_provenance_marker_fn=consumer_provenance_marker,
    )


def default_review_comment_checker(
    *,
    gh_runner: GitHubRunnerFn | None = None,
) -> _comment_pr_shell_wiring.CommentMarkerCheckerFn:
    """Build the default marker-check helper through ReviewStatePort."""
    return _comment_pr_shell_wiring.default_review_comment_checker(gh_runner=gh_runner)


def runtime_comment_poster(
    owner: str,
    repo: str,
    number: int,
    body: str,
    *,
    gh_runner: GitHubRunnerFn | None = None,
) -> None:
    """Post an issue comment through the runtime port boundary."""
    _comment_pr_shell_wiring.runtime_comment_poster(
        owner,
        repo,
        number,
        body,
        gh_runner=gh_runner,
    )


def runtime_issue_closer(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: GitHubRunnerFn | None = None,
) -> None:
    """Close an issue through the runtime port boundary."""
    _comment_pr_shell_wiring.runtime_issue_closer(
        owner,
        repo,
        number,
        gh_runner=gh_runner,
    )


def runtime_automerge_enabler(
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: GitHubRunnerFn | None = None,
) -> str:
    """Enable auto-merge through the runtime port boundary."""
    return _comment_pr_shell_wiring.runtime_automerge_enabler(
        pr_repo,
        pr_number,
        gh_runner=gh_runner,
    )


def runtime_failed_check_rerun(
    pr_repo: str,
    run_id: int,
    *,
    gh_runner: GitHubRunnerFn | None = None,
) -> None:
    """Re-run a failed check through the runtime port boundary."""
    _comment_pr_shell_wiring.runtime_failed_check_rerun(
        pr_repo,
        run_id,
        gh_runner=gh_runner,
    )


def post_consumer_claim_comment(
    issue_ref: str,
    session_id: str,
    repo_prefix: str,
    branch_name: str,
    executor: str,
    config: CriticalPathConfig,
    *,
    comment_checker: _comment_pr_shell_wiring.CommentMarkerCheckerFn | None = None,
    comment_poster: _comment_pr_shell_wiring.CommentPosterFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> None:
    """Post the deterministic claim provenance marker."""
    _comment_pr_shell_wiring.post_consumer_claim_comment(
        issue_ref,
        session_id,
        repo_prefix,
        branch_name,
        executor,
        config,
        consumer_provenance_marker_fn=consumer_provenance_marker,
        default_review_comment_checker_fn=default_review_comment_checker,
        runtime_comment_poster_fn=runtime_comment_poster,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def list_open_pr_candidates(
    owner: str,
    repo: str,
    issue_number: int,
    *,
    pr_port: PullRequestPort | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> list[OpenPullRequestMatch]:
    """Return open PRs that reference an issue number in the repository."""
    return _comment_pr_shell_wiring.list_open_pr_candidates(
        owner,
        repo,
        issue_number,
        pr_port=pr_port,
        gh_runner=gh_runner,
        parse_consumer_provenance_fn=parse_consumer_provenance,
    )


def classify_open_pr_candidates(
    issue_ref: str,
    owner: str,
    repo: str,
    issue_number: int,
    automation_config: BoardAutomationConfig,
    *,
    expected_branch: str | None = None,
    pr_port: PullRequestPort | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> tuple[str, OpenPullRequestMatch | None, str]:
    """Classify open PRs for an issue as adoptable, ambiguous, non-local, or none."""
    return _comment_pr_shell_wiring.classify_open_pr_candidates(
        issue_ref,
        owner,
        repo,
        issue_number,
        automation_config,
        expected_branch=expected_branch,
        pr_port=pr_port,
        gh_runner=gh_runner,
        list_open_pr_candidates_fn=list_open_pr_candidates,
    )


def post_result_comment(
    issue_ref: str,
    result: CodexSessionResult,
    session_id: str,
    config: CriticalPathConfig,
    *,
    comment_checker: _comment_pr_shell_wiring.CommentMarkerCheckerFn | None = None,
    comment_poster: _comment_pr_shell_wiring.CommentPosterFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> None:
    """Post a machine-marker result comment on the issue."""
    _comment_pr_shell_wiring.post_result_comment(
        issue_ref,
        result,
        session_id,
        config,
        default_review_comment_checker_fn=default_review_comment_checker,
        runtime_comment_poster_fn=runtime_comment_poster,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def post_pr_codex_verdict(
    pr_url: str,
    session_id: str,
    *,
    comment_checker: _comment_pr_shell_wiring.CommentMarkerCheckerFn | None = None,
    comment_poster: _comment_pr_shell_wiring.CommentPosterFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> bool:
    """Post the machine-readable codex pass verdict required for auto-merge."""
    return _comment_pr_shell_wiring.post_pr_codex_verdict(
        pr_url,
        session_id,
        default_review_comment_checker_fn=default_review_comment_checker,
        runtime_comment_poster_fn=runtime_comment_poster,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def backfill_review_verdicts(
    db: StatusStorePort,
    *,
    session_limit: int = 50,
    review_refs: tuple[str, ...] | None = None,
    comment_checker: _comment_pr_shell_wiring.CommentMarkerCheckerFn | None = None,
    comment_poster: _comment_pr_shell_wiring.CommentPosterFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> tuple[str, ...]:
    """Re-post missing codex verdict markers for successful review sessions."""
    return _comment_pr_shell_wiring.backfill_review_verdicts(
        db,
        session_limit=session_limit,
        review_refs=review_refs,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        post_pr_codex_verdict_fn=post_pr_codex_verdict,
    )


def backfill_review_verdicts_from_snapshots(
    store: SessionStorePort,
    entries: list[ReviewQueueEntry],
    snapshots: dict[tuple[str, int], ReviewSnapshot],
    *,
    pr_port: PullRequestPort,
    comment_poster: _comment_pr_shell_wiring.CommentPosterFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> tuple[str, ...]:
    """Backfill missing verdict markers using already-fetched PR comment payloads."""
    return _comment_pr_shell_wiring.backfill_review_verdicts_from_snapshots(
        store,
        entries,
        snapshots,
        pr_port=pr_port,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        post_pr_codex_verdict_fn=post_pr_codex_verdict,
    )


def pre_backfill_verdicts_for_due_prs(
    store: SessionStorePort,
    due_items: list[ReviewQueueEntry],
    *,
    pr_port: PullRequestPort | None = None,
    comment_checker: _comment_pr_shell_wiring.CommentMarkerCheckerFn | None = None,
    comment_poster: _comment_pr_shell_wiring.CommentPosterFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> tuple[str, ...]:
    """Post missing verdicts before snapshot build for due PRs."""
    return _comment_pr_shell_wiring.pre_backfill_verdicts_for_due_prs(
        store,
        due_items,
        pr_port=pr_port,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        post_pr_codex_verdict_fn=post_pr_codex_verdict,
    )


def extract_acceptance_criteria(body: str) -> str:
    """Extract acceptance criteria from issue body text."""
    return _comment_pr_shell_wiring.extract_acceptance_criteria(body)


def repo_to_prefix_for_repo(repo: str) -> str:
    """Best-effort repo name to board prefix mapping."""
    return _comment_pr_shell_wiring.repo_to_prefix_for_repo(repo)


def consumer_provenance_marker(
    *,
    session_id: str,
    issue_ref: str,
    repo_prefix: str,
    branch_name: str,
    executor: str,
) -> str:
    """Build the machine-readable provenance marker for issues and PRs."""
    return _comment_pr_shell_wiring.consumer_provenance_marker(
        session_id=session_id,
        issue_ref=issue_ref,
        repo_prefix=repo_prefix,
        branch_name=branch_name,
        executor=executor,
    )


def parse_consumer_provenance(text: str) -> dict[str, str] | None:
    """Parse the consumer provenance marker from free text."""
    return _comment_pr_shell_wiring.parse_consumer_provenance(text)
