"""Shell-facing codex/comment/PR wiring extracted from board_consumer."""

from __future__ import annotations

from pathlib import Path
import logging
import subprocess
from collections.abc import Callable
from typing import Any, cast

import startupai_controller.consumer_comment_pr_helpers as _comment_pr_helpers
import startupai_controller.consumer_codex_runtime_wiring as _codex_runtime_wiring
import startupai_controller.consumer_comment_pr_wiring as _comment_pr_wiring
import startupai_controller.consumer_review_queue_processing as _review_queue_processing
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.board_graph import _resolve_issue_coordinates
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_workflow import render_workflow_prompt
from startupai_controller.consumer_workflow import WorkflowDefinition
from startupai_controller.domain.launch_policy import (
    classify_pr_candidates as _classify_pr_candidates_pure,
)
from startupai_controller.domain.models import (
    OpenPullRequestMatch,
    ReviewQueueEntry,
    ReviewSnapshot,
)
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.ready_flow import GitHubRunnerFn
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.ports.status_store import StatusStorePort
from startupai_controller.domain.repair_policy import (
    marker_for as _marker_for,
    parse_pr_url as _parse_pr_url,
)
from startupai_controller.domain.resolution_policy import normalize_resolution_payload
from startupai_controller.domain.verdict_policy import (
    verdict_comment_body as _verdict_comment_body,
    verdict_marker_text as _verdict_marker_text,
)
from startupai_controller.runtime.wiring import (
    build_github_port_bundle,
    run_runtime_gh as _run_gh,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
    parse_issue_ref,
)

logger = logging.getLogger("board-consumer")
_BUILD_GITHUB_PORT_BUNDLE = cast(
    _comment_pr_helpers.BuildGitHubPortBundleFn,
    build_github_port_bundle,
)


def assemble_codex_prompt(
    issue_context: dict[str, Any],
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
        extract_acceptance_criteria=_comment_pr_wiring.extract_acceptance_criteria,
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
        subprocess_runner=subprocess_runner,
        resolve_cli_command_fn=_codex_runtime_wiring.resolve_cli_command,
        logger=logger,
    )


def parse_codex_result(
    output_path: Path,
    *,
    file_reader: Callable[[Path], str] | None = None,
) -> dict[str, Any] | None:
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
    return _codex_runtime_wiring.create_or_update_pr(
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
        run_gh=_run_gh,
        build_pr_body_fn=build_pr_body,
        repo_to_prefix_for_repo=_comment_pr_wiring.repo_to_prefix_for_repo,
        parse_issue_ref=parse_issue_ref,
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
    return _codex_runtime_wiring.build_pr_body(
        title,
        issue_number,
        issue_ref=issue_ref,
        session_id=session_id,
        repo_prefix=repo_prefix,
        branch_name=branch_name,
        consumer_provenance_marker=_comment_pr_wiring.consumer_provenance_marker,
    )


def default_review_comment_checker(
    *,
    gh_runner: GitHubRunnerFn | None = None,
) -> _comment_pr_helpers.CommentMarkerCheckerFn:
    """Build the default marker-check helper through ReviewStatePort."""
    return _comment_pr_wiring.default_review_comment_checker(
        build_github_port_bundle=_BUILD_GITHUB_PORT_BUNDLE,
        gh_runner=gh_runner,
    )


def runtime_comment_poster(
    owner: str,
    repo: str,
    number: int,
    body: str,
    *,
    gh_runner: GitHubRunnerFn | None = None,
) -> None:
    """Post an issue comment through the runtime port boundary."""
    _comment_pr_wiring.runtime_comment_poster(
        owner,
        repo,
        number,
        body,
        build_github_port_bundle=_BUILD_GITHUB_PORT_BUNDLE,
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
    _comment_pr_wiring.runtime_issue_closer(
        owner,
        repo,
        number,
        build_github_port_bundle=_BUILD_GITHUB_PORT_BUNDLE,
        gh_runner=gh_runner,
    )


def runtime_automerge_enabler(
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: GitHubRunnerFn | None = None,
) -> str:
    """Enable auto-merge through the runtime port boundary."""
    return _comment_pr_wiring.runtime_automerge_enabler(
        pr_repo,
        pr_number,
        build_github_port_bundle=_BUILD_GITHUB_PORT_BUNDLE,
        gh_runner=gh_runner,
    )


def runtime_failed_check_rerun(
    pr_repo: str,
    run_id: int,
    *,
    gh_runner: GitHubRunnerFn | None = None,
) -> None:
    """Re-run a failed check through the runtime port boundary."""
    _comment_pr_wiring.runtime_failed_check_rerun(
        pr_repo,
        run_id,
        build_github_port_bundle=_BUILD_GITHUB_PORT_BUNDLE,
        gh_query_error_cls=GhQueryError,
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
    comment_checker: _comment_pr_helpers.CommentMarkerCheckerFn | None = None,
    comment_poster: _comment_pr_helpers.CommentPosterFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> None:
    """Post the deterministic claim provenance marker."""
    _comment_pr_wiring.post_consumer_claim_comment(
        issue_ref,
        session_id,
        repo_prefix,
        branch_name,
        executor,
        config,
        resolve_issue_coordinates=_resolve_issue_coordinates,
        consumer_provenance_marker_fn=_comment_pr_wiring.consumer_provenance_marker,
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
    return _comment_pr_wiring.list_open_pr_candidates(
        owner,
        repo,
        issue_number,
        build_github_port_bundle=_BUILD_GITHUB_PORT_BUNDLE,
        open_pr_match_factory=OpenPullRequestMatch,
        parse_consumer_provenance_fn=_comment_pr_wiring.parse_consumer_provenance,
        pr_port=pr_port,
        gh_runner=gh_runner,
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
    return _comment_pr_wiring.classify_open_pr_candidates(
        issue_ref,
        owner,
        repo,
        issue_number,
        automation_config,
        list_open_pr_candidates_fn=list_open_pr_candidates,
        classify_pr_candidates_pure=_classify_pr_candidates_pure,
        expected_branch=expected_branch,
        pr_port=pr_port,
        gh_runner=gh_runner,
    )


def post_result_comment(
    issue_ref: str,
    result: dict[str, Any],
    session_id: str,
    config: CriticalPathConfig,
    *,
    comment_checker: _comment_pr_helpers.CommentMarkerCheckerFn | None = None,
    comment_poster: _comment_pr_helpers.CommentPosterFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> None:
    """Post a machine-marker result comment on the issue."""
    _comment_pr_wiring.post_result_comment(
        issue_ref,
        result,
        session_id,
        config,
        marker_for=_marker_for,
        resolve_issue_coordinates=_resolve_issue_coordinates,
        normalize_resolution_payload=normalize_resolution_payload,
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
    comment_checker: _comment_pr_helpers.CommentMarkerCheckerFn | None = None,
    comment_poster: _comment_pr_helpers.CommentPosterFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> bool:
    """Post the machine-readable codex pass verdict required for auto-merge."""
    return _comment_pr_wiring.post_pr_codex_verdict(
        pr_url,
        session_id,
        parse_pr_url=_parse_pr_url,
        verdict_marker_text=_verdict_marker_text,
        default_review_comment_checker_fn=default_review_comment_checker,
        verdict_comment_body=_verdict_comment_body,
        runtime_comment_poster_fn=runtime_comment_poster,
        gh_query_error_cls=GhQueryError,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def backfill_review_verdicts(
    db: StatusStorePort,
    *,
    session_limit: int = 50,
    review_refs: tuple[str, ...] | None = None,
    comment_checker: _comment_pr_helpers.CommentMarkerCheckerFn | None = None,
    comment_poster: _comment_pr_helpers.CommentPosterFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> tuple[str, ...]:
    """Re-post missing codex verdict markers for successful review sessions."""
    return _comment_pr_wiring.backfill_review_verdicts(
        db,
        post_pr_codex_verdict_fn=post_pr_codex_verdict,
        log_warning=lambda issue_ref, session_id, err: logger.warning(
            "Review verdict backfill failed for %s (%s): %s",
            issue_ref,
            session_id,
            err,
        ),
        session_limit=session_limit,
        review_refs=review_refs,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def backfill_review_verdicts_from_snapshots(
    store: SessionStorePort,
    entries: list[ReviewQueueEntry],
    snapshots: dict[tuple[str, int], ReviewSnapshot],
    *,
    pr_port: PullRequestPort,
    comment_poster: _comment_pr_helpers.CommentPosterFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> tuple[str, ...]:
    """Backfill missing verdict markers using already-fetched PR comment payloads."""
    return _review_queue_processing.backfill_review_verdicts_from_snapshots(
        store,
        entries,
        snapshots,
        pr_port=pr_port,
        post_pr_codex_verdict=post_pr_codex_verdict,
        log_warning=lambda issue_ref, session_id, err: logger.warning(
            "Review verdict backfill failed for %s (%s): %s",
            issue_ref,
            session_id,
            err,
        ),
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def pre_backfill_verdicts_for_due_prs(
    store: SessionStorePort,
    due_items: list[ReviewQueueEntry],
    *,
    pr_port: PullRequestPort | None = None,
    comment_checker: _comment_pr_helpers.CommentMarkerCheckerFn | None = None,
    comment_poster: _comment_pr_helpers.CommentPosterFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> tuple[str, ...]:
    """Post missing verdicts before snapshot build for due PRs."""
    return _review_queue_processing.pre_backfill_verdicts_for_due_prs(
        store,
        due_items,
        post_pr_codex_verdict=post_pr_codex_verdict,
        pr_port=pr_port,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        log_warning=lambda issue_ref, err: logger.warning(
            "Pre-backfill verdict failed for %s: %s",
            issue_ref,
            err,
        ),
    )
