"""Shell-facing codex/comment/PR wiring extracted from board_consumer."""

from __future__ import annotations

from pathlib import Path
import subprocess
from typing import Any, Callable

import startupai_controller.consumer_codex_runtime_wiring as _codex_runtime_wiring
import startupai_controller.consumer_comment_pr_wiring as _comment_pr_wiring
import startupai_controller.consumer_review_queue_helpers as _review_queue_helpers


def _shell_module():
    """Import the consumer shell lazily to avoid import cycles."""
    from startupai_controller import board_consumer_compat

    return board_consumer_compat


def assemble_codex_prompt(
    issue_context: dict[str, Any],
    issue_ref: str,
    config: Any,
    consumer_config: Any,
    worktree_path: str,
    branch_name: str,
    *,
    dependency_summary: str = "",
    workflow_definition: Any | None = None,
    session_kind: str = "new_work",
    repair_pr_url: str | None = None,
    branch_reconcile_state: str | None = None,
    branch_reconcile_error: str | None = None,
) -> str:
    """Build the codex execution prompt from the current shell seams."""
    shell = _shell_module()
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
        parse_issue_ref=shell.parse_issue_ref,
        resolve_issue_coordinates=shell._resolve_issue_coordinates,
        extract_acceptance_criteria=shell._extract_acceptance_criteria,
        render_workflow_prompt=shell.render_workflow_prompt,
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
    shell = _shell_module()
    return _codex_runtime_wiring.run_codex_session(
        worktree_path,
        prompt,
        schema_path,
        output_path,
        timeout_seconds,
        heartbeat_fn=heartbeat_fn,
        subprocess_runner=subprocess_runner,
        resolve_cli_command_fn=shell._resolve_cli_command,
        logger=shell.logger,
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
    config: Any | None = None,
    issue_ref: str | None = None,
    session_id: str = "legacy-session",
    *,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Ensure a PR exists for the branch."""
    shell = _shell_module()
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
        run_gh=shell._run_gh,
        build_pr_body_fn=shell._build_pr_body,
        repo_to_prefix_for_repo=_comment_pr_wiring.repo_to_prefix_for_repo,
        parse_issue_ref=shell.parse_issue_ref,
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
    gh_runner: Callable[..., str] | None = None,
) -> Callable[[str, str, int, str], bool]:
    """Build the default marker-check helper through ReviewStatePort."""
    shell = _shell_module()
    return _comment_pr_wiring.default_review_comment_checker(
        build_github_port_bundle=shell.build_github_port_bundle,
        gh_runner=gh_runner,
    )


def runtime_comment_poster(
    owner: str,
    repo: str,
    number: int,
    body: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Post an issue comment through the runtime port boundary."""
    shell = _shell_module()
    _comment_pr_wiring.runtime_comment_poster(
        owner,
        repo,
        number,
        body,
        build_github_port_bundle=shell.build_github_port_bundle,
        gh_runner=gh_runner,
    )


def runtime_issue_closer(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Close an issue through the runtime port boundary."""
    shell = _shell_module()
    _comment_pr_wiring.runtime_issue_closer(
        owner,
        repo,
        number,
        build_github_port_bundle=shell.build_github_port_bundle,
        gh_runner=gh_runner,
    )


def runtime_automerge_enabler(
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Enable auto-merge through the runtime port boundary."""
    shell = _shell_module()
    return _comment_pr_wiring.runtime_automerge_enabler(
        pr_repo,
        pr_number,
        build_github_port_bundle=shell.build_github_port_bundle,
        gh_runner=gh_runner,
    )


def runtime_failed_check_rerun(
    pr_repo: str,
    run_id: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Re-run a failed check through the runtime port boundary."""
    shell = _shell_module()
    _comment_pr_wiring.runtime_failed_check_rerun(
        pr_repo,
        run_id,
        build_github_port_bundle=shell.build_github_port_bundle,
        gh_query_error_cls=shell.GhQueryError,
        gh_runner=gh_runner,
    )


def post_consumer_claim_comment(
    issue_ref: str,
    session_id: str,
    repo_prefix: str,
    branch_name: str,
    executor: str,
    config: Any,
    *,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Post the deterministic claim provenance marker."""
    shell = _shell_module()
    _comment_pr_wiring.post_consumer_claim_comment(
        issue_ref,
        session_id,
        repo_prefix,
        branch_name,
        executor,
        config,
        resolve_issue_coordinates=shell._resolve_issue_coordinates,
        consumer_provenance_marker_fn=_comment_pr_wiring.consumer_provenance_marker,
        default_review_comment_checker_fn=shell._default_review_comment_checker,
        runtime_comment_poster_fn=shell._runtime_comment_poster,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def list_open_pr_candidates(
    owner: str,
    repo: str,
    issue_number: int,
    *,
    pr_port: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[Any]:
    """Return open PRs that reference an issue number in the repository."""
    shell = _shell_module()
    return _comment_pr_wiring.list_open_pr_candidates(
        owner,
        repo,
        issue_number,
        build_github_port_bundle=shell.build_github_port_bundle,
        open_pr_match_factory=shell.OpenPullRequestMatch,
        parse_consumer_provenance_fn=_comment_pr_wiring.parse_consumer_provenance,
        pr_port=pr_port,
        gh_runner=gh_runner,
    )


def classify_open_pr_candidates(
    issue_ref: str,
    owner: str,
    repo: str,
    issue_number: int,
    automation_config: Any,
    *,
    expected_branch: str | None = None,
    pr_port: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, Any | None, str]:
    """Classify open PRs for an issue as adoptable, ambiguous, non-local, or none."""
    shell = _shell_module()
    return _comment_pr_wiring.classify_open_pr_candidates(
        issue_ref,
        owner,
        repo,
        issue_number,
        automation_config,
        list_open_pr_candidates_fn=shell._list_open_pr_candidates,
        classify_pr_candidates_pure=shell._classify_pr_candidates_pure,
        expected_branch=expected_branch,
        pr_port=pr_port,
        gh_runner=gh_runner,
    )


def post_result_comment(
    issue_ref: str,
    result: dict[str, Any],
    session_id: str,
    config: Any,
    *,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Post a machine-marker result comment on the issue."""
    shell = _shell_module()
    _comment_pr_wiring.post_result_comment(
        issue_ref,
        result,
        session_id,
        config,
        marker_for=shell._marker_for,
        resolve_issue_coordinates=shell._resolve_issue_coordinates,
        normalize_resolution_payload=shell.normalize_resolution_payload,
        default_review_comment_checker_fn=shell._default_review_comment_checker,
        runtime_comment_poster_fn=shell._runtime_comment_poster,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def post_pr_codex_verdict(
    pr_url: str,
    session_id: str,
    *,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Post the machine-readable codex pass verdict required for auto-merge."""
    shell = _shell_module()
    return _comment_pr_wiring.post_pr_codex_verdict(
        pr_url,
        session_id,
        parse_pr_url=shell._parse_pr_url,
        verdict_marker_text=shell._verdict_marker_text,
        default_review_comment_checker_fn=shell._default_review_comment_checker,
        verdict_comment_body=shell._verdict_comment_body,
        runtime_comment_poster_fn=shell._runtime_comment_poster,
        gh_query_error_cls=shell.GhQueryError,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def backfill_review_verdicts(
    db: Any,
    *,
    session_limit: int = 50,
    review_refs: tuple[str, ...] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, ...]:
    """Re-post missing codex verdict markers for successful review sessions."""
    shell = _shell_module()
    return _comment_pr_wiring.backfill_review_verdicts(
        db,
        post_pr_codex_verdict_fn=shell._post_pr_codex_verdict,
        log_warning=lambda issue_ref, session_id, err: shell.logger.warning(
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
    store: Any,
    entries: list[Any],
    snapshots: dict[tuple[str, int], Any],
    *,
    pr_port: Any,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, ...]:
    """Backfill missing verdict markers using already-fetched PR comment payloads."""
    shell = _shell_module()
    return _review_queue_helpers.backfill_review_verdicts_from_snapshots(
        store,
        entries,
        snapshots,
        pr_port=pr_port,
        post_pr_codex_verdict=shell._post_pr_codex_verdict,
        log_warning=lambda issue_ref, session_id, err: shell.logger.warning(
            "Review verdict backfill failed for %s (%s): %s",
            issue_ref,
            session_id,
            err,
        ),
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def pre_backfill_verdicts_for_due_prs(
    store: Any,
    due_items: list[Any],
    *,
    pr_port: Any | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, ...]:
    """Post missing verdicts before snapshot build for due PRs."""
    shell = _shell_module()
    return _review_queue_helpers.pre_backfill_verdicts_for_due_prs(
        store,
        due_items,
        post_pr_codex_verdict=shell._post_pr_codex_verdict,
        pr_port=pr_port,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        log_warning=lambda issue_ref, err: shell.logger.warning(
            "Pre-backfill verdict failed for %s: %s",
            issue_ref,
            err,
        ),
    )
