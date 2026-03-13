"""Codex execution and PR wiring extracted from board_consumer."""

from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import time
from typing import Any, Callable

from startupai_controller import consumer_codex_helpers as _codex_helpers
from startupai_controller.consumer_types import CodexSessionResult


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
    parse_issue_ref: Callable[[str], Any],
    resolve_issue_coordinates: Callable[[str, Any], tuple[str, str, int]],
    extract_acceptance_criteria: Callable[[str], str],
    render_workflow_prompt: Callable[[Any, dict[str, Any]], str],
) -> str:
    """Build the codex execution prompt from the repo contract."""
    return _codex_helpers.assemble_codex_prompt(
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
        resolve_issue_coordinates=resolve_issue_coordinates,
        extract_acceptance_criteria=extract_acceptance_criteria,
        render_workflow_prompt=render_workflow_prompt,
    )


def resolve_cli_command(command: str) -> str:
    """Resolve a CLI binary without relying on interactive shell PATH setup."""
    return _codex_helpers.resolve_cli_command(
        command,
        which=shutil.which,
        home_getter=Path.home,
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
    resolve_cli_command_fn: Callable[[str], str],
    logger: Any,
) -> int:
    """Run codex exec with a timeout wrapper."""
    return _codex_helpers.run_codex_session(
        worktree_path,
        prompt,
        schema_path,
        output_path,
        timeout_seconds,
        heartbeat_fn=heartbeat_fn,
        subprocess_runner=subprocess_runner,
        resolve_cli_command_fn=resolve_cli_command_fn,
        popen_factory=subprocess.Popen,
        sleep_fn=time.sleep,
        logger=logger,
    )


def parse_codex_result(
    output_path: Path,
    *,
    file_reader: Callable[[Path], str] | None = None,
) -> CodexSessionResult | None:
    """Parse the Codex result JSON."""
    return _codex_helpers.parse_codex_result(
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
    run_gh: Callable[..., str],
    build_pr_body_fn: Callable[..., str],
    repo_to_prefix_for_repo: Callable[[str], str],
    parse_issue_ref: Callable[[str], Any],
) -> str:
    """Ensure a PR exists for the branch and return its URL."""
    return _codex_helpers.create_or_update_pr(
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
        run_gh=run_gh,
        build_pr_body_fn=build_pr_body_fn,
        repo_to_prefix_for_repo=repo_to_prefix_for_repo,
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
    consumer_provenance_marker: Callable[..., str],
) -> str:
    """Build the consumer-owned PR body."""
    return _codex_helpers.build_pr_body(
        title,
        issue_number,
        issue_ref=issue_ref,
        session_id=session_id,
        repo_prefix=repo_prefix,
        branch_name=branch_name,
        consumer_provenance_marker=consumer_provenance_marker,
    )
