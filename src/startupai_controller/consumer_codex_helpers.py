"""Codex prompt, execution, and PR helper functions."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from typing import Any, Callable


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
    parsed = parse_issue_ref(issue_ref)
    owner, repo, number = resolve_issue_coordinates(issue_ref, config)
    title = issue_context.get("title", f"Issue #{parsed.number}")
    body = issue_context.get("body", "")
    acceptance = extract_acceptance_criteria(body)

    prompt = f"""\
Issue: {title} (#{number})
Repository: {owner}/{repo}
Base branch: main

Working directory: {worktree_path}
Branch: {branch_name}

Dependency summary:
{dependency_summary or "(No graph dependencies.)"}
(All listed predecessors are Done.)

Acceptance criteria:
{acceptance or "(See issue body for details.)"}

Constraints:
- You are working in an EXISTING worktree at the path above on the branch above.
  Do NOT create a new worktree, branch, or checkout.
- Do not modify board state, issue state, or project fields.
- Do not open or create pull requests — PR lifecycle is consumer-owned.
- Validate your work: {consumer_config.validation_cmd}
- Commit changes and push to origin/{branch_name}.
- If the issue is already satisfied on main and no code changes are needed, set
  `resolution` with concrete code refs, merged PRs or commits, and the exact
  validation result on canonical main. Do not leave `resolution` null in a
  successful no-op case.
- Return ONLY JSON matching the provided schema. Populate every schema field;
  use null or [] when applicable. No prose, no markdown."""

    if session_kind == "repair":
        prompt += (
            "\n\nRepair context:\n"
            f"- Existing PR: {repair_pr_url or '(unknown)'}\n"
            f"- Branch reconcile state: {branch_reconcile_state or 'not-run'}\n"
        )
        if branch_reconcile_error:
            prompt += f"- Branch reconcile error: {branch_reconcile_error}\n"
        prompt += (
            "- This is an in-place repair of an existing PR branch.\n"
            "- First make the branch cleanly mergeable with main.\n"
            "- If the branch currently has merge conflicts from origin/main, "
            "resolve them before running final validation.\n"
        )

    if workflow_definition is not None:
        workflow_context = {
            "issue_ref": issue_ref,
            "issue_title": title,
            "repository": f"{owner}/{repo}",
            "worktree_path": worktree_path,
            "branch_name": branch_name,
            "dependency_summary": dependency_summary or "(No graph dependencies.)",
            "acceptance_criteria": acceptance or "(See issue body for details.)",
            "validation_cmd": consumer_config.validation_cmd,
        }
        rendered = render_workflow_prompt(workflow_definition, workflow_context)
        prompt = f"{prompt}\n\nRepository workflow instructions:\n{rendered}"

    return prompt


def resolve_cli_command(
    command: str,
    *,
    which: Callable[[str], str | None] = shutil.which,
    home_getter: Callable[[], Path] = Path.home,
) -> str:
    """Resolve a CLI binary without relying on shell PATH setup."""
    resolved = which(command)
    if resolved:
        return resolved

    home = home_getter()
    candidates = [
        home / ".local" / "bin" / command,
        home / ".local" / "share" / "pnpm" / command,
        home / ".npm-global" / "bin" / command,
        Path("/usr/local/bin") / command,
        Path("/usr/bin") / command,
    ]
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)

    return command


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
    popen_factory: Callable[..., Any],
    sleep_fn: Callable[[float], None] = time.sleep,
    logger: Any,
) -> int:
    """Run codex exec with a timeout wrapper and return the exit code."""
    codex_cmd = resolve_cli_command_fn("codex")
    args = [
        "timeout",
        str(timeout_seconds),
        codex_cmd,
        "exec",
        "-C",
        worktree_path,
        "--full-auto",
        "--output-schema",
        str(schema_path),
        "-o",
        str(output_path),
        prompt,
    ]
    if subprocess_runner is not None:
        result = subprocess_runner(args, capture_output=True, text=True)
    else:
        proc_args = args[2:]
        with tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as stdout_log, tempfile.TemporaryFile(
            mode="w+t",
            encoding="utf-8",
        ) as stderr_log:
            process = popen_factory(
                proc_args,
                stdout=stdout_log,
                stderr=stderr_log,
                text=True,
            )
            deadline = time.monotonic() + timeout_seconds
            while True:
                if heartbeat_fn is not None:
                    heartbeat_fn()
                rc = process.poll()
                if rc is not None:
                    stdout_log.flush()
                    stderr_log.flush()
                    stdout_log.seek(0)
                    stderr_log.seek(0)
                    result = subprocess.CompletedProcess(
                        args=proc_args,
                        returncode=rc,
                        stdout=stdout_log.read(),
                        stderr=stderr_log.read(),
                    )
                    break
                if time.monotonic() >= deadline:
                    process.kill()
                    process.wait()
                    stdout_log.flush()
                    stderr_log.flush()
                    stdout_log.seek(0)
                    stderr_log.seek(0)
                    result = subprocess.CompletedProcess(
                        args=proc_args,
                        returncode=124,
                        stdout=stdout_log.read(),
                        stderr=stderr_log.read(),
                    )
                    break
                sleep_fn(15)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        if detail:
            logger.error("codex exec failed (exit %s): %s", result.returncode, detail)
        else:
            logger.error("codex exec failed (exit %s) with no output", result.returncode)
    elif not output_path.exists():
        logger.error("codex exec exited 0 but produced no output file: %s", output_path)
    return result.returncode


def parse_codex_result(
    output_path: Path,
    *,
    file_reader: Callable[[Path], str] | None = None,
) -> dict[str, Any] | None:
    """Parse the codex result JSON. Return None on read/parse failure."""
    reader = file_reader or (lambda path: path.read_text(encoding="utf-8"))
    try:
        return json.loads(reader(output_path))
    except (OSError, json.JSONDecodeError):
        return None


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
    """Build PR body with required tag-contract fields."""
    marker = consumer_provenance_marker(
        session_id=session_id,
        issue_ref=issue_ref,
        repo_prefix=repo_prefix,
        branch_name=branch_name,
        executor="codex",
    )
    return (
        "## Summary\n\n"
        f"Automated implementation for #{issue_number}.\n\n"
        f"Closes #{issue_number}\n\n"
        "Lead Agent: codex\n"
        "Handoff: none\n\n"
        f"{marker}\n"
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
    resolved_issue_ref = issue_ref or f"{repo_to_prefix_for_repo(repo)}#{issue_number}"
    try:
        existing = run_gh(
            [
                "pr",
                "view",
                branch,
                "--repo",
                f"{owner}/{repo}",
                "--json",
                "url,body",
            ],
            gh_runner=gh_runner,
        )
        pr_data = json.loads(existing)
        pr_url = pr_data.get("url", "")
        body = pr_data.get("body", "")

        needs_edit = False
        required_lines = {
            "Lead Agent:": "Lead Agent: codex",
            "Handoff:": "Handoff: none",
            f"Closes #{issue_number}": f"Closes #{issue_number}",
            f"issue={resolved_issue_ref}": f"issue={resolved_issue_ref}",
        }
        for marker in required_lines:
            if marker not in body:
                needs_edit = True
                break

        if needs_edit:
            new_body = build_pr_body_fn(
                title,
                issue_number,
                issue_ref=resolved_issue_ref,
                session_id=session_id,
                repo_prefix=parse_issue_ref(resolved_issue_ref).prefix,
                branch_name=branch,
            )
            run_gh(
                [
                    "pr",
                    "edit",
                    branch,
                    "--repo",
                    f"{owner}/{repo}",
                    "--body",
                    new_body,
                ],
                gh_runner=gh_runner,
            )

        return pr_url
    except Exception:
        pass

    body = build_pr_body_fn(
        title,
        issue_number,
        issue_ref=resolved_issue_ref,
        session_id=session_id,
        repo_prefix=parse_issue_ref(resolved_issue_ref).prefix,
        branch_name=branch,
    )
    output = run_gh(
        [
            "pr",
            "create",
            "--repo",
            f"{owner}/{repo}",
            "--head",
            branch,
            "--title",
            f"{title} (#{issue_number})",
            "--body",
            body,
        ],
        gh_runner=gh_runner,
    )
    return output.strip()
