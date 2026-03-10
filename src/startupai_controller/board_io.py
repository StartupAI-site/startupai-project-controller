"""GitHub API interaction layer for board automation.

Extracted from board_automation.py (ADR-018 step 3). Contains all GitHub
I/O functions: gh CLI runner, comment operations, project field queries,
issue/PR queries, and codex/gate queries.

All functions preserve existing DI signatures (gh_runner, board_info_resolver,
board_mutator callables).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Sequence


from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    ConfigError,
    GhQueryError,
    parse_issue_ref,
)
from startupai_controller.promote_ready import (
    BoardInfo,
)
from startupai_controller.domain.models import (
    CheckObservation,
    OpenPullRequest,
    PrGateStatus,
)
from startupai_controller.gh_cli_timeout import gh_command_timeout_seconds
from startupai_controller.github_http import GitHubTransportError, run_github_command

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MARKER_PREFIX = "startupai-board-bot"
from startupai_controller.domain.scheduling_policy import (  # noqa: E402
    VALID_EXECUTORS,
    priority_rank as _priority_rank,  # re-export (compat)
)
COPILOT_CODING_AGENT_LOGINS = {
    "app/copilot-swe-agent",
    "copilot-swe-agent[bot]",
    "copilot",
}
_GH_RETRY_DELAYS_SECONDS = (1.0, 2.0, 4.0)
_GH_RETRYABLE_ERROR_MARKERS = (
    "error connecting to api.github.com",
    "connection reset by peer",
    "tls handshake timeout",
    "i/o timeout",
    "timeout awaiting response headers",
    "timed out after",
    "temporary failure in name resolution",
)
_GH_RATE_LIMIT_ERROR_MARKERS = (
    "api rate limit exceeded",
    "secondary rate limit",
)
_GH_COMMAND_TIMEOUT_SECONDS = gh_command_timeout_seconds()
_REQUIRED_STATUS_CHECKS_CACHE_TTL_SECONDS = 900
_required_status_checks_ttl_cache: dict[
    tuple[str, str],
    tuple[float, set[str]],
] = {}
_BOARD_SNAPSHOT_CACHE_TTL_SECONDS = 15
_cycle_board_snapshot_cache: dict[
    tuple[str, int],
    tuple[float, "CycleBoardSnapshot"],
] = {}


class GhCommandError(GhQueryError):
    """Structured GitHub CLI failure with normalized operation and kind."""

    def __init__(
        self,
        *,
        operation_type: str,
        failure_kind: str,
        command_excerpt: str,
        detail: str,
        rate_limit_reset_at: int | None = None,
    ) -> None:
        self.operation_type = operation_type
        self.failure_kind = failure_kind
        self.command_excerpt = command_excerpt
        self.detail = detail
        self.rate_limit_reset_at = rate_limit_reset_at
        super().__init__(
            f"{operation_type}:{failure_kind}:Failed running gh "
            f"{command_excerpt}: {detail}"
        )


def _classify_gh_failure_kind(detail: str) -> str:
    """Normalize GitHub CLI failure text into a stable reason code."""
    text = detail.strip().lower()
    if any(marker in text for marker in _GH_RATE_LIMIT_ERROR_MARKERS):
        return "rate_limit"
    if any(marker in text for marker in _GH_RETRYABLE_ERROR_MARKERS):
        return "network"
    if "authentication failed" in text or "http 401" in text or "must authenticate" in text:
        return "auth"
    if "http 403" in text and "rate limit" not in text:
        return "auth"
    if "http 5" in text or "server error" in text or "bad gateway" in text:
        return "github_outage"
    if "invalid character" in text or "unexpected token" in text:
        return "invalid_response"
    return "query_failed"


def _gh_error(
    args: Sequence[str],
    *,
    operation_type: str,
    detail: str,
) -> GhCommandError:
    """Build a structured GitHub command error."""
    return GhCommandError(
        operation_type=operation_type,
        failure_kind=_classify_gh_failure_kind(detail),
        command_excerpt=" ".join(args[:3]),
        detail=detail.strip() or "unknown-gh-error",
        rate_limit_reset_at=None,
    )


def gh_reason_code(error: Exception) -> str:
    """Return a stable machine-readable reason code for GitHub failures."""
    if isinstance(error, GhCommandError):
        return error.failure_kind
    return _classify_gh_failure_kind(str(error))

# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def _run_gh(
    args: list[str],
    *,
    gh_runner: Callable[..., str] | None = None,
    operation_type: str = "query",
) -> str:
    """Run a gh CLI command and return stdout. DI point: gh_runner."""
    if gh_runner is not None:
        try:
            return gh_runner(args)
        except GhQueryError:
            raise
        except subprocess.CalledProcessError as error:
            detail = (error.output or "").strip() or str(error)
            raise _gh_error(
                args,
                operation_type=operation_type,
                detail=detail,
            ) from error

    try:
        http_output = run_github_command(
            args,
            operation_type=operation_type,
            timeout_seconds=_GH_COMMAND_TIMEOUT_SECONDS,
            retry_delays=_GH_RETRY_DELAYS_SECONDS,
        )
        if http_output is not None:
            return http_output
    except GitHubTransportError as error:
        raise GhCommandError(
            operation_type=error.operation_type,
            failure_kind=error.failure_kind,
            command_excerpt=error.command_excerpt,
            detail=error.detail,
            rate_limit_reset_at=error.rate_limit_reset_at,
        ) from error

    gh_command = ["gh"] + args
    for attempt in range(len(_GH_RETRY_DELAYS_SECONDS) + 1):
        try:
            return subprocess.check_output(
                gh_command,
                text=True,
                stderr=subprocess.STDOUT,
                timeout=_GH_COMMAND_TIMEOUT_SECONDS,
            )
        except OSError as error:
            raise GhCommandError(
                operation_type=operation_type,
                failure_kind="network",
                command_excerpt=" ".join(args[:3]),
                detail=str(error),
            ) from error
        except subprocess.TimeoutExpired as error:
            detail = f"timed out after {_GH_COMMAND_TIMEOUT_SECONDS:.1f}s"
            raise GhCommandError(
                operation_type=operation_type,
                failure_kind="network",
                command_excerpt=" ".join(args[:3]),
                detail=detail,
            ) from error
        except subprocess.CalledProcessError as error:
            output = error.output.strip()
            failure_kind = _classify_gh_failure_kind(output)
            is_retryable = failure_kind in {"network", "rate_limit", "github_outage"}
            if is_retryable and attempt < len(_GH_RETRY_DELAYS_SECONDS):
                time.sleep(_GH_RETRY_DELAYS_SECONDS[attempt])
                continue
            raise GhCommandError(
                operation_type=operation_type,
                failure_kind=failure_kind,
                command_excerpt=" ".join(args[:3]),
                detail=output,
            ) from error


def rerun_actions_run(
    pr_repo: str,
    run_id: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Rerun a GitHub Actions workflow run."""
    _run_gh(
        ["run", "rerun", str(run_id), "--repo", pr_repo],
        gh_runner=gh_runner,
        operation_type="check_rerun",
    )


def update_pull_request_branch(
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Update a PR branch from its base branch."""
    _run_gh(
        ["pr", "update-branch", str(pr_number), "--repo", pr_repo],
        gh_runner=gh_runner,
        operation_type="mutation",
    )


def enable_pull_request_automerge(
    pr_repo: str,
    pr_number: int,
    *,
    delete_branch: bool = False,
    confirm_retries: int = 3,
    confirm_delay_seconds: float = 1.0,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Enable squash auto-merge on a PR.

    Returns:
        "confirmed" — GitHub persisted autoMergeRequest
        "pending"   — enable call succeeded but GitHub hasn't confirmed yet

    Raises:
        GhQueryError — if the enable call itself fails (transport/API error).
            Callers should let this propagate for partial-failure backoff.
    """
    args = ["pr", "merge", str(pr_number), "--repo", pr_repo, "--auto", "--squash"]
    if delete_branch:
        args.append("--delete-branch")
    # Enable call — let GhQueryError propagate (no catch).
    _run_gh(args, gh_runner=gh_runner, operation_type="automerge")

    # Bounded verification — catch transport errors per-read.
    for _attempt in range(confirm_retries):
        time.sleep(confirm_delay_seconds)
        try:
            raw = _run_gh(
                ["pr", "view", str(pr_number), "--repo", pr_repo,
                 "--json", "autoMergeRequest"],
                gh_runner=gh_runner,
            )
            data = json.loads(raw) if raw else {}
            if data.get("autoMergeRequest") is not None:
                return "confirmed"
        except (GhQueryError, json.JSONDecodeError):
            continue
    return "pending"


def close_pull_request(
    pr_repo: str,
    pr_number: int,
    *,
    comment: str,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Close a PR with a deterministic policy comment."""
    _run_gh(
        [
            "pr",
            "close",
            str(pr_number),
            "--repo",
            pr_repo,
            "--comment",
            comment,
        ],
        gh_runner=gh_runner,
        operation_type="mutation",
    )


def close_issue(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Close an issue without altering any other fields."""
    _run_gh(
        [
            "api",
            f"repos/{owner}/{repo}/issues/{number}",
            "-X",
            "PATCH",
            "-f",
            "state=closed",
        ],
        gh_runner=gh_runner,
        operation_type="mutation",
    )


def _is_copilot_coding_agent_actor(login: str) -> bool:
    """Return True when actor is Copilot coding-agent identity."""
    normalized = login.strip().lower()
    return normalized in COPILOT_CODING_AGENT_LOGINS


def _is_automation_login(login: str) -> bool:
    """Return True when a comment author is an automation identity."""
    normalized = login.strip().lower()
    if not normalized:
        return False
    return (
        normalized.endswith("[bot]")
        or normalized.startswith("app/")
        or normalized in COPILOT_CODING_AGENT_LOGINS
        or normalized in {"codex-bot", "codex", "claude"}
    )


def _parse_github_timestamp(raw: str) -> datetime | None:
    """Parse an ISO timestamp from GitHub payloads."""
    text = raw.strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Marker & comment operations
# ---------------------------------------------------------------------------


def _marker_for(kind: str, ref: str) -> str:
    """Generate an HTML comment marker for idempotency."""
    return f"<!-- {MARKER_PREFIX}:{kind}:{ref} -->"


def _comment_exists(
    owner: str,
    repo: str,
    number: int,
    marker: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Check if a comment with the given marker already exists on an issue."""
    try:
        output = _run_gh(
            [
                "api",
                f"repos/{owner}/{repo}/issues/{number}/comments",
                "--paginate",
                "-q",
                ".[].body",
            ],
            gh_runner=gh_runner,
        )
        return marker in output
    except GhQueryError:
        return False


def _post_comment(
    owner: str,
    repo: str,
    number: int,
    body: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Post a comment on a GitHub issue."""
    _run_gh(
        [
            "api",
            f"repos/{owner}/{repo}/issues/{number}/comments",
            "-f",
            f"body={body}",
        ],
        gh_runner=gh_runner,
    )


def list_issue_comment_bodies(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Return comment bodies for an issue in ascending API order."""
    output = _run_gh(
        [
            "api",
            f"repos/{owner}/{repo}/issues/{number}/comments",
            "--paginate",
        ],
        gh_runner=gh_runner,
        operation_type="query",
    )
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise GhQueryError(
            f"Failed querying comments for {owner}/{repo}#{number}: invalid JSON."
        ) from error
    if not isinstance(payload, list):
        raise GhQueryError(
            f"Failed querying comments for {owner}/{repo}#{number}: invalid payload."
        )
    results: list[str] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        body = str(item.get("body") or "")
        if body:
            results.append(body)
    return results


def memoized_list_issue_comment_bodies(
    memo: CycleGitHubMemo,
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Return issue comment bodies using a cycle-local cache."""
    key = (owner, repo, number)
    cached = memo.issue_comment_bodies.get(key)
    if cached is not None:
        return list(cached)
    bodies = list_issue_comment_bodies(owner, repo, number, gh_runner=gh_runner)
    memo.issue_comment_bodies[key] = list(bodies)
    return bodies


def _query_issue_comments(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[dict]:
    """Compatibility wrapper for adapter-owned issue comment queries."""
    from startupai_controller.adapters.github_cli import _query_issue_comments as _adapter_query_issue_comments

    return _adapter_query_issue_comments(
        owner,
        repo,
        number,
        gh_runner=gh_runner or _run_gh,
    )


def _comment_activity_timestamp(comment: dict) -> datetime | None:
    """Compatibility wrapper for adapter-owned comment activity timestamps."""
    from startupai_controller.adapters.github_cli import _comment_activity_timestamp as _adapter_comment_activity_timestamp

    return _adapter_comment_activity_timestamp(comment)


def _query_latest_matching_comment_timestamp(
    owner: str,
    repo: str,
    number: int,
    markers: Sequence[str],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Compatibility wrapper for adapter-owned marker timestamp queries."""
    from startupai_controller.adapters.github_cli import (
        _query_latest_matching_comment_timestamp as _adapter_query_latest_matching_comment_timestamp,
    )

    return _adapter_query_latest_matching_comment_timestamp(
        owner,
        repo,
        number,
        markers,
        gh_runner=gh_runner or _run_gh,
    )


def _query_latest_non_automation_comment_timestamp(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Compatibility wrapper for adapter-owned activity timestamp queries."""
    from startupai_controller.adapters.github_cli import (
        _query_latest_non_automation_comment_timestamp as _adapter_query_latest_non_automation_comment_timestamp,
    )

    return _adapter_query_latest_non_automation_comment_timestamp(
        owner,
        repo,
        number,
        gh_runner=gh_runner or _run_gh,
    )


def _query_latest_marker_timestamp(
    owner: str,
    repo: str,
    number: int,
    marker_prefix: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Compatibility wrapper for adapter-owned marker timestamp queries."""
    from startupai_controller.adapters.github_cli import (
        _query_latest_marker_timestamp as _adapter_query_latest_marker_timestamp,
    )

    return _adapter_query_latest_marker_timestamp(
        owner,
        repo,
        number,
        marker_prefix,
        gh_runner=gh_runner or _run_gh,
    )


# ---------------------------------------------------------------------------
# Repo / issue-ref helpers
# ---------------------------------------------------------------------------


def _repo_to_prefix(
    full_repo: str,
    config: CriticalPathConfig,
) -> str | None:
    """Reverse-lookup issue_prefixes to find prefix for a full repo slug."""
    for prefix, repo_slug in config.issue_prefixes.items():
        if repo_slug == full_repo:
            return prefix
    return None


def _issue_ref_to_repo_parts(
    issue_ref: str,
    config: CriticalPathConfig,
) -> tuple[str, str, int]:
    """Parse issue_ref and return (owner, repo, number)."""
    parsed = parse_issue_ref(issue_ref)
    repo_slug = config.issue_prefixes.get(parsed.prefix)
    if not repo_slug:
        raise ConfigError(
            f"Missing repo mapping for prefix '{parsed.prefix}'."
        )
    owner, repo = repo_slug.split("/", maxsplit=1)
    return owner, repo, parsed.number


def _snapshot_to_issue_ref(
    snapshot: _ProjectItemSnapshot,
    config: CriticalPathConfig,
) -> str | None:
    """Convert a _ProjectItemSnapshot repo#number to a config-prefix ref.

    e.g. 'StartupAI-site/startupai-crew#88' -> 'crew#88'
    """
    # snapshot.issue_ref is "owner/repo#number"
    parts = snapshot.issue_ref.split("#", maxsplit=1)
    if len(parts) != 2:
        return None
    full_repo = parts[0]
    number = parts[1]
    prefix = _repo_to_prefix(full_repo, config)
    if prefix is None:
        return None
    return f"{prefix}#{number}"


# ---------------------------------------------------------------------------
# Check-run / PR-SHA queries
# ---------------------------------------------------------------------------


def _query_failed_check_runs(
    owner: str,
    repo: str,
    head_sha: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[str] | None:
    """Query check runs for a commit and return names of failed ones.

    Returns None on API failure (caller should treat as "unknown").
    """
    try:
        output = _run_gh(
            ["api", f"repos/{owner}/{repo}/commits/{head_sha}/check-runs"],
            gh_runner=gh_runner,
        )
    except GhQueryError:
        return None

    try:
        data = json.loads(output)
    except json.JSONDecodeError:
        return None

    failed: list[str] = []
    for run in data.get("check_runs", []):
        if isinstance(run, dict) and run.get("conclusion") == "failure":
            name = run.get("name", "")
            if name:
                failed.append(name)
    return failed


def _query_pr_head_sha(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> str | None:
    """Get the head SHA of a PR. Returns None on failure."""
    try:
        output = _run_gh(
            ["api", f"repos/{owner}/{repo}/pulls/{pr_number}"],
            gh_runner=gh_runner,
        )
    except GhQueryError:
        return None

    try:
        data = json.loads(output)
    except json.JSONDecodeError:
        return None

    return data.get("head", {}).get("sha")


# ---------------------------------------------------------------------------
# Project field operations
# ---------------------------------------------------------------------------


def _query_issue_board_info(
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> BoardInfo:
    """Compatibility wrapper for adapter-owned board info resolution."""
    from startupai_controller.adapters.github_cli import (
        _query_issue_board_info as _adapter_query_issue_board_info,
    )

    return _adapter_query_issue_board_info(
        issue_ref,
        config,
        project_owner,
        project_number,
        gh_runner=gh_runner or _run_gh,
    )


def _query_status_field_option(
    project_id: str,
    option_name: str = "Ready",
    *,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, str]:
    """Compatibility wrapper for adapter-owned Status option resolution."""
    from startupai_controller.adapters.github_cli import (
        _query_status_field_option as _adapter_query_status_field_option,
    )

    return _adapter_query_status_field_option(
        project_id,
        option_name,
        gh_runner=gh_runner or _run_gh,
    )


def _set_board_status(
    project_id: str,
    item_id: str,
    field_id: str,
    option_id: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility wrapper for adapter-owned Status mutation."""
    from startupai_controller.adapters.github_cli import (
        _set_board_status as _adapter_set_board_status,
    )

    _adapter_set_board_status(
        project_id,
        item_id,
        field_id,
        option_id,
        gh_runner=gh_runner or _run_gh,
    )


def _query_project_item_field(
    issue_ref: str,
    field_name: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Compatibility wrapper for adapter-owned project field reads."""
    from startupai_controller.adapters.github_cli import (
        _query_project_item_field as _adapter_query_project_item_field,
    )

    return _adapter_query_project_item_field(
        issue_ref,
        field_name,
        config,
        project_owner,
        project_number,
        gh_runner=gh_runner or _run_gh,
    )


def _set_text_field(
    project_id: str,
    item_id: str,
    field_name: str,
    value: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility wrapper for adapter-owned text field mutation."""
    from startupai_controller.adapters.github_cli import (
        _set_text_field as _adapter_set_text_field,
    )

    _adapter_set_text_field(
        project_id,
        item_id,
        field_name,
        value,
        gh_runner=gh_runner or _run_gh,
    )


def _query_single_select_field_option(
    project_id: str,
    field_name: str,
    option_name: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, str]:
    """Compatibility wrapper for adapter-owned single-select resolution."""
    from startupai_controller.adapters.github_cli import (
        _query_single_select_field_option as _adapter_query_single_select_field_option,
    )

    return _adapter_query_single_select_field_option(
        project_id,
        field_name,
        option_name,
        gh_runner=gh_runner or _run_gh,
    )


def _set_single_select_field(
    project_id: str,
    item_id: str,
    field_name: str,
    option_name: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility wrapper for adapter-owned single-select mutation."""
    from startupai_controller.adapters.github_cli import (
        _set_single_select_field as _adapter_set_single_select_field,
    )

    _adapter_set_single_select_field(
        project_id,
        item_id,
        field_name,
        option_name,
        gh_runner=gh_runner or _run_gh,
    )


def _set_status_if_changed(
    issue_ref: str,
    from_statuses: set[str],
    to_status: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[bool, str]:
    """Compatibility wrapper for adapter-owned safe status mutation."""
    from startupai_controller.adapters.github_cli import (
        _set_status_if_changed as _adapter_set_status_if_changed,
    )

    return _adapter_set_status_if_changed(
        issue_ref,
        from_statuses,
        to_status,
        config,
        project_owner,
        project_number,
        board_info_resolver=board_info_resolver or _query_issue_board_info,
        board_mutator=board_mutator,
        gh_runner=gh_runner or _run_gh,
    )


# ---------------------------------------------------------------------------
# Issue timestamp & PR queries
# ---------------------------------------------------------------------------


def _query_issue_updated_at(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime:
    """Compatibility wrapper for adapter-owned issue timestamp queries."""
    from startupai_controller.adapters.github_cli import _query_issue_updated_at as _adapter_query_issue_updated_at

    return _adapter_query_issue_updated_at(
        owner,
        repo,
        number,
        gh_runner=gh_runner or _run_gh,
    )


def _parse_pr_url(pr_field: str) -> tuple[str, str, int] | None:
    """Extract owner/repo/pr_number from a GitHub PR URL in project field."""
    from startupai_controller.domain.repair_policy import parse_pr_url  # canonical (M5)

    return parse_pr_url(pr_field)


def _is_pr_open(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Return True when PR exists and is open."""
    try:
        state = _run_gh(
            [
                "api",
                f"repos/{owner}/{repo}/pulls/{pr_number}",
                "-q",
                ".state",
            ],
            gh_runner=gh_runner,
        ).strip()
    except GhQueryError:
        return False
    return state.upper() == "OPEN"


def _query_open_pr_updated_at(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Compatibility wrapper for adapter-owned PR timestamp queries."""
    from startupai_controller.adapters.github_cli import _query_open_pr_updated_at as _adapter_query_open_pr_updated_at

    return _adapter_query_open_pr_updated_at(
        owner,
        repo,
        pr_number,
        gh_runner=gh_runner or _run_gh,
    )


def _extract_run_id(details_url: str) -> int | None:
    """Parse a GitHub Actions run id from a status details URL."""
    match = re.search(r"/actions/runs/(\d+)(?:/|$)", details_url or "")
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def query_open_pull_requests(
    pr_repo: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[OpenPullRequest]:
    """List open PRs for a repository."""
    output = _run_gh(
        [
            "pr",
            "list",
            "--repo",
            pr_repo,
            "--state",
            "open",
            "--limit",
            "100",
            "--json",
            "number,url,headRefName,isDraft,body,author",
        ],
        gh_runner=gh_runner,
    )
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise GhQueryError(
            f"Failed querying open PRs for {pr_repo}: invalid JSON."
        ) from error

    results: list[OpenPullRequest] = []
    for item in payload or []:
        if not isinstance(item, dict):
            continue
        number = item.get("number")
        if not isinstance(number, int):
            continue
        results.append(
            OpenPullRequest(
                number=number,
                url=str(item.get("url") or ""),
                head_ref_name=str(item.get("headRefName") or ""),
                is_draft=bool(item.get("isDraft", False)),
                body=str(item.get("body") or ""),
                author=str(((item.get("author") or {}).get("login") or "")).strip().lower(),
            )
        )
    return results


def memoized_query_open_pull_requests(
    memo: CycleGitHubMemo,
    pr_repo: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[OpenPullRequest]:
    """Return open PRs for a repo using a cycle-local cache."""
    cached = memo.open_pull_requests.get(pr_repo)
    if cached is not None:
        return list(cached)
    results = query_open_pull_requests(pr_repo, gh_runner=gh_runner)
    memo.open_pull_requests[pr_repo] = list(results)
    return results


def query_pull_request_view_payload(
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> PullRequestViewPayload:
    """Return one expanded PR payload for review rescue/automerge decisions."""
    payloads = query_pull_request_view_payloads(
        pr_repo,
        (pr_number,),
        gh_runner=gh_runner,
    )
    payload = payloads.get(pr_number)
    if payload is None:
        raise GhQueryError(f"Failed querying PR {pr_repo}#{pr_number}: pull request not found.")
    return payload


def _pull_request_view_payload_from_json(
    pr_repo: str,
    pr_number: int,
    payload: dict[str, Any],
) -> PullRequestViewPayload:
    """Normalize one PR payload into the shared review-rescue shape."""
    comments = tuple(
        item for item in (payload.get("comments", []) or []) if isinstance(item, dict)
    )
    reviews = tuple(
        item for item in (payload.get("reviews", []) or []) if isinstance(item, dict)
    )
    status_check_rollup = tuple(
        item
        for item in (payload.get("statusCheckRollup", []) or [])
        if isinstance(item, dict)
    )
    return PullRequestViewPayload(
        pr_repo=pr_repo,
        pr_number=pr_number,
        author=str(((payload.get("author") or {}).get("login") or "")).strip().lower(),
        body=str(payload.get("body") or ""),
        state=str(payload.get("state") or ""),
        is_draft=bool(payload.get("isDraft", False)),
        merge_state_status=str(payload.get("mergeStateStatus") or ""),
        mergeable=str(payload.get("mergeable") or ""),
        base_ref_name=str(payload.get("baseRefName") or "main"),
        auto_merge_enabled=payload.get("autoMergeRequest") is not None,
        comments=comments,
        reviews=reviews,
        status_check_rollup=status_check_rollup,
    )


def _normalize_graphql_rollup_node(node: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize GraphQL status-check nodes into the shared rollup shape."""
    typename = str(node.get("__typename") or "")
    if typename == "CheckRun":
        return {
            "__typename": "CheckRun",
            "name": str(node.get("name") or ""),
            "status": str(node.get("status") or "").lower(),
            "conclusion": str(node.get("conclusion") or "").lower(),
            "detailsUrl": str(node.get("detailsUrl") or ""),
            "completedAt": str(node.get("completedAt") or ""),
            "startedAt": str(node.get("startedAt") or ""),
        }
    if typename == "StatusContext":
        return {
            "__typename": "StatusContext",
            "context": str(node.get("context") or ""),
            "state": str(node.get("state") or "").lower(),
            "targetUrl": str(node.get("targetUrl") or ""),
            "startedAt": str(node.get("createdAt") or ""),
        }
    return None


def _pull_request_view_payload_from_graphql_node(
    pr_repo: str,
    pr_number: int,
    node: dict[str, Any],
) -> PullRequestViewPayload:
    """Normalize one GraphQL PR node into the shared review-rescue shape."""
    review_nodes = (
        (node.get("reviews") or {}).get("nodes", [])
        if isinstance(node.get("reviews"), dict)
        else []
    )
    comment_nodes = (
        (node.get("comments") or {}).get("nodes", [])
        if isinstance(node.get("comments"), dict)
        else []
    )
    commit_nodes = (
        (node.get("commits") or {}).get("nodes", [])
        if isinstance(node.get("commits"), dict)
        else []
    )
    status_nodes: list[dict[str, Any]] = []
    if commit_nodes:
        latest_commit = commit_nodes[-1]
        rollup = (((latest_commit.get("commit") or {}).get("statusCheckRollup") or {}).get("contexts") or {})
        for item in rollup.get("nodes", []) or []:
            if not isinstance(item, dict):
                continue
            normalized = _normalize_graphql_rollup_node(item)
            if normalized is not None:
                status_nodes.append(normalized)

    return PullRequestViewPayload(
        pr_repo=pr_repo,
        pr_number=pr_number,
        author=str(((node.get("author") or {}).get("login") or "")).strip().lower(),
        body=str(node.get("body") or ""),
        state=str(node.get("state") or ""),
        is_draft=bool(node.get("isDraft", False)),
        merge_state_status=str(node.get("mergeStateStatus") or ""),
        mergeable=str(node.get("mergeable") or ""),
        base_ref_name=str(node.get("baseRefName") or "main"),
        auto_merge_enabled=node.get("autoMergeRequest") is not None,
        comments=tuple(item for item in comment_nodes if isinstance(item, dict)),
        reviews=tuple(item for item in review_nodes if isinstance(item, dict)),
        status_check_rollup=tuple(status_nodes),
    )


def _latest_node_timestamp(nodes: Sequence[dict[str, Any]], *keys: str) -> str:
    """Return the latest available timestamp from a list of GraphQL nodes."""
    timestamps: list[str] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        for key in keys:
            value = str(node.get(key) or "")
            if value:
                timestamps.append(value)
    return max(timestamps) if timestamps else ""


def _pull_request_state_probe_from_graphql_node(
    pr_repo: str,
    pr_number: int,
    node: dict[str, Any],
) -> PullRequestStateProbe:
    """Normalize one lightweight GraphQL PR node into a review-state probe."""
    review_nodes = (
        (node.get("reviews") or {}).get("nodes", [])
        if isinstance(node.get("reviews"), dict)
        else []
    )
    comment_nodes = (
        (node.get("comments") or {}).get("nodes", [])
        if isinstance(node.get("comments"), dict)
        else []
    )
    commit_nodes = (
        (node.get("commits") or {}).get("nodes", [])
        if isinstance(node.get("commits"), dict)
        else []
    )
    status_nodes: list[dict[str, Any]] = []
    if commit_nodes:
        latest_commit = commit_nodes[-1]
        rollup = (
            (((latest_commit.get("commit") or {}).get("statusCheckRollup") or {}).get("contexts") or {})
        )
        for item in rollup.get("nodes", []) or []:
            if not isinstance(item, dict):
                continue
            normalized = _normalize_graphql_rollup_node(item)
            if normalized is not None:
                status_nodes.append(normalized)

    return PullRequestStateProbe(
        pr_repo=pr_repo,
        pr_number=pr_number,
        state=str(node.get("state") or ""),
        is_draft=bool(node.get("isDraft", False)),
        merge_state_status=str(node.get("mergeStateStatus") or ""),
        mergeable=str(node.get("mergeable") or ""),
        base_ref_name=str(node.get("baseRefName") or "main"),
        auto_merge_enabled=node.get("autoMergeRequest") is not None,
        head_ref_oid=str(node.get("headRefOid") or ""),
        updated_at=str(node.get("updatedAt") or ""),
        latest_comment_at=_latest_node_timestamp(comment_nodes, "createdAt"),
        latest_review_at=_latest_node_timestamp(review_nodes, "submittedAt"),
        status_check_rollup=tuple(status_nodes),
    )


def review_state_probe_from_payload(
    payload: PullRequestViewPayload,
) -> PullRequestStateProbe:
    """Build a lightweight review-state probe from an expanded PR payload."""
    latest_comment_at = ""
    if payload.comments:
        latest_comment_at = max(
            str(comment.get("createdAt") or "")
            for comment in payload.comments
            if isinstance(comment, dict)
        )
    latest_review_at = ""
    if payload.reviews:
        latest_review_at = max(
            str(review.get("submittedAt") or "")
            for review in payload.reviews
            if isinstance(review, dict)
        )
    return PullRequestStateProbe(
        pr_repo=payload.pr_repo,
        pr_number=payload.pr_number,
        state=payload.state,
        is_draft=payload.is_draft,
        merge_state_status=payload.merge_state_status,
        mergeable=payload.mergeable,
        base_ref_name=payload.base_ref_name,
        auto_merge_enabled=payload.auto_merge_enabled,
        head_ref_oid="",
        updated_at="",
        latest_comment_at=latest_comment_at,
        latest_review_at=latest_review_at,
        status_check_rollup=payload.status_check_rollup,
    )


def query_pull_request_state_probes(
    pr_repo: str,
    pr_numbers: Sequence[int],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[int, PullRequestStateProbe]:
    """Return lightweight PR probes for digest-based review scheduling."""
    if "/" not in pr_repo:
        raise ConfigError(f"pr_repo must be owner/repo, got '{pr_repo}'.")
    owner, repo = pr_repo.split("/", maxsplit=1)
    numbers = tuple(sorted({int(number) for number in pr_numbers}))
    if not numbers:
        return {}

    fields = """
      number
      state
      isDraft
      mergeStateStatus
      mergeable
      baseRefName
      headRefOid
      updatedAt
      autoMergeRequest { enabledAt }
      reviews(last: 1) {
        nodes {
          submittedAt
        }
      }
      comments(last: 1) {
        nodes {
          createdAt
        }
      }
      commits(last: 1) {
        nodes {
          commit {
            statusCheckRollup {
              contexts(first: 100) {
                nodes {
                  __typename
                  ... on CheckRun {
                    name
                    status
                    conclusion
                    detailsUrl
                    completedAt
                    startedAt
                  }
                  ... on StatusContext {
                    context
                    state
                    targetUrl
                    createdAt
                  }
                }
              }
            }
          }
        }
      }
    """
    query_parts = "\n".join(
        f"pr_{number}: pullRequest(number: {number}) {{ {fields} }}"
        for number in numbers
    )
    query = (
        "query($owner: String!, $repo: String!) {\n"
        "  repository(owner: $owner, name: $repo) {\n"
        f"{query_parts}\n"
        "  }\n"
        "}"
    )
    output = _run_gh(
        [
            "api",
            "graphql",
            "-f",
            f"query={query}",
            "-f",
            f"owner={owner}",
            "-f",
            f"repo={repo}",
        ],
        gh_runner=gh_runner,
    )
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise GhQueryError(
            f"Failed querying PR probes for {pr_repo}: invalid JSON."
        ) from error

    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        messages = [
            err.get("message", "unknown GraphQL error")
            for err in errors
            if isinstance(err, dict)
        ]
        raise GhQueryError(
            f"Failed querying PR probes for {pr_repo}: {'; '.join(messages) or 'unknown GraphQL error'}"
        )

    repository = (payload.get("data") or {}).get("repository") or {}
    results: dict[int, PullRequestStateProbe] = {}
    for number in numbers:
        node = repository.get(f"pr_{number}")
        if not isinstance(node, dict):
            continue
        results[number] = _pull_request_state_probe_from_graphql_node(
            pr_repo,
            number,
            node,
        )
    return results


def query_pull_request_view_payloads(
    pr_repo: str,
    pr_numbers: Sequence[int],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[int, PullRequestViewPayload]:
    """Return expanded PR payloads for a bounded set of PR numbers in one query."""
    if "/" not in pr_repo:
        raise ConfigError(f"pr_repo must be owner/repo, got '{pr_repo}'.")
    owner, repo = pr_repo.split("/", maxsplit=1)
    numbers = tuple(sorted({int(number) for number in pr_numbers}))
    if not numbers:
        return {}

    if len(numbers) == 1:
        number = numbers[0]
        output = _run_gh(
            [
                "pr",
                "view",
                str(number),
                "--repo",
                pr_repo,
                "--json",
                (
                    "author,body,comments,reviews,state,isDraft,mergeStateStatus,"
                    "mergeable,baseRefName,autoMergeRequest,statusCheckRollup"
                ),
            ],
            gh_runner=gh_runner,
        )
        try:
            payload = json.loads(output)
        except json.JSONDecodeError as error:
            raise GhQueryError(
                f"Failed querying PR {pr_repo}#{number}: invalid JSON."
            ) from error
        return {number: _pull_request_view_payload_from_json(pr_repo, number, payload)}

    fields = """
      number
      state
      isDraft
      mergeStateStatus
      mergeable
      baseRefName
      autoMergeRequest { enabledAt }
      body
      author { login }
      reviews(last: 100) {
        nodes {
          body
          submittedAt
          state
          author { login }
        }
      }
      comments(last: 100) {
        nodes {
          body
          createdAt
          author { login }
        }
      }
      commits(last: 1) {
        nodes {
          commit {
            statusCheckRollup {
              contexts(first: 100) {
                nodes {
                  __typename
                  ... on CheckRun {
                    name
                    status
                    conclusion
                    detailsUrl
                    completedAt
                    startedAt
                  }
                  ... on StatusContext {
                    context
                    state
                    targetUrl
                    createdAt
                  }
                }
              }
            }
          }
        }
      }
    """
    query_parts = "\n".join(
        f"pr_{number}: pullRequest(number: {number}) {{ {fields} }}"
        for number in numbers
    )
    query = (
        "query($owner: String!, $repo: String!) {\n"
        "  repository(owner: $owner, name: $repo) {\n"
        f"{query_parts}\n"
        "  }\n"
        "}"
    )
    output = _run_gh(
        [
            "api",
            "graphql",
            "-f",
            f"query={query}",
            "-f",
            f"owner={owner}",
            "-f",
            f"repo={repo}",
        ],
        gh_runner=gh_runner,
    )
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise GhQueryError(
            f"Failed querying PR payloads for {pr_repo}: invalid JSON."
        ) from error

    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        messages = [
            err.get("message", "unknown GraphQL error")
            for err in errors
            if isinstance(err, dict)
        ]
        raise GhQueryError(
            f"Failed querying PR payloads for {pr_repo}: {'; '.join(messages) or 'unknown GraphQL error'}"
        )

    repository = (payload.get("data") or {}).get("repository") or {}
    results: dict[int, PullRequestViewPayload] = {}
    for number in numbers:
        node = repository.get(f"pr_{number}")
        if not isinstance(node, dict):
            continue
        results[number] = _pull_request_view_payload_from_graphql_node(
            pr_repo,
            number,
            node,
        )
    return results


def memoized_query_pull_request_view_payload(
    memo: CycleGitHubMemo,
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> PullRequestViewPayload:
    """Return an expanded PR payload using cycle-local memoization."""
    payloads = memoized_query_pull_request_view_payloads(
        memo,
        pr_repo,
        (pr_number,),
        gh_runner=gh_runner,
    )
    payload = payloads.get(pr_number)
    if payload is None:
        raise GhQueryError(f"Failed querying PR {pr_repo}#{pr_number}: pull request not found.")
    return payload


def memoized_query_pull_request_view_payloads(
    memo: CycleGitHubMemo,
    pr_repo: str,
    pr_numbers: Sequence[int],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[int, PullRequestViewPayload]:
    """Return expanded PR payloads using cycle-local memoization."""
    numbers = tuple(sorted({int(number) for number in pr_numbers}))
    missing = [
        number
        for number in numbers
        if (pr_repo, number) not in memo.review_pull_requests
    ]
    if missing:
        fetched = query_pull_request_view_payloads(
            pr_repo,
            tuple(missing),
            gh_runner=gh_runner,
        )
        for number, payload in fetched.items():
            memo.review_pull_requests[(pr_repo, number)] = payload
    return {
        number: memo.review_pull_requests[(pr_repo, number)]
        for number in numbers
        if (pr_repo, number) in memo.review_pull_requests
    }


def memoized_query_pull_request_state_probes(
    memo: CycleGitHubMemo,
    pr_repo: str,
    pr_numbers: Sequence[int],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[int, PullRequestStateProbe]:
    """Return lightweight PR probes using cycle-local memoization."""
    numbers = tuple(sorted({int(number) for number in pr_numbers}))
    missing = [
        number
        for number in numbers
        if (pr_repo, number) not in memo.review_state_probes
    ]
    if missing:
        fetched = query_pull_request_state_probes(
            pr_repo,
            tuple(missing),
            gh_runner=gh_runner,
        )
        for number, payload in fetched.items():
            memo.review_state_probes[(pr_repo, number)] = payload
    return {
        number: memo.review_state_probes[(pr_repo, number)]
        for number in numbers
        if (pr_repo, number) in memo.review_state_probes
    }


def query_required_status_checks(
    pr_repo: str,
    base_ref_name: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> set[str]:
    """Return required status-check contexts for one repo/base branch."""
    cache_key = (pr_repo, base_ref_name)
    cached = _required_status_checks_ttl_cache.get(cache_key)
    now_monotonic = time.monotonic()
    if cached is not None:
        expires_at, required = cached
        if expires_at > now_monotonic:
            return set(required)
    if "/" not in pr_repo:
        raise ConfigError(f"pr_repo must be owner/repo, got '{pr_repo}'.")
    owner, repo = pr_repo.split("/", maxsplit=1)
    try:
        bp_output = _run_gh(
            [
                "api",
                f"repos/{owner}/{repo}/branches/{base_ref_name}/protection/required_status_checks",
            ],
            gh_runner=gh_runner,
        )
    except GhQueryError:
        if cached is not None:
            return set(cached[1])
        raise
    try:
        bp_data = json.loads(bp_output)
    except json.JSONDecodeError as error:
        raise GhQueryError(
            f"Failed parsing branch protection for {pr_repo}:{base_ref_name}."
        ) from error

    required: set[str] = set()
    for context in bp_data.get("contexts", []) or []:
        if isinstance(context, str):
            required.add(context)
    for check in bp_data.get("checks", []) or []:
        if isinstance(check, dict):
            name = check.get("context")
            if isinstance(name, str) and name:
                required.add(name)
    _required_status_checks_ttl_cache[cache_key] = (
        now_monotonic + _REQUIRED_STATUS_CHECKS_CACHE_TTL_SECONDS,
        set(required),
    )
    return required


def memoized_query_required_status_checks(
    memo: CycleGitHubMemo,
    pr_repo: str,
    base_ref_name: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> set[str]:
    """Return required checks with cycle-local branch-protection caching."""
    key = (pr_repo, base_ref_name)
    cached = memo.required_status_checks.get(key)
    if cached is not None:
        return set(cached)
    required = query_required_status_checks(
        pr_repo,
        base_ref_name,
        gh_runner=gh_runner,
    )
    memo.required_status_checks[key] = set(required)
    return set(required)


def clear_required_status_checks_cache() -> None:
    """Clear the process-local required-check TTL cache."""
    _required_status_checks_ttl_cache.clear()


def query_issue_body(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Return the raw issue body."""
    output = _run_gh(
        [
            "api",
            f"repos/{owner}/{repo}/issues/{number}",
            "--jq",
            ".body",
        ],
        gh_runner=gh_runner,
        operation_type="query",
    )
    return str(output or "")


def memoized_query_issue_body(
    memo: CycleGitHubMemo,
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Return issue body using a cycle-local cache."""
    key = (owner, repo, number)
    cached = memo.issue_bodies.get(key)
    if cached is not None:
        return cached
    body = query_issue_body(owner, repo, number, gh_runner=gh_runner)
    memo.issue_bodies[key] = body
    return body


def _query_latest_wip_activity_timestamp(
    issue_ref: str,
    owner: str,
    repo: str,
    number: int,
    pr_field: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Compatibility wrapper for adapter-owned WIP activity queries."""
    from startupai_controller.adapters.github_cli import _query_latest_wip_activity_timestamp as _adapter_query_latest_wip_activity_timestamp

    return _adapter_query_latest_wip_activity_timestamp(
        issue_ref,
        owner,
        repo,
        number,
        pr_field,
        gh_runner=gh_runner or _run_gh,
    )


# ---------------------------------------------------------------------------
# Issue assignee operations
# ---------------------------------------------------------------------------


def _query_issue_assignees(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Compatibility wrapper for adapter-owned assignee reads."""
    from startupai_controller.adapters.github_cli import _query_issue_assignees as _adapter_query_issue_assignees

    return _adapter_query_issue_assignees(
        owner,
        repo,
        number,
        gh_runner=gh_runner or _run_gh,
    )


def _set_issue_assignees(
    owner: str,
    repo: str,
    number: int,
    assignees: list[str],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility wrapper for adapter-owned assignee writes."""
    from startupai_controller.adapters.github_cli import _set_issue_assignees as _adapter_set_issue_assignees

    _adapter_set_issue_assignees(
        owner,
        repo,
        number,
        assignees,
        gh_runner=gh_runner or _run_gh,
    )


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class _ProjectItemSnapshot:
    """A single project board item with key field values."""

    issue_ref: str
    status: str
    executor: str
    handoff_to: str
    priority: str = ""
    item_id: str = ""
    project_id: str = ""
    sprint: str = ""
    agent: str = ""
    owner_field: str = ""
    title: str = ""
    body: str = ""
    repo_slug: str = ""
    repo_name: str = ""
    repo_owner: str = ""
    issue_number: int = 0
    issue_updated_at: str = ""


@dataclass(frozen=True)
class CycleBoardSnapshot:
    """Thin per-cycle view of board items reused across hot-path phases."""

    items: tuple[_ProjectItemSnapshot, ...]
    by_status: dict[str, tuple[_ProjectItemSnapshot, ...]] = field(default_factory=dict)

    def items_with_status(self, status: str) -> tuple[_ProjectItemSnapshot, ...]:
        """Return cached items in the given status."""
        return self.by_status.get(status, ())


@dataclass
class CycleGitHubMemo:
    """Cycle-local memoization for expensive GitHub reads."""

    issue_bodies: dict[tuple[str, str, int], str] = field(default_factory=dict)
    issue_comment_bodies: dict[tuple[str, str, int], list[str]] = field(default_factory=dict)
    open_pull_requests: dict[str, list["OpenPullRequest"]] = field(default_factory=dict)
    dependency_ready: dict[str, bool] = field(default_factory=dict)
    review_pull_requests: dict[tuple[str, int], PullRequestViewPayload] = field(
        default_factory=dict
    )
    review_state_probes: dict[tuple[str, int], PullRequestStateProbe] = field(
        default_factory=dict
    )
    required_status_checks: dict[tuple[str, str], set[str]] = field(
        default_factory=dict
    )



# _priority_rank re-exported from domain.scheduling_policy (M5)


def _list_project_items_by_status(
    status: str,
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[_ProjectItemSnapshot]:
    """Compatibility wrapper for adapter-owned project status queries."""
    from startupai_controller.adapters.github_cli import _list_project_items_by_status as _adapter_list_project_items_by_status

    return _adapter_list_project_items_by_status(
        status,
        project_owner,
        project_number,
        gh_runner=gh_runner or _run_gh,
    )


def build_cycle_board_snapshot(
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> CycleBoardSnapshot:
    """Compatibility wrapper for adapter-owned cycle board snapshots."""
    from startupai_controller.adapters.github_cli import build_cycle_board_snapshot as _adapter_build_cycle_board_snapshot

    return _adapter_build_cycle_board_snapshot(
        project_owner,
        project_number,
        gh_runner=gh_runner or _run_gh,
    )


def clear_cycle_board_snapshot_cache() -> None:
    """Compatibility wrapper for adapter-owned snapshot cache clearing."""
    from startupai_controller.adapters.github_cli import clear_cycle_board_snapshot_cache as _adapter_clear_cycle_board_snapshot_cache

    _adapter_clear_cycle_board_snapshot_cache()


def _list_project_items(
    project_owner: str,
    project_number: int,
    *,
    statuses: set[str] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[_ProjectItemSnapshot]:
    """Compatibility wrapper for adapter-owned rich project item reads."""
    from startupai_controller.adapters.github_cli import _list_project_items as _adapter_list_project_items

    return _adapter_list_project_items(
        project_owner,
        project_number,
        statuses=statuses,
        gh_runner=gh_runner or _run_gh,
    )


# ---------------------------------------------------------------------------
# Closing issues query
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LinkedIssue:
    owner: str
    repo: str
    number: int
    ref: str  # e.g., "crew#88"


def query_closing_issues(
    pr_owner: str,
    pr_repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[LinkedIssue]:
    """Query PR's closingIssuesReferences and return LinkedIssue list."""
    query = """
query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      closingIssuesReferences(first: 50) {
        nodes {
          number
          repository { nameWithOwner }
        }
      }
    }
  }
}
"""

    output = _run_gh(
        [
            "api",
            "graphql",
            "-f",
            f"query={query}",
            "-f",
            f"owner={pr_owner}",
            "-f",
            f"repo={pr_repo}",
            "-F",
            f"number={pr_number}",
        ],
        gh_runner=gh_runner,
    )

    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise GhQueryError(
            f"Failed querying closing issues for PR {pr_owner}/{pr_repo}#{pr_number}: "
            "invalid JSON."
        ) from error

    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        messages = [
            err.get("message", "unknown GraphQL error")
            for err in errors
            if isinstance(err, dict)
        ]
        joined = "; ".join(messages) if messages else "unknown GraphQL error"
        raise GhQueryError(
            f"Failed querying closing issues for PR "
            f"{pr_owner}/{pr_repo}#{pr_number}: {joined}"
        )

    nodes = (
        payload.get("data", {})
        .get("repository", {})
        .get("pullRequest", {})
        .get("closingIssuesReferences", {})
        .get("nodes", [])
    )

    result: list[LinkedIssue] = []
    for node in nodes:
        issue_number = node.get("number")
        repo_with_owner = (node.get("repository") or {}).get(
            "nameWithOwner", ""
        )
        if not issue_number or not repo_with_owner:
            continue

        prefix = _repo_to_prefix(repo_with_owner, config)
        if prefix is None:
            # Skip issues whose repo is not in config.issue_prefixes
            continue

        owner, repo = repo_with_owner.split("/", maxsplit=1)
        result.append(
            LinkedIssue(
                owner=owner,
                repo=repo,
                number=issue_number,
                ref=f"{prefix}#{issue_number}",
            )
        )

    return result


# ---------------------------------------------------------------------------
# Codex / gate queries
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CodexReviewVerdict:
    """Parsed codex review verdict marker from PR comments/reviews."""

    decision: str  # pass|fail
    route: str  # none|codex|executor|claude|human
    source: str  # comment|review
    timestamp: str
    actor: str
    checklist: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PullRequestViewPayload:
    """Expanded PR payload used to make one review decision without requerying."""

    pr_repo: str
    pr_number: int
    author: str
    body: str
    state: str
    is_draft: bool
    merge_state_status: str
    mergeable: str
    base_ref_name: str
    auto_merge_enabled: bool
    comments: tuple[dict[str, Any], ...] = ()
    reviews: tuple[dict[str, Any], ...] = ()
    status_check_rollup: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class PullRequestStateProbe:
    """Lightweight PR state used to avoid rehydrating unchanged review items."""

    pr_repo: str
    pr_number: int
    state: str
    is_draft: bool
    merge_state_status: str
    mergeable: str
    base_ref_name: str
    auto_merge_enabled: bool
    head_ref_oid: str
    updated_at: str
    latest_comment_at: str
    latest_review_at: str
    status_check_rollup: tuple[dict[str, Any], ...] = ()


def _parse_codex_verdict_from_text(
    text: str,
) -> tuple[str | None, str | None, list[str]]:
    """Extract codex verdict markers from free text."""
    decision_match = re.search(r"\bcodex-review\s*:\s*(pass|fail)\b", text, re.I)
    route_match = re.search(
        r"\bcodex-route\s*:\s*(none|codex|executor|claude|human)\b",
        text,
        re.I,
    )
    checklist = re.findall(r"^\s*-\s*\[\s\]\s+(.+)$", text, flags=re.M)
    decision = decision_match.group(1).lower() if decision_match else None
    route = route_match.group(1).lower() if route_match else None
    return decision, route, checklist


def query_latest_codex_verdict(
    pr_repo: str,
    pr_number: int,
    *,
    trusted_actors: set[str] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> CodexReviewVerdict | None:
    """Query PR comments/reviews and return latest codex verdict marker."""
    payload = query_pull_request_view_payload(
        pr_repo,
        pr_number,
        gh_runner=gh_runner,
    )
    return latest_codex_verdict_from_payload(
        payload,
        trusted_actors=trusted_actors,
    )


def latest_codex_verdict_from_payload(
    payload: PullRequestViewPayload,
    *,
    trusted_actors: set[str] | None = None,
) -> CodexReviewVerdict | None:
    """Return the latest codex verdict marker from one expanded PR payload."""
    candidates: list[CodexReviewVerdict] = []

    for comment in payload.comments:
        body = comment.get("body") or ""
        decision, route, checklist = _parse_codex_verdict_from_text(body)
        if decision is None:
            continue
        actor = (
            (
                (comment.get("author") or {}).get("login")
                or (comment.get("user") or {}).get("login")
                or ""
            )
            .strip()
            .lower()
        )
        if trusted_actors and actor not in trusted_actors:
            continue
        ts = comment.get("createdAt", "")
        chosen_route = "none" if decision == "pass" else (route or "executor")
        candidates.append(
            CodexReviewVerdict(
                decision=decision,
                route=chosen_route,
                source="comment",
                timestamp=ts,
                actor=actor,
                checklist=checklist,
            )
        )

    for review in payload.reviews:
        body = review.get("body") or ""
        decision, route, checklist = _parse_codex_verdict_from_text(body)
        if decision is None:
            continue
        actor = (
            (
                (review.get("author") or {}).get("login")
                or (review.get("user") or {}).get("login")
                or ""
            )
            .strip()
            .lower()
        )
        if trusted_actors and actor not in trusted_actors:
            continue
        ts = review.get("submittedAt", "")
        chosen_route = "none" if decision == "pass" else (route or "executor")
        candidates.append(
            CodexReviewVerdict(
                decision=decision,
                route=chosen_route,
                source="review",
                timestamp=ts,
                actor=actor,
                checklist=checklist,
            )
        )

    if not candidates:
        return None

    candidates.sort(key=lambda item: item.timestamp)
    return candidates[-1]


def has_copilot_review_signal_from_payload(payload: PullRequestViewPayload) -> bool:
    """Return True when Copilot has submitted approved/commented review."""
    accepted_states = {"APPROVED", "COMMENTED"}
    for review in payload.reviews:
        state = str(review.get("state", "")).upper()
        actor = ((review.get("author") or {}).get("login") or "").lower()
        if "copilot" in actor and state in accepted_states:
            return True
    return False


def build_pr_gate_status_from_payload(
    payload: PullRequestViewPayload,
    *,
    required: set[str],
) -> PrGateStatus:
    """Build gate readiness from one expanded PR payload and required checks."""
    latest: dict[str, tuple[str, CheckObservation]] = {}
    for check in payload.status_check_rollup:
        typename = check.get("__typename", "")
        if typename == "CheckRun":
            name = check.get("name", "")
            ts = check.get("completedAt") or check.get("startedAt") or ""
            status = str(check.get("status", "")).lower()
            conclusion = str(check.get("conclusion", "")).lower()
            details_url = str(check.get("detailsUrl") or "")
            workflow_name = str(check.get("workflowName") or "")
            if not name:
                continue
            result = (
                "pending"
                if status != "completed"
                else (
                    "pass"
                    if conclusion in {"success", "neutral", "skipped"}
                    else (
                        "cancelled"
                        if conclusion in {"cancelled", "startup_failure", "stale"}
                        else "fail"
                    )
                )
            )
            observation = CheckObservation(
                name=name,
                result=result,
                status=status,
                conclusion=conclusion,
                details_url=details_url,
                workflow_name=workflow_name,
                run_id=_extract_run_id(details_url),
            )
            prev = latest.get(name)
            if prev is None or ts >= prev[0]:
                latest[name] = (ts, observation)
        elif typename == "StatusContext":
            name = check.get("context", "")
            ts = check.get("startedAt") or ""
            state = str(check.get("state", "")).lower()
            details_url = str(check.get("targetUrl") or "")
            if not name:
                continue
            if state == "success":
                result = "pass"
            elif state in {"error", "failure"}:
                result = "fail"
            else:
                result = "pending"
            observation = CheckObservation(
                name=name,
                result=result,
                status=state,
                conclusion=state,
                details_url=details_url,
                workflow_name="",
                run_id=_extract_run_id(details_url),
            )
            prev = latest.get(name)
            if prev is None or ts >= prev[0]:
                latest[name] = (ts, observation)

    passed: set[str] = set()
    failed: set[str] = set()
    pending: set[str] = set()
    cancelled: set[str] = set()
    for context in required:
        if context not in latest:
            pending.add(context)
            continue
        _ts, observation = latest[context]
        result = observation.result
        if result == "pass":
            passed.add(context)
        elif result == "fail":
            failed.add(context)
        elif result == "cancelled":
            cancelled.add(context)
            pending.add(context)
        else:
            pending.add(context)

    return PrGateStatus(
        required=required,
        passed=passed,
        failed=failed,
        pending=pending,
        cancelled=cancelled,
        merge_state_status=payload.merge_state_status,
        mergeable=payload.mergeable,
        is_draft=payload.is_draft,
        state=payload.state.strip().upper(),
        auto_merge_enabled=payload.auto_merge_enabled,
        checks={name: observation for name, (_ts, observation) in latest.items()},
    )


def review_state_digest_from_probe(probe: PullRequestStateProbe) -> str:
    """Return a stable digest for the lightweight state of a review PR."""
    latest_checks: dict[str, tuple[str, str]] = {}
    for check in probe.status_check_rollup:
        typename = check.get("__typename", "")
        if typename == "CheckRun":
            name = str(check.get("name") or "")
            timestamp = str(check.get("completedAt") or check.get("startedAt") or "")
            status = str(check.get("status") or "").lower()
            conclusion = str(check.get("conclusion") or "").lower()
            result = (
                "pending"
                if status != "completed"
                else (
                    "pass"
                    if conclusion in {"success", "neutral", "skipped"}
                    else (
                        "cancelled"
                        if conclusion in {"cancelled", "startup_failure", "stale"}
                        else "fail"
                    )
                )
            )
        elif typename == "StatusContext":
            name = str(check.get("context") or "")
            timestamp = str(check.get("startedAt") or "")
            state = str(check.get("state") or "").lower()
            result = "pass" if state == "success" else ("fail" if state in {"error", "failure"} else "pending")
        else:
            continue
        if not name:
            continue
        previous = latest_checks.get(name)
        if previous is None or timestamp >= previous[0]:
            latest_checks[name] = (timestamp, result)

    payload = {
        "state": probe.state.strip().upper(),
        "is_draft": bool(probe.is_draft),
        "merge_state_status": probe.merge_state_status,
        "mergeable": probe.mergeable,
        "base_ref_name": probe.base_ref_name,
        "auto_merge_enabled": bool(probe.auto_merge_enabled),
        "head_ref_oid": probe.head_ref_oid,
        "updated_at": probe.updated_at,
        "latest_comment_at": probe.latest_comment_at,
        "latest_review_at": probe.latest_review_at,
        "checks": sorted((name, result) for name, (_ts, result) in latest_checks.items()),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def review_state_digest_from_payload(payload: PullRequestViewPayload) -> str:
    """Return a stable review-state digest from an expanded PR payload."""
    return review_state_digest_from_probe(review_state_probe_from_payload(payload))


def _query_pr_gate_status(
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> PrGateStatus:
    """Build required-check and merge-state snapshot for a PR."""
    payload = query_pull_request_view_payload(
        pr_repo,
        pr_number,
        gh_runner=gh_runner,
    )
    required = query_required_status_checks(
        pr_repo,
        payload.base_ref_name or "main",
        gh_runner=gh_runner,
    )
    return build_pr_gate_status_from_payload(payload, required=required)
