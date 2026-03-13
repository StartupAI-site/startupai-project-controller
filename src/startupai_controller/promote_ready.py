#!/usr/bin/env python3
"""Atomic Backlog/Blocked -> Ready promotion with critical-path validation.

Combines the read-only validator with board mutation in one command:
1. Query current status of target issue
2. Reject if not in {Backlog, Blocked}
3. Run strict validator (--require-in-graph)
4. Mutate board Status -> Ready

Exit codes match the validator conventions:
  0 - Promoted successfully (or dry-run pass)
  2 - Blocked (status not promotable, or validator blocked)
  3 - Config error
  4 - GitHub API error
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
import sys
import time
from typing import Callable

from startupai_controller.adapters.github_http_adapter import (
    GitHubTransportError,
    run_github_command,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    ConfigError,
    GhQueryError,
    evaluate_ready_promotion,
    load_config,
    parse_issue_ref,
)
from startupai_controller.gh_cli_timeout import gh_command_timeout_seconds

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_AUTOMATION_CONFIG_PATH = str(
    _REPO_ROOT / "config" / "board-automation-config.json"
)
PROMOTABLE_STATUSES = frozenset({"Backlog", "Blocked"})
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
_GH_COMMAND_TIMEOUT_SECONDS = gh_command_timeout_seconds()


@dataclass(frozen=True)
class BoardInfo:
    """Issue's current board state needed for promotion."""

    status: str
    item_id: str
    project_id: str


def _run_gh_command(command: list[str], error_prefix: str) -> str:
    """Run gh CLI command with bounded retries for transient transport errors."""
    try:
        http_output = run_github_command(
            command,
            operation_type="query",
            timeout_seconds=_GH_COMMAND_TIMEOUT_SECONDS,
            retry_delays=_GH_RETRY_DELAYS_SECONDS,
        )
        if http_output is not None:
            return http_output
    except GitHubTransportError as error:
        raise GhQueryError(f"{error_prefix}: {error.detail}") from error

    for attempt in range(len(_GH_RETRY_DELAYS_SECONDS) + 1):
        try:
            return subprocess.check_output(
                command,
                text=True,
                stderr=subprocess.STDOUT,
                timeout=_GH_COMMAND_TIMEOUT_SECONDS,
            )
        except OSError as error:
            raise GhQueryError(f"{error_prefix}: {error}") from error
        except subprocess.TimeoutExpired as error:
            raise GhQueryError(
                f"{error_prefix}: timed out after {_GH_COMMAND_TIMEOUT_SECONDS:.1f}s"
            ) from error
        except subprocess.CalledProcessError as error:
            output = error.output.strip()
            is_retryable = any(
                marker in output.lower() for marker in _GH_RETRYABLE_ERROR_MARKERS
            )
            if is_retryable and attempt < len(_GH_RETRY_DELAYS_SECONDS):
                time.sleep(_GH_RETRY_DELAYS_SECONDS[attempt])
                continue
            raise GhQueryError(f"{error_prefix}: {output}") from error
    raise AssertionError("gh retry loop exited without returning or raising")


def _load_controller_owned_prefixes(path: Path) -> tuple[bool, tuple[str, ...]]:
    """Return whether local admission owns protected Backlog -> Ready."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError:
        return False, ()
    except json.JSONDecodeError:
        return False, ()

    authority = payload.get("execution_authority") or {}
    admission = payload.get("admission") or {}
    if not isinstance(authority, dict) or not isinstance(admission, dict):
        return False, ()
    if str(authority.get("mode", "")).strip().lower() != "single_machine":
        return False, ()
    if not bool(admission.get("enabled", False)):
        return False, ()
    repos_raw = authority.get("repos", [])
    if not isinstance(repos_raw, list):
        return False, ()
    repos = tuple(
        sorted({str(repo).strip().lower() for repo in repos_raw if str(repo).strip()})
    )
    return bool(repos), repos


def _query_issue_board_info(
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
) -> BoardInfo:
    """Query the issue's project board item, returning status + IDs for mutation."""
    parsed = parse_issue_ref(issue_ref)
    repo_slug = config.issue_prefixes.get(parsed.prefix)
    if not repo_slug:
        raise ConfigError(
            f"Missing repo mapping for prefix '{parsed.prefix}' in issue_prefixes."
        )

    owner, repo = repo_slug.split("/", maxsplit=1)

    query = """
query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    issue(number: $number) {
      projectItems(first: 20) {
        nodes {
          id
          project {
            id
            owner {
              ... on Organization { login }
              ... on User { login }
            }
            number
          }
          statusField: fieldValueByName(name: "Status") {
            ... on ProjectV2ItemFieldSingleSelectValue {
              name
            }
          }
        }
      }
    }
  }
}
"""

    command = [
        "gh",
        "api",
        "graphql",
        "-f",
        f"query={query}",
        "-f",
        f"owner={owner}",
        "-f",
        f"repo={repo}",
        "-F",
        f"number={parsed.number}",
    ]
    output = _run_gh_command(command, f"Failed querying board info for {issue_ref}")

    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise GhQueryError(
            f"Failed querying board info for {issue_ref}: invalid JSON response."
        ) from error

    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        messages = [
            err.get("message", "unknown GraphQL error")
            for err in errors
            if isinstance(err, dict)
        ]
        joined = "; ".join(messages) if messages else "unknown GraphQL error"
        raise GhQueryError(f"Failed querying board info for {issue_ref}: {joined}")

    nodes = (
        payload.get("data", {})
        .get("repository", {})
        .get("issue", {})
        .get("projectItems", {})
        .get("nodes", [])
    )

    for node in nodes:
        project = node.get("project") or {}
        owner_data = project.get("owner") or {}
        owner_login = owner_data.get("login")
        number = project.get("number")
        if owner_login == project_owner and number == project_number:
            status = (node.get("statusField") or {}).get("name") or "UNKNOWN"
            item_id = node.get("id", "")
            project_id = project.get("id", "")
            return BoardInfo(status=status, item_id=item_id, project_id=project_id)

    return BoardInfo(status="NOT_ON_BOARD", item_id="", project_id="")


def _query_status_field_option(
    project_id: str,
    option_name: str = "Ready",
    *,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, str]:
    """Query the Status field ID and a named option ID for a project.

    Returns (field_id, option_id).
    """
    query = """
query($projectId: ID!) {
  node(id: $projectId) {
    ... on ProjectV2 {
      field(name: "Status") {
        ... on ProjectV2SingleSelectField {
          id
          options { id name }
        }
      }
    }
  }
}
"""

    if gh_runner is None:
        command = [
            "gh",
            "api",
            "graphql",
            "-f",
            f"query={query}",
            "-f",
            f"projectId={project_id}",
        ]
        output = _run_gh_command(command, "Failed querying Status field")
    else:
        output = gh_runner(
            [
                "api",
                "graphql",
                "-f",
                f"query={query}",
                "-f",
                f"projectId={project_id}",
            ]
        )

    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise GhQueryError(
            "Failed querying Status field: invalid JSON response."
        ) from error

    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        messages = [
            err.get("message", "unknown GraphQL error")
            for err in errors
            if isinstance(err, dict)
        ]
        joined = "; ".join(messages) if messages else "unknown GraphQL error"
        raise GhQueryError(f"Failed querying Status field: {joined}")

    field = payload.get("data", {}).get("node", {}).get("field") or {}
    field_id = field.get("id")
    if not field_id:
        raise GhQueryError("Status field not found on project.")

    options = field.get("options", [])
    for option in options:
        if option.get("name") == option_name:
            return field_id, option["id"]

    raise GhQueryError(f"'{option_name}' option not found in Status field.")


# Backward-compatible alias
_query_status_field_ready_option = _query_status_field_option


def _set_board_status(
    project_id: str,
    item_id: str,
    field_id: str,
    option_id: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Mutate the board item's Status field to the given option."""
    mutation = """
mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $optionId: String!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $projectId,
    itemId: $itemId,
    fieldId: $fieldId,
    value: { singleSelectOptionId: $optionId }
  }) {
    projectV2Item { id }
  }
}
"""

    if gh_runner is None:
        command = [
            "gh",
            "api",
            "graphql",
            "-f",
            f"query={mutation}",
            "-f",
            f"projectId={project_id}",
            "-f",
            f"itemId={item_id}",
            "-f",
            f"fieldId={field_id}",
            "-f",
            f"optionId={option_id}",
        ]
        output = _run_gh_command(command, "Failed setting board status")
    else:
        output = gh_runner(
            [
                "api",
                "graphql",
                "-f",
                f"query={mutation}",
                "-f",
                f"projectId={project_id}",
                "-f",
                f"itemId={item_id}",
                "-f",
                f"fieldId={field_id}",
                "-f",
                f"optionId={option_id}",
            ]
        )

    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise GhQueryError(
            "Failed setting board status: invalid JSON response."
        ) from error

    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        messages = [
            err.get("message", "unknown GraphQL error")
            for err in errors
            if isinstance(err, dict)
        ]
        joined = "; ".join(messages) if messages else "unknown GraphQL error"
        raise GhQueryError(f"Failed setting board status: {joined}")


def promote_to_ready(
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    dry_run: bool = False,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    controller_owned_resolver: Callable[[str], bool] | None = None,
) -> tuple[int, str]:
    """Validate and promote an issue from Backlog/Blocked to Ready.

    Steps:
    1. Query current status of target issue (via board_info_resolver)
    2. Reject if not in PROMOTABLE_STATUSES
    3. Run strict validator (require_in_graph=True, uses status_resolver)
    4. If dry_run: report what would happen
    5. If pass: mutate board Status -> Ready (via board_mutator)

    Dependency-injection points (for testing):
    - status_resolver: replaces _query_board_status for predecessor checks
    - board_info_resolver: replaces _query_issue_board_info for target issue
    - board_mutator: replaces the field-query + mutation sequence
    - controller_owned_resolver: blocks governed items when admission is controller-owned
    """
    parse_issue_ref(issue_ref)
    if (
        not dry_run
        and controller_owned_resolver is not None
        and controller_owned_resolver(issue_ref)
    ):
        return 2, (
            "REJECTED: controller_owned_admission\n"
            f"{issue_ref} is governed by the local admission controller."
        )

    resolve_info = board_info_resolver or _query_issue_board_info
    info = resolve_info(issue_ref, config, project_owner, project_number)

    if info.status not in PROMOTABLE_STATUSES:
        return 2, (
            f"Current status: {info.status}\n"
            f"REJECTED: {issue_ref} has Status={info.status}; "
            "must be Backlog or Blocked."
        )

    # Run strict validation (predecessors must all be Done, issue must be in graph)
    val_code, val_output = evaluate_ready_promotion(
        issue_ref=issue_ref,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        status_resolver=status_resolver,
        require_in_graph=True,
    )

    if val_code != 0:
        lines = [f"Current status: {info.status}"]
        if val_output:
            lines.append(val_output)
        return val_code, "\n".join(lines)

    if dry_run:
        return 0, (
            f"Current status: {info.status}\n"
            f"Validator: PASS (all predecessors Done)\n"
            f"Transition: {info.status} -> Ready (would promote)"
        )

    # Perform the mutation
    mutate = board_mutator
    if mutate is None:
        field_id, option_id = _query_status_field_option(
            info.project_id,
            "Ready",
        )
        _set_board_status(info.project_id, info.item_id, field_id, option_id)
    else:
        mutate(info.project_id, info.item_id)

    return 0, (
        f"Current status: {info.status}\n"
        f"Validator: PASS (all predecessors Done)\n"
        f"Transition: {info.status} -> Ready (promoted)"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate and promote an issue from Backlog/Blocked to Ready "
            "on the GitHub Project board."
        )
    )
    parser.add_argument("--issue", required=True, help="Issue ref, e.g. crew#88")
    parser.add_argument(
        "--file",
        default=str(_REPO_ROOT / "config" / "critical-paths.json"),
        help="Path to critical-paths JSON file",
    )
    parser.add_argument(
        "--project-owner",
        default="StartupAI-site",
        help="GitHub org/user that owns the Project board",
    )
    parser.add_argument(
        "--project-number",
        type=int,
        default=1,
        help="GitHub Project number",
    )
    parser.add_argument(
        "--automation-config",
        default=DEFAULT_AUTOMATION_CONFIG_PATH,
        help="Path to board automation config JSON",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Report what would happen without mutating the board",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load_config(Path(args.file))
        owns_admission, governed_repos = _load_controller_owned_prefixes(
            Path(args.automation_config)
        )
        code, output = promote_to_ready(
            issue_ref=args.issue,
            config=config,
            project_owner=args.project_owner,
            project_number=args.project_number,
            dry_run=args.dry_run,
            controller_owned_resolver=(
                lambda issue_ref: owns_admission
                and parse_issue_ref(issue_ref).prefix in governed_repos
            ),
        )
    except ConfigError as error:
        print(f"CONFIG ERROR: {error}", file=sys.stderr)
        return 3
    except GhQueryError as error:
        print(f"GH QUERY ERROR: {error}", file=sys.stderr)
        return 4

    if output:
        print(output)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
