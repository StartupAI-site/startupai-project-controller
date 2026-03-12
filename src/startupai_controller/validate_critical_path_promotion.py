#!/usr/bin/env python3
"""Validate whether an issue can move to Ready on a critical path.

Rules:
- Direct predecessors only (no transitive ancestor traversal)
- If any direct predecessor is not Done on the project board, block promotion
- If issue is not present in any critical path, warn but do not block
"""

from __future__ import annotations

import argparse
from collections import defaultdict, deque
from dataclasses import dataclass
import json
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Callable

from startupai_controller.gh_cli_timeout import gh_command_timeout_seconds
from startupai_controller.github_http import GitHubTransportError, run_github_command

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
ISSUE_REF_RE = re.compile(r"^(app|crew|site)#(\d+)$")
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


class ConfigError(ValueError):
    """Raised when critical-path config is invalid."""


class GhQueryError(RuntimeError):
    """Raised when GitHub query fails."""


@dataclass(frozen=True)
class IssueRef:
    prefix: str
    number: int

    @property
    def text(self) -> str:
        return f"{self.prefix}#{self.number}"


@dataclass(frozen=True)
class CriticalPath:
    name: str
    goal: str
    first_value_at: str
    edges: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class CriticalPathConfig:
    issue_prefixes: dict[str, str]
    critical_paths: dict[str, CriticalPath]


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


def parse_issue_ref(value: str) -> IssueRef:
    match = ISSUE_REF_RE.match(value)
    if not match:
        raise ConfigError(
            "Invalid issue ref "
            f"'{value}'. Expected format <prefix>#123 where "
            "prefix is one of app|crew|site."
        )
    return IssueRef(prefix=match.group(1), number=int(match.group(2)))


def _validate_graph_acyclic(path_name: str, edges: list[tuple[str, str]]) -> None:
    in_degree: dict[str, int] = defaultdict(int)
    adjacency: dict[str, list[str]] = defaultdict(list)
    nodes: set[str] = set()

    for source, target in edges:
        nodes.add(source)
        nodes.add(target)
        adjacency[source].append(target)
        in_degree[target] += 1
        in_degree[source] += 0

    queue: deque[str] = deque(node for node in nodes if in_degree[node] == 0)
    visited = 0

    while queue:
        node = queue.popleft()
        visited += 1
        for neighbor in adjacency[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if visited != len(nodes):
        raise ConfigError(
            f"Critical path '{path_name}' contains a cycle; graph must be acyclic."
        )


def load_config(path: Path) -> CriticalPathConfig:
    if not path.exists():
        raise ConfigError(f"Critical path file not found: {path}")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ConfigError(f"Invalid JSON in {path}: {error}") from error

    if not isinstance(payload, dict):
        raise ConfigError("Config root must be an object.")

    issue_prefixes = payload.get("issue_prefixes")
    if not isinstance(issue_prefixes, dict):
        raise ConfigError("issue_prefixes must be an object mapping prefix -> repo.")

    critical_paths_raw = payload.get("critical_paths")
    if not isinstance(critical_paths_raw, dict):
        raise ConfigError("critical_paths must be an object.")

    critical_paths: dict[str, CriticalPath] = {}

    for path_name, path_payload in critical_paths_raw.items():
        if not isinstance(path_payload, dict):
            raise ConfigError(f"critical_paths.{path_name} must be an object.")

        goal = path_payload.get("goal")
        first_value_at = path_payload.get("first_value_at")
        edges_raw = path_payload.get("edges")

        if not isinstance(goal, str) or not goal.strip():
            raise ConfigError(
                f"critical_paths.{path_name}.goal must be a non-empty string."
            )
        if not isinstance(first_value_at, str):
            raise ConfigError(
                f"critical_paths.{path_name}.first_value_at must be a string issue ref."
            )
        parse_issue_ref(first_value_at)

        if not isinstance(edges_raw, list):
            raise ConfigError(f"critical_paths.{path_name}.edges must be a list.")

        edges: list[tuple[str, str]] = []
        nodes: set[str] = set()

        for index, edge in enumerate(edges_raw):
            if (
                not isinstance(edge, list)
                or len(edge) != 2
                or not all(isinstance(item, str) for item in edge)
            ):
                raise ConfigError(
                    f"critical_paths.{path_name}.edges[{index}] must be [source, target]."
                )

            source, target = edge
            parse_issue_ref(source)
            parse_issue_ref(target)

            if source == target:
                raise ConfigError(
                    f"critical_paths.{path_name}.edges[{index}] is a self-loop: {source}."
                )

            nodes.add(source)
            nodes.add(target)
            edges.append((source, target))

        if first_value_at not in nodes:
            raise ConfigError(
                f"critical_paths.{path_name}.first_value_at ({first_value_at}) "
                "must appear in at least one edge endpoint."
            )

        _validate_graph_acyclic(path_name, edges)

        critical_paths[path_name] = CriticalPath(
            name=path_name,
            goal=goal,
            first_value_at=first_value_at,
            edges=tuple(edges),
        )

    # Validate prefix repositories are usable owner/repo slugs.
    for prefix, repo_slug in issue_prefixes.items():
        if not isinstance(prefix, str) or not isinstance(repo_slug, str):
            raise ConfigError("issue_prefixes entries must be string -> string.")
        if "/" not in repo_slug:
            raise ConfigError(
                f"issue_prefixes.{prefix} must be '<owner>/<repo>', got '{repo_slug}'."
            )

    return CriticalPathConfig(
        issue_prefixes=issue_prefixes, critical_paths=critical_paths
    )


def direct_predecessors(config: CriticalPathConfig, issue_ref: str) -> set[str]:
    predecessors: set[str] = set()
    for path in config.critical_paths.values():
        for source, target in path.edges:
            if target == issue_ref:
                predecessors.add(source)
    return predecessors


def direct_successors(config: CriticalPathConfig, issue_ref: str) -> set[str]:
    """Return issues that directly depend on issue_ref."""
    successors: set[str] = set()
    for path in config.critical_paths.values():
        for source, target in path.edges:
            if source == issue_ref:
                successors.add(target)
    return successors


def in_any_critical_path(config: CriticalPathConfig, issue_ref: str) -> bool:
    for path in config.critical_paths.values():
        for source, target in path.edges:
            if source == issue_ref or target == issue_ref:
                return True
    return False


def _query_board_status(
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
) -> str:
    parsed = parse_issue_ref(issue_ref)
    repo_slug = config.issue_prefixes.get(parsed.prefix)
    if not repo_slug:
        raise ConfigError(
            f"Missing repo mapping for prefix '{parsed.prefix}' in issue_prefixes."
        )

    try:
        owner, repo = repo_slug.split("/", maxsplit=1)
    except ValueError as error:
        raise ConfigError(
            f"Invalid repo mapping for '{parsed.prefix}': '{repo_slug}'."
        ) from error

    query = """
query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    issue(number: $number) {
      projectItems(first: 20) {
        nodes {
          project {
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
    output = _run_gh_command(command, f"Failed querying status for {issue_ref}")

    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise GhQueryError(
            f"Failed querying status for {issue_ref}: invalid JSON response from gh api."
        ) from error

    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        messages = [
            err.get("message", "unknown GraphQL error")
            for err in errors
            if isinstance(err, dict)
        ]
        joined = "; ".join(messages) if messages else "unknown GraphQL error"
        raise GhQueryError(f"Failed querying status for {issue_ref}: {joined}")

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
            status = (node.get("statusField") or {}).get("name")
            return status or "UNKNOWN"

    return "NOT_ON_BOARD"


def evaluate_ready_promotion(
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    status_resolver: Callable[[str, CriticalPathConfig, str, int], str] | None = None,
    require_in_graph: bool = False,
) -> tuple[int, str]:
    parse_issue_ref(issue_ref)
    resolver = status_resolver or _query_board_status

    if not in_any_critical_path(config, issue_ref):
        msg = f"WARN: issue {issue_ref} is not present in any critical path."
        if require_in_graph:
            return 2, f"BLOCKED: {msg} (--require-in-graph is set)"
        return 0, msg

    predecessors = sorted(direct_predecessors(config, issue_ref))
    if not predecessors:
        return 0, ""

    unmet: list[tuple[str, str]] = []
    for predecessor in predecessors:
        status = resolver(predecessor, config, project_owner, project_number)
        if status != "Done":
            unmet.append((predecessor, status))

    if unmet:
        lines = [
            f"BLOCKED: {issue_ref} cannot move to Ready.",
            "Unmet predecessors:",
        ]
        lines.extend(f"- {ref} (Status={status})" for ref, status in unmet)
        return 2, "\n".join(lines)

    return 0, ""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate whether an issue can be promoted to Ready based on direct "
            "critical-path predecessors."
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
        "--require-in-graph",
        action="store_true",
        default=False,
        help="Exit 2 (blocked) if the issue is not in any critical path",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load_config(Path(args.file))
        code, output = evaluate_ready_promotion(
            issue_ref=args.issue,
            config=config,
            project_owner=args.project_owner,
            project_number=args.project_number,
            require_in_graph=args.require_in_graph,
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
