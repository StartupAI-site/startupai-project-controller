"""Shared GitHub adapter base helpers.

Adapter-private infrastructure for GitHub-backed ports. This module owns the
common transport/error/config mechanics used by capability adapters.
"""

from __future__ import annotations

from collections.abc import Callable
import json

from startupai_controller.adapters.github_transport import _run_gh
from startupai_controller.adapters.github_types import CycleGitHubMemo
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
)


class GitHubAdapterBase:
    """Common GitHub adapter plumbing shared by capability adapters."""

    def __init__(
        self,
        *,
        project_owner: str,
        project_number: int,
        config: CriticalPathConfig | None = None,
        github_memo: CycleGitHubMemo | None = None,
        gh_runner: Callable[..., str] | None = None,
    ) -> None:
        self._project_owner = project_owner
        self._project_number = project_number
        self._config = config
        self._github_memo = github_memo or CycleGitHubMemo()
        self._gh_runner = gh_runner

    def _require_config(self) -> CriticalPathConfig:
        if self._config is None:
            raise ValueError(
                "GitHub adapter requires config for board-state operations"
            )
        return self._config

    def _graphql(self, query: str, *, fields: list[str]) -> dict:
        """Run a GraphQL request and return parsed JSON payload."""
        output = _run_gh(
            [
                "api",
                "graphql",
                "-f",
                f"query={query}",
                *fields,
            ],
            gh_runner=self._gh_runner,
        )
        try:
            payload = json.loads(output)
        except json.JSONDecodeError as error:
            raise GhQueryError("Invalid GraphQL JSON response.") from error
        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            messages = [
                err.get("message", "unknown GraphQL error")
                for err in errors
                if isinstance(err, dict)
            ]
            joined = "; ".join(messages) if messages else "unknown GraphQL error"
            raise GhQueryError(joined)
        return payload

    def _gh_json(
        self,
        args: list[str],
        *,
        operation_type: str = "query",
        error_message: str,
    ) -> object:
        """Run gh and parse the JSON payload with adapter-owned error shaping."""
        output = _run_gh(
            args,
            gh_runner=self._gh_runner,
            operation_type=operation_type,
        )
        try:
            return json.loads(output) if output else None
        except json.JSONDecodeError as error:
            raise GhQueryError(error_message) from error

    def _issue_ref_from_repo_slug(self, repo_slug: str, issue_number: int) -> str:
        """Map a repo slug + issue number to canonical issue_ref when config is known."""
        if self._config is not None:
            for prefix, slug in self._config.issue_prefixes.items():
                if slug == repo_slug:
                    return f"{prefix}#{issue_number}"
        return f"{repo_slug}#{issue_number}"
