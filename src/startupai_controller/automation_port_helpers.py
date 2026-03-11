"""Port-convenience helpers extracted from board_automation.py.

These are outer-layer helpers that resolve a default port when none is
provided, then delegate to a single port method.  They are used by shell
wiring in ``board_automation`` and passed as injected ``_fn`` callables to
application/wiring modules.

The module deliberately duplicates the four small port-factory helpers
(``_default_pr_port``, etc.) rather than importing them from
``board_automation`` to avoid circular imports.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from startupai_controller.ports.board_mutations import BoardMutationPort as _BoardMutationPort
    from startupai_controller.ports.issue_context import IssueContextPort as _IssueContextPort
    from startupai_controller.ports.pull_requests import PullRequestPort as _PullRequestPort
    from startupai_controller.ports.review_state import ReviewStatePort as _ReviewStatePort
else:
    _BoardMutationPort = None
    _IssueContextPort = None
    _PullRequestPort = None
    _ReviewStatePort = None

from startupai_controller.board_automation_config import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_PROJECT_NUMBER,
    DEFAULT_PROJECT_OWNER,
)
from startupai_controller.board_graph import _resolve_issue_coordinates
from startupai_controller.domain.models import LinkedIssue, OpenPullRequest
from startupai_controller.domain.repair_policy import parse_pr_url as _parse_pr_url
from startupai_controller.runtime.wiring import (
    GitHubRuntimeMemo as CycleGitHubMemo,
    build_github_port_bundle,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    load_config,
)

# ---------------------------------------------------------------------------
# Port-factory helpers (local copies to avoid circular imports)
# ---------------------------------------------------------------------------


def _default_pr_port(
    project_owner: str,
    project_number: int,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> _PullRequestPort:
    return build_github_port_bundle(
        project_owner,
        project_number,
        config=config,
        gh_runner=gh_runner,
    ).pull_requests


def _default_review_state_port(
    project_owner: str,
    project_number: int,
    config: CriticalPathConfig,
    gh_runner: Callable[..., str] | None = None,
) -> _ReviewStatePort:
    return build_github_port_bundle(
        project_owner,
        project_number,
        config=config,
        gh_runner=gh_runner,
    ).review_state


def _default_board_mutation_port(
    project_owner: str,
    project_number: int,
    config: CriticalPathConfig,
    gh_runner: Callable[..., str] | None = None,
) -> _BoardMutationPort:
    return build_github_port_bundle(
        project_owner,
        project_number,
        config=config,
        gh_runner=gh_runner,
    ).board_mutations


def _default_issue_context_port(
    project_owner: str,
    project_number: int,
    config: CriticalPathConfig,
    *,
    github_memo: CycleGitHubMemo | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> _IssueContextPort:
    return build_github_port_bundle(
        project_owner,
        project_number,
        config=config,
        github_memo=github_memo,
        gh_runner=gh_runner,
    ).issue_context


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _parse_github_timestamp(raw: str | None) -> datetime | None:
    """Parse one GitHub timestamp string into an aware datetime."""
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Query / port-convenience helpers
# ---------------------------------------------------------------------------


def _query_issue_updated_at(
    owner: str,
    repo: str,
    number: int,
    *,
    review_state_port: _ReviewStatePort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Return one issue updated timestamp through ReviewStatePort."""
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config or load_config(Path(DEFAULT_CONFIG_PATH)),
        gh_runner=gh_runner,
    )
    return review_state_port.issue_updated_at(f"{owner}/{repo}", number)


def _query_open_pr_updated_at(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Return one open PR updated timestamp through PullRequestPort."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    return _parse_github_timestamp(
        pr_port.pull_request_updated_at(f"{owner}/{repo}", pr_number)
    )


def _query_latest_wip_activity_timestamp(
    issue_ref: str,
    owner: str,
    repo: str,
    number: int,
    pr_field: str,
    *,
    review_state_port: _ReviewStatePort | None = None,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Return the latest meaningful activity timestamp for one WIP issue."""
    issue_updated = _query_issue_updated_at(
        owner,
        repo,
        number,
        review_state_port=review_state_port,
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        gh_runner=gh_runner,
    )
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config or load_config(Path(DEFAULT_CONFIG_PATH)),
        gh_runner=gh_runner,
    )
    latest_comment = review_state_port.latest_non_automation_comment_timestamp(
        f"{owner}/{repo}",
        number,
    )
    latest_pr = None
    parsed_pr = _parse_pr_url(pr_field)
    if parsed_pr is not None:
        pr_owner, pr_repo, pr_number = parsed_pr
        latest_pr = _query_open_pr_updated_at(
            pr_owner,
            pr_repo,
            pr_number,
            pr_port=pr_port,
            project_owner=project_owner,
            project_number=project_number,
            config=config,
            gh_runner=gh_runner,
        )
    values = [ts for ts in (issue_updated, latest_comment, latest_pr) if ts is not None]
    return max(values) if values else None


def _is_pr_open(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Return whether the PR is currently open."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    return pr_port.is_pull_request_open(f"{owner}/{repo}", pr_number)


def _query_issue_assignees(
    owner: str,
    repo: str,
    number: int,
    *,
    review_state_port: _ReviewStatePort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, ...]:
    """Return issue assignees through ReviewStatePort."""
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config or load_config(Path(DEFAULT_CONFIG_PATH)),
        gh_runner=gh_runner,
    )
    return review_state_port.issue_assignees(f"{owner}/{repo}", number)


def _set_issue_assignees(
    owner: str,
    repo: str,
    number: int,
    assignees: tuple[str, ...] | list[str],
    *,
    board_port: _BoardMutationPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set issue assignees through BoardMutationPort."""
    board_port = board_port or _default_board_mutation_port(
        project_owner,
        project_number,
        config or load_config(Path(DEFAULT_CONFIG_PATH)),
        gh_runner=gh_runner,
    )
    board_port.set_issue_assignees(f"{owner}/{repo}", number, tuple(assignees))


def query_closing_issues(
    owner: str,
    repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    *,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    gh_runner: Callable[..., str] | None = None,
) -> list[LinkedIssue]:
    """Resolve linked issues for one PR through PullRequestPort."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    linked: list[LinkedIssue] = []
    for issue_ref in pr_port.linked_issue_refs(f"{owner}/{repo}", pr_number):
        issue_owner, issue_repo, issue_number = _resolve_issue_coordinates(
            issue_ref, config
        )
        linked.append(
            LinkedIssue(
                owner=issue_owner,
                repo=issue_repo,
                number=issue_number,
                ref=issue_ref,
            )
        )
    return linked


def query_open_pull_requests(
    repo_prefix: str,
    config: CriticalPathConfig,
    *,
    github_memo: CycleGitHubMemo | None = None,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    gh_runner: Callable[..., str] | None = None,
) -> list[OpenPullRequest]:
    """List open pull requests for one repo prefix through PullRequestPort."""
    repo_slug = config.issue_prefixes[repo_prefix]
    pr_port = pr_port or build_github_port_bundle(
        project_owner,
        project_number,
        config=config,
        github_memo=github_memo,
        gh_runner=gh_runner,
    ).pull_requests
    return pr_port.list_open_prs(repo_slug)


def query_required_status_checks(
    pr_repo: str,
    base_ref_name: str = "main",
    *,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> set[str]:
    """Return required status checks through PullRequestPort."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    return pr_port.required_status_checks(pr_repo, base_ref_name)


def query_latest_codex_verdict(
    pr_repo: str,
    pr_number: int,
    *,
    trusted_actors: set[str],
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> object | None:
    """Return the latest trusted codex verdict through ReviewSnapshot reads."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    snapshots = pr_port.review_snapshots(
        {(pr_repo, pr_number): ()},
        trusted_codex_actors=frozenset(trusted_actors),
    )
    snapshot = snapshots.get((pr_repo, pr_number))
    return None if snapshot is None else snapshot.codex_verdict


def _query_failed_check_runs(
    owner: str,
    repo: str,
    head_sha: str,
    *,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str] | None:
    """Return failed check runs through PullRequestPort."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    result = pr_port.failed_check_runs(f"{owner}/{repo}", head_sha)
    return None if result is None else list(result)


def _query_pr_head_sha(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> str | None:
    """Return the PR head SHA through PullRequestPort."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    return pr_port.pull_request_head_sha(f"{owner}/{repo}", pr_number)


def close_issue(
    owner: str,
    repo: str,
    number: int,
    *,
    board_port: _BoardMutationPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Close an issue through BoardMutationPort."""
    board_port = board_port or _default_board_mutation_port(
        project_owner,
        project_number,
        config or load_config(Path(DEFAULT_CONFIG_PATH)),
        gh_runner=gh_runner,
    )
    board_port.close_issue(f"{owner}/{repo}", number)


def close_pull_request(
    pr_repo: str,
    pr_number: int,
    *,
    comment: str | None = None,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Close a pull request through PullRequestPort."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    pr_port.close_pull_request(pr_repo, pr_number, comment=comment)


def memoized_query_issue_body(
    memo: CycleGitHubMemo,
    owner: str,
    repo: str,
    number: int,
    *,
    issue_context_port: _IssueContextPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Return one issue body through IssueContextPort with cycle-local memoization."""
    key = (owner, repo, number)
    cached = memo.issue_bodies.get(key)
    if cached is not None:
        return cached
    issue_context_port = issue_context_port or _default_issue_context_port(
        project_owner,
        project_number,
        config or load_config(Path(DEFAULT_CONFIG_PATH)),
        github_memo=memo,
        gh_runner=gh_runner,
    )
    body = issue_context_port.get_issue_context(owner, repo, number).body
    memo.issue_bodies[key] = body
    return body


def rerun_actions_run(
    pr_repo: str,
    run_id: int,
    *,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Re-run one failed check run through PullRequestPort."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    pr_port.rerun_failed_check(pr_repo, "", run_id)
