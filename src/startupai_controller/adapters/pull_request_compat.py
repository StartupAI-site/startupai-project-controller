"""Compatibility function surface for pull-request adapter helpers."""

from __future__ import annotations

from collections.abc import Callable, Sequence
import time
from typing import TYPE_CHECKING

from startupai_controller.adapters import (
    pull_request_review_state as _review_state_builders,
)
from startupai_controller.adapters.github_transport import _run_gh
from startupai_controller.adapters.github_types import (
    CycleGitHubMemo,
    CodexReviewVerdict,
    LinkedIssue,
    PullRequestStateProbe as _PullRequestStateProbe,
    PullRequestViewPayload,
)
from startupai_controller.domain.models import OpenPullRequest
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
    parse_issue_ref,
)

if TYPE_CHECKING:
    from startupai_controller.adapters.pull_requests import GitHubPullRequestAdapter


_REQUIRED_STATUS_CHECKS_CACHE_TTL_SECONDS = 900
_required_status_checks_ttl_cache: dict[
    tuple[str, str],
    tuple[float, set[str]],
] = {}


def _build_pull_request_adapter(
    *,
    memo: CycleGitHubMemo | None = None,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> GitHubPullRequestAdapter:
    from startupai_controller.adapters.pull_requests import GitHubPullRequestAdapter

    return GitHubPullRequestAdapter(
        project_owner="",
        project_number=0,
        github_memo=memo,
        config=config,
        gh_runner=gh_runner,
    )


def query_open_pull_requests(
    pr_repo: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[OpenPullRequest]:
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
    return _build_pull_request_adapter(gh_runner=gh_runner).list_open_prs(pr_repo)


def _post_comment(
    owner: str,
    repo: str,
    number: int,
    body: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility wrapper implemented on the adapter-owned comment path."""
    _build_pull_request_adapter(gh_runner=gh_runner)._post_issue_comment(
        owner,
        repo,
        number,
        body,
    )


def query_pull_request_view_payload(
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> PullRequestViewPayload:
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
    return _build_pull_request_adapter(
        gh_runner=gh_runner
    )._query_pull_request_view_payload(
        pr_repo,
        pr_number,
    )


def query_pull_request_view_payloads(
    pr_repo: str,
    pr_numbers: Sequence[int],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[int, PullRequestViewPayload]:
    """Compatibility wrapper for batched PR payload reads."""
    return _build_pull_request_adapter(
        gh_runner=gh_runner
    )._query_pull_request_view_payloads(pr_repo, pr_numbers)


def memoized_query_pull_request_view_payloads(
    memo: CycleGitHubMemo,
    pr_repo: str,
    pr_numbers: Sequence[int],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[int, PullRequestViewPayload]:
    """Compatibility wrapper for memoized batched PR payload reads."""
    return _build_pull_request_adapter(
        memo=memo,
        gh_runner=gh_runner,
    )._memoized_pull_request_view_payloads(pr_repo, pr_numbers)


def query_pull_request_state_probes(
    pr_repo: str,
    pr_numbers: Sequence[int],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[int, _PullRequestStateProbe]:
    """Compatibility wrapper for batched PR state-probe reads."""
    return _build_pull_request_adapter(
        gh_runner=gh_runner
    )._query_pull_request_state_probes(pr_repo, pr_numbers)


def memoized_query_pull_request_state_probes(
    memo: CycleGitHubMemo,
    pr_repo: str,
    pr_numbers: Sequence[int],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[int, _PullRequestStateProbe]:
    """Compatibility wrapper for memoized batched PR state-probe reads."""
    return _build_pull_request_adapter(
        memo=memo,
        gh_runner=gh_runner,
    )._memoized_pull_request_state_probes(pr_repo, pr_numbers)


def query_latest_codex_verdict(
    pr_repo: str,
    pr_number: int,
    *,
    trusted_actors: set[str] | frozenset[str] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> CodexReviewVerdict | None:
    """Compatibility wrapper implemented on the adapter-owned PR payload path."""
    payload = query_pull_request_view_payload(
        pr_repo,
        pr_number,
        gh_runner=gh_runner,
    )
    return _review_state_builders.latest_codex_verdict_from_payload(
        payload,
        trusted_actors=trusted_actors,
    )


def query_required_status_checks(
    pr_repo: str,
    base_ref_name: str = "main",
    *,
    gh_runner: Callable[..., str] | None = None,
) -> set[str]:
    """Compatibility wrapper with process TTL cache and stale-on-error fallback."""
    cache_key = (pr_repo, base_ref_name)
    cached = _required_status_checks_ttl_cache.get(cache_key)
    now_monotonic = time.monotonic()
    if cached is not None:
        expires_at, required = cached
        if expires_at > now_monotonic:
            return set(required)

    adapter = _build_pull_request_adapter(gh_runner=gh_runner)
    try:
        required = adapter._query_required_status_checks(pr_repo, base_ref_name)
    except GhQueryError:
        if cached is not None:
            return set(cached[1])
        raise

    _required_status_checks_ttl_cache[cache_key] = (
        now_monotonic + _REQUIRED_STATUS_CHECKS_CACHE_TTL_SECONDS,
        set(required),
    )
    return set(required)


def clear_required_status_checks_cache() -> None:
    """Clear the process-local required-check TTL cache."""
    _required_status_checks_ttl_cache.clear()


def query_closing_issues(
    pr_owner: str,
    pr_repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[LinkedIssue]:
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
    refs = _build_pull_request_adapter(
        config=config,
        gh_runner=gh_runner,
    )._query_closing_issue_refs(f"{pr_owner}/{pr_repo}", pr_number)
    issues: list[LinkedIssue] = []
    for ref in refs:
        parsed = parse_issue_ref(ref)
        repo_slug = config.issue_prefixes.get(parsed.prefix)
        if not repo_slug:
            continue
        owner, repo = repo_slug.split("/", maxsplit=1)
        issues.append(
            LinkedIssue(
                owner=owner,
                repo=repo,
                number=parsed.number,
                ref=ref,
            )
        )
    return issues


def close_pull_request(
    pr_repo: str,
    pr_number: int,
    *,
    comment: str | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
    args = ["pr", "close", str(pr_number), "--repo", pr_repo]
    if comment is not None:
        args.extend(["--comment", comment])
    _run_gh(
        args,
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
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
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


def rerun_actions_run(
    pr_repo: str,
    run_id: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
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
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
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
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
    args = ["pr", "merge", str(pr_number), "--repo", pr_repo, "--auto", "--squash"]
    if delete_branch:
        args.append("--delete-branch")
    _run_gh(
        args,
        gh_runner=gh_runner,
        operation_type="automerge",
    )
    adapter = _build_pull_request_adapter(gh_runner=gh_runner)
    for _attempt in range(confirm_retries):
        time.sleep(confirm_delay_seconds)
        try:
            payload = adapter._gh_json(
                [
                    "pr",
                    "view",
                    str(pr_number),
                    "--repo",
                    pr_repo,
                    "--json",
                    "autoMergeRequest",
                ],
                error_message=(
                    f"Failed querying automerge state for {pr_repo}#{pr_number}: invalid JSON."
                ),
            )
            if (
                isinstance(payload, dict)
                and payload.get("autoMergeRequest") is not None
            ):
                return "confirmed"
        except GhQueryError:
            continue
    return "pending"
