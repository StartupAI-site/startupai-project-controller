"""Event resolution use case — parse GitHub events into issue-ref tuples."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

from startupai_controller.domain.models import LinkedIssue
from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    parse_issue_ref,
)


def resolve_issues_from_event(
    event_path: str,
    config,
    *,
    pr_port=None,
    gh_runner: Callable[..., str] | None = None,
    query_closing_issues_fn: Callable[..., tuple] | None = None,
    query_failed_check_runs_fn: Callable[..., list[str]] | None = None,
) -> list[tuple[str, str, list[str] | None]]:
    """Parse GITHUB_EVENT_PATH -> list of (issue_ref, event_kind, failed_checks).

    failed_checks is populated for check_suite failure events (names of failed
    check runs queried from the API). None for all other event types.
    """
    try:
        event_data = json.loads(Path(event_path).read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as error:
        raise ConfigError(
            f"Failed reading event file {event_path}: {error}"
        ) from error

    results: list[tuple[str, str, list[str] | None]] = []

    # Determine event type from structure
    if "pull_request" in event_data:
        pr = event_data["pull_request"]
        pr_number = pr.get("number")
        pr_repo = pr.get("base", {}).get("repo", {}).get("full_name", "")
        merged = pr.get("merged", False)
        action = event_data.get("action", "")

        if not pr_number or not pr_repo:
            return results

        if "/" not in pr_repo:
            return results
        owner, repo = pr_repo.split("/", maxsplit=1)

        linked = _resolve_linked_issues(
            owner, repo, pr_number, config,
            pr_port=pr_port,
            gh_runner=gh_runner,
            query_closing_issues_fn=query_closing_issues_fn,
        )

        if action in ("opened", "reopened", "synchronize"):
            event_kind = "pr_open"
        elif action == "ready_for_review":
            event_kind = "pr_ready_for_review"
        elif action == "closed" and merged:
            event_kind = "pr_close_merged"
        elif action == "closed" and not merged:
            return results  # Closed without merge, no state change
        else:
            return results

        for issue in linked:
            results.append((issue.ref, event_kind, None))

    elif "review" in event_data:
        review = event_data["review"]
        review_state = review.get("state", "")
        pr = event_data.get("pull_request", {})
        pr_number = pr.get("number")
        pr_repo = pr.get("base", {}).get("repo", {}).get("full_name", "")

        if not pr_number or not pr_repo:
            return results

        if "/" not in pr_repo:
            return results
        owner, repo = pr_repo.split("/", maxsplit=1)

        linked = _resolve_linked_issues(
            owner, repo, pr_number, config,
            pr_port=pr_port,
            gh_runner=gh_runner,
            query_closing_issues_fn=query_closing_issues_fn,
        )

        if review_state == "changes_requested":
            for issue in linked:
                results.append((issue.ref, "changes_requested", None))
        elif review_state in {"approved", "commented"}:
            for issue in linked:
                results.append((issue.ref, "review_submitted", None))

    elif "check_suite" in event_data:
        check_suite = event_data["check_suite"]
        conclusion = check_suite.get("conclusion", "")
        head_sha = check_suite.get("head_sha", "")
        pull_requests = check_suite.get("pull_requests", [])

        for pr_info in pull_requests:
            pr_number = pr_info.get("number")
            pr_repo_full = (
                pr_info.get("base", {}).get("repo", {}).get("full_name", "")
            )

            if not pr_number or not pr_repo_full:
                continue

            if "/" not in pr_repo_full:
                continue
            owner, repo = pr_repo_full.split("/", maxsplit=1)

            linked = _resolve_linked_issues(
                owner, repo, pr_number, config,
                pr_port=pr_port,
                gh_runner=gh_runner,
                query_closing_issues_fn=query_closing_issues_fn,
            )

            if conclusion == "failure":
                event_kind = "checks_failed"
            elif conclusion == "success":
                event_kind = "checks_passed"
            else:
                continue

            # For failure events, query the actual failed check run names
            failed_names: list[str] | None = None
            if event_kind == "checks_failed" and head_sha:
                if query_failed_check_runs_fn is not None:
                    failed_names = query_failed_check_runs_fn(
                        owner,
                        repo,
                        head_sha,
                        pr_port=pr_port,
                        gh_runner=gh_runner,
                    )

            for issue in linked:
                results.append((issue.ref, event_kind, failed_names))

    return results


def resolve_pr_to_issues(
    pr_repo: str,
    pr_number: int,
    config,
    *,
    pr_port=None,
    gh_runner: Callable[..., str] | None = None,
    query_closing_issues_fn: Callable[..., tuple] | None = None,
) -> list[str]:
    """Resolve PR -> linked issue refs using closingIssuesReferences."""
    if "/" not in pr_repo:
        raise ConfigError(f"pr_repo must be 'owner/repo', got '{pr_repo}'.")
    owner, repo = pr_repo.split("/", maxsplit=1)
    if pr_port is not None:
        return list(pr_port.linked_issue_refs(pr_repo, pr_number))
    if query_closing_issues_fn is None:
        raise ConfigError("query_closing_issues_fn is required when pr_port is None")
    linked = query_closing_issues_fn(
        owner, repo, pr_number, config, gh_runner=gh_runner
    )
    return [issue.ref for issue in linked]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_linked_issues(
    owner: str,
    repo: str,
    pr_number: int,
    config,
    *,
    pr_port=None,
    gh_runner: Callable[..., str] | None = None,
    query_closing_issues_fn: Callable[..., tuple] | None = None,
) -> tuple:
    """Resolve linked issues via port or fallback query function."""
    if pr_port is not None:
        return tuple(
            LinkedIssue(
                owner=owner,
                repo=repo,
                number=parse_issue_ref(issue_ref).number,
                ref=issue_ref,
            )
            for issue_ref in pr_port.linked_issue_refs(f"{owner}/{repo}", pr_number)
        )
    if query_closing_issues_fn is not None:
        return query_closing_issues_fn(
            owner, repo, pr_number, config, gh_runner=gh_runner
        )
    return ()
