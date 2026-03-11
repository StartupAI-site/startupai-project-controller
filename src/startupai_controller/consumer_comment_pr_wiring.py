"""Comment, provenance, and PR wiring extracted from board_consumer."""

from __future__ import annotations

import re
from typing import Any, Callable

from startupai_controller import consumer_comment_pr_helpers as _comment_pr_helpers
from startupai_controller.domain.repair_policy import (
    consumer_provenance_marker as _domain_consumer_provenance_marker,
    deterministic_branch_pattern as _domain_deterministic_branch_pattern,
    extract_acceptance_criteria as _domain_extract_acceptance_criteria,
    parse_consumer_provenance as _domain_parse_consumer_provenance,
    repo_to_prefix_for_repo as _domain_repo_to_prefix_for_repo,
)


def extract_acceptance_criteria(body: str) -> str:
    """Extract acceptance criteria from issue body text."""
    return _domain_extract_acceptance_criteria(body)


def deterministic_branch_pattern(
    issue_ref: str,
    *,
    parse_issue_ref: Callable[[str], Any],
) -> re.Pattern[str]:
    """Return the canonical deterministic branch pattern for an issue."""
    parsed = parse_issue_ref(issue_ref)
    return _domain_deterministic_branch_pattern(parsed.number)


def repo_to_prefix_for_repo(repo: str) -> str:
    """Best-effort repo name to board prefix mapping."""
    return _domain_repo_to_prefix_for_repo(repo)


def consumer_provenance_marker(
    *,
    session_id: str,
    issue_ref: str,
    repo_prefix: str,
    branch_name: str,
    executor: str,
) -> str:
    """Build the machine-readable provenance marker for issues and PRs."""
    return _domain_consumer_provenance_marker(
        session_id=session_id,
        issue_ref=issue_ref,
        repo_prefix=repo_prefix,
        branch_name=branch_name,
        executor=executor,
    )


def parse_consumer_provenance(text: str) -> dict[str, str] | None:
    """Parse the consumer provenance marker from free text."""
    return _domain_parse_consumer_provenance(text)


def default_review_comment_checker(
    *,
    build_github_port_bundle: Callable[..., Any],
    gh_runner: Callable[..., str] | None = None,
) -> Callable[[str, str, int, str], bool]:
    """Build the default marker-check helper through ReviewStatePort."""
    review_state_port = build_github_port_bundle(
        "",
        0,
        gh_runner=gh_runner,
    ).review_state

    def checker(
        owner: str,
        repo: str,
        number: int,
        marker: str,
        *,
        gh_runner: Callable[..., str] | None = None,
    ) -> bool:
        return review_state_port.comment_exists(f"{owner}/{repo}", number, marker)

    return checker


def runtime_comment_poster(
    owner: str,
    repo: str,
    number: int,
    body: str,
    *,
    build_github_port_bundle: Callable[..., Any],
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Post an issue comment through the runtime port boundary."""
    bundle = build_github_port_bundle("", 0, gh_runner=gh_runner)
    bundle.board_mutations.post_issue_comment(f"{owner}/{repo}", number, body)


def runtime_issue_closer(
    owner: str,
    repo: str,
    number: int,
    *,
    build_github_port_bundle: Callable[..., Any],
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Close an issue through the runtime port boundary."""
    bundle = build_github_port_bundle("", 0, gh_runner=gh_runner)
    bundle.board_mutations.close_issue(f"{owner}/{repo}", number)


def runtime_automerge_enabler(
    pr_repo: str,
    pr_number: int,
    *,
    build_github_port_bundle: Callable[..., Any],
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Enable auto-merge through the runtime port boundary."""
    bundle = build_github_port_bundle("", 0, gh_runner=gh_runner)
    return bundle.pull_requests.enable_automerge(pr_repo, pr_number)


def runtime_failed_check_rerun(
    pr_repo: str,
    run_id: int,
    *,
    build_github_port_bundle: Callable[..., Any],
    gh_query_error_cls: type[Exception],
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Re-run a failed check through the runtime port boundary."""
    bundle = build_github_port_bundle("", 0, gh_runner=gh_runner)
    if not bundle.pull_requests.rerun_failed_check(pr_repo, "", run_id):
        raise gh_query_error_cls(f"Failed rerunning check for {pr_repo} run {run_id}")


def post_consumer_claim_comment(
    issue_ref: str,
    session_id: str,
    repo_prefix: str,
    branch_name: str,
    executor: str,
    config: Any,
    *,
    resolve_issue_coordinates: Callable[..., tuple[str, str, int]],
    consumer_provenance_marker_fn: Callable[..., str],
    default_review_comment_checker_fn: Callable[..., Callable[..., bool]],
    runtime_comment_poster_fn: Callable[..., None],
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Post a deterministic claim provenance marker on the issue."""
    _comment_pr_helpers.post_consumer_claim_comment(
        issue_ref,
        session_id,
        repo_prefix,
        branch_name,
        executor,
        config,
        resolve_issue_coordinates=resolve_issue_coordinates,
        consumer_provenance_marker=consumer_provenance_marker_fn,
        default_review_comment_checker=default_review_comment_checker_fn,
        runtime_comment_poster=runtime_comment_poster_fn,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def list_open_pr_candidates(
    owner: str,
    repo: str,
    issue_number: int,
    *,
    build_github_port_bundle: Callable[..., Any],
    open_pr_match_factory: Callable[..., Any],
    parse_consumer_provenance_fn: Callable[[str], dict[str, str] | None],
    pr_port: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[Any]:
    """Return open PRs that reference an issue number in the repository."""
    return _comment_pr_helpers.list_open_pr_candidates(
        owner,
        repo,
        issue_number,
        build_github_port_bundle=build_github_port_bundle,
        open_pr_match_factory=open_pr_match_factory,
        parse_consumer_provenance=parse_consumer_provenance_fn,
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
    list_open_pr_candidates_fn: Callable[..., list[Any]],
    classify_pr_candidates_pure: Callable[..., tuple[str, Any | None, str]],
    expected_branch: str | None = None,
    pr_port: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, Any | None, str]:
    """Classify open PRs for an issue as adoptable, ambiguous, non-local, or none."""
    return _comment_pr_helpers.classify_open_pr_candidates(
        issue_ref,
        owner,
        repo,
        issue_number,
        automation_config,
        list_open_pr_candidates=list_open_pr_candidates_fn,
        classify_pr_candidates_pure=classify_pr_candidates_pure,
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
    marker_for: Callable[..., str],
    resolve_issue_coordinates: Callable[..., tuple[str, str, int]],
    normalize_resolution_payload: Callable[..., Any],
    default_review_comment_checker_fn: Callable[..., Callable[..., bool]],
    runtime_comment_poster_fn: Callable[..., None],
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Post a machine-marker result comment on the issue."""
    _comment_pr_helpers.post_result_comment(
        issue_ref,
        result,
        session_id,
        config,
        marker_for=marker_for,
        resolve_issue_coordinates=resolve_issue_coordinates,
        normalize_resolution_payload=normalize_resolution_payload,
        default_review_comment_checker=default_review_comment_checker_fn,
        runtime_comment_poster=runtime_comment_poster_fn,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def post_pr_codex_verdict(
    pr_url: str,
    session_id: str,
    *,
    parse_pr_url: Callable[..., Any],
    verdict_marker_text: Callable[[str], str],
    default_review_comment_checker_fn: Callable[..., Callable[..., bool]],
    verdict_comment_body: Callable[[str], str],
    runtime_comment_poster_fn: Callable[..., None],
    gh_query_error_cls: type[Exception],
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Post the machine-readable codex pass verdict required for auto-merge."""
    return _comment_pr_helpers.post_pr_codex_verdict(
        pr_url,
        session_id,
        parse_pr_url=parse_pr_url,
        verdict_marker_text=verdict_marker_text,
        default_review_comment_checker=default_review_comment_checker_fn,
        verdict_comment_body=verdict_comment_body,
        runtime_comment_poster=runtime_comment_poster_fn,
        gh_query_error_cls=gh_query_error_cls,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def backfill_review_verdicts(
    db: Any,
    *,
    post_pr_codex_verdict_fn: Callable[..., bool],
    log_warning: Callable[[str, str, Exception], None],
    session_limit: int = 50,
    review_refs: tuple[str, ...] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, ...]:
    """Re-post missing codex verdict markers for successful review sessions."""
    return _comment_pr_helpers.backfill_review_verdicts(
        db,
        post_pr_codex_verdict=post_pr_codex_verdict_fn,
        log_warning=log_warning,
        session_limit=session_limit,
        review_refs=review_refs,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )
