"""Comment and PR helper cluster extracted from board_consumer."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.domain.models import OpenPullRequest, OpenPullRequestMatch
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.ready_flow import GitHubBundleView, GitHubRunnerFn
from startupai_controller.ports.status_store import StatusStorePort
from startupai_controller.runtime.wiring import GitHubRuntimeMemo
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig


class CommentMarkerCheckerFn(Protocol):
    """Check whether a marker comment already exists on an issue or PR."""

    def __call__(
        self,
        owner: str,
        repo: str,
        number: int,
        marker: str,
        *,
        gh_runner: GitHubRunnerFn | None = None,
    ) -> bool: ...


class CommentPosterFn(Protocol):
    """Post an issue or pull-request comment through the shell boundary."""

    def __call__(
        self,
        owner: str,
        repo: str,
        number: int,
        body: str,
        *,
        gh_runner: GitHubRunnerFn | None = None,
    ) -> None: ...


class BuildGitHubPortBundleFn(Protocol):
    """Build the runtime GitHub bundle used by comment and PR helpers."""

    def __call__(
        self,
        project_owner: str,
        project_number: int,
        *,
        config: CriticalPathConfig | None = None,
        github_memo: GitHubRuntimeMemo | None = None,
        gh_runner: GitHubRunnerFn | None = None,
    ) -> GitHubBundleView: ...


def post_consumer_claim_comment(
    issue_ref: str,
    session_id: str,
    repo_prefix: str,
    branch_name: str,
    executor: str,
    config: CriticalPathConfig,
    *,
    resolve_issue_coordinates: Callable[
        [str, CriticalPathConfig],
        tuple[str, str, int],
    ],
    consumer_provenance_marker: Callable[..., str],
    default_review_comment_checker: Callable[..., CommentMarkerCheckerFn],
    runtime_comment_poster: CommentPosterFn,
    comment_checker: CommentMarkerCheckerFn | None = None,
    comment_poster: CommentPosterFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> None:
    """Post a deterministic claim provenance marker on the issue."""
    owner, repo, number = resolve_issue_coordinates(issue_ref, config)
    marker = consumer_provenance_marker(
        session_id=session_id,
        issue_ref=issue_ref,
        repo_prefix=repo_prefix,
        branch_name=branch_name,
        executor=executor,
    )
    checker = comment_checker or default_review_comment_checker(gh_runner=gh_runner)
    if checker(owner, repo, number, marker, gh_runner=gh_runner):
        return
    body = "\n".join(
        [
            marker,
            f"Local consumer claimed `{issue_ref}` for `{executor}` execution.",
            f"Branch: `{branch_name}`",
            f"Session: `{session_id}`",
        ]
    )
    poster = comment_poster or runtime_comment_poster
    poster(owner, repo, number, body, gh_runner=gh_runner)


def list_open_pr_candidates(
    owner: str,
    repo: str,
    issue_number: int,
    *,
    build_github_port_bundle: BuildGitHubPortBundleFn,
    open_pr_match_factory: type[OpenPullRequestMatch],
    parse_consumer_provenance: Callable[[str], dict[str, str] | None],
    pr_port: PullRequestPort | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> list[OpenPullRequestMatch]:
    """Return open PRs that reference an issue number in the repository."""
    port = (
        pr_port
        or build_github_port_bundle(
            "",
            0,
            gh_runner=gh_runner,
        ).pull_requests
    )
    payload = port.list_open_prs_for_issue(f"{owner}/{repo}", issue_number)
    matches: list[OpenPullRequestMatch] = []
    for item in payload:
        typed_item: OpenPullRequest = item
        body = typed_item.body
        matches.append(
            open_pr_match_factory(
                url=typed_item.url,
                number=typed_item.number,
                author=typed_item.author,
                body=body,
                branch_name=typed_item.head_ref_name,
                provenance=parse_consumer_provenance(body),
            )
        )
    return matches


def classify_open_pr_candidates(
    issue_ref: str,
    owner: str,
    repo: str,
    issue_number: int,
    automation_config: BoardAutomationConfig,
    *,
    list_open_pr_candidates: Callable[..., list[OpenPullRequestMatch]],
    classify_pr_candidates_pure: Callable[
        ...,
        tuple[str, OpenPullRequestMatch | None, str],
    ],
    expected_branch: str | None = None,
    pr_port: PullRequestPort | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> tuple[str, OpenPullRequestMatch | None, str]:
    """Classify open PRs for an issue as adoptable, ambiguous, non-local, or none."""
    candidates = list_open_pr_candidates(
        owner,
        repo,
        issue_number,
        pr_port=pr_port,
        gh_runner=gh_runner,
    )
    return classify_pr_candidates_pure(
        issue_ref,
        candidates,
        trusted_authors=automation_config.trusted_local_authors,
        expected_branch=expected_branch,
        issue_number=issue_number,
    )


def post_result_comment(
    issue_ref: str,
    result: dict[str, Any],
    session_id: str,
    config: CriticalPathConfig,
    *,
    marker_for: Callable[..., str],
    resolve_issue_coordinates: Callable[
        [str, CriticalPathConfig],
        tuple[str, str, int],
    ],
    normalize_resolution_payload: Callable[[object], dict[str, Any] | None],
    default_review_comment_checker: Callable[..., CommentMarkerCheckerFn],
    runtime_comment_poster: CommentPosterFn,
    comment_checker: CommentMarkerCheckerFn | None = None,
    comment_poster: CommentPosterFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> None:
    """Post a machine-marker result comment on the issue."""
    marker = marker_for("consumer-result", issue_ref)
    owner, repo, number = resolve_issue_coordinates(issue_ref, config)

    checker = comment_checker or default_review_comment_checker(gh_runner=gh_runner)
    outcome = result.get("outcome", "unknown")
    summary = result.get("summary", "No summary provided.")
    tests_run = result.get("tests_run")
    tests_passed = result.get("tests_passed")
    changed_files = result.get("changed_files", [])
    pr_url = result.get("pr_url")
    duration = result.get("duration_seconds")
    resolution = normalize_resolution_payload(result.get("resolution"))

    lines = [
        marker,
        f"**Consumer result**: `{outcome}` (session: `{session_id}`)",
        "",
        f"> {summary}",
    ]
    if tests_run is not None:
        lines.append(f"\nTests: {tests_passed}/{tests_run} passed")
    if changed_files:
        lines.append(f"\nChanged files: {len(changed_files)}")
    if pr_url:
        lines.append(f"\nPR: {pr_url}")
    if resolution is not None:
        lines.append(
            "\nResolution: " f"{resolution['kind']} ({resolution['equivalence_claim']})"
        )
    if duration is not None:
        lines.append(f"\nDuration: {duration:.0f}s")

    body = "\n".join(lines)
    poster = comment_poster or runtime_comment_poster
    poster(owner, repo, number, body, gh_runner=gh_runner)


def post_pr_codex_verdict(
    pr_url: str,
    session_id: str,
    *,
    parse_pr_url: Callable[[str], tuple[str, str, int] | None],
    verdict_marker_text: Callable[[str], str],
    default_review_comment_checker: Callable[..., CommentMarkerCheckerFn],
    verdict_comment_body: Callable[[str], str],
    runtime_comment_poster: CommentPosterFn,
    gh_query_error_cls: type[Exception],
    comment_checker: CommentMarkerCheckerFn | None = None,
    comment_poster: CommentPosterFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> bool:
    """Post the machine-readable codex pass verdict required for auto-merge."""
    parsed = parse_pr_url(pr_url)
    if parsed is None:
        raise gh_query_error_cls(f"Invalid PR URL for codex verdict: {pr_url}")

    owner, repo, pr_number = parsed
    marker = verdict_marker_text(session_id)
    checker = comment_checker or default_review_comment_checker(gh_runner=gh_runner)
    if checker(owner, repo, pr_number, marker, gh_runner=gh_runner):
        return False

    body = verdict_comment_body(session_id)
    poster = comment_poster or runtime_comment_poster
    poster(owner, repo, pr_number, body, gh_runner=gh_runner)
    return True


def backfill_review_verdicts(
    db: StatusStorePort,
    *,
    post_pr_codex_verdict: Callable[..., bool],
    log_warning: Callable[[str, str, Exception], None],
    session_limit: int = 50,
    review_refs: tuple[str, ...] | None = None,
    comment_checker: CommentMarkerCheckerFn | None = None,
    comment_poster: CommentPosterFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> tuple[str, ...]:
    """Re-post missing codex verdict markers for successful review sessions."""
    backfilled: list[str] = []
    scoped_review_refs = set(review_refs or ())
    for session in db.recent_sessions(limit=session_limit):
        if session.status != "success":
            continue
        if session.phase != "review":
            continue
        if not session.pr_url:
            continue
        if scoped_review_refs and session.issue_ref not in scoped_review_refs:
            continue
        try:
            posted = post_pr_codex_verdict(
                session.pr_url,
                session.id,
                comment_checker=comment_checker,
                comment_poster=comment_poster,
                gh_runner=gh_runner,
            )
        except Exception as err:
            log_warning(session.issue_ref, session.id, err)
            continue
        if posted:
            backfilled.append(session.issue_ref)
    return tuple(backfilled)
