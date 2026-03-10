"""GitHub CLI adapter implementing review, PR, and board ports."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
import re
import time
from typing import Any

from startupai_controller.domain.models import (
    CheckObservation,
    IssueContext,
    OpenPullRequest,
    PrGateStatus,
    ReviewSnapshot,
)
from startupai_controller.domain.verdict_policy import (
    verdict_comment_body,
    verdict_marker_text,
)
from startupai_controller.domain.repair_policy import MARKER_PREFIX, marker_for
from startupai_controller.domain.scheduling_policy import (
    snapshot_to_issue_ref as _canonical_snapshot_to_issue_ref,
)
from startupai_controller.promote_ready import BoardInfo
from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    CriticalPathConfig,
    GhQueryError,
    parse_issue_ref,
)
from startupai_controller.adapters.review_state import (
    GitHubReviewStateAdapter,
    _comment_exists,
    _list_project_items_by_status,
    _parse_github_timestamp,
    _query_latest_matching_comment_timestamp,
    _query_latest_non_automation_comment_timestamp,
    build_cycle_board_snapshot,
    clear_cycle_board_snapshot_cache,
    list_issue_comment_bodies,
)
from startupai_controller.adapters.board_mutation import GitHubBoardMutationAdapter
from startupai_controller.adapters.github_transport import (
    _run_gh,
    gh_reason_code,
)
from startupai_controller.adapters.github_types import (
    COPILOT_CODING_AGENT_LOGINS,
    CycleGitHubMemo,
    CodexReviewVerdict,
    LinkedIssue,
    PullRequestViewPayload,
    PullRequestStateProbe as _PullRequestStateProbe,
)
from startupai_controller.domain.repair_policy import parse_pr_url as _parse_pr_url


@dataclass(frozen=True)
class _PullRequestListItem:
    """Minimal PR list payload used to build OpenPullRequest objects."""

    number: int
    url: str
    head_ref_name: str
    is_draft: bool
    body: str
    author: str

_REQUIRED_STATUS_CHECKS_CACHE_TTL_SECONDS = 900
_required_status_checks_ttl_cache: dict[
    tuple[str, str],
    tuple[float, set[str]],
] = {}


def _is_copilot_coding_agent_actor(login: str) -> bool:
    """Return True when actor is a Copilot coding-agent identity."""
    normalized = login.strip().lower()
    return normalized in COPILOT_CODING_AGENT_LOGINS


def _marker_for(kind: str, ref: str) -> str:
    """Compatibility wrapper for the canonical domain marker helper."""
    return marker_for(kind, ref)


def _query_project_item_field(
    issue_ref: str,
    field_name: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Read a single project field value through the adapter-owned mechanism."""
    adapter = GitHubBoardMutationAdapter(
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        gh_runner=gh_runner,
    )
    return adapter._query_project_field_value(issue_ref, field_name)


def _query_single_select_field_option(
    project_id: str,
    field_name: str,
    option_name: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, str]:
    """Resolve a single-select field option through the adapter-owned mechanism."""
    adapter = GitHubBoardMutationAdapter(
        project_owner="",
        project_number=0,
        gh_runner=gh_runner,
    )
    return adapter._query_single_select_field_option(
        project_id,
        field_name,
        option_name,
    )


def _set_text_field(
    project_id: str,
    item_id: str,
    field_name: str,
    value: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set a project text field through the adapter-owned mechanism."""
    adapter = GitHubBoardMutationAdapter(
        project_owner="",
        project_number=0,
        gh_runner=gh_runner,
    )
    field_id = adapter._query_field_id(project_id, field_name)
    adapter._set_project_text_field(
        project_id=project_id,
        item_id=item_id,
        field_id=field_id,
        value=value,
    )


def _set_single_select_field(
    project_id: str,
    item_id: str,
    field_name: str,
    option_name: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set a project single-select field through the adapter-owned mechanism."""
    adapter = GitHubBoardMutationAdapter(
        project_owner="",
        project_number=0,
        gh_runner=gh_runner,
    )
    field_id, option_id = adapter._query_single_select_field_option(
        project_id,
        field_name,
        option_name,
    )
    adapter._set_project_single_select(
        project_id=project_id,
        item_id=item_id,
        field_id=field_id,
        option_id=option_id,
    )


def _set_status_if_changed(
    issue_ref: str,
    from_statuses: set[str],
    to_status: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[bool, str]:
    """Safely transition status through the adapter-owned board mutation path."""
    adapter = GitHubBoardMutationAdapter(
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        gh_runner=gh_runner,
    )
    if board_info_resolver is None:
        info = adapter._query_board_info(issue_ref)
    else:
        info = board_info_resolver(issue_ref, config, project_owner, project_number)

    if info.status not in from_statuses:
        return False, info.status

    if board_mutator is None:
        field_id, option_id = adapter._query_single_select_field_option(
            info.project_id,
            "Status",
            to_status,
        )
        adapter._set_project_single_select(
            project_id=info.project_id,
            item_id=info.item_id,
            field_id=field_id,
            option_id=option_id,
        )
    else:
        board_mutator(info.project_id, info.item_id)

    return True, info.status


def _query_issue_board_info(
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> BoardInfo:
    """Query board identity/status through the adapter-owned mechanism."""
    adapter = GitHubBoardMutationAdapter(
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        gh_runner=gh_runner,
    )
    info = adapter._query_board_info(issue_ref)
    return BoardInfo(
        status=info.status,
        item_id=info.item_id,
        project_id=info.project_id,
    )


def _query_status_field_option(
    project_id: str,
    option_name: str = "Ready",
    *,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, str]:
    """Resolve Status field option IDs through the adapter-owned mechanism."""
    return _query_single_select_field_option(
        project_id,
        "Status",
        option_name,
        gh_runner=gh_runner,
    )


def _set_board_status(
    project_id: str,
    item_id: str,
    field_id: str,
    option_id: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Mutate Status via the adapter-owned project single-select path."""
    adapter = GitHubBoardMutationAdapter(
        project_owner="",
        project_number=0,
        gh_runner=gh_runner,
    )
    adapter._set_project_single_select(
        project_id=project_id,
        item_id=item_id,
        field_id=field_id,
        option_id=option_id,
    )


def _extract_run_id(details_url: str) -> int | None:
    """Extract a GitHub Actions run ID from a details URL when present."""
    match = re.search(r"/actions/runs/(\d+)", details_url)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


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


def _query_latest_marker_timestamp(
    owner: str,
    repo: str,
    number: int,
    marker_prefix: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Return most recent marker timestamp encoded in issue comments."""
    try:
        comments = _run_gh(
            [
                "api",
                f"repos/{owner}/{repo}/issues/{number}/comments",
                "--paginate",
                "-q",
                ".[].body",
            ],
            gh_runner=gh_runner,
        )
    except GhQueryError:
        return None

    pattern = re.compile(
        rf"{re.escape(marker_prefix)}:([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T"
        rf"[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}Z)"
    )
    latest: datetime | None = None
    for match in pattern.finditer(comments):
        raw = match.group(1)
        try:
            ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        if latest is None or ts > latest:
            latest = ts
    return latest


def _query_issue_updated_at(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime:
    """Get issue updated_at timestamp (UTC)."""
    output = _run_gh(
        [
            "api",
            f"repos/{owner}/{repo}/issues/{number}",
            "-q",
            ".updated_at",
        ],
        gh_runner=gh_runner,
    ).strip()
    try:
        return datetime.fromisoformat(output.replace("Z", "+00:00"))
    except ValueError as error:
        raise GhQueryError(
            f"Invalid updated_at for {owner}/{repo}#{number}: {output}"
        ) from error


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
    """Return an issue body using the cycle-local cache."""
    key = (owner, repo, number)
    cached = memo.issue_bodies.get(key)
    if cached is not None:
        return cached
    body = query_issue_body(owner, repo, number, gh_runner=gh_runner)
    memo.issue_bodies[key] = body
    return body


def _query_open_pr_updated_at(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Return updated_at for an open PR, or None when closed/unavailable."""
    try:
        output = _run_gh(
            [
                "api",
                f"repos/{owner}/{repo}/pulls/{pr_number}",
                "--jq",
                "{state: .state, updated_at: .updated_at}",
            ],
            gh_runner=gh_runner,
        )
    except GhQueryError:
        return None

    try:
        payload = json.loads(output)
    except json.JSONDecodeError:
        return None

    if str(payload.get("state") or "").upper() != "OPEN":
        return None
    return _parse_github_timestamp(str(payload.get("updated_at") or ""))


def _is_pr_open(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Return True when a PR exists and is open."""
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


def _query_failed_check_runs(
    owner: str,
    repo: str,
    head_sha: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[str] | None:
    """Query failed check-run names for one commit head SHA."""
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
    """Get the head SHA for one PR, or None on failure."""
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


def _query_latest_wip_activity_timestamp(
    issue_ref: str,
    owner: str,
    repo: str,
    number: int,
    pr_field: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Return latest execution-relevant activity for a WIP issue."""
    candidates: list[datetime] = []

    parsed_pr = _parse_pr_url(pr_field)
    if parsed_pr is not None:
        pr_owner, pr_repo, pr_number = parsed_pr
        pr_ts = _query_open_pr_updated_at(
            pr_owner, pr_repo, pr_number, gh_runner=gh_runner
        )
        if pr_ts is not None:
            candidates.append(pr_ts)

    comment_ts = _query_latest_non_automation_comment_timestamp(
        owner, repo, number, gh_runner=gh_runner
    )
    if comment_ts is not None:
        candidates.append(comment_ts)

    baseline_ts = _query_latest_matching_comment_timestamp(
        owner,
        repo,
        number,
        (
            f"{MARKER_PREFIX}:claim-ready:{issue_ref}",
            f"{MARKER_PREFIX}:dispatch-agent:{issue_ref}",
        ),
        gh_runner=gh_runner,
    )
    if baseline_ts is not None:
        candidates.append(baseline_ts)

    return max(candidates) if candidates else None


def _query_issue_assignees(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Fetch current assignee logins for an issue."""
    output = _run_gh(
        [
            "api",
            f"repos/{owner}/{repo}/issues/{number}",
            "-q",
            ".assignees[].login",
        ],
        gh_runner=gh_runner,
    )
    return [line.strip() for line in output.splitlines() if line.strip()]


def _set_issue_assignees(
    owner: str,
    repo: str,
    number: int,
    assignees: list[str],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set issue assignees explicitly."""
    args = [
        "api",
        f"repos/{owner}/{repo}/issues/{number}",
        "-X",
        "PATCH",
    ]
    for login in assignees:
        args.extend(["-f", f"assignees[]={login}"])
    _run_gh(args, gh_runner=gh_runner)


def _pull_request_state_probe_from_payload(
    payload: PullRequestViewPayload,
) -> _PullRequestStateProbe:
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
    return _PullRequestStateProbe(
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


def _review_state_digest_from_probe(probe: _PullRequestStateProbe) -> str:
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
            result = (
                "pass"
                if state == "success"
                else ("fail" if state in {"error", "failure"} else "pending")
            )
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
        "checks": sorted(
            (name, result) for name, (_ts, result) in latest_checks.items()
        ),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


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


def _repo_prefix_for_slug(
    repo_slug: str,
    config: CriticalPathConfig,
) -> str | None:
    """Return the configured issue prefix for one repo slug."""
    for prefix, configured_slug in config.issue_prefixes.items():
        if configured_slug == repo_slug:
            return prefix
    return None


def _repo_to_prefix(
    full_repo: str,
    config: CriticalPathConfig,
) -> str | None:
    """Compatibility helper returning the configured prefix for one repo slug."""
    return _repo_prefix_for_slug(full_repo, config)


def _issue_ref_to_repo_parts(
    issue_ref: str,
    config: CriticalPathConfig,
) -> tuple[str, str, int]:
    """Parse issue_ref and return owner, repo, and number."""
    parsed = parse_issue_ref(issue_ref)
    repo_slug = config.issue_prefixes.get(parsed.prefix)
    if not repo_slug:
        raise ConfigError(f"Missing repo mapping for prefix '{parsed.prefix}'.")
    owner, repo = repo_slug.split("/", maxsplit=1)
    return owner, repo, parsed.number


def _snapshot_to_issue_ref(
    snapshot: _ProjectItemSnapshot,
    config: CriticalPathConfig,
) -> str | None:
    """Compatibility wrapper for the canonical domain issue-ref normalizer."""
    return _canonical_snapshot_to_issue_ref(snapshot.issue_ref, config.issue_prefixes)


def latest_codex_verdict_from_payload(
    payload: PullRequestViewPayload,
    *,
    trusted_actors: set[str] | frozenset[str] | None = None,
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
    """Return True when Copilot has submitted an approved/commented review."""
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
            name = str(check.get("name") or "")
            timestamp = str(check.get("completedAt") or check.get("startedAt") or "")
            status = str(check.get("status") or "").lower()
            conclusion = str(check.get("conclusion") or "").lower()
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
            previous = latest.get(name)
            if previous is None or timestamp >= previous[0]:
                latest[name] = (timestamp, observation)
        elif typename == "StatusContext":
            name = str(check.get("context") or "")
            timestamp = str(check.get("startedAt") or "")
            state = str(check.get("state") or "").lower()
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
            previous = latest.get(name)
            if previous is None or timestamp >= previous[0]:
                latest[name] = (timestamp, observation)

    passed: set[str] = set()
    failed: set[str] = set()
    pending: set[str] = set()
    cancelled: set[str] = set()
    for context in required:
        if context not in latest:
            pending.add(context)
            continue
        _timestamp, observation = latest[context]
        if observation.result == "pass":
            passed.add(context)
        elif observation.result == "fail":
            failed.add(context)
        elif observation.result == "cancelled":
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


def review_state_digest_from_probe(probe: _PullRequestStateProbe) -> str:
    """Public wrapper for the review-state digest builder."""
    return _review_state_digest_from_probe(probe)


def review_state_digest_from_payload(payload: PullRequestViewPayload) -> str:
    """Return a stable review-state digest from an expanded PR payload."""
    return review_state_digest_from_probe(_pull_request_state_probe_from_payload(payload))


class GitHubCliAdapter(GitHubReviewStateAdapter):
    """Adapter wrapping gh CLI interactions behind port protocols."""

    def _pull_request_list_items(self, payload: object) -> list[_PullRequestListItem]:
        """Normalize gh `pr list` output into typed list items."""
        results: list[_PullRequestListItem] = []
        for item in payload or []:
            if not isinstance(item, dict):
                continue
            number = item.get("number")
            if not isinstance(number, int):
                continue
            results.append(
                _PullRequestListItem(
                    number=number,
                    url=str(item.get("url") or ""),
                    head_ref_name=str(item.get("headRefName") or ""),
                    is_draft=bool(item.get("isDraft", False)),
                    body=str(item.get("body") or ""),
                    author=str(((item.get("author") or {}).get("login") or "")).strip().lower(),
                )
            )
        return results

    def _to_open_pull_request(self, item: _PullRequestListItem) -> OpenPullRequest:
        """Convert a typed list payload into the domain PR type."""
        return OpenPullRequest(
            number=item.number,
            url=item.url,
            head_ref_name=item.head_ref_name,
            is_draft=item.is_draft,
            body=item.body,
            author=item.author,
        )

    def _query_pull_request_view_payload(
        self,
        pr_repo: str,
        pr_number: int,
    ) -> PullRequestViewPayload:
        """Return one expanded PR payload directly from gh."""
        payload = self._gh_json(
            [
                "pr",
                "view",
                str(pr_number),
                "--repo",
                pr_repo,
                "--json",
                (
                    "author,body,comments,reviews,state,isDraft,mergeStateStatus,"
                    "mergeable,baseRefName,autoMergeRequest,statusCheckRollup"
                ),
            ],
            error_message=f"Failed querying PR {pr_repo}#{pr_number}: invalid JSON.",
        )
        if not isinstance(payload, dict):
            raise GhQueryError(
                f"Failed querying PR {pr_repo}#{pr_number}: pull request not found."
            )
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

    def _query_closing_issue_refs(self, pr_repo: str, pr_number: int) -> tuple[str, ...]:
        """Return linked issue refs for one PR using the configured repo-prefix map."""
        config = self._require_config()
        pr_owner, pr_repo_name = pr_repo.split("/", maxsplit=1)
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
        payload = self._graphql(
            query,
            fields=[
                "-f",
                f"owner={pr_owner}",
                "-f",
                f"repo={pr_repo_name}",
                "-F",
                f"number={pr_number}",
            ],
        )
        nodes = (
            payload.get("data", {})
            .get("repository", {})
            .get("pullRequest", {})
            .get("closingIssuesReferences", {})
            .get("nodes", [])
        )
        refs: list[str] = []
        for node in nodes:
            if not isinstance(node, dict):
                continue
            issue_number = node.get("number")
            repo_with_owner = (node.get("repository") or {}).get("nameWithOwner", "")
            if not issue_number or not repo_with_owner:
                continue
            prefix = _repo_prefix_for_slug(repo_with_owner, config)
            if prefix is None:
                continue
            refs.append(f"{prefix}#{issue_number}")
        return tuple(refs)

    def _query_required_status_checks(
        self,
        pr_repo: str,
        base_ref_name: str = "main",
    ) -> set[str]:
        """Query required status checks directly from branch protection."""
        key = (pr_repo, base_ref_name)
        cached = self._github_memo.required_status_checks.get(key)
        if cached is not None:
            return set(cached)
        owner, repo = pr_repo.split("/", maxsplit=1)
        payload = self._gh_json(
            [
                "api",
                f"repos/{owner}/{repo}/branches/{base_ref_name}/protection/required_status_checks",
            ],
            error_message=(
                f"Failed parsing branch protection for {pr_repo}:{base_ref_name}."
            ),
        )
        if not isinstance(payload, dict):
            raise GhQueryError(
                f"Failed querying branch protection for {pr_repo}:{base_ref_name}."
            )
        required: set[str] = set()
        for context in payload.get("contexts", []) or []:
            if isinstance(context, str) and context:
                required.add(context)
        for check in payload.get("checks", []) or []:
            if isinstance(check, dict):
                name = check.get("context")
                if isinstance(name, str) and name:
                    required.add(name)
        self._github_memo.required_status_checks[key] = set(required)
        return set(required)

    def _query_pull_request_view_payloads(
        self,
        pr_repo: str,
        pr_numbers: Sequence[int],
    ) -> dict[int, PullRequestViewPayload]:
        """Return expanded PR payloads for a bounded set of PR numbers."""
        if "/" not in pr_repo:
            raise ValueError(f"pr_repo must be owner/repo, got '{pr_repo}'.")
        owner, repo = pr_repo.split("/", maxsplit=1)
        numbers = tuple(sorted({int(number) for number in pr_numbers}))
        if not numbers:
            return {}
        if len(numbers) == 1:
            number = numbers[0]
            return {number: self._query_pull_request_view_payload(pr_repo, number)}

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
        payload = self._graphql(
            query,
            fields=[
                "-f",
                f"owner={owner}",
                "-f",
                f"repo={repo}",
            ],
        )
        repository = (payload.get("data") or {}).get("repository") or {}
        results: dict[int, PullRequestViewPayload] = {}
        for number in numbers:
            node = repository.get(f"pr_{number}")
            if not isinstance(node, dict):
                continue
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
            results[number] = PullRequestViewPayload(
                pr_repo=pr_repo,
                pr_number=number,
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
        return results

    def _memoized_pull_request_view_payloads(
        self,
        pr_repo: str,
        pr_numbers: Sequence[int],
    ) -> dict[int, PullRequestViewPayload]:
        """Return expanded PR payloads using cycle-local memoization."""
        numbers = tuple(sorted({int(number) for number in pr_numbers}))
        missing = [
            number
            for number in numbers
            if (pr_repo, number) not in self._github_memo.review_pull_requests
        ]
        if missing:
            fetched = self._query_pull_request_view_payloads(pr_repo, tuple(missing))
            for number, payload in fetched.items():
                self._github_memo.review_pull_requests[(pr_repo, number)] = payload
        return {
            number: self._github_memo.review_pull_requests[(pr_repo, number)]
            for number in numbers
            if (pr_repo, number) in self._github_memo.review_pull_requests
        }

    def _query_pull_request_state_probes(
        self,
        pr_repo: str,
        pr_numbers: Sequence[int],
    ) -> dict[int, _PullRequestStateProbe]:
        """Return lightweight PR probes for digest-based review scheduling."""
        if "/" not in pr_repo:
            raise ValueError(f"pr_repo must be owner/repo, got '{pr_repo}'.")
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
        nodes { submittedAt }
      }
      comments(last: 1) {
        nodes { createdAt }
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
        payload = self._graphql(
            query,
            fields=[
                "-f",
                f"owner={owner}",
                "-f",
                f"repo={repo}",
            ],
        )
        repository = (payload.get("data") or {}).get("repository") or {}
        results: dict[int, _PullRequestStateProbe] = {}
        for number in numbers:
            node = repository.get(f"pr_{number}")
            if not isinstance(node, dict):
                continue
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
            results[number] = _PullRequestStateProbe(
                pr_repo=pr_repo,
                pr_number=number,
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
        return results

    def _memoized_pull_request_state_probes(
        self,
        pr_repo: str,
        pr_numbers: Sequence[int],
    ) -> dict[int, _PullRequestStateProbe]:
        """Return lightweight PR probes using cycle-local memoization."""
        numbers = tuple(sorted({int(number) for number in pr_numbers}))
        missing = [
            number
            for number in numbers
            if (pr_repo, number) not in self._github_memo.review_state_probes
        ]
        if missing:
            fetched = self._query_pull_request_state_probes(pr_repo, tuple(missing))
            for number, payload in fetched.items():
                self._github_memo.review_state_probes[(pr_repo, number)] = payload
        return {
            number: self._github_memo.review_state_probes[(pr_repo, number)]
            for number in numbers
            if (pr_repo, number) in self._github_memo.review_state_probes
        }

    def _post_issue_comment(self, owner: str, repo: str, number: int, body: str) -> None:
        """Post a comment on a GitHub issue or PR and update the memo cache."""
        _run_gh(
            [
                "api",
                f"repos/{owner}/{repo}/issues/{number}/comments",
                "-f",
                f"body={body}",
            ],
            gh_runner=self._gh_runner,
        )
        key = (owner, repo, number)
        cached = self._github_memo.issue_comment_bodies.get(key)
        if cached is not None:
            self._github_memo.issue_comment_bodies[key] = [*cached, body]


    # -- PullRequestPort methods --

    def list_open_prs(self, repo: str) -> list[OpenPullRequest]:
        payload = self._gh_json(
            [
                "pr",
                "list",
                "--repo",
                repo,
                "--state",
                "open",
                "--limit",
                "100",
                "--json",
                "number,url,headRefName,isDraft,body,author",
            ],
            error_message=f"Failed querying open PRs for {repo}: invalid JSON.",
        )
        return [
            self._to_open_pull_request(item)
            for item in self._pull_request_list_items(payload)
        ]

    def get_pull_request(self, repo: str, number: int) -> OpenPullRequest | None:
        try:
            payload = self._query_pull_request_view_payload(repo, number)
        except Exception:
            return None
        return OpenPullRequest(
            number=number,
            url=payload.url,
            head_ref_name=payload.head_ref_name,
            is_draft=payload.is_draft,
            body=payload.body,
            author=payload.author,
        )

    def linked_issue_refs(self, pr_repo: str, pr_number: int) -> tuple[str, ...]:
        return self._query_closing_issue_refs(pr_repo, pr_number)

    def has_copilot_review_signal(self, pr_repo: str, pr_number: int) -> bool:
        payload = self._query_pull_request_view_payload(pr_repo, pr_number)
        return has_copilot_review_signal_from_payload(payload)

    def get_gate_status(self, pr_repo: str, pr_number: int) -> PrGateStatus:
        payload = self._query_pull_request_view_payload(pr_repo, pr_number)
        required = self._query_required_status_checks(
            pr_repo,
            payload.base_ref_name or "main",
        )
        return build_pr_gate_status_from_payload(payload, required=required)

    def required_status_checks(
        self, pr_repo: str, base_ref_name: str = "main"
    ) -> set[str]:
        return self._query_required_status_checks(pr_repo, base_ref_name)

    def list_open_prs_for_issue(
        self, repo: str, issue_number: int
    ) -> list[OpenPullRequest]:
        payload = self._gh_json(
            [
                "pr",
                "list",
                "--repo",
                repo,
                "--state",
                "open",
                "--search",
                f"Closes #{issue_number}",
                "--json",
                "number,url,headRefName,isDraft,body,author",
            ],
            error_message=f"Failed querying open PRs for {repo}: invalid JSON.",
        )
        return [
            self._to_open_pull_request(item)
            for item in self._pull_request_list_items(payload)
        ]

    def enable_automerge(
        self, pr_repo: str, pr_number: int, *, delete_branch: bool = False
    ) -> str:
        args = ["pr", "merge", str(pr_number), "--repo", pr_repo, "--auto", "--squash"]
        if delete_branch:
            args.append("--delete-branch")
        _run_gh(
            args,
            gh_runner=self._gh_runner,
            operation_type="automerge",
        )
        for _attempt in range(3):
            time.sleep(1.0)
            try:
                payload = self._gh_json(
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
                if isinstance(payload, dict) and payload.get("autoMergeRequest") is not None:
                    return "confirmed"
            except GhQueryError:
                continue
        return "pending"

    def rerun_failed_check(
        self, pr_repo: str, check_name: str, run_id: int
    ) -> bool:
        del check_name  # run id is the actual rerun handle
        try:
            _run_gh(
                ["run", "rerun", str(run_id), "--repo", pr_repo],
                gh_runner=self._gh_runner,
                operation_type="check_rerun",
            )
            return True
        except Exception:
            return False

    def update_branch(self, pr_repo: str, pr_number: int) -> None:
        _run_gh(
            ["pr", "update-branch", str(pr_number), "--repo", pr_repo],
            gh_runner=self._gh_runner,
            operation_type="mutation",
        )

    def review_state_digests(
        self, pr_refs: list[tuple[str, int]]
    ) -> dict[tuple[str, int], str]:
        digests: dict[tuple[str, int], str] = {}
        numbers_by_repo: dict[str, list[int]] = {}
        for pr_repo, pr_number in pr_refs:
            numbers_by_repo.setdefault(pr_repo, []).append(pr_number)

        for pr_repo, pr_numbers in sorted(numbers_by_repo.items()):
            probes = self._memoized_pull_request_state_probes(
                pr_repo,
                tuple(sorted(set(pr_numbers))),
            )
            for pr_number, probe in probes.items():
                digests[(pr_repo, pr_number)] = _review_state_digest_from_probe(probe)
        return digests

    def review_snapshots(
        self,
        review_refs_by_pr: dict[tuple[str, int], tuple[str, ...]],
        *,
        trusted_codex_actors: frozenset[str],
    ) -> dict[tuple[str, int], ReviewSnapshot]:
        snapshots: dict[tuple[str, int], ReviewSnapshot] = {}
        numbers_by_repo: dict[str, list[int]] = {}
        for pr_repo, pr_number in review_refs_by_pr:
            numbers_by_repo.setdefault(pr_repo, []).append(pr_number)

        for pr_repo, pr_numbers in sorted(numbers_by_repo.items()):
            payloads = self._memoized_pull_request_view_payloads(
                pr_repo,
                tuple(sorted(set(pr_numbers))),
            )
            for pr_number in sorted(set(pr_numbers)):
                payload = payloads.get(pr_number)
                if payload is None:
                    from startupai_controller.validate_critical_path_promotion import (
                        GhQueryError,
                    )

                    raise GhQueryError(
                        f"Failed querying PR {pr_repo}#{pr_number}: pull request not found."
                    )
                required_checks = self._query_required_status_checks(
                    pr_repo,
                    payload.base_ref_name or "main",
                )
                snapshots[(pr_repo, pr_number)] = _build_review_snapshot_from_payload(
                    pr_repo=pr_repo,
                    pr_number=pr_number,
                    review_refs=review_refs_by_pr[(pr_repo, pr_number)],
                    pr_payload=payload,
                    trusted_codex_actors=trusted_codex_actors,
                    required_checks=required_checks,
                )
        return snapshots

    def post_codex_verdict_if_missing(self, pr_url: str, session_id: str) -> bool:
        parsed = _parse_pr_url(pr_url)
        if parsed is None:
            from startupai_controller.validate_critical_path_promotion import GhQueryError

            raise GhQueryError(f"Invalid PR URL for codex verdict: {pr_url}")
        owner, repo, pr_number = parsed
        marker = verdict_marker_text(session_id)
        if self._comment_exists(owner, repo, pr_number, marker):
            return False
        body = verdict_comment_body(session_id)
        self._post_issue_comment(owner, repo, pr_number, body)
        return True

    # -- IssueContextPort methods --

    def get_issue_context(self, owner: str, repo: str, number: int) -> IssueContext:
        output = _run_gh(
            [
                "api",
                f"repos/{owner}/{repo}/issues/{number}",
                "--jq",
                '{title: .title, body: .body, labels: [.labels[].name], updated_at: .updated_at}',
            ],
            gh_runner=self._gh_runner,
        )
        payload = json.loads(output)
        labels = payload.get("labels")
        if not isinstance(labels, list):
            labels = []
        return IssueContext(
            title=str(payload.get("title") or ""),
            body=str(payload.get("body") or ""),
            labels=tuple(str(label) for label in labels if str(label)),
            updated_at=str(payload.get("updated_at") or ""),
        )

def _list_project_items(
    project_owner: str,
    project_number: int,
    *,
    statuses: set[str] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[_ProjectItemSnapshot]:
    """List issue-backed project items with richer field snapshots."""
    query = """
query($owner: String!, $number: Int!, $cursor: String) {
  organization(login: $owner) {
    projectV2(number: $number) {
      id
      items(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          statusField: fieldValueByName(name: "Status") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          executorField: fieldValueByName(name: "Executor") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          handoffField: fieldValueByName(name: "Handoff To") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          priorityField: fieldValueByName(name: "Priority") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          sprintField: fieldValueByName(name: "Sprint") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          agentField: fieldValueByName(name: "Agent") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          ownerField: fieldValueByName(name: "Owner") {
            ... on ProjectV2ItemFieldTextValue { text }
          }
          content {
            ... on Issue {
              number
              title
              body
              repository {
                name
                nameWithOwner
                owner { login }
              }
            }
          }
        }
      }
    }
  }
}
"""

    items: list[_ProjectItemSnapshot] = []
    cursor = ""
    has_next = True

    while has_next:
        gh_args = [
            "api",
            "graphql",
            "-f",
            f"query={query}",
            "-f",
            f"owner={project_owner}",
            "-F",
            f"number={project_number}",
        ]
        gh_args.extend(["-f", f"cursor={cursor}" if cursor else "cursor="])
        output = _run_gh(gh_args, gh_runner=gh_runner)

        try:
            payload = json.loads(output)
        except json.JSONDecodeError as error:
            raise GhQueryError("Failed listing project items: invalid JSON.") from error

        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            messages = [
                err.get("message", "unknown GraphQL error")
                for err in errors
                if isinstance(err, dict)
            ]
            joined = "; ".join(messages) if messages else "unknown GraphQL error"
            raise GhQueryError(f"Failed listing project items: {joined}")

        project_data = payload.get("data", {}).get("organization", {}).get("projectV2", {})
        project_id = str(project_data.get("id") or "")
        items_data = project_data.get("items", {})
        page_info = items_data.get("pageInfo", {})
        has_next = bool(page_info.get("hasNextPage", False))
        cursor = str(page_info.get("endCursor") or "")

        for node in items_data.get("nodes", []):
            status = str((node.get("statusField") or {}).get("name") or "")
            if statuses is not None and status not in statuses:
                continue
            content = node.get("content") or {}
            issue_number = content.get("number")
            repo = content.get("repository") or {}
            repo_with_owner = str(repo.get("nameWithOwner") or "")
            repo_name = str(repo.get("name") or "")
            repo_owner = str((repo.get("owner") or {}).get("login") or "")
            if not issue_number or not repo_with_owner:
                continue
            items.append(
                _ProjectItemSnapshot(
                    issue_ref=f"{repo_with_owner}#{issue_number}",
                    status=status,
                    executor=str((node.get("executorField") or {}).get("name") or ""),
                    handoff_to=str((node.get("handoffField") or {}).get("name") or ""),
                    priority=str((node.get("priorityField") or {}).get("name") or ""),
                    item_id=str(node.get("id") or ""),
                    project_id=project_id,
                    sprint=str((node.get("sprintField") or {}).get("name") or ""),
                    agent=str((node.get("agentField") or {}).get("name") or ""),
                    owner_field=str((node.get("ownerField") or {}).get("text") or ""),
                    title=str(content.get("title") or ""),
                    body=str(content.get("body") or ""),
                    repo_slug=repo_with_owner,
                    repo_name=repo_name,
                    repo_owner=repo_owner,
                    issue_number=int(issue_number),
                )
            )
    return items


def query_open_pull_requests(
    pr_repo: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[OpenPullRequest]:
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
    adapter = GitHubCliAdapter(project_owner="", project_number=0, gh_runner=gh_runner)
    return adapter.list_open_prs(pr_repo)


def _post_comment(
    owner: str,
    repo: str,
    number: int,
    body: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility wrapper implemented on the adapter-owned comment path."""
    adapter = GitHubCliAdapter(project_owner="", project_number=0, gh_runner=gh_runner)
    adapter._post_issue_comment(owner, repo, number, body)


def query_pull_request_view_payload(
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> PullRequestViewPayload:
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
    adapter = GitHubCliAdapter(project_owner="", project_number=0, gh_runner=gh_runner)
    return adapter._query_pull_request_view_payload(pr_repo, pr_number)


def query_pull_request_view_payloads(
    pr_repo: str,
    pr_numbers: Sequence[int],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[int, PullRequestViewPayload]:
    """Compatibility wrapper for batched PR payload reads."""
    adapter = GitHubCliAdapter(project_owner="", project_number=0, gh_runner=gh_runner)
    return adapter._query_pull_request_view_payloads(pr_repo, pr_numbers)


def memoized_query_pull_request_view_payloads(
    memo: CycleGitHubMemo,
    pr_repo: str,
    pr_numbers: Sequence[int],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[int, PullRequestViewPayload]:
    """Compatibility wrapper for memoized batched PR payload reads."""
    adapter = GitHubCliAdapter(
        project_owner="",
        project_number=0,
        github_memo=memo,
        gh_runner=gh_runner,
    )
    return adapter._memoized_pull_request_view_payloads(pr_repo, pr_numbers)


def query_pull_request_state_probes(
    pr_repo: str,
    pr_numbers: Sequence[int],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[int, _PullRequestStateProbe]:
    """Compatibility wrapper for batched PR state-probe reads."""
    adapter = GitHubCliAdapter(project_owner="", project_number=0, gh_runner=gh_runner)
    return adapter._query_pull_request_state_probes(pr_repo, pr_numbers)


def memoized_query_pull_request_state_probes(
    memo: CycleGitHubMemo,
    pr_repo: str,
    pr_numbers: Sequence[int],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[int, _PullRequestStateProbe]:
    """Compatibility wrapper for memoized batched PR state-probe reads."""
    adapter = GitHubCliAdapter(
        project_owner="",
        project_number=0,
        github_memo=memo,
        gh_runner=gh_runner,
    )
    return adapter._memoized_pull_request_state_probes(pr_repo, pr_numbers)


def query_latest_codex_verdict(
    pr_repo: str,
    pr_number: int,
    *,
    trusted_actors: set[str] | frozenset[str] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> CodexReviewVerdict | None:
    """Compatibility wrapper implemented on the adapter-owned PR payload path."""
    adapter = GitHubCliAdapter(project_owner="", project_number=0, gh_runner=gh_runner)
    payload = adapter._query_pull_request_view_payload(pr_repo, pr_number)
    return latest_codex_verdict_from_payload(
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

    adapter = GitHubCliAdapter(project_owner="", project_number=0, gh_runner=gh_runner)
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
    adapter = GitHubCliAdapter(
        project_owner="",
        project_number=0,
        config=config,
        gh_runner=gh_runner,
    )
    refs = adapter._query_closing_issue_refs(f"{pr_owner}/{pr_repo}", pr_number)
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
    comment: str,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
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
    adapter = GitHubCliAdapter(project_owner="", project_number=0, gh_runner=gh_runner)
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
            if isinstance(payload, dict) and payload.get("autoMergeRequest") is not None:
                return "confirmed"
        except GhQueryError:
            continue
    return "pending"


def _codex_gate_from_payload(
    pr_repo: str,
    pr_number: int,
    *,
    review_refs: tuple[str, ...],
    verdict: CodexReviewVerdict | None,
) -> tuple[int, str]:
    if not review_refs:
        return 0, (
            f"{pr_repo}#{pr_number}: codex gate not applicable "
            "(linked issues not in Review)"
        )
    if verdict is None:
        return 2, (
            f"{pr_repo}#{pr_number}: missing codex verdict marker "
            "(codex-review: pass|fail from trusted actor)"
        )
    if verdict.decision == "pass":
        return 0, (
            f"{pr_repo}#{pr_number}: codex-review=pass "
            f"(source={verdict.source}, actor={verdict.actor})"
        )
    return 2, (
        f"{pr_repo}#{pr_number}: codex-review=fail "
        f"(route={verdict.route}, source={verdict.source}, actor={verdict.actor})"
    )


def _build_review_snapshot_from_payload(
    *,
    pr_repo: str,
    pr_number: int,
    review_refs: tuple[str, ...],
    pr_payload: PullRequestViewPayload,
    trusted_codex_actors: frozenset[str],
    required_checks: set[str],
) -> ReviewSnapshot:
    copilot_review_present = has_copilot_review_signal_from_payload(pr_payload)
    verdict = latest_codex_verdict_from_payload(
        pr_payload,
        trusted_actors=trusted_codex_actors,
    )
    codex_gate_code, codex_gate_message = _codex_gate_from_payload(
        pr_repo,
        pr_number,
        review_refs=review_refs,
        verdict=verdict,
    )
    gate_status = build_pr_gate_status_from_payload(
        pr_payload,
        required=required_checks,
    )

    rescue_checks = tuple(sorted(gate_status.required))
    rescue_passed: set[str] = set()
    rescue_pending: set[str] = set()
    rescue_failed: set[str] = set()
    rescue_cancelled: set[str] = set()
    rescue_missing: set[str] = set()
    for name in rescue_checks:
        observation = gate_status.checks.get(name)
        if observation is None:
            rescue_missing.add(name)
            continue
        if observation.result == "pass":
            rescue_passed.add(name)
        elif observation.result == "cancelled":
            rescue_cancelled.add(name)
        elif observation.result == "fail":
            rescue_failed.add(name)
        else:
            rescue_pending.add(name)

    return ReviewSnapshot(
        pr_repo=pr_repo,
        pr_number=pr_number,
        review_refs=review_refs,
        pr_author=pr_payload.author,
        pr_body=pr_payload.body,
        pr_comment_bodies=tuple(
            str(comment.get("body") or "") for comment in pr_payload.comments
        ),
        copilot_review_present=copilot_review_present,
        codex_verdict=verdict,
        codex_gate_code=codex_gate_code,
        codex_gate_message=codex_gate_message,
        gate_status=gate_status,
        rescue_checks=rescue_checks,
        rescue_passed=rescue_passed,
        rescue_pending=rescue_pending,
        rescue_failed=rescue_failed,
        rescue_cancelled=rescue_cancelled,
        rescue_missing=rescue_missing,
    )
