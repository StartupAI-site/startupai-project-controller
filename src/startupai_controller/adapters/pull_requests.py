"""GitHub pull-request adapter implementing PR/review capability ports."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
import json
import time
from typing import cast

import startupai_controller.adapters.pull_request_batch_queries as _pull_request_batch_queries
import startupai_controller.adapters.pull_request_board_helpers as _pull_request_board_helpers
import startupai_controller.adapters.pull_request_query_helpers as _pull_request_query_helpers
from startupai_controller.adapters.github_base import GitHubAdapterBase
from startupai_controller.adapters import (
    pull_request_review_state as _review_state_builders,
)
from startupai_controller.domain.models import (
    IssueContext,
    OpenPullRequest,
    PrGateStatus,
    ReviewSnapshot,
)
from startupai_controller.domain.verdict_policy import (
    verdict_comment_body,
    verdict_marker_text,
)
from startupai_controller.domain.repair_policy import marker_for
from startupai_controller.domain.scheduling_policy import (
    snapshot_to_issue_ref as _canonical_snapshot_to_issue_ref,
)
from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    CriticalPathConfig,
    GhQueryError,
    parse_issue_ref,
)
from startupai_controller.adapters.review_state import (
    _comment_exists,
)
from startupai_controller.adapters.github_transport import (
    _run_gh,
)
from startupai_controller.adapters.github_types import (
    COPILOT_CODING_AGENT_LOGINS,
    CycleGitHubMemo,
    CodexReviewVerdict,
    GitHubCommentNode,
    GitHubReviewNode,
    GitHubStatusCheckRollupNode,
    LinkedIssue,
    PullRequestViewPayload,
    PullRequestStateProbe as _PullRequestStateProbe,
    _ProjectItemSnapshot,
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


_query_project_item_field = _pull_request_board_helpers._query_project_item_field
_query_single_select_field_option = (
    _pull_request_board_helpers._query_single_select_field_option
)
_set_text_field = _pull_request_board_helpers._set_text_field
_set_single_select_field = _pull_request_board_helpers._set_single_select_field
_set_status_if_changed = _pull_request_board_helpers._set_status_if_changed
_query_issue_board_info = _pull_request_board_helpers._query_issue_board_info
_query_status_field_option = _pull_request_board_helpers._query_status_field_option
_set_board_status = _pull_request_board_helpers._set_board_status
_extract_run_id = _pull_request_query_helpers._extract_run_id
_normalize_graphql_rollup_node = (
    _pull_request_query_helpers._normalize_graphql_rollup_node
)
_latest_node_timestamp = _pull_request_query_helpers._latest_node_timestamp
_query_latest_marker_timestamp = (
    _pull_request_query_helpers._query_latest_marker_timestamp
)
_query_issue_updated_at = _pull_request_query_helpers._query_issue_updated_at
query_issue_body = _pull_request_query_helpers.query_issue_body
memoized_query_issue_body = _pull_request_query_helpers.memoized_query_issue_body
_query_open_pr_updated_at = _pull_request_query_helpers._query_open_pr_updated_at
_is_pr_open = _pull_request_query_helpers._is_pr_open
_query_failed_check_runs = _pull_request_query_helpers._query_failed_check_runs
_query_pr_head_sha = _pull_request_query_helpers._query_pr_head_sha
_query_latest_wip_activity_timestamp = (
    _pull_request_query_helpers._query_latest_wip_activity_timestamp
)
_query_issue_assignees = _pull_request_query_helpers._query_issue_assignees
_set_issue_assignees = _pull_request_query_helpers._set_issue_assignees


_pull_request_state_probe_from_payload = (
    _review_state_builders.pull_request_state_probe_from_payload
)
_review_state_digest_from_probe = _review_state_builders.review_state_digest_from_probe


_parse_codex_verdict_from_text = _review_state_builders.parse_codex_verdict_from_text


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
    return _review_state_builders.latest_codex_verdict_from_payload(
        payload,
        trusted_actors=trusted_actors,
    )


def has_copilot_review_signal_from_payload(payload: PullRequestViewPayload) -> bool:
    """Return True when Copilot has submitted an approved/commented review."""
    return _review_state_builders.has_copilot_review_signal_from_payload(payload)


def build_pr_gate_status_from_payload(
    payload: PullRequestViewPayload,
    *,
    required: set[str],
) -> PrGateStatus:
    """Build gate readiness from one expanded PR payload and required checks."""
    return _review_state_builders.build_pr_gate_status_from_payload(
        payload,
        required=required,
    )


def review_state_digest_from_probe(probe: _PullRequestStateProbe) -> str:
    """Public wrapper for the review-state digest builder."""
    return _review_state_builders.review_state_digest_from_probe(probe)


def review_state_digest_from_payload(payload: PullRequestViewPayload) -> str:
    """Return a stable review-state digest from an expanded PR payload."""
    return _review_state_builders.review_state_digest_from_payload(payload)


class GitHubPullRequestAdapter(GitHubAdapterBase):
    """Adapter wrapping PR/review GitHub CLI interactions behind port protocols."""

    def _pull_request_list_items(self, payload: object) -> list[_PullRequestListItem]:
        """Normalize gh `pr list` output into typed list items."""
        results: list[_PullRequestListItem] = []
        for item in payload if isinstance(payload, list) else []:
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
                    author=str(((item.get("author") or {}).get("login") or ""))
                    .strip()
                    .lower(),
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
                    "mergeable,baseRefName,headRefName,mergedAt,url,"
                    "autoMergeRequest,statusCheckRollup"
                ),
            ],
            error_message=f"Failed querying PR {pr_repo}#{pr_number}: invalid JSON.",
        )
        if not isinstance(payload, dict):
            raise GhQueryError(
                f"Failed querying PR {pr_repo}#{pr_number}: pull request not found."
            )
        comments = tuple(
            item
            for item in (payload.get("comments", []) or [])
            if isinstance(item, dict)
        )
        reviews = tuple(
            item
            for item in (payload.get("reviews", []) or [])
            if isinstance(item, dict)
        )
        status_check_rollup = tuple(
            item
            for item in (payload.get("statusCheckRollup", []) or [])
            if isinstance(item, dict)
        )
        return PullRequestViewPayload(
            pr_repo=pr_repo,
            pr_number=pr_number,
            url=str(payload.get("url") or ""),
            head_ref_name=str(payload.get("headRefName") or ""),
            author=str(((payload.get("author") or {}).get("login") or ""))
            .strip()
            .lower(),
            body=str(payload.get("body") or ""),
            state=str(payload.get("state") or ""),
            is_draft=bool(payload.get("isDraft", False)),
            merge_state_status=str(payload.get("mergeStateStatus") or ""),
            mergeable=str(payload.get("mergeable") or ""),
            base_ref_name=str(payload.get("baseRefName") or "main"),
            merged_at=str(payload.get("mergedAt") or ""),
            auto_merge_enabled=payload.get("autoMergeRequest") is not None,
            comments=cast(tuple[GitHubCommentNode, ...], comments),
            reviews=cast(tuple[GitHubReviewNode, ...], reviews),
            status_check_rollup=cast(
                tuple[GitHubStatusCheckRollupNode, ...],
                status_check_rollup,
            ),
        )

    def _query_closing_issue_refs(
        self, pr_repo: str, pr_number: int
    ) -> tuple[str, ...]:
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
        numbers = tuple(sorted({int(number) for number in pr_numbers}))
        if len(numbers) == 1:
            number = numbers[0]
            return {number: self._query_pull_request_view_payload(pr_repo, number)}
        return _pull_request_batch_queries.query_pull_request_view_payloads(
            graphql=self._graphql,
            pr_repo=pr_repo,
            pr_numbers=numbers,
            normalize_graphql_rollup_node=_normalize_graphql_rollup_node,
        )

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
        numbers = tuple(sorted({int(number) for number in pr_numbers}))
        return _pull_request_batch_queries.query_pull_request_state_probes(
            graphql=self._graphql,
            pr_repo=pr_repo,
            pr_numbers=numbers,
            normalize_graphql_rollup_node=_normalize_graphql_rollup_node,
            latest_node_timestamp=_latest_node_timestamp,
        )

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

    def _comment_exists(self, owner: str, repo: str, number: int, marker: str) -> bool:
        """Delegate marker checks to the review-state comment helper."""
        return _comment_exists(
            owner,
            repo,
            number,
            marker,
            gh_runner=self._gh_runner,
        )

    def _post_issue_comment(
        self, owner: str, repo: str, number: int, body: str
    ) -> None:
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
            state=payload.state,
        )

    def linked_issue_refs(self, pr_repo: str, pr_number: int) -> tuple[str, ...]:
        return self._query_closing_issue_refs(pr_repo, pr_number)

    def has_copilot_review_signal(self, pr_repo: str, pr_number: int) -> bool:
        payload = self._query_pull_request_view_payload(pr_repo, pr_number)
        from startupai_controller.adapters.github_cli import (
            has_copilot_review_signal_from_payload as _compat_has_copilot_review_signal_from_payload,
        )

        return _compat_has_copilot_review_signal_from_payload(payload)

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
                if (
                    isinstance(payload, dict)
                    and payload.get("autoMergeRequest") is not None
                ):
                    return "confirmed"
            except GhQueryError:
                continue
        return "pending"

    def rerun_failed_check(self, pr_repo: str, check_name: str, run_id: int) -> bool:
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

    def is_pull_request_open(self, pr_repo: str, pr_number: int) -> bool:
        owner, repo = pr_repo.split("/", maxsplit=1)
        return _is_pr_open(owner, repo, pr_number, gh_runner=self._gh_runner)

    def is_pull_request_merged(self, pr_repo: str, pr_number: int) -> bool:
        payload = self._query_pull_request_view_payload(pr_repo, pr_number)
        merged_at = payload.merged_at
        state = str(payload.state or "").strip().upper()
        return bool(merged_at) or state == "MERGED"

    def pull_request_updated_at(
        self,
        pr_repo: str,
        pr_number: int,
    ) -> str | None:
        owner, repo = pr_repo.split("/", maxsplit=1)
        ts = _query_open_pr_updated_at(
            owner, repo, pr_number, gh_runner=self._gh_runner
        )
        return ts.isoformat() if ts is not None else None

    def pull_request_head_sha(self, pr_repo: str, pr_number: int) -> str | None:
        owner, repo = pr_repo.split("/", maxsplit=1)
        return _query_pr_head_sha(owner, repo, pr_number, gh_runner=self._gh_runner)

    def failed_check_runs(
        self,
        pr_repo: str,
        head_sha: str,
    ) -> tuple[str, ...] | None:
        owner, repo = pr_repo.split("/", maxsplit=1)
        failed = _query_failed_check_runs(
            owner, repo, head_sha, gh_runner=self._gh_runner
        )
        return tuple(failed) if failed is not None else None

    def close_pull_request(
        self,
        pr_repo: str,
        pr_number: int,
        *,
        comment: str | None = None,
    ) -> None:
        close_pull_request(
            pr_repo,
            pr_number,
            comment=comment,
            gh_runner=self._gh_runner,
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
            from startupai_controller.validate_critical_path_promotion import (
                GhQueryError,
            )

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
                "{title: .title, body: .body, labels: [.labels[].name], updated_at: .updated_at}",
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


_list_project_items = _pull_request_board_helpers._list_project_items


GitHubCliAdapter = GitHubPullRequestAdapter


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
            if (
                isinstance(payload, dict)
                and payload.get("autoMergeRequest") is not None
            ):
                return "confirmed"
        except GhQueryError:
            continue
    return "pending"


_codex_gate_from_payload = _review_state_builders.codex_gate_from_payload
_build_review_snapshot_from_payload = (
    _review_state_builders.build_review_snapshot_from_payload
)
