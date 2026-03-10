"""Compatibility shell for the pull-request capability adapter.

The canonical PR/review mechanism now lives in ``adapters.pull_requests`` and
its sibling adapter modules. This module remains a backward-compatible surface
for legacy imports and direct monkeypatch-based tests while runtime wiring and
orchestration use the capability adapters directly.
"""

from __future__ import annotations

from datetime import datetime
import json

from startupai_controller.adapters.github_transport import _run_gh
from startupai_controller.adapters.pull_requests import (
    CycleGitHubMemo,
    GitHubCliAdapter as _BaseGitHubCliAdapter,
    GitHubPullRequestAdapter,
    _codex_gate_from_payload,
    _list_project_items as _impl_list_project_items,
    _is_pr_open as _impl_is_pr_open,
    _query_failed_check_runs as _impl_query_failed_check_runs,
    _query_issue_assignees as _impl_query_issue_assignees,
    _query_issue_updated_at as _impl_query_issue_updated_at,
    _query_issue_board_info as _impl_query_issue_board_info,
    _query_open_pr_updated_at as _impl_query_open_pr_updated_at,
    _query_pr_head_sha as _impl_query_pr_head_sha,
    _query_project_item_field as _impl_query_project_item_field,
    _query_single_select_field_option as _impl_query_single_select_field_option,
    _set_board_status as _impl_set_board_status,
    _set_issue_assignees as _impl_set_issue_assignees,
    _set_single_select_field as _impl_set_single_select_field,
    _set_status_if_changed as _impl_set_status_if_changed,
    _set_text_field as _impl_set_text_field,
    _review_state_digest_from_probe,
    build_pr_gate_status_from_payload,
    clear_required_status_checks_cache,
    enable_pull_request_automerge,
    has_copilot_review_signal_from_payload,
    latest_codex_verdict_from_payload,
    memoized_query_pull_request_state_probes,
    memoized_query_pull_request_view_payloads,
    query_closing_issues,
    query_latest_codex_verdict,
    query_open_pull_requests,
    query_pull_request_state_probes,
    query_pull_request_view_payload,
    query_pull_request_view_payloads,
    query_required_status_checks,
)
from startupai_controller.adapters.review_state import (
    _comment_exists,
    _query_latest_matching_comment_timestamp,
    _query_latest_non_automation_comment_timestamp,
)
from startupai_controller.domain.models import IssueContext, ReviewSnapshot
from startupai_controller.domain.repair_policy import MARKER_PREFIX


def _query_project_item_field(issue_ref, field_name, config, project_owner, project_number, *, gh_runner=None):
    return _impl_query_project_item_field(
        issue_ref,
        field_name,
        config,
        project_owner,
        project_number,
        gh_runner=gh_runner or _run_gh,
    )


def _query_single_select_field_option(project_id, field_name, option_name, *, gh_runner=None):
    return _impl_query_single_select_field_option(
        project_id,
        field_name,
        option_name,
        gh_runner=gh_runner or _run_gh,
    )


def _query_status_field_option(project_id, option_name="Ready", *, gh_runner=None):
    return _query_single_select_field_option(
        project_id,
        "Status",
        option_name,
        gh_runner=gh_runner,
    )


def _set_text_field(project_id, item_id, field_name, value, *, gh_runner=None):
    return _impl_set_text_field(
        project_id,
        item_id,
        field_name,
        value,
        gh_runner=gh_runner or _run_gh,
    )


def _set_single_select_field(project_id, item_id, field_name, option_name, *, gh_runner=None):
    return _impl_set_single_select_field(
        project_id,
        item_id,
        field_name,
        option_name,
        gh_runner=gh_runner or _run_gh,
    )


def _set_board_status(project_id, item_id, field_id, option_id, *, gh_runner=None):
    return _impl_set_board_status(
        project_id,
        item_id,
        field_id,
        option_id,
        gh_runner=gh_runner or _run_gh,
    )


def _set_status_if_changed(issue_ref, from_statuses, to_status, config, project_owner, project_number, *, board_info_resolver=None, board_mutator=None, gh_runner=None):
    return _impl_set_status_if_changed(
        issue_ref,
        from_statuses,
        to_status,
        config,
        project_owner,
        project_number,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner or _run_gh,
    )


def query_issue_body(owner, repo, number, *, gh_runner=None):
    return _run_gh(
        [
            "api",
            f"repos/{owner}/{repo}/issues/{number}",
            "--jq",
            ".body",
        ],
        gh_runner=gh_runner,
        operation_type="query",
    )


def memoized_query_issue_body(memo, owner, repo, number, *, gh_runner=None):
    key = (owner, repo, number)
    cached = memo.issue_bodies.get(key)
    if cached is not None:
        return cached
    body = query_issue_body(owner, repo, number, gh_runner=gh_runner)
    memo.issue_bodies[key] = body
    return body


def _is_pr_open(owner, repo, pr_number, *, gh_runner=None):
    return _impl_is_pr_open(owner, repo, pr_number, gh_runner=gh_runner or _run_gh)


def _query_failed_check_runs(owner, repo, head_sha, *, gh_runner=None):
    return _impl_query_failed_check_runs(owner, repo, head_sha, gh_runner=gh_runner or _run_gh)


def _query_pr_head_sha(owner, repo, pr_number, *, gh_runner=None):
    return _impl_query_pr_head_sha(owner, repo, pr_number, gh_runner=gh_runner or _run_gh)


def _query_issue_updated_at(owner, repo, number, *, gh_runner=None):
    return _impl_query_issue_updated_at(owner, repo, number, gh_runner=gh_runner or _run_gh)


def _query_open_pr_updated_at(owner, repo, pr_number, *, gh_runner=None):
    return _impl_query_open_pr_updated_at(owner, repo, pr_number, gh_runner=gh_runner or _run_gh)


def _query_latest_wip_activity_timestamp(issue_ref, owner, repo, number, pr_field, *, gh_runner=None):
    candidates: list[datetime] = []
    pr_ts = _query_open_pr_updated_at(owner, repo, number, gh_runner=gh_runner)
    if pr_ts is not None:
        candidates.append(pr_ts)
    comment_ts = _query_latest_non_automation_comment_timestamp(owner, repo, number, gh_runner=gh_runner or _run_gh)
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
        gh_runner=gh_runner or _run_gh,
    )
    if baseline_ts is not None:
        candidates.append(baseline_ts)
    return max(candidates) if candidates else None


def _query_issue_assignees(owner, repo, number, *, gh_runner=None):
    return _impl_query_issue_assignees(owner, repo, number, gh_runner=gh_runner or _run_gh)


def _set_issue_assignees(owner, repo, number, assignees, *, gh_runner=None):
    return _impl_set_issue_assignees(owner, repo, number, assignees, gh_runner=gh_runner or _run_gh)


def _query_issue_board_info(issue_ref, config, project_owner, project_number, *, gh_runner=None):
    return _impl_query_issue_board_info(
        issue_ref,
        config,
        project_owner,
        project_number,
        gh_runner=gh_runner or _run_gh,
    )


def _list_project_items(project_owner, project_number, *, statuses=None, gh_runner=None):
    return _impl_list_project_items(
        project_owner,
        project_number,
        statuses=statuses,
        gh_runner=gh_runner or _run_gh,
    )


class GitHubCliAdapter(_BaseGitHubCliAdapter):
    """Compatibility facade that keeps legacy monkeypatch points working."""

    def has_copilot_review_signal(self, pr_repo: str, pr_number: int) -> bool:
        payload = self._query_pull_request_view_payload(pr_repo, pr_number)
        return has_copilot_review_signal_from_payload(payload)

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
                    from startupai_controller.validate_critical_path_promotion import GhQueryError

                    raise GhQueryError(
                        f"Failed querying PR {pr_repo}#{pr_number}: pull request not found."
                    )
                required_checks = self._query_required_status_checks(
                    pr_repo,
                    getattr(payload, "base_ref_name", None) or "main",
                )
                review_refs = review_refs_by_pr[(pr_repo, pr_number)]
                verdict = latest_codex_verdict_from_payload(
                    payload,
                    trusted_actors=trusted_codex_actors,
                )
                codex_gate_code, codex_gate_message = _codex_gate_from_payload(
                    pr_repo,
                    pr_number,
                    review_refs=review_refs,
                    verdict=verdict,
                )
                gate_status = build_pr_gate_status_from_payload(
                    payload,
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
                snapshots[(pr_repo, pr_number)] = ReviewSnapshot(
                    pr_repo=pr_repo,
                    pr_number=pr_number,
                    review_refs=review_refs,
                    pr_author=str(getattr(payload, "author", "") or ""),
                    pr_body=str(getattr(payload, "body", "") or ""),
                    pr_comment_bodies=tuple(
                        str(comment.get("body") or "")
                        for comment in getattr(payload, "comments", ())
                    ),
                    copilot_review_present=has_copilot_review_signal_from_payload(
                        payload
                    ),
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
        return snapshots

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


__all__ = [
    "CycleGitHubMemo",
    "GitHubCliAdapter",
    "GitHubPullRequestAdapter",
    "_comment_exists",
    "_is_pr_open",
    "_list_project_items",
    "_query_failed_check_runs",
    "_query_issue_assignees",
    "_query_issue_board_info",
    "_query_issue_updated_at",
    "_query_latest_matching_comment_timestamp",
    "_query_latest_non_automation_comment_timestamp",
    "_query_latest_wip_activity_timestamp",
    "_query_open_pr_updated_at",
    "_query_pr_head_sha",
    "_query_project_item_field",
    "_query_single_select_field_option",
    "_query_status_field_option",
    "_review_state_digest_from_probe",
    "_run_gh",
    "_set_board_status",
    "_set_issue_assignees",
    "_set_single_select_field",
    "_set_status_if_changed",
    "_set_text_field",
    "build_pr_gate_status_from_payload",
    "clear_required_status_checks_cache",
    "enable_pull_request_automerge",
    "has_copilot_review_signal_from_payload",
    "latest_codex_verdict_from_payload",
    "memoized_query_issue_body",
    "memoized_query_pull_request_state_probes",
    "memoized_query_pull_request_view_payloads",
    "query_closing_issues",
    "query_issue_body",
    "query_latest_codex_verdict",
    "query_open_pull_requests",
    "query_pull_request_state_probes",
    "query_pull_request_view_payload",
    "query_pull_request_view_payloads",
    "query_required_status_checks",
]
