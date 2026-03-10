"""GitHub CLI adapter implementing review, PR, and board ports."""

from __future__ import annotations

from collections.abc import Callable
import json

from startupai_controller.domain.models import (
    IssueFields,
    IssueSnapshot,
    OpenPullRequest,
    PrGateStatus,
)
from startupai_controller.promote_ready import BoardInfo
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig
from startupai_controller.board_io import (  # noqa: F401
    COPILOT_CODING_AGENT_LOGINS,
    CodexReviewVerdict,
    CycleBoardSnapshot,
    CycleGitHubMemo,
    LinkedIssue,
    PullRequestViewPayload,
    _ProjectItemSnapshot,
    _comment_activity_timestamp,
    _comment_exists,
    _is_automation_login,
    _is_copilot_coding_agent_actor,
    _is_pr_open,
    _issue_ref_to_repo_parts,
    _list_project_items,
    _list_project_items_by_status,
    _marker_for,
    _parse_codex_verdict_from_text,
    _parse_github_timestamp,
    _parse_pr_url,
    _post_comment,
    _query_failed_check_runs,
    _query_issue_assignees,
    _query_issue_comments,
    _query_issue_updated_at,
    _query_latest_marker_timestamp,
    _query_latest_matching_comment_timestamp,
    _query_latest_non_automation_comment_timestamp,
    _query_latest_wip_activity_timestamp,
    _query_open_pr_updated_at,
    _query_pr_head_sha,
    _query_project_item_field,
    _query_single_select_field_option,
    _repo_to_prefix,
    _run_gh,
    _set_issue_assignees,
    _set_text_field,
    _set_single_select_field,
    _set_status_if_changed,
    _snapshot_to_issue_ref,
    build_cycle_board_snapshot,
    build_pr_gate_status_from_payload,
    clear_cycle_board_snapshot_cache,
    close_issue,
    close_pull_request,
    enable_pull_request_automerge,
    gh_reason_code,
    has_copilot_review_signal_from_payload,
    latest_codex_verdict_from_payload,
    list_issue_comment_bodies,
    memoized_query_issue_body,
    memoized_query_pull_request_state_probes,
    memoized_query_pull_request_view_payloads,
    memoized_query_required_status_checks,
    query_closing_issues,
    query_latest_codex_verdict,
    query_open_pull_requests,
    query_pull_request_view_payload,
    query_required_status_checks,
    rerun_actions_run,
    review_state_digest_from_payload,
    review_state_digest_from_probe,
)


class GitHubCliAdapter:
    """Adapter wrapping gh CLI interactions behind port protocols."""

    def __init__(
        self,
        *,
        project_owner: str,
        project_number: int,
        config: CriticalPathConfig | None = None,
        gh_runner: Callable[..., str] | None = None,
    ) -> None:
        self._project_owner = project_owner
        self._project_number = project_number
        self._config = config
        self._gh_runner = gh_runner

    def _require_config(self) -> CriticalPathConfig:
        if self._config is None:
            raise ValueError(
                "GitHubCliAdapter requires config for board-state operations"
            )
        return self._config

    def _query_board_info(self, issue_ref: str) -> BoardInfo:
        from startupai_controller.promote_ready import _query_issue_board_info

        return _query_issue_board_info(
            issue_ref,
            self._require_config(),
            self._project_owner,
            self._project_number,
        )

    # -- PullRequestPort methods --

    def get_gate_status(self, pr_repo: str, pr_number: int) -> PrGateStatus:
        from startupai_controller.board_io import _query_pr_gate_status

        return _query_pr_gate_status(pr_repo, pr_number, gh_runner=self._gh_runner)

    def list_open_prs_for_issue(
        self, repo: str, issue_number: int
    ) -> list[OpenPullRequest]:
        from startupai_controller.board_io import _run_gh

        output = _run_gh(
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
            gh_runner=self._gh_runner,
        )
        payload = json.loads(output or "[]")
        return [
            OpenPullRequest(
                number=int(item.get("number") or 0),
                url=str(item.get("url") or ""),
                head_ref_name=str(item.get("headRefName") or ""),
                is_draft=bool(item.get("isDraft", False)),
                body=str(item.get("body") or ""),
                author=str(((item.get("author") or {}).get("login") or "")).strip().lower(),
            )
            for item in payload
            if isinstance(item, dict) and isinstance(item.get("number"), int)
        ]

    def enable_automerge(
        self, pr_repo: str, pr_number: int, *, delete_branch: bool = False
    ) -> str:
        from startupai_controller.board_io import enable_pull_request_automerge

        return enable_pull_request_automerge(
            pr_repo,
            pr_number,
            delete_branch=delete_branch,
            gh_runner=self._gh_runner,
        )

    def rerun_failed_check(
        self, pr_repo: str, check_name: str, run_id: int
    ) -> bool:
        del check_name  # run id is the actual rerun handle
        from startupai_controller.board_io import rerun_actions_run

        try:
            rerun_actions_run(pr_repo, run_id, gh_runner=self._gh_runner)
            return True
        except Exception:
            return False

    def update_branch(self, pr_repo: str, pr_number: int) -> None:
        from startupai_controller.board_io import update_pull_request_branch

        update_pull_request_branch(pr_repo, pr_number, gh_runner=self._gh_runner)

    # -- BoardMutationPort methods --

    def set_issue_status(self, issue_ref: str, status: str) -> None:
        from startupai_controller.board_io import (
            _query_status_field_option,
            _set_board_status,
        )

        info = self._query_board_info(issue_ref)
        field_id, option_id = _query_status_field_option(
            info.project_id,
            status,
            gh_runner=self._gh_runner,
        )
        _set_board_status(
            info.project_id,
            info.item_id,
            field_id,
            option_id,
            gh_runner=self._gh_runner,
        )

    def set_issue_field(self, issue_ref: str, field_name: str, value: str) -> None:
        from startupai_controller.board_io import _set_text_field

        info = self._query_board_info(issue_ref)
        _set_text_field(
            info.project_id,
            info.item_id,
            field_name,
            value,
            gh_runner=self._gh_runner,
        )

    def post_issue_comment(self, repo: str, issue_number: int, body: str) -> None:
        from startupai_controller.board_io import _post_comment

        owner, repo_name = repo.split("/", maxsplit=1) if "/" in repo else ("", repo)
        _post_comment(owner, repo_name, issue_number, body, gh_runner=self._gh_runner)

    def close_issue(self, repo: str, issue_number: int) -> None:
        from startupai_controller.board_io import close_issue

        owner, repo_name = repo.split("/", maxsplit=1)
        close_issue(owner, repo_name, issue_number, gh_runner=self._gh_runner)

    # -- ReviewStatePort methods --

    def get_issue_status(self, issue_ref: str) -> str | None:
        info = self._query_board_info(issue_ref)
        return info.status if info else None

    def list_issues_by_status(self, status: str) -> list[IssueSnapshot]:
        from startupai_controller.board_io import (
            _list_project_items_by_status,
            _snapshot_to_issue_ref,
        )

        items = _list_project_items_by_status(
            status,
            self._project_owner,
            self._project_number,
            gh_runner=self._gh_runner,
        )
        results: list[IssueSnapshot] = []
        for item in items:
            issue_ref = item.issue_ref
            if self._config is not None:
                issue_ref = _snapshot_to_issue_ref(item, self._config) or issue_ref
            results.append(
                IssueSnapshot(
                    issue_ref=issue_ref,
                    status=item.status or status,
                    executor=item.executor,
                    priority=item.priority,
                    title=item.title,
                    item_id=item.item_id,
                    project_id=item.project_id,
                )
            )
        return results

    def get_issue_fields(self, issue_ref: str) -> IssueFields:
        from startupai_controller.board_io import _query_project_item_field

        config = self._require_config()

        def field(name: str) -> str:
            return (
                _query_project_item_field(
                    issue_ref,
                    name,
                    config,
                    self._project_owner,
                    self._project_number,
                    gh_runner=self._gh_runner,
                )
                or ""
            )

        return IssueFields(
            issue_ref=issue_ref,
            status=field("Status"),
            priority=field("Priority"),
            sprint=field("Sprint"),
            executor=field("Executor"),
            owner=field("Owner"),
            handoff_to=field("Handoff To"),
            blocked_reason=field("Blocked Reason"),
        )
