"""GitHub CLI adapter — wraps board_io.py functions behind port protocols.

Implements PullRequestPort, BoardMutationPort, and ReviewStatePort by
delegating to existing board_io functions.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from startupai_controller.domain.models import (
    IssueFields,
    IssueSnapshot,
    OpenPullRequest,
    PrGateStatus,
)


class GitHubCliAdapter:
    """Adapter wrapping gh CLI interactions from board_io.py.

    Satisfies PullRequestPort, BoardMutationPort, and ReviewStatePort
    protocols via structural typing.
    """

    def __init__(
        self,
        *,
        project_owner: str,
        project_number: int,
        gh_runner: Callable[..., str] | None = None,
    ) -> None:
        self._project_owner = project_owner
        self._project_number = project_number
        self._gh_runner = gh_runner

    # -- PullRequestPort methods --

    def get_gate_status(self, pr_repo: str, pr_number: int) -> PrGateStatus:
        from startupai_controller.board_io import _query_pr_gate_status

        return _query_pr_gate_status(
            pr_repo, pr_number, gh_runner=self._gh_runner
        )

    def list_open_prs_for_issue(
        self, repo: str, issue_number: int
    ) -> list[OpenPullRequest]:
        from startupai_controller.board_io import query_open_pull_requests

        return list(
            query_open_pull_requests(repo, issue_number, gh_runner=self._gh_runner)
        )

    def enable_automerge(
        self, pr_repo: str, pr_number: int, *, delete_branch: bool = False
    ) -> str:
        from startupai_controller.board_io import enable_pull_request_automerge

        return enable_pull_request_automerge(
            pr_repo, pr_number, delete_branch=delete_branch,
            gh_runner=self._gh_runner,
        )

    def rerun_failed_check(
        self, pr_repo: str, check_name: str, run_id: int
    ) -> bool:
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
        from startupai_controller.board_io import _set_status_if_changed

        _set_status_if_changed(
            issue_ref,
            status,
            self._project_owner,
            self._project_number,
            gh_runner=self._gh_runner,
        )

    def set_issue_field(
        self, issue_ref: str, field_name: str, value: str
    ) -> None:
        from startupai_controller.board_io import (
            _set_text_field,
        )

        _set_text_field(
            issue_ref,
            field_name,
            value,
            self._project_owner,
            self._project_number,
            gh_runner=self._gh_runner,
        )

    def post_issue_comment(
        self, repo: str, issue_number: int, body: str
    ) -> None:
        from startupai_controller.board_io import _post_comment

        owner, repo_name = repo.split("/", maxsplit=1) if "/" in repo else ("", repo)
        _post_comment(
            owner, repo_name, issue_number, body, gh_runner=self._gh_runner
        )

    def close_issue(self, repo: str, issue_number: int) -> None:
        from startupai_controller.board_io import close_issue

        close_issue(repo, issue_number, gh_runner=self._gh_runner)

    # -- ReviewStatePort methods --

    def get_issue_status(self, issue_ref: str) -> str | None:
        from startupai_controller.promote_ready import _query_issue_board_info

        info = _query_issue_board_info(
            issue_ref,
            None,  # config — not needed for status-only
            self._project_owner,
            self._project_number,
        )
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
            results.append(
                IssueSnapshot(
                    issue_ref="",  # populated by caller with config
                    status=status,
                    executor=item.executor,
                    priority=item.priority,
                    title=item.title,
                    item_id=item.item_id,
                    project_id="",
                )
            )
        return results

    def get_issue_fields(self, issue_ref: str) -> IssueFields:
        from startupai_controller.board_io import _query_project_item_field

        status = _query_project_item_field(
            issue_ref, "Status", self._project_owner, self._project_number,
            gh_runner=self._gh_runner,
        ) or ""
        priority = _query_project_item_field(
            issue_ref, "Priority", self._project_owner, self._project_number,
            gh_runner=self._gh_runner,
        ) or ""
        sprint = _query_project_item_field(
            issue_ref, "Sprint", self._project_owner, self._project_number,
            gh_runner=self._gh_runner,
        ) or ""
        executor = _query_project_item_field(
            issue_ref, "Executor", self._project_owner, self._project_number,
            gh_runner=self._gh_runner,
        ) or ""
        owner = _query_project_item_field(
            issue_ref, "Owner", self._project_owner, self._project_number,
            gh_runner=self._gh_runner,
        ) or ""
        handoff_to = _query_project_item_field(
            issue_ref, "Handoff To", self._project_owner, self._project_number,
            gh_runner=self._gh_runner,
        ) or ""
        blocked_reason = _query_project_item_field(
            issue_ref, "Blocked Reason", self._project_owner, self._project_number,
            gh_runner=self._gh_runner,
        ) or ""
        return IssueFields(
            issue_ref=issue_ref,
            status=status,
            priority=priority,
            sprint=sprint,
            executor=executor,
            owner=owner,
            handoff_to=handoff_to,
            blocked_reason=blocked_reason,
        )
