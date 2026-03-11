"""Adapter for ready-queue routing and backlog admission operations."""

from __future__ import annotations


class BoardAutomationReadyFlowAdapter:
    """Expose the existing ready-flow behavior behind a typed port."""

    def route_protected_queue_executors(self, *args, **kwargs):
        from startupai_controller.board_automation import route_protected_queue_executors

        return route_protected_queue_executors(*args, **kwargs)

    def admit_backlog_items(self, *args, **kwargs):
        from startupai_controller.board_automation import admit_backlog_items

        return admit_backlog_items(*args, **kwargs)

    def admission_summary_payload(self, *args, **kwargs):
        from startupai_controller.board_automation import admission_summary_payload

        return admission_summary_payload(*args, **kwargs)

    def claim_ready_issue(
        self,
        config,
        project_owner: str,
        project_number: int,
        **kwargs,
    ):
        from startupai_controller.application.automation.ready_claim import (
            claim_ready_issue,
        )
        from startupai_controller.runtime.wiring import build_github_port_bundle

        gh_runner = kwargs.get("gh_runner")
        review_state_port = kwargs.get("review_state_port")
        board_port = kwargs.get("board_port")
        if review_state_port is None or board_port is None:
            bundle = build_github_port_bundle(
                project_owner,
                project_number,
                config=config,
                gh_runner=gh_runner,
            )
            review_state_port = review_state_port or bundle.review_state
            board_port = board_port or bundle.board_mutations
            kwargs["review_state_port"] = review_state_port
            kwargs["board_port"] = board_port

        return claim_ready_issue(
            config,
            project_owner,
            project_number,
            **kwargs,
        )
