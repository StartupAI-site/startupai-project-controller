"""Adapter for ready-queue routing and backlog admission operations."""

from __future__ import annotations


class BoardAutomationReadyFlowAdapter:
    """Expose the existing ready-flow behavior behind a typed port."""

    def route_protected_queue_executors(self, *args, **kwargs):
        from startupai_controller.board_automation import (
            route_protected_queue_executors,
        )

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
        from startupai_controller.automation_ready_review_wiring import (
            claim_ready_issue,
        )

        return claim_ready_issue(
            config,
            project_owner,
            project_number,
            **kwargs,
        )
