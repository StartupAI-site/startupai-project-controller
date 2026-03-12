"""Handoff reconciliation use case — retry/escalate stale cross-repo handoffs."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from startupai_controller.domain.repair_policy import MARKER_PREFIX
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.validate_critical_path_promotion import GhQueryError


def reconcile_handoffs(
    config,
    project_owner: str,
    project_number: int,
    *,
    ack_timeout_minutes: int = 30,
    max_retries: int = 1,
    dry_run: bool = False,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
) -> dict[str, int]:
    """Reconcile handoff jobs. Returns {completed, retried, escalated, pending}."""
    now = datetime.now(timezone.utc)
    ack_timeout = timedelta(minutes=max(0, ack_timeout_minutes))
    counters = {
        "completed": 0,
        "retried": 0,
        "escalated": 0,
        "pending": 0,
    }
    # Scan all issue prefixes for handoff markers
    for prefix, repo_slug in config.issue_prefixes.items():
        try:
            issue_numbers = (
                review_state_port.search_open_issue_numbers_with_comment_marker(
                    repo_slug,
                    f"{MARKER_PREFIX}:handoff:job=",
                )
            )
        except GhQueryError:
            continue

        for issue_number in issue_numbers:
            issue_ref = f"{prefix}#{issue_number}"

            # Check if this issue has been acknowledged (moved past Backlog)
            current_status = review_state_port.get_issue_status(issue_ref)

            ack_statuses = {"Ready", "In Progress", "Review", "Done"}
            if current_status in ack_statuses:
                counters["completed"] += 1
                continue

            try:
                comments = review_state_port.list_issue_comment_bodies(
                    repo_slug,
                    issue_number,
                )
            except GhQueryError:
                counters["pending"] += 1
                continue

            retry_marker = f"{MARKER_PREFIX}:handoff-retry:{issue_ref}:"
            retry_count = 0
            for body in comments:
                if retry_marker in body:
                    retry_count += 1

            latest_signal = review_state_port.latest_matching_comment_timestamp(
                repo_slug,
                issue_number,
                (
                    f"{MARKER_PREFIX}:handoff:job=",
                    f"{MARKER_PREFIX}:handoff-retry:{issue_ref}:",
                ),
            )
            if latest_signal is None or (now - latest_signal) < ack_timeout:
                counters["pending"] += 1
                continue

            if retry_count < max_retries:
                if not dry_run:
                    retry_id = (
                        f"{retry_marker}"
                        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
                    )
                    body = (
                        f"<!-- {retry_id} -->\n"
                        f"**Handoff retry**: `{issue_ref}` has an unacknowledged "
                        f"handoff. Retry #{retry_count + 1} of {max_retries}.\n\n"
                        f"Run:\n```\nmake promote-ready ISSUE={issue_ref}\n```"
                    )
                    board_port.post_issue_comment(repo_slug, issue_number, body)
                counters["retried"] += 1
            else:
                if not dry_run:
                    try:
                        board_port.set_issue_status(issue_ref, "Blocked")
                        board_port.set_issue_field(
                            issue_ref,
                            "Blocked Reason",
                            "handoff-timeout:retries-exhausted",
                        )
                    except GhQueryError:
                        pass
                counters["escalated"] += 1

    return counters
