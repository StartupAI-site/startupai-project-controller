"""Review handoff helper cluster extracted from board_consumer."""

from __future__ import annotations

from typing import Any, Callable


def transition_claimed_session_to_review(
    *,
    db: Any,
    issue_ref: str,
    session_id: str,
    config: Any,
    critical_path_config: Any,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    transition_issue_to_review: Callable[..., None],
    record_successful_github_mutation: Callable[..., None],
    mark_degraded: Callable[..., None],
    queue_status_transition: Callable[..., None],
    log_error: Callable[[Exception], None],
) -> None:
    """Move one claimed issue into Review or queue the transition on failure."""
    try:
        transition_issue_to_review(
            issue_ref,
            critical_path_config,
            config.project_owner,
            config.project_number,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        record_successful_github_mutation(db)
    except Exception as err:
        log_error(err)
        mark_degraded(db, f"review-transition:{err}")
        queue_status_transition(
            db,
            issue_ref,
            to_status="Review",
            from_statuses={"In Progress"},
        )
        db.update_session(session_id, phase="review")


def post_claimed_session_verdict_marker(
    *,
    db: Any,
    pr_url: str,
    session_id: str,
    gh_runner: Callable[..., str] | None,
    post_pr_codex_verdict: Callable[..., bool],
    record_successful_github_mutation: Callable[..., None],
    mark_degraded: Callable[..., None],
    queue_verdict_marker: Callable[..., None],
    log_error: Callable[[Exception], None],
) -> None:
    """Post the codex verdict marker for a newly handed-off review PR."""
    try:
        post_pr_codex_verdict(
            pr_url,
            session_id,
            gh_runner=gh_runner,
        )
        record_successful_github_mutation(db)
    except Exception as err:
        log_error(err)
        mark_degraded(db, f"verdict-marker:{err}")
        queue_verdict_marker(db, pr_url, session_id)


def queue_claimed_session_for_review(
    *,
    store: Any,
    issue_ref: str,
    pr_url: str,
    session_id: str,
    queue_review_item: Callable[..., Any | None],
) -> Any | None:
    """Queue one claimed session for immediate review handling."""
    return queue_review_item(
        store,
        issue_ref,
        pr_url,
        session_id=session_id,
    )


def run_immediate_review_handoff(
    *,
    config: Any,
    critical_path_config: Any,
    automation_config: Any,
    store: Any,
    queue_entry: Any,
    gh_runner: Callable[..., str] | None,
    db: Any,
    build_github_port_bundle: Callable[..., Any],
    github_memo_factory: Callable[[], Any],
    build_review_snapshots_for_queue_entries: Callable[..., dict[tuple[str, int], Any]],
    review_rescue: Callable[..., Any],
    apply_review_queue_result: Callable[..., None],
    mark_degraded: Callable[..., None],
    gh_reason_code: Callable[..., str],
    summary_factory: Callable[..., Any],
    log_warning: Callable[[str, str, Exception], None],
) -> Any:
    """Run immediate rescue for the just-opened review PR."""
    try:
        review_memo = github_memo_factory()
        handoff_pr_port = build_github_port_bundle(
            config.project_owner,
            config.project_number,
            config=critical_path_config,
            github_memo=review_memo,
            gh_runner=gh_runner,
        ).pull_requests
        queue_entries = [
            entry
            for entry in store.list_review_queue_items()
            if entry.pr_repo == queue_entry.pr_repo
            and entry.pr_number == queue_entry.pr_number
        ]
        review_snapshots = build_review_snapshots_for_queue_entries(
            queue_entries=queue_entries,
            review_refs={entry.issue_ref for entry in queue_entries},
            pr_port=handoff_pr_port,
            trusted_codex_actors=frozenset(automation_config.trusted_codex_actors),
        )
        snapshot = review_snapshots.get((queue_entry.pr_repo, queue_entry.pr_number))
        result = review_rescue(
            pr_repo=queue_entry.pr_repo,
            pr_number=queue_entry.pr_number,
            config=critical_path_config,
            automation_config=automation_config,
            project_owner=config.project_owner,
            project_number=config.project_number,
            dry_run=False,
            snapshot=snapshot,
            gh_runner=gh_runner,
        )
        state_digest = handoff_pr_port.review_state_digests(
            [(queue_entry.pr_repo, queue_entry.pr_number)]
        ).get((queue_entry.pr_repo, queue_entry.pr_number))
        for entry in queue_entries or [queue_entry]:
            apply_review_queue_result(
                store,
                entry,
                result,
                last_state_digest=state_digest,
            )
        pr_ref = f"{queue_entry.pr_repo}#{queue_entry.pr_number}"
        return summary_factory(
            queued_count=len(queue_entries) or 1,
            due_count=len(queue_entries) or 1,
            rerun=(
                (f"{pr_ref}:{','.join(result.rerun_checks)}",)
                if result.rerun_checks
                else ()
            ),
            auto_merge_enabled=((pr_ref,) if result.auto_merge_enabled else ()),
            requeued=result.requeued_refs,
            blocked=(
                (f"{pr_ref}:{result.blocked_reason}",) if result.blocked_reason else ()
            ),
            skipped=(
                (f"{pr_ref}:{result.skipped_reason}",) if result.skipped_reason else ()
            ),
        )
    except Exception as err:
        log_warning(queue_entry.issue_ref, str(err), err)
        mark_degraded(db, f"review-queue:{gh_reason_code(err)}:{err}")
        return summary_factory()
