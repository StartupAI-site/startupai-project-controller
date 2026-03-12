"""Review handoff helper cluster extracted from board_consumer."""

from __future__ import annotations

from typing import Any, Callable, Protocol

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.domain.models import (
    ReviewQueueDrainSummary,
    ReviewQueueEntry,
    ReviewRescueResult,
    ReviewSnapshot,
)
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.ready_flow import (
    BoardInfoResolverFn,
    BoardStatusMutatorFn,
    GitHubRunnerFn,
)
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig


class ReviewHandoffStatePort(ConsumerRuntimeStatePort, Protocol):
    """Runtime state needed during review handoff."""

    def update_session(self, session_id: str, **fields: Any) -> None:
        """Persist legacy session-field updates during shell extraction."""
        ...


class ImmediateReviewGitHubBundle(Protocol):
    """Minimal GitHub bundle surface needed for immediate review rescue."""

    pull_requests: PullRequestPort


class TransitionIssueToReviewFn(Protocol):
    """Move one issue to Review on the project board."""

    def __call__(
        self,
        issue_ref: str,
        config: CriticalPathConfig,
        project_owner: str,
        project_number: int,
        *,
        review_state_port: Any | None = None,
        board_port: Any | None = None,
        board_info_resolver: BoardInfoResolverFn | None = None,
        board_mutator: BoardStatusMutatorFn | None = None,
        gh_runner: GitHubRunnerFn | None = None,
    ) -> None: ...


class PostPrCodexVerdictFn(Protocol):
    """Post the codex verdict marker to one PR if missing."""

    def __call__(
        self,
        pr_url: str,
        session_id: str,
        *,
        gh_runner: GitHubRunnerFn | None = None,
    ) -> bool: ...


class QueueVerdictMarkerFn(Protocol):
    """Persist a deferred verdict-marker replay action."""

    def __call__(
        self,
        db: ConsumerRuntimeStatePort,
        pr_url: str,
        session_id: str,
    ) -> None: ...


class QueueStatusTransitionFn(Protocol):
    """Persist a deferred board-status transition for replay."""

    def __call__(
        self,
        db: ConsumerRuntimeStatePort,
        issue_ref: str,
        *,
        to_status: str,
        from_statuses: set[str],
        blocked_reason: str | None = None,
    ) -> None: ...


class QueueReviewItemFn(Protocol):
    """Enqueue one issue/PR pair into the bounded review queue."""

    def __call__(
        self,
        store: SessionStorePort,
        issue_ref: str,
        pr_url: str,
        *,
        session_id: str | None = None,
    ) -> ReviewQueueEntry | None: ...


class BuildGitHubPortBundleFn(Protocol):
    """Build the minimal GitHub bundle required for immediate review rescue."""

    def __call__(
        self,
        project_owner: str,
        project_number: int,
        *,
        config: Any,
        github_memo: Any,
        gh_runner: GitHubRunnerFn | None = None,
    ) -> ImmediateReviewGitHubBundle: ...


class BuildReviewSnapshotsForQueueEntriesFn(Protocol):
    """Build typed review snapshots for one set of queued review entries."""

    def __call__(
        self,
        *,
        queue_entries: list[ReviewQueueEntry],
        review_refs: set[str],
        pr_port: PullRequestPort,
        trusted_codex_actors: frozenset[str],
    ) -> dict[tuple[str, int], ReviewSnapshot]: ...


class ReviewRescueFn(Protocol):
    """Run review rescue for one PR in Review."""

    def __call__(
        self,
        *,
        pr_repo: str,
        pr_number: int,
        config: CriticalPathConfig,
        automation_config: BoardAutomationConfig,
        project_owner: str,
        project_number: int,
        dry_run: bool = False,
        snapshot: ReviewSnapshot | None = None,
        gh_runner: GitHubRunnerFn | None = None,
    ) -> ReviewRescueResult: ...


class ApplyReviewQueueResultFn(Protocol):
    """Persist one review rescue outcome back into the review queue."""

    def __call__(
        self,
        store: SessionStorePort,
        entry: ReviewQueueEntry,
        result: Any,
        *,
        now: Any | None = None,
        retry_seconds: int | None = None,
        last_state_digest: str | None = None,
    ) -> bool: ...


def transition_claimed_session_to_review(
    *,
    db: ReviewHandoffStatePort,
    issue_ref: str,
    session_id: str,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    gh_runner: GitHubRunnerFn | None,
    transition_issue_to_review: TransitionIssueToReviewFn,
    record_successful_github_mutation: Callable[[ConsumerRuntimeStatePort], None],
    mark_degraded: Callable[[ConsumerRuntimeStatePort, str], None],
    queue_status_transition: QueueStatusTransitionFn,
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
    db: ConsumerRuntimeStatePort,
    pr_url: str,
    session_id: str,
    gh_runner: GitHubRunnerFn | None,
    post_pr_codex_verdict: PostPrCodexVerdictFn,
    record_successful_github_mutation: Callable[[ConsumerRuntimeStatePort], None],
    mark_degraded: Callable[[ConsumerRuntimeStatePort, str], None],
    queue_verdict_marker: QueueVerdictMarkerFn,
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
    store: SessionStorePort,
    issue_ref: str,
    pr_url: str,
    session_id: str,
    queue_review_item: QueueReviewItemFn,
) -> ReviewQueueEntry | None:
    """Queue one claimed session for immediate review handling."""
    return queue_review_item(
        store,
        issue_ref,
        pr_url,
        session_id=session_id,
    )


def run_immediate_review_handoff(
    *,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    store: SessionStorePort,
    queue_entry: ReviewQueueEntry,
    gh_runner: GitHubRunnerFn | None,
    db: ConsumerRuntimeStatePort,
    build_github_port_bundle: BuildGitHubPortBundleFn,
    github_memo_factory: Callable[[], Any],
    build_review_snapshots_for_queue_entries: BuildReviewSnapshotsForQueueEntriesFn,
    review_rescue: ReviewRescueFn,
    apply_review_queue_result: ApplyReviewQueueResultFn,
    mark_degraded: Callable[[ConsumerRuntimeStatePort, str], None],
    gh_reason_code: Callable[[Any], str],
    summary_factory: type[ReviewQueueDrainSummary],
    log_warning: Callable[[str, str, Exception], None],
) -> ReviewQueueDrainSummary:
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
