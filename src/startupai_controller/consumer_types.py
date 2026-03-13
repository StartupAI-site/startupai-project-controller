"""Shared consumer coordination types extracted from board_consumer."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, TypedDict

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_workflow import WorkflowDefinition
from startupai_controller.domain.resolution_policy import ResolutionPayload
from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    CycleResult,
    RepairBranchReconcileOutcome,
    ResolutionEvaluation,
    ReviewQueueDrainSummary,
    ReviewQueueEntry,
    ReviewSnapshot,
)
from startupai_controller.ports.ready_flow import ReadyFlowPort
from startupai_controller.runtime.wiring import GitHubRuntimeMemo
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig


class CodexSessionResult(TypedDict):
    """Structured Codex session result matching the output schema contract."""

    outcome: Literal["success", "failed", "blocked"]
    summary: str
    tests_run: int | None
    tests_passed: int | None
    changed_files: list[str]
    commit_shas: list[str]
    pr_url: str | None
    resolution: ResolutionPayload | None
    blocker_reason: str | None
    needs_handoff_to: Literal["claude"] | None
    duration_seconds: float


class IssueContextPayload(TypedDict):
    """Normalized issue context payload used across launch and codex paths."""

    title: str
    body: str
    labels: list[str]
    updated_at: str


@dataclass(frozen=True)
class PreparedCycleContext:
    """Preflight context reused across worker launches in one daemon tick."""

    cp_config: CriticalPathConfig
    auto_config: BoardAutomationConfig | None
    main_workflows: dict[str, WorkflowDefinition]
    workflow_statuses: dict[str, Any]
    dispatchable_repo_prefixes: tuple[str, ...]
    effective_interval: int
    global_limit: int
    board_snapshot: CycleBoardSnapshot
    github_memo: GitHubRuntimeMemo
    ready_flow_port: ReadyFlowPort
    admission_summary: dict[str, Any]
    review_queue_summary: ReviewQueueDrainSummary = field(
        default_factory=ReviewQueueDrainSummary
    )
    timings_ms: dict[str, int] = field(default_factory=dict)
    github_request_counts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class ActiveWorkerTask:
    """Bookkeeping for one asynchronously executing worker slot."""

    issue_ref: str
    slot_id: int
    launched_at: str


@dataclass(frozen=True)
class PreparedLaunchContext:
    """Locally prepared work that is safe to claim and launch."""

    issue_ref: str
    repo_prefix: str
    owner: str
    repo: str
    number: int
    title: str
    issue_context: IssueContextPayload
    session_kind: str
    repair_pr_url: str | None
    repair_branch_name: str | None
    worktree_path: str
    branch_name: str
    workflow_definition: WorkflowDefinition
    effective_consumer_config: ConsumerConfig
    dependency_summary: str
    branch_reconcile_state: str | None = None
    branch_reconcile_error: str | None = None


@dataclass(frozen=True)
class ClaimedSessionContext:
    """Claimed and started local session ready for Codex execution."""

    session_id: str
    effective_max_retries: int
    slot_id: int


@dataclass(frozen=True)
class PendingClaimContext:
    """Session state prepared for board claim after local launch prep."""

    session_id: str
    effective_max_retries: int


@dataclass(frozen=True)
class SessionExecutionOutcome:
    """Outcome of executing a claimed local session."""

    session_status: str
    failure_reason: str | None
    pr_url: str | None
    has_commits: bool
    codex_result: CodexSessionResult | None
    should_transition_to_review: bool
    immediate_review_summary: ReviewQueueDrainSummary
    resolution_evaluation: ResolutionEvaluation | None = None
    done_reason: str | None = None


@dataclass(frozen=True)
class PrCreationOutcome:
    """PR creation/salvage result for a claimed session."""

    pr_url: str | None
    has_commits: bool
    session_status: str
    failure_reason: str | None


@dataclass(frozen=True)
class PreparedReviewQueueBatch:
    """Prepared review-queue workset for one drain cycle."""

    review_refs: frozenset[str]
    queue_items: tuple[ReviewQueueEntry, ...]
    due_items: tuple[ReviewQueueEntry, ...]
    due_pr_groups: tuple[tuple[tuple[str, int], tuple[ReviewQueueEntry, ...]], ...]
    selected_snapshot_entries: tuple[ReviewQueueEntry, ...]
    seeded: tuple[str, ...]
    removed: tuple[str, ...]


@dataclass(frozen=True)
class ReviewQueueProcessingOutcome:
    """Processed review-queue results for a prepared batch."""

    due_count: int
    verdict_backfilled: tuple[str, ...]
    rerun: tuple[str, ...]
    auto_merge_enabled: tuple[str, ...]
    requeued: tuple[str, ...]
    blocked: tuple[str, ...]
    skipped: tuple[str, ...]
    escalated: tuple[str, ...]
    partial_failure: bool
    error: str | None
    updated_snapshot: CycleBoardSnapshot


@dataclass(frozen=True)
class PreparedDueReviewProcessing:
    """Prepared changed due-review groups ready for rescue processing."""

    due_items: tuple[ReviewQueueEntry, ...]
    due_pr_groups: tuple[tuple[tuple[str, int], tuple[ReviewQueueEntry, ...]], ...]
    snapshots: dict[tuple[str, int], ReviewSnapshot]
    verdict_backfilled: tuple[str, ...]
    partial_failure: bool
    error: str | None


@dataclass(frozen=True)
class ReviewGroupProcessingOutcome:
    """Outcome of processing one due PR group from the review queue."""

    rerun: tuple[str, ...]
    auto_merge_enabled: tuple[str, ...]
    requeued: tuple[str, ...]
    blocked: tuple[str, ...]
    skipped: tuple[str, ...]
    escalated: tuple[str, ...]
    updated_snapshot: CycleBoardSnapshot
    partial_failure: bool = False
    error: str | None = None


@dataclass(frozen=True)
class SelectedLaunchCandidate:
    """A Ready issue selected for launch preparation."""

    issue_ref: str
    repo_prefix: str
    main_workflow: WorkflowDefinition


class WorktreePrepareError(RuntimeError):
    """Raised when a worktree cannot be safely prepared for launch."""

    def __init__(self, reason_code: str, detail: str) -> None:
        self.reason_code = reason_code
        self.detail = detail
        super().__init__(detail)
