"""Policy-oriented domain types.

Only types consumed by domain policy functions or port contracts belong here.
Adapter-internal types (GitHub parsing shapes, persistence plumbing) stay in
their adapter modules.

Dependency rule: this module imports ONLY stdlib.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
# Policy outcome types (decisions, results, coordination state)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CycleResult:
    """Outcome of a single poll-claim-execute cycle."""

    action: str  # "claimed", "idle", "error"
    issue_ref: str | None = None
    session_id: str | None = None
    reason: str = ""
    pr_url: str | None = None


@dataclass(frozen=True)
class ReviewQueueDrainSummary:
    """Summary of one bounded review-queue drain pass."""

    queued_count: int = 0
    due_count: int = 0
    seeded: tuple[str, ...] = ()
    removed: tuple[str, ...] = ()
    verdict_backfilled: tuple[str, ...] = ()
    rerun: tuple[str, ...] = ()
    auto_merge_enabled: tuple[str, ...] = ()
    requeued: tuple[str, ...] = ()
    blocked: tuple[str, ...] = ()
    skipped: tuple[str, ...] = ()
    escalated: tuple[str, ...] = ()
    partial_failure: bool = False
    error: str | None = None


@dataclass(frozen=True)
class RepairBranchReconcileOutcome:
    """Outcome of reconciling a repair branch against origin/main."""

    state: str
    error: str | None = None


@dataclass(frozen=True)
class ResolutionEvaluation:
    """Deterministic resolution verification result."""

    resolution_kind: str | None
    verification_class: str
    final_action: str
    summary: str
    evidence: dict[str, Any] = field(default_factory=dict)
    blocked_reason: str | None = None


@dataclass(frozen=True)
class OpenPullRequestMatch:
    """Open PR candidate for consumer adoption/reconciliation."""

    url: str
    number: int
    author: str
    body: str
    branch_name: str
    provenance: dict[str, str] | None


# From board_automation.py

@dataclass
class PromotionResult:
    promoted: list[str] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)  # (ref, reason)
    cross_repo_pending: list[str] = field(default_factory=list)
    handoff_jobs: list[str] = field(default_factory=list)


@dataclass
class SchedulingDecision:
    claimable: list[str] = field(default_factory=list)
    claimed: list[str] = field(default_factory=list)
    deferred_dependency: list[str] = field(default_factory=list)
    deferred_wip: list[str] = field(default_factory=list)
    blocked_invalid_ready: list[str] = field(default_factory=list)
    blocked_missing_executor: list[str] = field(default_factory=list)
    skipped_non_graph: list[str] = field(default_factory=list)
    skipped_missing_executor: list[str] = field(default_factory=list)


@dataclass
class ClaimReadyResult:
    claimed: str | None = None
    reason: str = ""


@dataclass
class ExecutorRoutingDecision:
    routed: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class ExecutionPolicyDecision:
    skipped_reason: str | None = None
    enforced_pr: bool = False
    pr_closed: bool = False
    requeued: list[str] = field(default_factory=list)
    blocked: list[str] = field(default_factory=list)
    copilot_unassigned: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ReviewSnapshot:
    """Typed review-state projection for one PR."""

    pr_repo: str
    pr_number: int
    review_refs: tuple[str, ...]
    pr_author: str
    pr_body: str
    pr_comment_bodies: tuple[str, ...]
    copilot_review_present: bool
    codex_verdict: Any  # CodexReviewVerdict | None (adapter-internal type)
    codex_gate_code: int
    codex_gate_message: str
    gate_status: PrGateStatus
    rescue_checks: tuple[str, ...]
    rescue_passed: set[str]
    rescue_pending: set[str]
    rescue_failed: set[str]
    rescue_cancelled: set[str]
    rescue_missing: set[str]


@dataclass(frozen=True)
class ReviewRescueResult:
    """Result of reconciling one PR in Review."""

    pr_repo: str
    pr_number: int
    rerun_checks: tuple[str, ...] = ()
    auto_merge_enabled: bool = False
    requeued_refs: tuple[str, ...] = ()
    skipped_reason: str | None = None
    blocked_reason: str | None = None


@dataclass(frozen=True)
class ReviewRescueSweep:
    """Summary of one cross-repo review rescue sweep."""

    scanned_repos: tuple[str, ...]
    scanned_prs: int
    rerun: tuple[str, ...] = ()
    auto_merge_enabled: tuple[str, ...] = ()
    requeued: tuple[str, ...] = ()
    blocked: tuple[str, ...] = ()
    skipped: tuple[str, ...] = ()


# From board_graph.py

@dataclass(frozen=True)
class AdmissionCandidate:
    """Eligible backlog item that may be admitted to Ready."""

    issue_ref: str
    repo_prefix: str
    item_id: str
    project_id: str
    priority: str
    title: str
    is_graph_member: bool


@dataclass(frozen=True)
class AdmissionSkip:
    """Ineligible backlog item and the reason it was skipped."""

    issue_ref: str
    reason_code: str


@dataclass(frozen=True)
class AdmissionDecision:
    """Pure admission planning output before board mutations occur."""

    ready_count: int
    ready_floor: int
    ready_cap: int
    needed: int
    scanned_backlog: int
    eligible: tuple[AdmissionCandidate, ...] = ()
    admitted: tuple[str, ...] = ()
    skipped: tuple[AdmissionSkip, ...] = ()
    resolved: tuple[str, ...] = ()
    blocked: tuple[str, ...] = ()
    partial_failure: bool = False
    error: str | None = None
    deep_evaluation_performed: bool = False
    deep_evaluation_truncated: bool = False

    @property
    def eligible_count(self) -> int:
        return len(self.eligible)

    @property
    def skip_reason_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for skip in self.skipped:
            counts[skip.reason_code] = counts.get(skip.reason_code, 0) + 1
        return counts


# ---------------------------------------------------------------------------
# State types consumed by domain policy
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SessionInfo:
    """Read-only view of a session record."""

    id: str
    issue_ref: str
    repo_prefix: str | None
    worktree_path: str | None
    branch_name: str | None
    executor: str
    slot_id: int | None
    status: str
    phase: str | None
    started_at: str | None
    completed_at: str | None
    outcome_json: str | None
    failure_reason: str | None
    retry_count: int
    pr_url: str | None
    provenance_id: str | None
    session_kind: str
    repair_pr_url: str | None
    branch_reconcile_state: str | None
    branch_reconcile_error: str | None
    resolution_kind: str | None
    verification_class: str | None
    resolution_evidence_json: str | None
    resolution_action: str | None
    done_reason: str | None


@dataclass(frozen=True)
class IssueContext:
    """Typed issue context used during launch preparation."""

    title: str
    body: str
    labels: tuple[str, ...]
    updated_at: str


@dataclass(frozen=True)
class WorktreeEntry:
    """One local git worktree record."""

    path: str
    branch_name: str


@dataclass(frozen=True)
class ReviewQueueEntry:
    """Persisted review-clearance work item."""

    issue_ref: str
    pr_url: str
    pr_repo: str
    pr_number: int
    source_session_id: str | None
    enqueued_at: str
    updated_at: str
    next_attempt_at: str
    last_attempt_at: str | None
    attempt_count: int
    last_result: str | None
    last_reason: str | None
    last_state_digest: str | None
    blocked_streak: int = 0
    blocked_class: str | None = None

    def next_attempt_datetime(self) -> datetime:
        """Return the next-attempt timestamp normalized to UTC."""
        parsed = datetime.fromisoformat(self.next_attempt_at)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class CheckObservation:
    """Latest observed GitHub check state for one check/context name."""

    name: str
    result: str  # pass|pending|fail|cancelled
    status: str = ""
    conclusion: str = ""
    details_url: str = ""
    workflow_name: str = ""
    run_id: int | None = None


@dataclass(frozen=True)
class PrGateStatus:
    """Gate readiness snapshot for autonomous merge decisions."""

    required: set[str]
    passed: set[str]
    failed: set[str]
    pending: set[str]
    cancelled: set[str]
    merge_state_status: str
    mergeable: str
    is_draft: bool
    state: str
    auto_merge_enabled: bool
    checks: dict[str, CheckObservation] = field(default_factory=dict)


@dataclass(frozen=True)
class OpenPullRequest:
    """Minimal open-PR snapshot used for review reconciliation scans."""

    number: int
    url: str
    head_ref_name: str
    is_draft: bool
    body: str = ""
    author: str = ""


# ---------------------------------------------------------------------------
# New port contract types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProjectItemSnapshot:
    """Thin project-board item view used by scheduling and snapshot reads."""

    issue_ref: str
    status: str
    executor: str
    handoff_to: str
    priority: str = ""
    item_id: str = ""
    project_id: str = ""
    sprint: str = ""
    agent: str = ""
    owner_field: str = ""
    title: str = ""
    body: str = ""
    repo_slug: str = ""
    repo_name: str = ""
    repo_owner: str = ""
    issue_number: int = 0
    issue_updated_at: str = ""


@dataclass(frozen=True)
class CycleBoardSnapshot:
    """Thin per-cycle view of board items reused across hot-path phases."""

    items: tuple[ProjectItemSnapshot, ...]
    by_status: dict[str, tuple[ProjectItemSnapshot, ...]] = field(default_factory=dict)

    def items_with_status(self, status: str) -> tuple[ProjectItemSnapshot, ...]:
        """Return cached items in the given status."""
        return self.by_status.get(status, ())


@dataclass(frozen=True)
class IssueSnapshot:
    """Typed board state for one issue — returned by ReviewStatePort."""

    issue_ref: str
    status: str
    executor: str
    priority: str
    title: str
    item_id: str
    project_id: str


@dataclass(frozen=True)
class IssueFields:
    """Typed field bundle for one issue — returned by ReviewStatePort."""

    issue_ref: str
    status: str
    priority: str
    sprint: str
    executor: str
    owner: str
    handoff_to: str
    blocked_reason: str
