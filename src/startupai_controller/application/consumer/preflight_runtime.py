"""Preflight/runtime orchestration for the consumer application layer.

This module contains the real bodies of the per-cycle preflight phases:
runtime initialization, board snapshot loading, deferred replay, executor
routing, reconciliation, review queue drain, admission, and the top-level
prepare_cycle orchestrator.

board_consumer.py retains thin compatibility wrappers that inject concrete
dependencies and delegate here.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import (
    GitHubRuntimeMemo,
    PreparedCycleContext,
)
from startupai_controller.consumer_workflow import (
    WorkflowDefinition,
    WorkflowRepoStatus,
)
from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    ReviewQueueDrainSummary,
)
from startupai_controller.payload_types import AdmissionSummaryPayload
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.control_plane_state import ControlValueStorePort
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.process_runner import GhRunnerPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.ready_flow import ReadyFlowPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    IssueRef,
)


class CurrentMainWorkflowsFn(Protocol):
    """Load canonical workflow definitions and statuses for one cycle."""

    def __call__(
        self,
        config: ConsumerConfig,
    ) -> tuple[dict[str, WorkflowDefinition], dict[str, WorkflowRepoStatus], int]:
        """Return workflows, statuses, and effective poll interval."""
        ...


class ReconcileBoardTruthResult(Protocol):
    """Board-truth reconciliation summary needed by preflight runtime."""

    moved_ready: tuple[str, ...]
    moved_in_progress: tuple[str, ...]
    moved_review: tuple[str, ...]
    moved_blocked: tuple[str, ...]


class GitHubRequestStatsView(Protocol):
    """Runtime GitHub request counters emitted at the end of preflight."""

    graphql: int
    rest: int
    retries: int
    cli_fallbacks: int
    latency_le_250_ms: int
    latency_le_1000_ms: int
    latency_gt_1000_ms: int
    error_counts: dict[str, int]


class ExecutePrepareCyclePhasesResult(Protocol):
    """Prepared phase execution result consumed by preflight runtime."""

    board_snapshot: CycleBoardSnapshot
    review_queue_summary: ReviewQueueDrainSummary
    admission_summary: AdmissionSummaryPayload
    timings_ms: dict[str, int]


class ExecutePrepareCyclePhasesFn(Protocol):
    """Run the preflight phases for one cycle."""

    def __call__(
        self,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        *,
        runtime: CycleRuntimeContext,
        deps: object,
        dry_run: bool = False,
    ) -> ExecutePrepareCyclePhasesResult:
        """Return board snapshot, review queue, admission, and timing results."""
        ...


# ---------------------------------------------------------------------------
# Cycle runtime context (moved from board_consumer.py)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CycleRuntimeContext:
    """Cycle-scoped runtime wiring and configuration."""

    session_store: SessionStorePort
    cp_config: CriticalPathConfig
    auto_config: BoardAutomationConfig | None
    main_workflows: dict[str, WorkflowDefinition]
    workflow_statuses: dict[str, WorkflowRepoStatus]
    dispatchable_repo_prefixes: tuple[str, ...]
    effective_interval: int
    global_limit: int
    github_memo: GitHubRuntimeMemo
    ready_flow_port: ReadyFlowPort
    pr_port: PullRequestPort
    review_state_port: ReviewStatePort
    board_port: BoardMutationPort
    gh_port: GhRunnerPort | None


# ---------------------------------------------------------------------------
# Deps dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class InitializeCycleRuntimeDeps:
    """Injected seams for building cycle-scoped runtime context."""

    load_automation_config: Callable[[Path], BoardAutomationConfig]
    apply_automation_runtime: Callable[
        [ConsumerConfig, BoardAutomationConfig | None], None
    ]
    current_main_workflows: CurrentMainWorkflowsFn
    config_error_type: type[Exception]
    logger: logging.Logger


@dataclass(frozen=True)
class PhaseHelperDeps:
    """Injected seams shared across individual phase functions."""

    replay_deferred_actions: Callable[..., tuple[int, ...]]
    drain_review_queue: Callable[
        ...,
        tuple[ReviewQueueDrainSummary, CycleBoardSnapshot],
    ]
    reconcile_board_truth: Callable[..., ReconcileBoardTruthResult]
    record_successful_github_mutation: Callable[[ControlValueStorePort], None]
    record_successful_board_sync: Callable[[ControlValueStorePort], None]
    clear_degraded: Callable[[ControlValueStorePort], None]
    mark_degraded: Callable[[ControlValueStorePort, str], None]
    persist_admission_summary: Callable[
        [ControlValueStorePort, AdmissionSummaryPayload],
        None,
    ]
    logger: logging.Logger


@dataclass(frozen=True)
class PrepareCycleDeps:
    """Injected seams for the top-level _prepare_cycle orchestrator."""

    initialize_cycle_runtime: Callable[..., CycleRuntimeContext]
    phase_helper_deps: PhaseHelperDeps
    begin_runtime_request_stats: Callable[[], object]
    end_runtime_request_stats: Callable[[object], GitHubRequestStatsView]
    snapshot_to_issue_ref: Callable[[str, dict[str, str]], str | None]
    parse_issue_ref: Callable[[str], IssueRef]
    record_metric: Callable[..., None]
    control_key_degraded: str
    prepare_cycle_phases_deps_factory: Callable[..., object]
    execute_prepare_cycle_phases: ExecutePrepareCyclePhasesFn


# ---------------------------------------------------------------------------
# Phase implementations
# ---------------------------------------------------------------------------


def initialize_cycle_runtime(
    config: ConsumerConfig,
    *,
    deps: InitializeCycleRuntimeDeps,
    session_store: SessionStorePort,
    cp_config: CriticalPathConfig,
    github_memo: GitHubRuntimeMemo,
    ready_flow_port: ReadyFlowPort,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    gh_port: GhRunnerPort | None = None,
) -> CycleRuntimeContext:
    """Build cycle-scoped runtime wiring and effective config."""
    try:
        auto_config = deps.load_automation_config(config.automation_config_path)
    except deps.config_error_type as err:
        deps.logger.warning("Automation config error (proceeding without): %s", err)
        auto_config = None
    deps.apply_automation_runtime(config, auto_config)

    main_workflows, workflow_statuses, effective_interval = deps.current_main_workflows(
        config
    )
    dispatchable_repo_prefixes = tuple(
        repo_prefix
        for repo_prefix in config.repo_prefixes
        if workflow_statuses[repo_prefix].available
    )
    config.poll_interval_seconds = effective_interval
    global_limit = (
        auto_config.global_concurrency
        if auto_config is not None
        else config.global_concurrency
    )

    return CycleRuntimeContext(
        session_store=session_store,
        cp_config=cp_config,
        auto_config=auto_config,
        main_workflows=main_workflows,
        workflow_statuses=workflow_statuses,
        dispatchable_repo_prefixes=dispatchable_repo_prefixes,
        effective_interval=effective_interval,
        global_limit=global_limit,
        github_memo=github_memo,
        ready_flow_port=ready_flow_port,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        gh_port=gh_port,
    )


def run_deferred_replay_phase(
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    runtime: CycleRuntimeContext,
    *,
    deps: PhaseHelperDeps,
    timings_ms: dict[str, int],
    dry_run: bool,
) -> None:
    """Replay deferred actions for the cycle when enabled."""
    if not config.deferred_replay_enabled or dry_run:
        return
    phase_started = time.monotonic()
    replayed_actions = deps.replay_deferred_actions(
        db,
        config,
        runtime.cp_config,
        pr_port=runtime.pr_port,
        review_state_port=runtime.review_state_port,
        board_port=runtime.board_port,
        gh_runner=runtime.gh_port.run_gh if runtime.gh_port is not None else None,
    )
    timings_ms["deferred_replay"] = int((time.monotonic() - phase_started) * 1000)
    if replayed_actions:
        deps.logger.info(
            "Replayed deferred control-plane actions: %s", replayed_actions
        )


def load_board_snapshot_phase(
    config: ConsumerConfig,
    runtime: CycleRuntimeContext,
    *,
    timings_ms: dict[str, int],
) -> CycleBoardSnapshot:
    """Load the cycle board snapshot."""
    phase_started = time.monotonic()
    board_snapshot = runtime.review_state_port.build_board_snapshot()
    timings_ms["board_snapshot"] = int((time.monotonic() - phase_started) * 1000)
    return board_snapshot


def run_executor_routing_phase(
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    runtime: CycleRuntimeContext,
    *,
    deps: PhaseHelperDeps,
    board_snapshot: CycleBoardSnapshot,
    timings_ms: dict[str, int],
    dry_run: bool,
) -> None:
    """Normalize executor routing for the protected queue."""
    phase_started = time.monotonic()
    routing_decision = runtime.ready_flow_port.route_protected_queue_executors(
        runtime.cp_config,
        runtime.auto_config,
        config.project_owner,
        config.project_number,
        dry_run=dry_run,
        board_snapshot=board_snapshot,
        gh_runner=runtime.gh_port.run_gh if runtime.gh_port is not None else None,
    )
    timings_ms["executor_routing"] = int((time.monotonic() - phase_started) * 1000)
    if routing_decision.routed:
        if not dry_run:
            deps.record_successful_github_mutation(db)
        deps.logger.info(
            "Executor routing normalized protected queue: %s",
            routing_decision.routed,
        )


def run_reconciliation_phase(
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    runtime: CycleRuntimeContext,
    *,
    deps: PhaseHelperDeps,
    board_snapshot: CycleBoardSnapshot,
    timings_ms: dict[str, int],
) -> None:
    """Run truthful board reconciliation for the cycle."""
    phase_started = time.monotonic()
    reconciliation = deps.reconcile_board_truth(
        config,
        runtime.cp_config,
        runtime.auto_config,
        db,
        session_store=runtime.session_store,
        pr_port=runtime.pr_port,
        review_state_port=runtime.review_state_port,
        board_port=runtime.board_port,
        board_snapshot=board_snapshot,
        gh_runner=runtime.gh_port.run_gh if runtime.gh_port is not None else None,
    )
    timings_ms["reconciliation"] = int((time.monotonic() - phase_started) * 1000)
    deps.record_successful_board_sync(db)
    deps.clear_degraded(db)
    if (
        reconciliation.moved_ready
        or reconciliation.moved_in_progress
        or reconciliation.moved_review
        or reconciliation.moved_blocked
    ):
        deps.logger.info("Board reconciliation: %s", reconciliation)


def run_review_queue_phase(
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    runtime: CycleRuntimeContext,
    *,
    deps: PhaseHelperDeps,
    board_snapshot: CycleBoardSnapshot,
    timings_ms: dict[str, int],
    dry_run: bool,
) -> tuple[ReviewQueueDrainSummary, CycleBoardSnapshot]:
    """Drain the review queue for the current cycle."""
    phase_started = time.monotonic()
    review_queue_summary, updated_snapshot = deps.drain_review_queue(
        config,
        db,
        runtime.cp_config,
        runtime.auto_config,
        pr_port=runtime.pr_port,
        session_store=runtime.session_store,
        board_snapshot=board_snapshot,
        dry_run=dry_run,
        github_memo=runtime.github_memo,
        gh_runner=runtime.gh_port.run_gh if runtime.gh_port is not None else None,
    )
    timings_ms["review_queue"] = int((time.monotonic() - phase_started) * 1000)
    if review_queue_summary.error:
        deps.logger.warning(
            "Review queue partial failure: %s", review_queue_summary.error
        )
        if not dry_run:
            deps.mark_degraded(
                db,
                f"review-queue:partial-failure:{review_queue_summary.error}",
            )
    if (
        review_queue_summary.seeded
        or review_queue_summary.removed
        or review_queue_summary.verdict_backfilled
        or review_queue_summary.rerun
        or review_queue_summary.auto_merge_enabled
        or review_queue_summary.requeued
        or review_queue_summary.blocked
        or review_queue_summary.skipped
    ):
        deps.logger.info("Review queue: %s", review_queue_summary)
    return review_queue_summary, updated_snapshot


def run_admission_phase(
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    runtime: CycleRuntimeContext,
    *,
    deps: PhaseHelperDeps,
    board_snapshot: CycleBoardSnapshot,
    timings_ms: dict[str, int],
    dry_run: bool,
) -> AdmissionSummaryPayload:
    """Run backlog admission for the current cycle."""
    phase_started = time.monotonic()
    admission_decision = runtime.ready_flow_port.admit_backlog_items(
        runtime.cp_config,
        runtime.auto_config,
        config.project_owner,
        config.project_number,
        dispatchable_repo_prefixes=runtime.dispatchable_repo_prefixes,
        active_lease_issue_refs=tuple(db.active_lease_issue_refs()),
        dry_run=dry_run,
        board_snapshot=board_snapshot,
        github_memo=runtime.github_memo,
        gh_runner=runtime.gh_port.run_gh if runtime.gh_port is not None else None,
    )
    timings_ms["admission"] = int((time.monotonic() - phase_started) * 1000)
    admission_summary = runtime.ready_flow_port.admission_summary_payload(
        admission_decision,
        enabled=bool(
            runtime.auto_config is not None and runtime.auto_config.admission.enabled
        ),
    )
    if admission_decision.admitted:
        if not dry_run:
            deps.record_successful_github_mutation(db)
        deps.logger.info(
            "Backlog admission admitted: %s", list(admission_decision.admitted)
        )
    if admission_decision.partial_failure and admission_decision.error:
        deps.logger.warning(
            "Backlog admission partial failure: %s", admission_decision.error
        )
    if not dry_run:
        deps.persist_admission_summary(db, admission_summary)
    return admission_summary


# ---------------------------------------------------------------------------
# Top-level prepare_cycle orchestrator
# ---------------------------------------------------------------------------


def prepare_cycle(
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    *,
    deps: PrepareCycleDeps,
    prepared_cycle_context_factory: type[PreparedCycleContext],
    dry_run: bool = False,
    gh_runner: GhRunnerPort | None = None,
) -> PreparedCycleContext:
    """Run control-plane preflight once for a daemon tick."""
    request_stats_token = deps.begin_runtime_request_stats()
    expired = db.expire_stale_leases(config.heartbeat_expiry_seconds)
    if expired:
        deps.phase_helper_deps.logger.info("Expired stale leases: %s", expired)

    runtime = deps.initialize_cycle_runtime(
        config,
        db,
        gh_runner=gh_runner,
    )

    phase_deps = deps.phase_helper_deps

    # Build the preflight phases deps using the phase functions from this module,
    # wrapped to inject the PhaseHelperDeps.
    def _deferred_replay_phase(config, db, runtime, *, timings_ms, dry_run):
        return run_deferred_replay_phase(
            config,
            db,
            runtime,
            deps=phase_deps,
            timings_ms=timings_ms,
            dry_run=dry_run,
        )

    def _board_snapshot_phase(config, runtime, *, timings_ms):
        return load_board_snapshot_phase(
            config,
            runtime,
            timings_ms=timings_ms,
        )

    def _executor_routing_phase(
        config, db, runtime, *, board_snapshot, timings_ms, dry_run
    ):
        return run_executor_routing_phase(
            config,
            db,
            runtime,
            deps=phase_deps,
            board_snapshot=board_snapshot,
            timings_ms=timings_ms,
            dry_run=dry_run,
        )

    def _reconciliation_phase(config, db, runtime, *, board_snapshot, timings_ms):
        return run_reconciliation_phase(
            config,
            db,
            runtime,
            deps=phase_deps,
            board_snapshot=board_snapshot,
            timings_ms=timings_ms,
        )

    def _review_queue_phase(
        config, db, runtime, *, board_snapshot, timings_ms, dry_run
    ):
        return run_review_queue_phase(
            config,
            db,
            runtime,
            deps=phase_deps,
            board_snapshot=board_snapshot,
            timings_ms=timings_ms,
            dry_run=dry_run,
        )

    def _admission_phase(config, db, runtime, *, board_snapshot, timings_ms, dry_run):
        return run_admission_phase(
            config,
            db,
            runtime,
            deps=phase_deps,
            board_snapshot=board_snapshot,
            timings_ms=timings_ms,
            dry_run=dry_run,
        )

    prepare_phases_deps = deps.prepare_cycle_phases_deps_factory(
        run_deferred_replay_phase=_deferred_replay_phase,
        load_board_snapshot_phase=_board_snapshot_phase,
        run_executor_routing_phase=_executor_routing_phase,
        run_reconciliation_phase=_reconciliation_phase,
        run_review_queue_phase=_review_queue_phase,
        run_admission_phase=_admission_phase,
    )

    result = deps.execute_prepare_cycle_phases(
        config,
        db,
        runtime=runtime,
        deps=prepare_phases_deps,
        dry_run=dry_run,
    )

    board_snapshot = result.board_snapshot
    review_queue_summary = result.review_queue_summary
    admission_summary = result.admission_summary
    timings_ms = result.timings_ms

    request_stats = deps.end_runtime_request_stats(request_stats_token)
    github_request_counts = {
        "graphql": request_stats.graphql,
        "rest": request_stats.rest,
    }
    total_preflight_ms = sum(timings_ms.values())
    if total_preflight_ms >= 5000:
        phase_deps.logger.info(
            "Preflight timings ms=%s github_requests=%s",
            timings_ms,
            github_request_counts,
        )
    if not dry_run:
        ready_for_executor = 0
        for snapshot in board_snapshot.items_with_status("Ready"):
            if snapshot.executor.strip().lower() != config.executor:
                continue
            issue_ref = deps.snapshot_to_issue_ref(
                snapshot.issue_ref, runtime.cp_config.issue_prefixes
            )
            if issue_ref is None:
                continue
            if (
                deps.parse_issue_ref(issue_ref).prefix
                not in runtime.dispatchable_repo_prefixes
            ):
                continue
            ready_for_executor += 1
        deps.record_metric(
            db,
            config,
            "cycle_observation",
            payload={
                "ready_for_executor": ready_for_executor,
                "active_leases": db.active_lease_count(),
                "global_limit": runtime.global_limit,
                "degraded": db.get_control_value(deps.control_key_degraded) == "true",
            },
        )
        if any(
            (
                request_stats.graphql,
                request_stats.rest,
                request_stats.retries,
                request_stats.cli_fallbacks,
                request_stats.latency_le_250_ms,
                request_stats.latency_le_1000_ms,
                request_stats.latency_gt_1000_ms,
                bool(request_stats.error_counts),
            )
        ):
            deps.record_metric(
                db,
                config,
                "github_transport_observation",
                payload={
                    "graphql_requests": request_stats.graphql,
                    "rest_requests": request_stats.rest,
                    "retry_attempts": request_stats.retries,
                    "cli_fallbacks": request_stats.cli_fallbacks,
                    "latency_le_250_ms": request_stats.latency_le_250_ms,
                    "latency_le_1000_ms": request_stats.latency_le_1000_ms,
                    "latency_gt_1000_ms": request_stats.latency_gt_1000_ms,
                    "error_counts": dict(sorted(request_stats.error_counts.items())),
                },
            )

    return prepared_cycle_context_factory(
        cp_config=runtime.cp_config,
        auto_config=runtime.auto_config,
        main_workflows=runtime.main_workflows,
        workflow_statuses=runtime.workflow_statuses,
        dispatchable_repo_prefixes=runtime.dispatchable_repo_prefixes,
        effective_interval=runtime.effective_interval,
        global_limit=runtime.global_limit,
        board_snapshot=board_snapshot,
        github_memo=runtime.github_memo,
        ready_flow_port=runtime.ready_flow_port,
        admission_summary=admission_summary,
        review_queue_summary=review_queue_summary,
        timings_ms=timings_ms,
        github_request_counts=github_request_counts,
    )
