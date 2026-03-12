"""Daemon helper orchestration for the consumer application layer."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import Any, Callable, Protocol, cast

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import (
    ActiveWorkerTask,
    PreparedCycleContext,
    PreparedLaunchContext,
)
from startupai_controller.consumer_workflow import (
    WorkflowDefinition,
    WorkflowRepoStatus,
)
from startupai_controller.application.consumer.recovery import (
    RecoveredLeaseInfo,
    RecoveryStatePort,
)
from startupai_controller.domain.models import CycleResult
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.process_runner import GhRunnerPort, ProcessRunnerPort


@dataclass(frozen=True)
class DaemonRuntime:
    """Typed runtime capabilities needed by the daemon application flow."""

    gh_runner: GhRunnerPort | None = None
    process_runner: ProcessRunnerPort | None = None
    file_reader: Callable[[Path], str] | None = None


class LoggerPort(Protocol):
    """Minimal logger surface consumed by the daemon flow."""

    def info(self, msg: str, *args: object) -> None: ...

    def error(self, msg: str, *args: object) -> None: ...

    def exception(self, msg: str, *args: object) -> None: ...


class WorkerFuture(Protocol):
    """Future surface needed for worker bookkeeping."""

    def done(self) -> bool: ...

    def result(self) -> CycleResult: ...


class WorkerExecutor(Protocol):
    """Executor surface used by the multi-worker daemon."""

    def __enter__(self) -> WorkerExecutor: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object | None,
    ) -> bool | None: ...

    def submit(
        self,
        fn: Callable[..., CycleResult],
        /,
        *args: object,
        **kwargs: object,
    ) -> WorkerFuture: ...


class RecoverInterruptedStatePort(
    ConsumerRuntimeStatePort,
    RecoveryStatePort,
    Protocol,
):
    """Runtime DB surface required for interrupted-session recovery."""


class PrepareCycleFn(Protocol):
    """Prepare one consumer cycle against runtime state storage."""

    def __call__(
        self,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        *,
        dry_run: bool = False,
        gh_runner: GhRunnerPort | Callable[..., str] | None = None,
    ) -> PreparedCycleContext: ...


class MarkDegradedFn(Protocol):
    """Persist a degraded-state marker for the consumer runtime."""

    def __call__(self, db: ConsumerRuntimeStatePort, reason: str) -> None: ...


class PrepareLaunchCandidateFn(Protocol):
    """Hydrate one candidate into launch context."""

    def __call__(
        self,
        issue_ref: str,
        *,
        config: ConsumerConfig,
        prepared: PreparedCycleContext,
        db: ConsumerRuntimeStatePort,
        runtime: DaemonRuntime | None = None,
    ) -> PreparedLaunchContext: ...


class MaybeActivateClaimSuppressionFn(Protocol):
    """Enable temporary claim suppression after a transient failure."""

    def __call__(
        self,
        db: ConsumerRuntimeStatePort,
        config: ConsumerConfig,
        *,
        scope: str,
        error: Exception,
        now: datetime | None = None,
    ) -> bool: ...


class SelectCandidateForCycleFn(Protocol):
    """Select the next launch candidate for the current cycle."""

    def __call__(
        self,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        *,
        target_issue: str | None = None,
        runtime: DaemonRuntime | None = None,
        excluded_issue_refs: set[str] | None = None,
    ) -> str | None: ...


class NextAvailableSlotsFn(Protocol):
    """Return the currently available worker slots."""

    def __call__(
        self,
        db: ConsumerRuntimeStatePort,
        limit: int,
        *,
        reserved_slots: set[int] | None = None,
    ) -> list[int]: ...


class ClaimSuppressionStateFn(Protocol):
    """Read the current claim-suppression state from runtime storage."""

    def __call__(
        self,
        db: ConsumerRuntimeStatePort,
        *,
        now: datetime | None = None,
    ) -> dict[str, str] | None: ...


class RecoverInterruptedSessionsFn(Protocol):
    """Recover interrupted sessions using the current runtime state."""

    def __call__(
        self,
        config: ConsumerConfig,
        db: RecoverInterruptedStatePort,
        *,
        automation_config: BoardAutomationConfig | None = None,
        runtime: DaemonRuntime | None = None,
    ) -> list[RecoveredLeaseInfo]: ...


class RunOneCycleFn(Protocol):
    """Run one single-worker consumer cycle."""

    def __call__(
        self,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        *,
        dry_run: bool = False,
        runtime: DaemonRuntime | None = None,
    ) -> CycleResult: ...


class RecordMetricFn(Protocol):
    """Persist one consumer runtime metric."""

    def __call__(
        self,
        db: ConsumerRuntimeStatePort,
        config: ConsumerConfig,
        metric_name: str,
        *,
        issue_ref: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None: ...


class BlockPrelaunchIssueFn(Protocol):
    """Move a launch candidate into Blocked before claim."""

    def __call__(
        self,
        issue_ref: str,
        blocked_reason: str,
        *,
        config: ConsumerConfig,
        cp_config: object,
        db: ConsumerRuntimeStatePort,
        runtime: DaemonRuntime | None = None,
    ) -> None: ...


class PrepareMultiWorkerLaunchContextFn(Protocol):
    """Prepare one launch context for a multi-worker dispatch pass."""

    def __call__(
        self,
        candidate: str,
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        dry_run: bool,
        runtime: DaemonRuntime | None = None,
    ) -> tuple[PreparedLaunchContext | None, bool]: ...


class SubmitMultiWorkerTaskFn(Protocol):
    """Submit one worker task to the executor."""

    def __call__(
        self,
        executor: WorkerExecutor,
        active_tasks: dict[WorkerFuture, ActiveWorkerTask],
        *,
        config: ConsumerConfig,
        candidate: str,
        slot_id: int,
        prepared: PreparedCycleContext,
        launch_context: PreparedLaunchContext | None,
        dry_run: bool,
        runtime: DaemonRuntime | None = None,
    ) -> None: ...


class PrepareMultiWorkerCycleFn(Protocol):
    """Prepare one multi-worker preflight pass."""

    def __call__(
        self,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        *,
        dry_run: bool,
        sleeper: Callable[[float], None],
        runtime: DaemonRuntime | None,
    ) -> PreparedCycleContext | None: ...


class MultiWorkerDispatchStateFn(Protocol):
    """Compute multi-worker slot availability and issue occupancy."""

    def __call__(
        self,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        active_tasks: dict[WorkerFuture, ActiveWorkerTask],
    ) -> tuple[list[int], set[str]]: ...


class SleepForClaimSuppressionFn(Protocol):
    """Sleep through an active claim-suppression window."""

    def __call__(
        self,
        db: ConsumerRuntimeStatePort,
        config: ConsumerConfig,
        *,
        sleeper: Callable[[float], None],
    ) -> bool: ...


class DispatchMultiWorkerLaunchesFn(Protocol):
    """Dispatch ready candidates across available worker slots."""

    def __call__(
        self,
        executor: WorkerExecutor,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        *,
        prepared: PreparedCycleContext,
        available_slots: list[int],
        active_issue_refs: set[str],
        active_tasks: dict[WorkerFuture, ActiveWorkerTask],
        dry_run: bool,
        runtime: DaemonRuntime | None = None,
    ) -> int: ...


class ExecutorFactoryFn(Protocol):
    """Build the worker executor used by the daemon loop."""

    def __call__(self, *args: object, **kwargs: object) -> WorkerExecutor: ...


class LoadAutomationConfigFn(Protocol):
    """Load the board automation config from disk."""

    def __call__(self, path: Path) -> BoardAutomationConfig: ...


class ApplyAutomationRuntimeFn(Protocol):
    """Apply automation-derived runtime settings to the consumer config."""

    def __call__(
        self,
        config: ConsumerConfig,
        automation_config: BoardAutomationConfig | None,
    ) -> None: ...


class CurrentMainWorkflowsFn(Protocol):
    """Load workflow definitions and status for the main branch."""

    def __call__(
        self,
        config: ConsumerConfig,
        *,
        persist_snapshot: bool = False,
    ) -> tuple[dict[str, WorkflowDefinition], dict[str, WorkflowRepoStatus], int]: ...


class RunMultiWorkerDaemonLoopFn(Protocol):
    """Run the continuous multi-worker daemon loop."""

    def __call__(
        self,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        *,
        dry_run: bool = False,
        sleep_fn: Callable[[float], None] | None = None,
        runtime: DaemonRuntime | None = None,
    ) -> None: ...


@dataclass(frozen=True)
class PrepareMultiWorkerCycleDeps:
    """Injected seams for one multi-worker preflight attempt."""

    config_error_type: type[Exception]
    workflow_config_error_type: type[Exception]
    gh_query_error_type: type[Exception]
    prepare_cycle: PrepareCycleFn
    mark_degraded: MarkDegradedFn
    gh_reason_code: Callable[[Exception], str]
    logger: LoggerPort


@dataclass(frozen=True)
class PrepareMultiWorkerLaunchContextDeps:
    """Injected seams for hydrating one worker launch context."""

    gh_query_error_type: type[Exception]
    workflow_config_error_type: type[Exception]
    worktree_prepare_error_type: type[Exception]
    record_metric: RecordMetricFn
    prepare_launch_candidate: PrepareLaunchCandidateFn
    maybe_activate_claim_suppression: MaybeActivateClaimSuppressionFn
    mark_degraded: MarkDegradedFn
    gh_reason_code: Callable[[Exception], str]
    block_prelaunch_issue: BlockPrelaunchIssueFn


class WorktreePrepareErrorInfo(Protocol):
    """Structured worktree-prepare failure surfaced by outer layers."""

    reason_code: str
    detail: str


@dataclass(frozen=True)
class DispatchMultiWorkerLaunchesDeps:
    """Injected seams for launching multiple worker candidates."""

    gh_query_error_type: type[Exception]
    select_candidate_for_cycle: SelectCandidateForCycleFn
    prepare_multi_worker_launch_context: PrepareMultiWorkerLaunchContextFn
    submit_multi_worker_task: SubmitMultiWorkerTaskFn
    mark_degraded: MarkDegradedFn
    gh_reason_code: Callable[[Exception], str]
    logger: LoggerPort


@dataclass(frozen=True)
class RunMultiWorkerDaemonLoopDeps:
    """Injected seams for the multi-worker daemon loop."""

    executor_factory: ExecutorFactoryFn
    log_completed_worker_results: Callable[[dict[WorkerFuture, ActiveWorkerTask]], None]
    drain_requested: Callable[[Path], bool]
    prepare_multi_worker_cycle: PrepareMultiWorkerCycleFn
    multi_worker_dispatch_state: MultiWorkerDispatchStateFn
    sleep_for_claim_suppression_if_needed: SleepForClaimSuppressionFn
    dispatch_multi_worker_launches: DispatchMultiWorkerLaunchesFn
    logger: LoggerPort


@dataclass(frozen=True)
class RunDaemonLoopDeps:
    """Injected seams for the outer consumer daemon loop."""

    config_error_type: type[Exception]
    load_automation_config: LoadAutomationConfigFn
    apply_automation_runtime: ApplyAutomationRuntimeFn
    current_main_workflows: CurrentMainWorkflowsFn
    recover_interrupted_sessions: RecoverInterruptedSessionsFn
    run_multi_worker_daemon_loop: RunMultiWorkerDaemonLoopFn
    drain_requested: Callable[[Path], bool]
    run_one_cycle: RunOneCycleFn
    logger: LoggerPort


def next_available_slots(
    db: ConsumerRuntimeStatePort,
    limit: int,
    *,
    reserved_slots: set[int] | None = None,
) -> list[int]:
    """Return deterministic lowest-available slot ids."""
    occupied = set(db.active_slot_ids())
    if reserved_slots:
        occupied.update(reserved_slots)
    return [slot_id for slot_id in range(1, limit + 1) if slot_id not in occupied]


def log_completed_worker_results(
    active_tasks: dict[WorkerFuture, ActiveWorkerTask],
    *,
    logger: LoggerPort,
) -> None:
    """Log and discard completed worker futures."""
    for future, task in list(active_tasks.items()):
        if not future.done():
            continue
        del active_tasks[future]
        try:
            result = future.result()
            logger.info(
                "Worker result [slot=%s issue=%s]: %s",
                task.slot_id,
                task.issue_ref,
                result,
            )
        except Exception:
            logger.exception(
                "Unhandled worker failure [slot=%s issue=%s]",
                task.slot_id,
                task.issue_ref,
            )


def prepare_multi_worker_cycle(
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    *,
    dry_run: bool,
    sleeper: Callable[[float], None],
    runtime: DaemonRuntime | None,
    deps: PrepareMultiWorkerCycleDeps,
) -> PreparedCycleContext | None:
    """Run one bounded preflight pass for the multi-worker daemon."""
    try:
        return deps.prepare_cycle(
            config,
            db,
            dry_run=dry_run,
            gh_runner=runtime.gh_runner if runtime is not None else None,
        )
    except deps.config_error_type:
        deps.logger.exception("Config error during multi-worker cycle")
    except deps.workflow_config_error_type:
        deps.logger.exception("Workflow config error during multi-worker cycle")
    except deps.gh_query_error_type as err:
        deps.logger.error("Multi-worker preflight failed: %s", err)
        deps.mark_degraded(db, f"control-plane:{deps.gh_reason_code(err)}:{err}")
    sleeper(config.poll_interval_seconds)
    return None


def multi_worker_dispatch_state(
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    active_tasks: dict[WorkerFuture, ActiveWorkerTask],
    *,
    next_available_slots_fn: NextAvailableSlotsFn,
) -> tuple[list[int], set[str]]:
    """Compute currently available slots and active issue refs."""
    reserved_slots = {task.slot_id for task in active_tasks.values()}
    active_issue_refs = {task.issue_ref for task in active_tasks.values()}
    active_issue_refs.update(worker.issue_ref for worker in db.active_workers())
    available_slots = next_available_slots_fn(
        db,
        prepared.global_limit,
        reserved_slots=reserved_slots,
    )
    return available_slots, active_issue_refs


def sleep_for_claim_suppression_if_needed(
    db: ConsumerRuntimeStatePort,
    config: ConsumerConfig,
    *,
    sleeper: Callable[[float], None],
    claim_suppression_state: ClaimSuppressionStateFn,
    parse_iso8601_timestamp: Callable[[str], datetime | None],
) -> bool:
    """Sleep until claim suppression clears, if active."""
    suppression_state = claim_suppression_state(db)
    if suppression_state is None:
        return False
    until = parse_iso8601_timestamp(suppression_state["until"])
    if until is None:
        sleeper(config.poll_interval_seconds)
        return True
    remaining = max(
        1.0,
        (until - datetime.now(timezone.utc)).total_seconds(),
    )
    sleeper(min(float(config.poll_interval_seconds), remaining))
    return True


def prepare_multi_worker_launch_context(
    candidate: str,
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    dry_run: bool,
    runtime: DaemonRuntime | None,
    deps: PrepareMultiWorkerLaunchContextDeps,
) -> tuple[PreparedLaunchContext | None, bool]:
    """Prepare launch context for one candidate.

    Returns ``(launch_context, stop_dispatch)``.
    """
    if dry_run:
        return None, False
    deps.record_metric(db, config, "candidate_selected", issue_ref=candidate)
    try:
        return (
            deps.prepare_launch_candidate(
                candidate,
                config=config,
                prepared=prepared,
                db=db,
                runtime=runtime,
            ),
            False,
        )
    except deps.gh_query_error_type as err:
        deps.record_metric(
            db,
            config,
            "context_hydration_failed",
            issue_ref=candidate,
            payload={"reason": deps.gh_reason_code(err), "detail": str(err)},
        )
        if not deps.maybe_activate_claim_suppression(
            db,
            config,
            scope="hydration",
            error=err,
        ):
            deps.mark_degraded(db, f"launch-prep:{deps.gh_reason_code(err)}:{err}")
        return None, True
    except deps.workflow_config_error_type as err:
        deps.block_prelaunch_issue(
            candidate,
            f"workflow-config:{err}",
            config=config,
            cp_config=prepared.cp_config,
            db=db,
            runtime=runtime,
        )
        deps.record_metric(
            db,
            config,
            "worker_start_failed",
            issue_ref=candidate,
            payload={"reason": "workflow_config_error", "detail": str(err)},
        )
        return None, False
    except deps.worktree_prepare_error_type as err:
        typed_err = cast(WorktreePrepareErrorInfo, err)
        deps.record_metric(
            db,
            config,
            "worker_start_failed",
            issue_ref=candidate,
            payload={"reason": typed_err.reason_code, "detail": typed_err.detail},
        )
        return None, False
    except RuntimeError as err:
        deps.block_prelaunch_issue(
            candidate,
            f"workflow-hook:{err}",
            config=config,
            cp_config=prepared.cp_config,
            db=db,
            runtime=runtime,
        )
        deps.record_metric(
            db,
            config,
            "worker_start_failed",
            issue_ref=candidate,
            payload={"reason": "workflow_hook_error", "detail": str(err)},
        )
        return None, False


def submit_multi_worker_task(
    executor: WorkerExecutor,
    active_tasks: dict[WorkerFuture, ActiveWorkerTask],
    *,
    config: ConsumerConfig,
    candidate: str,
    slot_id: int,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext | None,
    dry_run: bool,
    runtime: DaemonRuntime | None,
    run_worker_cycle: Callable[..., CycleResult],
    active_worker_task_type: type[ActiveWorkerTask],
) -> None:
    """Submit one prepared candidate to a worker slot."""
    future = executor.submit(
        run_worker_cycle,
        replace(config),
        target_issue=candidate,
        slot_id=slot_id,
        prepared=prepared,
        launch_context=launch_context,
        dry_run=dry_run,
        runtime=runtime,
    )
    active_tasks[future] = active_worker_task_type(
        issue_ref=candidate,
        slot_id=slot_id,
        launched_at=datetime.now(timezone.utc).isoformat(),
    )


def dispatch_multi_worker_launches(
    executor: WorkerExecutor,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    *,
    prepared: PreparedCycleContext,
    available_slots: list[int],
    active_issue_refs: set[str],
    active_tasks: dict[WorkerFuture, ActiveWorkerTask],
    dry_run: bool,
    runtime: DaemonRuntime | None,
    deps: DispatchMultiWorkerLaunchesDeps,
) -> int:
    """Launch as many ready candidates as the current hydration budget allows."""
    launched = 0
    hydration_budget = max(1, config.launch_hydration_concurrency)
    for slot_id in available_slots:
        if launched >= hydration_budget and not dry_run:
            break
        try:
            candidate = deps.select_candidate_for_cycle(
                config,
                db,
                prepared,
                runtime=runtime,
                excluded_issue_refs=active_issue_refs,
            )
        except deps.gh_query_error_type as err:
            deps.logger.error("Ready-item selection failed: %s", err)
            deps.mark_degraded(db, f"selection-error:{deps.gh_reason_code(err)}:{err}")
            break
        if not candidate:
            break

        active_issue_refs.add(candidate)
        launch_context, stop_dispatch = deps.prepare_multi_worker_launch_context(
            candidate,
            config=config,
            db=db,
            prepared=prepared,
            dry_run=dry_run,
            runtime=runtime,
        )
        if stop_dispatch:
            break

        deps.submit_multi_worker_task(
            executor,
            active_tasks,
            config=config,
            candidate=candidate,
            slot_id=slot_id,
            prepared=prepared,
            launch_context=launch_context,
            dry_run=dry_run,
            runtime=runtime,
        )
        launched += 1
    return launched


def run_multi_worker_daemon_loop(
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    *,
    dry_run: bool = False,
    sleep_fn: Callable[[float], None] | None = None,
    runtime: DaemonRuntime | None = None,
    deps: RunMultiWorkerDaemonLoopDeps,
) -> None:
    """Run the daemon loop with multiple concurrent worker slots."""
    sleeper = sleep_fn or time.sleep
    active_tasks: dict[WorkerFuture, ActiveWorkerTask] = {}
    with deps.executor_factory(
        max_workers=max(1, config.global_concurrency)
    ) as executor:
        while True:
            deps.log_completed_worker_results(active_tasks)

            if deps.drain_requested(config.drain_path):
                if not active_tasks and db.active_lease_count() == 0:
                    deps.logger.info(
                        "Drain requested via %s; stopping after worker drain",
                        config.drain_path,
                    )
                    return
                sleeper(min(5.0, float(config.poll_interval_seconds)))
                continue

            prepared = deps.prepare_multi_worker_cycle(
                config,
                db,
                dry_run=dry_run,
                sleeper=sleeper,
                runtime=runtime,
            )
            if prepared is None:
                continue

            available_slots, active_issue_refs = deps.multi_worker_dispatch_state(
                db,
                prepared,
                active_tasks,
            )
            if deps.sleep_for_claim_suppression_if_needed(
                db,
                config,
                sleeper=sleeper,
            ):
                continue

            launched = deps.dispatch_multi_worker_launches(
                executor,
                config,
                db,
                prepared=prepared,
                available_slots=available_slots,
                active_issue_refs=active_issue_refs,
                active_tasks=active_tasks,
                dry_run=dry_run,
                runtime=runtime,
            )

            sleeper(1.0 if active_tasks or launched else config.poll_interval_seconds)


def run_daemon_loop(
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    *,
    dry_run: bool = False,
    sleep_fn: Callable[[float], None] | None = None,
    runtime: DaemonRuntime | None = None,
    deps: RunDaemonLoopDeps,
) -> None:
    """Run the continuous consumer daemon loop."""
    sleeper = sleep_fn or time.sleep
    try:
        auto_config = deps.load_automation_config(config.automation_config_path)
    except deps.config_error_type:
        auto_config = None
    deps.apply_automation_runtime(config, auto_config)
    try:
        _workflows, workflow_statuses, effective_interval = deps.current_main_workflows(
            config,
            persist_snapshot=False,
        )
        config.poll_interval_seconds = effective_interval
        repo_summary = ",".join(
            repo_prefix
            for repo_prefix, status in workflow_statuses.items()
            if status.available
        ) or ",".join(config.repo_prefixes)
    except Exception:
        repo_summary = ",".join(config.repo_prefixes)
    deps.logger.info(
        "Starting consumer daemon (interval=%ds, executor=%s, repos=%s, concurrency=%s)",
        config.poll_interval_seconds,
        config.executor,
        repo_summary,
        config.global_concurrency,
    )
    try:
        auto_config = None
        try:
            auto_config = deps.load_automation_config(config.automation_config_path)
        except deps.config_error_type:
            auto_config = None
        deps.apply_automation_runtime(config, auto_config)
        recovery_db = cast(RecoverInterruptedStatePort, db)
        recovered = deps.recover_interrupted_sessions(
            config,
            recovery_db,
            automation_config=auto_config,
            runtime=runtime,
        )
        if recovered:
            deps.logger.info(
                "Recovered interrupted leases: %s",
                [lease.issue_ref for lease in recovered],
            )
    except Exception:
        deps.logger.exception("Unhandled error recovering interrupted sessions")

    if config.multi_worker_enabled:
        deps.run_multi_worker_daemon_loop(
            config,
            db,
            dry_run=dry_run,
            sleep_fn=sleep_fn,
            runtime=runtime,
        )
        return

    while True:
        if deps.drain_requested(config.drain_path):
            deps.logger.info(
                "Drain requested via %s; stopping before next claim",
                config.drain_path,
            )
            return
        try:
            result = deps.run_one_cycle(config, db, dry_run=dry_run, runtime=runtime)
            deps.logger.info("Cycle result: %s", result)
        except Exception:
            deps.logger.exception("Unhandled error in cycle")

        sleeper(config.poll_interval_seconds)
