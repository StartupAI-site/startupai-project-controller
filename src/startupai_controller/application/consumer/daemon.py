"""Daemon helper orchestration for the consumer application layer."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import time
from typing import Any, Callable

from startupai_controller.ports.process_runner import GhRunnerPort, ProcessRunnerPort


@dataclass(frozen=True)
class DaemonRuntime:
    """Typed runtime capabilities needed by the daemon application flow."""

    gh_runner: GhRunnerPort | None = None
    process_runner: ProcessRunnerPort | None = None
    file_reader: Callable[..., Any] | None = None


@dataclass(frozen=True)
class PrepareMultiWorkerCycleDeps:
    """Injected seams for one multi-worker preflight attempt."""

    config_error_type: type[Exception]
    workflow_config_error_type: type[Exception]
    gh_query_error_type: type[Exception]
    prepare_cycle: Callable[..., Any]
    mark_degraded: Callable[[Any, str], None]
    gh_reason_code: Callable[[Exception], str]
    logger: Any


@dataclass(frozen=True)
class PrepareMultiWorkerLaunchContextDeps:
    """Injected seams for hydrating one worker launch context."""

    gh_query_error_type: type[Exception]
    workflow_config_error_type: type[Exception]
    worktree_prepare_error_type: type[Exception]
    record_metric: Callable[..., None]
    prepare_launch_candidate: Callable[..., Any]
    maybe_activate_claim_suppression: Callable[..., bool]
    mark_degraded: Callable[[Any, str], None]
    gh_reason_code: Callable[[Exception], str]
    block_prelaunch_issue: Callable[..., None]


@dataclass(frozen=True)
class DispatchMultiWorkerLaunchesDeps:
    """Injected seams for launching multiple worker candidates."""

    gh_query_error_type: type[Exception]
    select_candidate_for_cycle: Callable[..., str | None]
    prepare_multi_worker_launch_context: Callable[..., tuple[Any | None, bool]]
    submit_multi_worker_task: Callable[..., None]
    mark_degraded: Callable[[Any, str], None]
    gh_reason_code: Callable[[Exception], str]
    logger: Any


@dataclass(frozen=True)
class RunMultiWorkerDaemonLoopDeps:
    """Injected seams for the multi-worker daemon loop."""

    executor_factory: Callable[..., Any]
    log_completed_worker_results: Callable[..., None]
    drain_requested: Callable[[Any], bool]
    prepare_multi_worker_cycle: Callable[..., Any | None]
    multi_worker_dispatch_state: Callable[..., tuple[list[int], set[str]]]
    sleep_for_claim_suppression_if_needed: Callable[..., bool]
    dispatch_multi_worker_launches: Callable[..., int]
    logger: Any


@dataclass(frozen=True)
class RunDaemonLoopDeps:
    """Injected seams for the outer consumer daemon loop."""

    config_error_type: type[Exception]
    load_automation_config: Callable[[Any], Any]
    apply_automation_runtime: Callable[[Any, Any | None], None]
    current_main_workflows: Callable[..., tuple[Any, dict[str, Any], int]]
    recover_interrupted_sessions: Callable[..., list[Any]]
    run_multi_worker_daemon_loop: Callable[..., None]
    drain_requested: Callable[[Any], bool]
    run_one_cycle: Callable[..., Any]
    logger: Any


def next_available_slots(
    db: Any,
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
    active_tasks: dict[Any, Any],
    *,
    logger: Any,
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
    config: Any,
    db: Any,
    *,
    dry_run: bool,
    sleeper: Callable[[float], None],
    runtime: DaemonRuntime | None,
    deps: PrepareMultiWorkerCycleDeps,
) -> Any | None:
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
    db: Any,
    prepared: Any,
    active_tasks: dict[Any, Any],
    *,
    next_available_slots_fn: Callable[..., list[int]],
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
    db: Any,
    config: Any,
    *,
    sleeper: Callable[[float], None],
    claim_suppression_state: Callable[[Any], dict[str, Any] | None],
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
    config: Any,
    db: Any,
    prepared: Any,
    dry_run: bool,
    runtime: DaemonRuntime | None,
    deps: PrepareMultiWorkerLaunchContextDeps,
) -> tuple[Any | None, bool]:
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
        deps.record_metric(
            db,
            config,
            "worker_start_failed",
            issue_ref=candidate,
            payload={"reason": err.reason_code, "detail": err.detail},
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
    executor: Any,
    active_tasks: dict[Any, Any],
    *,
    config: Any,
    candidate: str,
    slot_id: int,
    prepared: Any,
    launch_context: Any | None,
    dry_run: bool,
    runtime: DaemonRuntime | None,
    run_worker_cycle: Callable[..., Any],
    active_worker_task_type: type[Any],
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
    executor: Any,
    config: Any,
    db: Any,
    *,
    prepared: Any,
    available_slots: list[int],
    active_issue_refs: set[str],
    active_tasks: dict[Any, Any],
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
    config: Any,
    db: Any,
    *,
    dry_run: bool = False,
    sleep_fn: Callable[[float], None] | None = None,
    runtime: DaemonRuntime | None = None,
    deps: RunMultiWorkerDaemonLoopDeps,
) -> None:
    """Run the daemon loop with multiple concurrent worker slots."""
    sleeper = sleep_fn or time.sleep
    active_tasks: dict[Any, Any] = {}
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
    config: Any,
    db: Any,
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
        recovered = deps.recover_interrupted_sessions(
            config,
            db,
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
