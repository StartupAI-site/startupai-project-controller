"""Shell-facing runtime wiring extracted from board_consumer."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import inspect
import logging
from pathlib import Path
from typing import Any, Callable

import startupai_controller.consumer_cycle_wiring as _cycle_wiring
import startupai_controller.consumer_operational_wiring as _operational_wiring
import startupai_controller.consumer_preflight_wiring as _preflight_wiring
import startupai_controller.consumer_selection_retry_wiring as _selection_retry_wiring
import startupai_controller.consumer_support_wiring as _support_wiring
from startupai_controller.application.consumer.cycle import (
    run_prepared_cycle as _run_prepared_cycle_use_case,
)
from startupai_controller.application.consumer.daemon import (
    ClaimSuppressionStateFn,
    DaemonRuntime,
    DispatchMultiWorkerLaunchesDeps,
    PrepareMultiWorkerCycleDeps,
    PrepareMultiWorkerLaunchContextDeps,
    RunDaemonLoopDeps,
    RunMultiWorkerDaemonLoopDeps,
    dispatch_multi_worker_launches as _dispatch_multi_worker_launches_use_case,
    log_completed_worker_results as _log_completed_worker_results_use_case,
    multi_worker_dispatch_state as _multi_worker_dispatch_state_use_case,
    next_available_slots as _next_available_slots_use_case,
    prepare_multi_worker_cycle as _prepare_multi_worker_cycle_use_case,
    prepare_multi_worker_launch_context as _prepare_multi_worker_launch_context_use_case,
    run_daemon_loop as _run_daemon_loop_use_case,
    run_multi_worker_daemon_loop as _run_multi_worker_daemon_loop_use_case,
    sleep_for_claim_suppression_if_needed as _sleep_for_claim_suppression_if_needed_use_case,
    submit_multi_worker_task as _submit_multi_worker_task_use_case,
)
from startupai_controller.application.consumer.status import (
    CollectStatusPayloadDeps,
    collect_status_payload as _collect_status_payload_use_case,
)
from startupai_controller.automation_port_helpers import _list_project_items_by_status
from startupai_controller.board_automation_config import load_automation_config
from startupai_controller.consumer_types import ActiveWorkerTask, WorktreePrepareError
from startupai_controller.consumer_workflow import (
    WorkflowConfigError,
    read_workflow_snapshot,
    workflow_status_payload,
)
from startupai_controller.control_plane_runtime import (
    CONTROL_KEY_CLAIM_SUPPRESSED_REASON,
    CONTROL_KEY_CLAIM_SUPPRESSED_SCOPE,
    CONTROL_KEY_CLAIM_SUPPRESSED_UNTIL,
    CONTROL_KEY_DEGRADED,
    CONTROL_KEY_DEGRADED_REASON,
    CONTROL_KEY_LAST_RATE_LIMIT_AT,
    CONTROL_KEY_LAST_SUCCESSFUL_BOARD_SYNC_AT,
    CONTROL_KEY_LAST_SUCCESSFUL_GITHUB_MUTATION_AT,
    _apply_automation_runtime,
    _control_plane_health_summary,
    _current_main_workflows,
    _mark_degraded,
)
from startupai_controller.domain.models import CycleResult
from startupai_controller.domain.review_queue_policy import (
    parse_iso8601_timestamp as _parse_iso8601_timestamp,
)
from startupai_controller.runtime.wiring import (
    build_gh_runner_port,
    build_github_port_bundle,
    build_process_runner_port,
    gh_reason_code,
    open_consumer_db,
)
from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    GhQueryError,
    load_config,
    parse_issue_ref,
)

logger = logging.getLogger("board-consumer")


def _drain_requested(path: Path) -> bool:
    """Return True when a graceful drain has been requested."""
    return path.exists()


def _log_completed_worker_results(active_tasks: dict[Any, Any]) -> None:
    """Log and discard completed worker futures."""
    _log_completed_worker_results_use_case(active_tasks, logger=logger)


def _build_daemon_runtime(
    *,
    gh_runner: Callable[..., str] | None,
    subprocess_runner: Callable[..., Any] | None,
    file_reader: Callable[..., Any] | None,
) -> DaemonRuntime:
    """Build the typed runtime bundle used by the daemon application module."""
    return DaemonRuntime(
        gh_runner=build_gh_runner_port(gh_runner=gh_runner),
        process_runner=build_process_runner_port(
            gh_runner=gh_runner,
            subprocess_runner=subprocess_runner,
        ),
        file_reader=file_reader,
    )


def _effective_daemon_runtime(
    *,
    runtime: DaemonRuntime | None,
    di_kwargs: dict[str, Any] | None,
) -> DaemonRuntime:
    """Return the explicit runtime or build it from legacy di kwargs."""
    if runtime is not None:
        return runtime
    effective_di_kwargs = di_kwargs or {}
    return _build_daemon_runtime(
        gh_runner=effective_di_kwargs.get("gh_runner"),
        subprocess_runner=effective_di_kwargs.get("subprocess_runner"),
        file_reader=effective_di_kwargs.get("file_reader"),
    )


@dataclass(frozen=True)
class DaemonRuntimeWiringDeps:
    """Shell-facing seams for daemon/status wrapper wiring."""

    config_error_type: type[Exception]
    workflow_config_error_type: type[Exception]
    gh_query_error_type: type[Exception]
    worktree_prepare_error_type: type[Exception]
    prepare_cycle: Callable[..., Any]
    mark_degraded: Callable[..., None]
    gh_reason_code: Callable[..., str]
    logger: Any
    claim_suppression_state: ClaimSuppressionStateFn
    parse_iso8601_timestamp: Callable[[str], Any]
    record_metric: Callable[..., None]
    prepare_launch_candidate: Callable[..., Any]
    maybe_activate_claim_suppression: Callable[..., bool]
    block_prelaunch_issue: Callable[..., None]
    select_candidate_for_cycle: Callable[..., str | None]
    prepare_multi_worker_launch_context: Callable[..., tuple[Any | None, bool]]
    submit_multi_worker_task: Callable[..., None]
    run_worker_cycle: Callable[..., Any]
    active_worker_task_type: type[Any]
    next_available_slots: Callable[..., list[int]]
    executor_factory: Callable[..., Any]
    log_completed_worker_results: Callable[..., None]
    drain_requested: Callable[[Any], bool]
    prepare_multi_worker_cycle: Callable[..., Any | None]
    multi_worker_dispatch_state: Callable[..., tuple[list[int], set[str]]]
    sleep_for_claim_suppression_if_needed: Callable[..., bool]
    dispatch_multi_worker_launches: Callable[..., int]
    load_automation_config: Callable[[Any], Any]
    apply_automation_runtime: Callable[[Any, Any | None], None]
    current_main_workflows: Callable[..., tuple[Any, dict[str, Any], int]]
    recover_interrupted_sessions: Callable[..., list[Any]]
    run_multi_worker_daemon_loop: Callable[..., None]
    run_one_cycle: Callable[..., Any]


@dataclass(frozen=True)
class StatusRuntimeWiringDeps:
    """Shell-facing seams for status wrapper wiring."""

    config_error_type: type[Exception]
    load_automation_config: Callable[[Any], Any]
    apply_automation_runtime: Callable[[Any, Any | None], None]
    current_main_workflows: Callable[..., tuple[dict[str, Any], dict[str, Any], int]]
    read_workflow_snapshot: Callable[[Any], Any | None]
    open_consumer_db: Callable[[Any], Any]
    parse_issue_ref: Callable[[str], Any]
    load_admission_summary: Callable[[dict[str, str], Any | None], dict[str, Any]]
    control_plane_health_summary: Callable[..., Any]
    drain_requested: Callable[[Any], bool]
    workflow_status_payload: Callable[[Any], dict[str, Any]]
    session_retry_state: Callable[..., dict[str, Any]]
    parse_iso8601_timestamp: Callable[[str], Any]
    control_key_degraded: str
    control_key_degraded_reason: str
    control_key_claim_suppressed_until: str
    control_key_claim_suppressed_reason: str
    control_key_claim_suppressed_scope: str
    control_key_last_rate_limit_at: str
    control_key_last_successful_board_sync_at: str
    control_key_last_successful_github_mutation_at: str


def _daemon_runtime_wiring_deps(
    *,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
) -> DaemonRuntimeWiringDeps:
    """Build daemon-loop wiring deps without reaching back into compat."""

    def _run_worker_cycle(*args: Any, **kwargs: Any) -> Any:
        run_worker_cycle_fn = _support_wiring.run_worker_cycle
        parameters: tuple[inspect.Parameter, ...]
        try:
            parameters = tuple(
                inspect.signature(run_worker_cycle_fn).parameters.values()
            )
        except (TypeError, ValueError):
            parameters = ()
        supports_runtime = any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            or parameter.name == "runtime"
            for parameter in parameters
        )
        if not supports_runtime and "runtime" in kwargs:
            kwargs = dict(kwargs)
            kwargs.pop("runtime", None)
        return run_worker_cycle_fn(*args, **kwargs)

    def _prepare_launch_candidate(
        issue_ref: str,
        *,
        config: Any,
        prepared: Any,
        db: Any,
        runtime: DaemonRuntime | None = None,
    ) -> Any:
        gh_runner_fn = (
            runtime.gh_runner.run_gh
            if runtime and runtime.gh_runner is not None
            else None
        )
        github_bundle = build_github_port_bundle(
            config.project_owner,
            config.project_number,
            config=prepared.cp_config,
            github_memo=prepared.github_memo,
            gh_runner=gh_runner_fn,
        )
        return _cycle_wiring.prepare_launch_candidate(
            issue_ref,
            config=config,
            prepared=prepared,
            db=db,
            subprocess_runner=runtime.process_runner if runtime is not None else None,
            review_state_port=github_bundle.review_state,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=runtime.gh_runner if runtime is not None else None,
            pr_port=github_bundle.pull_requests,
        )

    def _select_candidate_for_cycle(
        config: Any,
        db: Any,
        prepared: Any,
        *,
        target_issue: str | None = None,
        runtime: DaemonRuntime | None = None,
        excluded_issue_refs: set[str] | None = None,
    ) -> str | None:
        gh_runner_fn = (
            runtime.gh_runner.run_gh
            if runtime and runtime.gh_runner is not None
            else None
        )
        review_state_port = build_github_port_bundle(
            config.project_owner,
            config.project_number,
            config=prepared.cp_config,
            github_memo=prepared.github_memo,
            gh_runner=gh_runner_fn,
        ).review_state
        return _selection_retry_wiring.select_candidate_for_cycle_from_shell(
            config,
            db,
            prepared,
            target_issue=target_issue,
            review_state_port=review_state_port,
            gh_runner=gh_runner_fn,
            excluded_issue_refs=excluded_issue_refs,
        )

    def _block_prelaunch_issue(
        issue_ref: str,
        blocked_reason: str,
        *,
        config: Any,
        cp_config: Any,
        db: Any,
        runtime: DaemonRuntime | None = None,
    ) -> None:
        _operational_wiring.block_prelaunch_issue(
            issue_ref,
            blocked_reason,
            config=config,
            cp_config=cp_config,
            db=db,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=(
                runtime.gh_runner.run_gh
                if runtime and runtime.gh_runner is not None
                else None
            ),
        )

    def _recover_interrupted_sessions(
        config: Any,
        db: Any,
        *,
        automation_config: Any | None = None,
        runtime: DaemonRuntime | None = None,
    ) -> list[Any]:
        return _operational_wiring.recover_interrupted_sessions(
            config,
            db,
            automation_config=automation_config,
            gh_runner=(
                runtime.gh_runner.run_gh
                if runtime and runtime.gh_runner is not None
                else None
            ),
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
        )

    def _run_one_cycle(
        config: Any,
        db: Any,
        *,
        dry_run: bool = False,
        runtime: DaemonRuntime | None = None,
        **kwargs: Any,
    ) -> Any:
        return run_one_cycle_live(
            config,
            db,
            dry_run=dry_run,
            gh_runner=(
                runtime.gh_runner.run_gh
                if runtime and runtime.gh_runner is not None
                else None
            ),
            subprocess_runner=(
                runtime.process_runner.run
                if runtime and runtime.process_runner is not None
                else None
            ),
            file_reader=runtime.file_reader if runtime is not None else None,
            status_resolver=status_resolver,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            **kwargs,
        )

    return DaemonRuntimeWiringDeps(
        config_error_type=ConfigError,
        workflow_config_error_type=WorkflowConfigError,
        gh_query_error_type=GhQueryError,
        worktree_prepare_error_type=WorktreePrepareError,
        prepare_cycle=_preflight_wiring.prepare_cycle,
        mark_degraded=_mark_degraded,
        gh_reason_code=gh_reason_code,
        logger=logger,
        claim_suppression_state=_support_wiring.claim_suppression_state,
        parse_iso8601_timestamp=_parse_iso8601_timestamp,
        record_metric=_support_wiring.record_metric,
        prepare_launch_candidate=_prepare_launch_candidate,
        maybe_activate_claim_suppression=_support_wiring.maybe_activate_claim_suppression,
        block_prelaunch_issue=_block_prelaunch_issue,
        select_candidate_for_cycle=_select_candidate_for_cycle,
        prepare_multi_worker_launch_context=_support_wiring.prepare_multi_worker_launch_context,
        submit_multi_worker_task=_support_wiring.submit_multi_worker_task,
        run_worker_cycle=_run_worker_cycle,
        active_worker_task_type=ActiveWorkerTask,
        next_available_slots=_next_available_slots_use_case,
        executor_factory=ThreadPoolExecutor,
        log_completed_worker_results=_log_completed_worker_results,
        drain_requested=_drain_requested,
        prepare_multi_worker_cycle=prepare_multi_worker_cycle,
        multi_worker_dispatch_state=multi_worker_dispatch_state,
        sleep_for_claim_suppression_if_needed=sleep_for_claim_suppression_if_needed,
        dispatch_multi_worker_launches=dispatch_multi_worker_launches,
        load_automation_config=load_automation_config,
        apply_automation_runtime=_apply_automation_runtime,
        current_main_workflows=_current_main_workflows,
        recover_interrupted_sessions=_recover_interrupted_sessions,
        run_multi_worker_daemon_loop=run_multi_worker_daemon_loop,
        run_one_cycle=_run_one_cycle,
    )


def _status_runtime_wiring_deps() -> StatusRuntimeWiringDeps:
    """Build status wiring deps without reaching back into compat."""
    return StatusRuntimeWiringDeps(
        config_error_type=ConfigError,
        load_automation_config=load_automation_config,
        apply_automation_runtime=_apply_automation_runtime,
        current_main_workflows=_current_main_workflows,
        read_workflow_snapshot=read_workflow_snapshot,
        open_consumer_db=open_consumer_db,
        parse_issue_ref=parse_issue_ref,
        load_admission_summary=_support_wiring.load_admission_summary,
        control_plane_health_summary=_control_plane_health_summary,
        drain_requested=_drain_requested,
        workflow_status_payload=workflow_status_payload,
        session_retry_state=_support_wiring.session_retry_state,
        parse_iso8601_timestamp=_parse_iso8601_timestamp,
        control_key_degraded=CONTROL_KEY_DEGRADED,
        control_key_degraded_reason=CONTROL_KEY_DEGRADED_REASON,
        control_key_claim_suppressed_until=CONTROL_KEY_CLAIM_SUPPRESSED_UNTIL,
        control_key_claim_suppressed_reason=CONTROL_KEY_CLAIM_SUPPRESSED_REASON,
        control_key_claim_suppressed_scope=CONTROL_KEY_CLAIM_SUPPRESSED_SCOPE,
        control_key_last_rate_limit_at=CONTROL_KEY_LAST_RATE_LIMIT_AT,
        control_key_last_successful_board_sync_at=CONTROL_KEY_LAST_SUCCESSFUL_BOARD_SYNC_AT,
        control_key_last_successful_github_mutation_at=CONTROL_KEY_LAST_SUCCESSFUL_GITHUB_MUTATION_AT,
    )


def run_one_cycle(
    config: Any,
    db: Any,
    *,
    dry_run: bool,
    target_issue: str | None,
    prepared: Any | None,
    launch_context: Any | None,
    slot_id_override: int | None,
    skip_control_plane: bool,
    gh_runner: Callable[..., str] | None,
    subprocess_runner: Callable[..., Any] | None,
    file_reader: Callable[..., Any] | None,
    status_resolver: Callable[..., str] | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    prepare_cycle: Callable[..., Any],
    config_error_type: type[Exception],
    workflow_config_error_type: type[Exception],
    gh_query_error_type: type[Exception],
    mark_degraded: Callable[..., None],
    gh_reason_code: Callable[..., str],
    cycle_result_factory: Callable[..., Any],
    build_gh_runner_port: Callable[..., Any],
    build_process_runner_port: Callable[..., Any],
    run_prepared_cycle: Callable[..., Any],
    prepared_cycle_deps: Any,
    logger: Any,
) -> Any:
    """Execute one poll-claim-execute cycle through the application layer."""
    try:
        if skip_control_plane:
            if prepared is None:
                raise ValueError(
                    "prepared cycle context is required when skip_control_plane=True"
                )
        else:
            prepared = prepare_cycle(
                config,
                db,
                dry_run=dry_run,
                gh_runner=gh_runner,
            )
    except config_error_type as err:
        logger.error("Config error: %s", err)
        return cycle_result_factory(action="error", reason=f"config-error:{err}")
    except workflow_config_error_type as err:
        logger.error("Workflow config error: %s", err)
        return cycle_result_factory(action="error", reason=f"workflow-config:{err}")
    except gh_query_error_type as err:
        logger.error("Control-plane preflight failed: %s", err)
        mark_degraded(db, f"control-plane:{gh_reason_code(err)}:{err}")
        return cycle_result_factory(action="error", reason=f"control-plane:{err}")

    assert prepared is not None
    github_bundle = build_github_port_bundle(
        config.project_owner,
        config.project_number,
        config=prepared.cp_config,
        github_memo=prepared.github_memo,
        gh_runner=gh_runner,
    )
    gh_port = build_gh_runner_port(gh_runner=gh_runner)
    process_runner = build_process_runner_port(
        gh_runner=gh_runner,
        subprocess_runner=subprocess_runner,
    )
    return run_prepared_cycle(
        config=config,
        db=db,
        prepared=prepared,
        deps=prepared_cycle_deps,
        dry_run=dry_run,
        launch_context=launch_context,
        target_issue=target_issue,
        slot_id_override=slot_id_override,
        gh_runner=gh_port,
        process_runner=process_runner,
        file_reader=file_reader,
        review_state_port=github_bundle.review_state,
        board_port=github_bundle.board_mutations,
        pr_port=github_bundle.pull_requests,
    )


def run_one_cycle_live(
    config: Any,
    db: Any,
    *,
    dry_run: bool = False,
    target_issue: str | None = None,
    prepared: Any | None = None,
    launch_context: Any | None = None,
    slot_id_override: int | None = None,
    skip_control_plane: bool = False,
    gh_runner: Callable[..., str] | None = None,
    subprocess_runner: Callable[..., Any] | None = None,
    file_reader: Callable[..., Any] | None = None,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    runtime: DaemonRuntime | None = None,
    di_kwargs: dict[str, Any] | None = None,
) -> Any:
    """Execute one poll-claim-execute cycle through direct runtime deps."""
    effective_di_kwargs = di_kwargs or {}
    runtime = _effective_daemon_runtime(runtime=runtime, di_kwargs=effective_di_kwargs)
    return run_one_cycle(
        config=config,
        db=db,
        dry_run=dry_run,
        target_issue=target_issue,
        prepared=prepared,
        launch_context=launch_context,
        slot_id_override=slot_id_override,
        skip_control_plane=skip_control_plane,
        gh_runner=gh_runner
        or (runtime.gh_runner.run_gh if runtime and runtime.gh_runner else None),
        subprocess_runner=subprocess_runner
        or (runtime.process_runner.run if runtime and runtime.process_runner else None),
        file_reader=file_reader
        or (runtime.file_reader if runtime is not None else None),
        status_resolver=status_resolver or effective_di_kwargs.get("status_resolver"),
        board_info_resolver=board_info_resolver
        or effective_di_kwargs.get("board_info_resolver"),
        board_mutator=board_mutator or effective_di_kwargs.get("board_mutator"),
        comment_checker=comment_checker or effective_di_kwargs.get("comment_checker"),
        comment_poster=comment_poster or effective_di_kwargs.get("comment_poster"),
        prepare_cycle=_preflight_wiring.prepare_cycle,
        config_error_type=ConfigError,
        workflow_config_error_type=WorkflowConfigError,
        gh_query_error_type=GhQueryError,
        mark_degraded=_mark_degraded,
        gh_reason_code=gh_reason_code,
        cycle_result_factory=CycleResult,
        build_gh_runner_port=build_gh_runner_port,
        build_process_runner_port=build_process_runner_port,
        run_prepared_cycle=_run_prepared_cycle_use_case,
        prepared_cycle_deps=_operational_wiring.prepared_cycle_deps(
            status_resolver=status_resolver,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
        ),
        logger=logger,
    )


def prepare_multi_worker_cycle(
    config: Any,
    db: Any,
    *,
    dry_run: bool,
    sleeper: Callable[[float], None],
    runtime: DaemonRuntime | None = None,
    di_kwargs: dict[str, Any] | None = None,
) -> Any | None:
    """Run one bounded preflight pass for the multi-worker daemon."""
    effective_di_kwargs = di_kwargs or {}
    runtime = _effective_daemon_runtime(runtime=runtime, di_kwargs=effective_di_kwargs)
    deps = _daemon_runtime_wiring_deps(
        status_resolver=effective_di_kwargs.get("status_resolver"),
        board_info_resolver=effective_di_kwargs.get("board_info_resolver"),
        board_mutator=effective_di_kwargs.get("board_mutator"),
        comment_checker=effective_di_kwargs.get("comment_checker"),
        comment_poster=effective_di_kwargs.get("comment_poster"),
    )
    return _prepare_multi_worker_cycle_use_case(
        config,
        db,
        dry_run=dry_run,
        sleeper=sleeper,
        runtime=runtime,
        deps=PrepareMultiWorkerCycleDeps(
            config_error_type=deps.config_error_type,
            workflow_config_error_type=deps.workflow_config_error_type,
            gh_query_error_type=deps.gh_query_error_type,
            prepare_cycle=deps.prepare_cycle,
            mark_degraded=deps.mark_degraded,
            gh_reason_code=deps.gh_reason_code,
            logger=deps.logger,
        ),
    )


def multi_worker_dispatch_state(
    db: Any,
    prepared: Any,
    active_tasks: dict[Any, Any],
) -> tuple[list[int], set[str]]:
    """Compute currently available slots and active issue refs."""
    deps = _daemon_runtime_wiring_deps()
    return _multi_worker_dispatch_state_use_case(
        db,
        prepared,
        active_tasks,
        next_available_slots_fn=deps.next_available_slots,
    )


def sleep_for_claim_suppression_if_needed(
    db: Any,
    config: Any,
    *,
    sleeper: Callable[[float], None],
) -> bool:
    """Sleep until claim suppression clears, if active."""
    deps = _daemon_runtime_wiring_deps()
    return _sleep_for_claim_suppression_if_needed_use_case(
        db,
        config,
        sleeper=sleeper,
        claim_suppression_state=deps.claim_suppression_state,
        parse_iso8601_timestamp=deps.parse_iso8601_timestamp,
    )


def prepare_multi_worker_launch_context(
    candidate: str,
    *,
    config: Any,
    db: Any,
    prepared: Any,
    dry_run: bool,
    runtime: DaemonRuntime | None = None,
    di_kwargs: dict[str, Any] | None = None,
) -> tuple[Any | None, bool]:
    """Prepare launch context for one candidate."""
    effective_di_kwargs = di_kwargs or {}
    runtime = _effective_daemon_runtime(runtime=runtime, di_kwargs=effective_di_kwargs)
    deps = _daemon_runtime_wiring_deps(
        status_resolver=effective_di_kwargs.get("status_resolver"),
        board_info_resolver=effective_di_kwargs.get("board_info_resolver"),
        board_mutator=effective_di_kwargs.get("board_mutator"),
        comment_checker=effective_di_kwargs.get("comment_checker"),
        comment_poster=effective_di_kwargs.get("comment_poster"),
    )
    return _prepare_multi_worker_launch_context_use_case(
        candidate,
        config=config,
        db=db,
        prepared=prepared,
        dry_run=dry_run,
        runtime=runtime,
        deps=PrepareMultiWorkerLaunchContextDeps(
            gh_query_error_type=deps.gh_query_error_type,
            workflow_config_error_type=deps.workflow_config_error_type,
            worktree_prepare_error_type=deps.worktree_prepare_error_type,
            record_metric=deps.record_metric,
            prepare_launch_candidate=deps.prepare_launch_candidate,
            maybe_activate_claim_suppression=deps.maybe_activate_claim_suppression,
            mark_degraded=deps.mark_degraded,
            gh_reason_code=deps.gh_reason_code,
            block_prelaunch_issue=deps.block_prelaunch_issue,
        ),
    )


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
    runtime: DaemonRuntime | None = None,
    di_kwargs: dict[str, Any] | None = None,
) -> None:
    """Submit one prepared candidate to a worker slot."""
    effective_di_kwargs = di_kwargs or {}
    runtime = _effective_daemon_runtime(runtime=runtime, di_kwargs=effective_di_kwargs)
    deps = _daemon_runtime_wiring_deps(
        status_resolver=effective_di_kwargs.get("status_resolver"),
        board_info_resolver=effective_di_kwargs.get("board_info_resolver"),
        board_mutator=effective_di_kwargs.get("board_mutator"),
        comment_checker=effective_di_kwargs.get("comment_checker"),
        comment_poster=effective_di_kwargs.get("comment_poster"),
    )
    _submit_multi_worker_task_use_case(
        executor,
        active_tasks,
        config=config,
        candidate=candidate,
        slot_id=slot_id,
        prepared=prepared,
        launch_context=launch_context,
        dry_run=dry_run,
        runtime=runtime,
        run_worker_cycle=deps.run_worker_cycle,
        active_worker_task_type=deps.active_worker_task_type,
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
    runtime: DaemonRuntime | None = None,
    di_kwargs: dict[str, Any] | None = None,
) -> int:
    """Launch as many ready candidates as the current hydration budget allows."""
    effective_di_kwargs = di_kwargs or {}
    runtime = _effective_daemon_runtime(runtime=runtime, di_kwargs=effective_di_kwargs)
    deps = _daemon_runtime_wiring_deps(
        status_resolver=effective_di_kwargs.get("status_resolver"),
        board_info_resolver=effective_di_kwargs.get("board_info_resolver"),
        board_mutator=effective_di_kwargs.get("board_mutator"),
        comment_checker=effective_di_kwargs.get("comment_checker"),
        comment_poster=effective_di_kwargs.get("comment_poster"),
    )
    return _dispatch_multi_worker_launches_use_case(
        executor,
        config,
        db,
        prepared=prepared,
        available_slots=available_slots,
        active_issue_refs=active_issue_refs,
        active_tasks=active_tasks,
        dry_run=dry_run,
        runtime=runtime,
        deps=DispatchMultiWorkerLaunchesDeps(
            gh_query_error_type=deps.gh_query_error_type,
            select_candidate_for_cycle=deps.select_candidate_for_cycle,
            prepare_multi_worker_launch_context=deps.prepare_multi_worker_launch_context,
            submit_multi_worker_task=deps.submit_multi_worker_task,
            mark_degraded=deps.mark_degraded,
            gh_reason_code=deps.gh_reason_code,
            logger=deps.logger,
        ),
    )


def run_multi_worker_daemon_loop(
    config: Any,
    db: Any,
    *,
    dry_run: bool = False,
    sleep_fn: Callable[[float], None] | None = None,
    runtime: DaemonRuntime | None = None,
    di_kwargs: dict[str, Any] | None = None,
) -> None:
    """Run the daemon loop with multiple concurrent worker slots."""
    effective_di_kwargs = di_kwargs or {}
    runtime = _effective_daemon_runtime(runtime=runtime, di_kwargs=effective_di_kwargs)
    deps = _daemon_runtime_wiring_deps(
        status_resolver=effective_di_kwargs.get("status_resolver"),
        board_info_resolver=effective_di_kwargs.get("board_info_resolver"),
        board_mutator=effective_di_kwargs.get("board_mutator"),
        comment_checker=effective_di_kwargs.get("comment_checker"),
        comment_poster=effective_di_kwargs.get("comment_poster"),
    )
    _run_multi_worker_daemon_loop_use_case(
        config,
        db,
        dry_run=dry_run,
        sleep_fn=sleep_fn,
        runtime=runtime,
        deps=RunMultiWorkerDaemonLoopDeps(
            executor_factory=deps.executor_factory,
            log_completed_worker_results=deps.log_completed_worker_results,
            drain_requested=deps.drain_requested,
            prepare_multi_worker_cycle=deps.prepare_multi_worker_cycle,
            multi_worker_dispatch_state=deps.multi_worker_dispatch_state,
            sleep_for_claim_suppression_if_needed=deps.sleep_for_claim_suppression_if_needed,
            dispatch_multi_worker_launches=deps.dispatch_multi_worker_launches,
            logger=deps.logger,
        ),
    )


def run_daemon_loop(
    config: Any,
    db: Any,
    *,
    dry_run: bool = False,
    sleep_fn: Callable[[float], None] | None = None,
    runtime: DaemonRuntime | None = None,
    di_kwargs: dict[str, Any] | None = None,
) -> None:
    """Run continuous poll-claim-execute loop."""
    effective_di_kwargs = di_kwargs or {}
    runtime = _effective_daemon_runtime(runtime=runtime, di_kwargs=effective_di_kwargs)
    deps = _daemon_runtime_wiring_deps(
        status_resolver=effective_di_kwargs.get("status_resolver"),
        board_info_resolver=effective_di_kwargs.get("board_info_resolver"),
        board_mutator=effective_di_kwargs.get("board_mutator"),
        comment_checker=effective_di_kwargs.get("comment_checker"),
        comment_poster=effective_di_kwargs.get("comment_poster"),
    )
    _run_daemon_loop_use_case(
        config,
        db,
        dry_run=dry_run,
        sleep_fn=sleep_fn,
        runtime=runtime,
        deps=RunDaemonLoopDeps(
            config_error_type=deps.config_error_type,
            load_automation_config=deps.load_automation_config,
            apply_automation_runtime=deps.apply_automation_runtime,
            current_main_workflows=deps.current_main_workflows,
            recover_interrupted_sessions=deps.recover_interrupted_sessions,
            run_multi_worker_daemon_loop=deps.run_multi_worker_daemon_loop,
            drain_requested=deps.drain_requested,
            run_one_cycle=deps.run_one_cycle,
            logger=deps.logger,
        ),
    )


def collect_status_payload(
    config: Any,
    *,
    local_only: bool = False,
) -> dict[str, Any]:
    """Collect consumer status as a JSON-serializable payload."""
    deps = _status_runtime_wiring_deps()
    db = deps.open_consumer_db(config.db_path)
    try:
        review_state_port = None
        if not local_only:
            cp_config = load_config(config.critical_paths_path)
            github_bundle = build_github_port_bundle(
                config.project_owner,
                config.project_number,
                config=cp_config,
            )
            base_review_state_port = github_bundle.review_state

            class _StatusReviewStatePort:
                def __getattr__(self, name: str) -> Any:
                    return getattr(base_review_state_port, name)

                def list_issues_by_status(self, status: str) -> list[Any]:
                    return _list_project_items_by_status(
                        status,
                        config.project_owner,
                        config.project_number,
                        config=cp_config,
                    )

            review_state_port = _StatusReviewStatePort()
        return _collect_status_payload_use_case(
            config,
            local_only=local_only,
            db=db,
            review_state_port=review_state_port,
            deps=CollectStatusPayloadDeps(
                config_error_type=deps.config_error_type,
                load_automation_config=deps.load_automation_config,
                apply_automation_runtime=deps.apply_automation_runtime,
                current_main_workflows=deps.current_main_workflows,
                read_workflow_snapshot=deps.read_workflow_snapshot,
                parse_issue_ref=deps.parse_issue_ref,
                load_admission_summary=deps.load_admission_summary,
                control_plane_health_summary=deps.control_plane_health_summary,
                drain_requested=deps.drain_requested,
                workflow_status_payload=deps.workflow_status_payload,
                session_retry_state=deps.session_retry_state,
                parse_iso8601_timestamp=deps.parse_iso8601_timestamp,
                control_keys={
                    "degraded": deps.control_key_degraded,
                    "degraded_reason": deps.control_key_degraded_reason,
                    "claim_suppressed_until": deps.control_key_claim_suppressed_until,
                    "claim_suppressed_reason": deps.control_key_claim_suppressed_reason,
                    "claim_suppressed_scope": deps.control_key_claim_suppressed_scope,
                    "last_rate_limit_at": deps.control_key_last_rate_limit_at,
                    "last_successful_board_sync_at": deps.control_key_last_successful_board_sync_at,
                    "last_successful_github_mutation_at": deps.control_key_last_successful_github_mutation_at,
                },
            ),
        )
    finally:
        db.close()
