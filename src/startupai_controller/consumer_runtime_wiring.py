"""Shell-facing runtime wiring extracted from board_consumer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from startupai_controller.application.consumer.daemon import (
    DispatchMultiWorkerLaunchesDeps,
    PrepareMultiWorkerCycleDeps,
    PrepareMultiWorkerLaunchContextDeps,
    RunDaemonLoopDeps,
    RunMultiWorkerDaemonLoopDeps,
    dispatch_multi_worker_launches as _dispatch_multi_worker_launches_use_case,
    multi_worker_dispatch_state as _multi_worker_dispatch_state_use_case,
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
    claim_suppression_state: Callable[[Any], dict[str, Any] | None]
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
    load_config: Callable[[Any], Any]
    list_project_items_by_status: Callable[..., list[Any]]
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


def _daemon_runtime_wiring_deps(shell: Any) -> DaemonRuntimeWiringDeps:
    """Build daemon-loop wiring deps from the live shell module."""
    return DaemonRuntimeWiringDeps(
        config_error_type=shell.ConfigError,
        workflow_config_error_type=shell.WorkflowConfigError,
        gh_query_error_type=shell.GhQueryError,
        worktree_prepare_error_type=shell.WorktreePrepareError,
        prepare_cycle=shell._prepare_cycle,
        mark_degraded=shell._mark_degraded,
        gh_reason_code=shell.gh_reason_code,
        logger=shell.logger,
        claim_suppression_state=shell._claim_suppression_state,
        parse_iso8601_timestamp=shell._parse_iso8601_timestamp,
        record_metric=shell._record_metric,
        prepare_launch_candidate=shell._prepare_launch_candidate,
        maybe_activate_claim_suppression=shell._maybe_activate_claim_suppression,
        block_prelaunch_issue=shell._block_prelaunch_issue,
        select_candidate_for_cycle=shell._select_candidate_for_cycle,
        prepare_multi_worker_launch_context=shell._prepare_multi_worker_launch_context,
        submit_multi_worker_task=shell._submit_multi_worker_task,
        run_worker_cycle=shell._run_worker_cycle,
        active_worker_task_type=shell.ActiveWorkerTask,
        next_available_slots=shell._next_available_slots,
        executor_factory=shell.ThreadPoolExecutor,
        log_completed_worker_results=shell._log_completed_worker_results,
        drain_requested=shell._drain_requested,
        prepare_multi_worker_cycle=shell._prepare_multi_worker_cycle,
        multi_worker_dispatch_state=shell._multi_worker_dispatch_state,
        sleep_for_claim_suppression_if_needed=shell._sleep_for_claim_suppression_if_needed,
        dispatch_multi_worker_launches=shell._dispatch_multi_worker_launches,
        load_automation_config=shell.load_automation_config,
        apply_automation_runtime=shell._apply_automation_runtime,
        current_main_workflows=shell._current_main_workflows,
        recover_interrupted_sessions=shell._recover_interrupted_sessions,
        run_multi_worker_daemon_loop=shell._run_multi_worker_daemon_loop,
        run_one_cycle=shell.run_one_cycle,
    )


def _status_runtime_wiring_deps(shell: Any) -> StatusRuntimeWiringDeps:
    """Build status wiring deps from the live shell module."""
    return StatusRuntimeWiringDeps(
        config_error_type=shell.ConfigError,
        load_automation_config=shell.load_automation_config,
        apply_automation_runtime=shell._apply_automation_runtime,
        current_main_workflows=shell._current_main_workflows,
        read_workflow_snapshot=shell.read_workflow_snapshot,
        open_consumer_db=shell.open_consumer_db,
        load_config=shell.load_config,
        list_project_items_by_status=shell._list_project_items_by_status,
        parse_issue_ref=shell.parse_issue_ref,
        load_admission_summary=shell._load_admission_summary,
        control_plane_health_summary=shell._control_plane_health_summary,
        drain_requested=shell._drain_requested,
        workflow_status_payload=shell.workflow_status_payload,
        session_retry_state=shell._session_retry_state,
        parse_iso8601_timestamp=shell._parse_iso8601_timestamp,
        control_key_degraded=shell.CONTROL_KEY_DEGRADED,
        control_key_degraded_reason=shell.CONTROL_KEY_DEGRADED_REASON,
        control_key_claim_suppressed_until=shell.CONTROL_KEY_CLAIM_SUPPRESSED_UNTIL,
        control_key_claim_suppressed_reason=shell.CONTROL_KEY_CLAIM_SUPPRESSED_REASON,
        control_key_claim_suppressed_scope=shell.CONTROL_KEY_CLAIM_SUPPRESSED_SCOPE,
        control_key_last_rate_limit_at=shell.CONTROL_KEY_LAST_RATE_LIMIT_AT,
        control_key_last_successful_board_sync_at=shell.CONTROL_KEY_LAST_SUCCESSFUL_BOARD_SYNC_AT,
        control_key_last_successful_github_mutation_at=shell.CONTROL_KEY_LAST_SUCCESSFUL_GITHUB_MUTATION_AT,
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
                raise ValueError("prepared cycle context is required when skip_control_plane=True")
        else:
            prepared = prepare_cycle(
                config,
                db,
                dry_run=dry_run,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                comment_checker=comment_checker,
                comment_poster=comment_poster,
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
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
    )


def prepare_multi_worker_cycle(
    config: Any,
    db: Any,
    *,
    dry_run: bool,
    sleeper: Callable[[float], None],
    di_kwargs: dict[str, Any],
    shell: Any,
) -> Any | None:
    """Run one bounded preflight pass for the multi-worker daemon."""
    deps = _daemon_runtime_wiring_deps(shell)
    return _prepare_multi_worker_cycle_use_case(
        config,
        db,
        dry_run=dry_run,
        sleeper=sleeper,
        di_kwargs=di_kwargs,
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
    *,
    shell: Any,
) -> tuple[list[int], set[str]]:
    """Compute currently available slots and active issue refs."""
    deps = _daemon_runtime_wiring_deps(shell)
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
    shell: Any,
) -> bool:
    """Sleep until claim suppression clears, if active."""
    deps = _daemon_runtime_wiring_deps(shell)
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
    di_kwargs: dict[str, Any],
    shell: Any,
) -> tuple[Any | None, bool]:
    """Prepare launch context for one candidate."""
    deps = _daemon_runtime_wiring_deps(shell)
    return _prepare_multi_worker_launch_context_use_case(
        candidate,
        config=config,
        db=db,
        prepared=prepared,
        dry_run=dry_run,
        di_kwargs=di_kwargs,
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
    di_kwargs: dict[str, Any],
    shell: Any,
) -> None:
    """Submit one prepared candidate to a worker slot."""
    deps = _daemon_runtime_wiring_deps(shell)
    _submit_multi_worker_task_use_case(
        executor,
        active_tasks,
        config=config,
        candidate=candidate,
        slot_id=slot_id,
        prepared=prepared,
        launch_context=launch_context,
        dry_run=dry_run,
        di_kwargs=di_kwargs,
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
    di_kwargs: dict[str, Any],
    shell: Any,
) -> int:
    """Launch as many ready candidates as the current hydration budget allows."""
    deps = _daemon_runtime_wiring_deps(shell)
    return _dispatch_multi_worker_launches_use_case(
        executor,
        config,
        db,
        prepared=prepared,
        available_slots=available_slots,
        active_issue_refs=active_issue_refs,
        active_tasks=active_tasks,
        dry_run=dry_run,
        di_kwargs=di_kwargs,
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
    di_kwargs: dict[str, Any] | None = None,
    shell: Any,
) -> None:
    """Run the daemon loop with multiple concurrent worker slots."""
    deps = _daemon_runtime_wiring_deps(shell)
    _run_multi_worker_daemon_loop_use_case(
        config,
        db,
        dry_run=dry_run,
        sleep_fn=sleep_fn,
        di_kwargs=di_kwargs,
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
    di_kwargs: dict[str, Any] | None = None,
    shell: Any,
) -> None:
    """Run continuous poll-claim-execute loop."""
    deps = _daemon_runtime_wiring_deps(shell)
    _run_daemon_loop_use_case(
        config,
        db,
        dry_run=dry_run,
        sleep_fn=sleep_fn,
        di_kwargs=di_kwargs,
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
    shell: Any,
) -> dict[str, Any]:
    """Collect consumer status as a JSON-serializable payload."""
    deps = _status_runtime_wiring_deps(shell)
    return _collect_status_payload_use_case(
        config,
        local_only=local_only,
        deps=CollectStatusPayloadDeps(
            config_error_type=deps.config_error_type,
            load_automation_config=deps.load_automation_config,
            apply_automation_runtime=deps.apply_automation_runtime,
            current_main_workflows=deps.current_main_workflows,
            read_workflow_snapshot=deps.read_workflow_snapshot,
            open_consumer_db=deps.open_consumer_db,
            load_config=deps.load_config,
            list_project_items_by_status=deps.list_project_items_by_status,
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
