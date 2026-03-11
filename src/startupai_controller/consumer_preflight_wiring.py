"""Shell-facing preflight wiring extracted from board_consumer."""

from __future__ import annotations

from typing import Any, Callable

from startupai_controller.application.consumer.preflight import (
    PrepareCyclePhasesDeps,
    execute_prepare_cycle_phases as _execute_prepare_cycle_phases_use_case,
)
from startupai_controller.application.consumer.preflight_runtime import (
    InitializeCycleRuntimeDeps,
    PhaseHelperDeps,
    PrepareCycleDeps,
    initialize_cycle_runtime as _initialize_cycle_runtime_use_case,
    load_board_snapshot_phase as _load_board_snapshot_phase_use_case,
    prepare_cycle as _prepare_cycle_use_case,
    run_admission_phase as _run_admission_phase_use_case,
    run_deferred_replay_phase as _run_deferred_replay_phase_use_case,
    run_executor_routing_phase as _run_executor_routing_phase_use_case,
    run_reconciliation_phase as _run_reconciliation_phase_use_case,
    run_review_queue_phase as _run_review_queue_phase_use_case,
)


def _shell_module():
    """Import the consumer shell lazily to avoid import cycles."""
    from startupai_controller import board_consumer

    return board_consumer


def build_init_cycle_runtime_deps() -> InitializeCycleRuntimeDeps:
    """Build the deps for cycle runtime initialization."""
    shell = _shell_module()
    return InitializeCycleRuntimeDeps(
        build_session_store=shell.build_session_store,
        load_config=shell.load_config,
        load_automation_config=shell.load_automation_config,
        apply_automation_runtime=shell._apply_automation_runtime,
        current_main_workflows=shell._current_main_workflows,
        build_github_port_bundle=shell.build_github_port_bundle,
        build_ready_flow_port=shell.build_ready_flow_port,
        cycle_github_memo_factory=shell.CycleGitHubMemo,
        config_error_type=shell.ConfigError,
        logger=shell.logger,
    )


def build_phase_helper_deps() -> PhaseHelperDeps:
    """Build the deps for preflight phase helpers."""
    shell = _shell_module()
    return PhaseHelperDeps(
        replay_deferred_actions=shell._replay_deferred_actions,
        drain_review_queue=shell._drain_review_queue,
        reconcile_board_truth=shell._reconcile_board_truth,
        record_successful_github_mutation=shell._record_successful_github_mutation,
        record_successful_board_sync=shell._record_successful_board_sync,
        clear_degraded=shell._clear_degraded,
        mark_degraded=shell._mark_degraded,
        persist_admission_summary=shell._persist_admission_summary,
        logger=shell.logger,
    )


def initialize_cycle_runtime(
    config: Any,
    db: Any,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> Any:
    """Build cycle-scoped runtime wiring and effective config."""
    return _initialize_cycle_runtime_use_case(
        config,
        db,
        deps=build_init_cycle_runtime_deps(),
        gh_runner=gh_runner,
    )


def run_deferred_replay_phase(
    config: Any,
    db: Any,
    runtime: Any,
    *,
    timings_ms: dict[str, int],
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    dry_run: bool,
) -> None:
    """Replay deferred actions for the cycle when enabled."""
    return _run_deferred_replay_phase_use_case(
        config,
        db,
        runtime,
        deps=build_phase_helper_deps(),
        timings_ms=timings_ms,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )


def load_board_snapshot_phase(
    config: Any,
    runtime: Any,
    *,
    timings_ms: dict[str, int],
    gh_runner: Callable[..., str] | None,
) -> Any:
    """Load the cycle board snapshot."""
    return _load_board_snapshot_phase_use_case(
        config,
        runtime,
        timings_ms=timings_ms,
        gh_runner=gh_runner,
    )


def run_executor_routing_phase(
    config: Any,
    db: Any,
    runtime: Any,
    *,
    board_snapshot: Any,
    timings_ms: dict[str, int],
    gh_runner: Callable[..., str] | None,
    dry_run: bool,
) -> None:
    """Normalize executor routing for the protected queue."""
    return _run_executor_routing_phase_use_case(
        config,
        db,
        runtime,
        deps=build_phase_helper_deps(),
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )


def run_reconciliation_phase(
    config: Any,
    db: Any,
    runtime: Any,
    *,
    board_snapshot: Any,
    timings_ms: dict[str, int],
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Run truthful board reconciliation for the cycle."""
    return _run_reconciliation_phase_use_case(
        config,
        db,
        runtime,
        deps=build_phase_helper_deps(),
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )


def run_review_queue_phase(
    config: Any,
    db: Any,
    runtime: Any,
    *,
    board_snapshot: Any,
    timings_ms: dict[str, int],
    gh_runner: Callable[..., str] | None,
    dry_run: bool,
) -> tuple[Any, Any]:
    """Drain the review queue for the current cycle."""
    return _run_review_queue_phase_use_case(
        config,
        db,
        runtime,
        deps=build_phase_helper_deps(),
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )


def run_admission_phase(
    config: Any,
    db: Any,
    runtime: Any,
    *,
    board_snapshot: Any,
    timings_ms: dict[str, int],
    gh_runner: Callable[..., str] | None,
    dry_run: bool,
) -> dict[str, Any]:
    """Run backlog admission for the current cycle."""
    return _run_admission_phase_use_case(
        config,
        db,
        runtime,
        deps=build_phase_helper_deps(),
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )


def execute_prepare_cycle_phases(
    config: Any,
    db: Any,
    runtime: Any,
    *,
    deps: Any | None = None,
    dry_run: bool = False,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> Any:
    """Execute the preflight phases for one cycle."""
    if deps is None:
        shell = _shell_module()
        deps = PrepareCyclePhasesDeps(
            run_deferred_replay_phase=shell._run_deferred_replay_phase,
            load_board_snapshot_phase=shell._load_board_snapshot_phase,
            run_executor_routing_phase=shell._run_executor_routing_phase,
            run_reconciliation_phase=shell._run_reconciliation_phase,
            run_review_queue_phase=shell._run_review_queue_phase,
            run_admission_phase=shell._run_admission_phase,
        )
        result = _execute_prepare_cycle_phases_use_case(
            config,
            db,
            runtime=runtime,
            deps=deps,
            dry_run=dry_run,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )
        return (
            result.board_snapshot,
            result.review_queue_summary,
            result.admission_summary,
            result.timings_ms,
        )

    result = _execute_prepare_cycle_phases_use_case(
        config,
        db,
        runtime=runtime,
        deps=deps,
        dry_run=dry_run,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )
    return result


def prepare_cycle(
    config: Any,
    db: Any,
    *,
    dry_run: bool = False,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> Any:
    """Run control-plane preflight once for a daemon tick."""
    shell = _shell_module()
    return _prepare_cycle_use_case(
        config,
        db,
        deps=PrepareCycleDeps(
            initialize_cycle_runtime_deps=shell._build_init_cycle_runtime_deps(),
            phase_helper_deps=shell._build_phase_helper_deps(),
            begin_runtime_request_stats=shell.begin_runtime_request_stats,
            end_runtime_request_stats=shell.end_runtime_request_stats,
            snapshot_to_issue_ref=shell._snapshot_to_issue_ref,
            parse_issue_ref=shell.parse_issue_ref,
            record_metric=shell._record_metric,
            control_key_degraded=shell.CONTROL_KEY_DEGRADED,
            prepare_cycle_phases_deps_factory=PrepareCyclePhasesDeps,
            execute_prepare_cycle_phases=shell._execute_prepare_cycle_phases,
        ),
        prepared_cycle_context_factory=shell.PreparedCycleContext,
        dry_run=dry_run,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )
