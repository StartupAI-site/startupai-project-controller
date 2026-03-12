"""Shell-facing preflight wiring extracted from board_consumer."""

from __future__ import annotations

import logging
from typing import Any, Callable

import startupai_controller.consumer_operational_wiring as _operational_wiring
import startupai_controller.consumer_support_wiring as _support_wiring
from startupai_controller.control_plane_rescue import (
    _drain_review_queue,
    _replay_deferred_actions,
)
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
from startupai_controller.consumer_types import PreparedCycleContext
from startupai_controller.control_plane_runtime import (
    CONTROL_KEY_DEGRADED,
    _apply_automation_runtime,
    _clear_degraded,
    _current_main_workflows,
    _mark_degraded,
    _persist_admission_summary,
    _record_successful_board_sync,
    _record_successful_github_mutation,
)
from startupai_controller.runtime.wiring import (
    GitHubRuntimeMemo as CycleGitHubMemo,
    begin_runtime_request_stats,
    build_gh_runner_port,
    build_github_port_bundle,
    build_ready_flow_port,
    build_session_store,
    end_runtime_request_stats,
)
from startupai_controller.domain.scheduling_policy import (
    snapshot_to_issue_ref as _snapshot_to_issue_ref,
)
from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    load_config,
    parse_issue_ref,
)
from startupai_controller.board_automation_config import load_automation_config

logger = logging.getLogger("board-consumer")


def build_init_cycle_runtime_deps() -> InitializeCycleRuntimeDeps:
    """Build the deps for cycle runtime initialization."""
    return InitializeCycleRuntimeDeps(
        load_automation_config=load_automation_config,
        apply_automation_runtime=_apply_automation_runtime,
        current_main_workflows=_current_main_workflows,
        config_error_type=ConfigError,
        logger=logger,
    )


def build_phase_helper_deps() -> PhaseHelperDeps:
    """Build the deps for preflight phase helpers."""
    return PhaseHelperDeps(
        replay_deferred_actions=_replay_deferred_actions,
        drain_review_queue=_drain_review_queue,
        reconcile_board_truth=_operational_wiring.reconcile_board_truth,
        record_successful_github_mutation=_record_successful_github_mutation,
        record_successful_board_sync=_record_successful_board_sync,
        clear_degraded=_clear_degraded,
        mark_degraded=_mark_degraded,
        persist_admission_summary=_persist_admission_summary,
        logger=logger,
    )


def initialize_cycle_runtime(
    config: Any,
    db: Any,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> Any:
    """Build cycle-scoped runtime wiring and effective config."""
    cp_config = load_config(config.critical_paths_path)
    gh_port = (
        gh_runner
        if hasattr(gh_runner, "run_gh")
        else build_gh_runner_port(gh_runner=gh_runner)
    )
    github_memo = CycleGitHubMemo()
    github_bundle = build_github_port_bundle(
        config.project_owner,
        config.project_number,
        config=cp_config,
        github_memo=github_memo,
        gh_runner=gh_port.run_gh if gh_port is not None else None,
    )
    return _initialize_cycle_runtime_use_case(
        config,
        deps=build_init_cycle_runtime_deps(),
        session_store=build_session_store(db),
        cp_config=cp_config,
        github_memo=github_memo,
        ready_flow_port=build_ready_flow_port(),
        pr_port=github_bundle.pull_requests,
        review_state_port=github_bundle.review_state,
        board_port=github_bundle.board_mutations,
        gh_port=gh_port,
    )


def run_deferred_replay_phase(
    config: Any,
    db: Any,
    runtime: Any,
    *,
    timings_ms: dict[str, int],
    dry_run: bool,
) -> None:
    """Replay deferred actions for the cycle when enabled."""
    return _run_deferred_replay_phase_use_case(
        config,
        db,
        runtime,
        deps=build_phase_helper_deps(),
        timings_ms=timings_ms,
        dry_run=dry_run,
    )


def load_board_snapshot_phase(
    config: Any,
    runtime: Any,
    *,
    timings_ms: dict[str, int],
) -> Any:
    """Load the cycle board snapshot."""
    return _load_board_snapshot_phase_use_case(
        config,
        runtime,
        timings_ms=timings_ms,
    )


def run_executor_routing_phase(
    config: Any,
    db: Any,
    runtime: Any,
    *,
    board_snapshot: Any,
    timings_ms: dict[str, int],
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
        dry_run=dry_run,
    )


def run_reconciliation_phase(
    config: Any,
    db: Any,
    runtime: Any,
    *,
    board_snapshot: Any,
    timings_ms: dict[str, int],
) -> None:
    """Run truthful board reconciliation for the cycle."""
    return _run_reconciliation_phase_use_case(
        config,
        db,
        runtime,
        deps=build_phase_helper_deps(),
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
    )


def run_review_queue_phase(
    config: Any,
    db: Any,
    runtime: Any,
    *,
    board_snapshot: Any,
    timings_ms: dict[str, int],
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
        dry_run=dry_run,
    )


def run_admission_phase(
    config: Any,
    db: Any,
    runtime: Any,
    *,
    board_snapshot: Any,
    timings_ms: dict[str, int],
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
        dry_run=dry_run,
    )


def execute_prepare_cycle_phases(
    config: Any,
    db: Any,
    runtime: Any,
    *,
    deps: Any | None = None,
    dry_run: bool = False,
) -> Any:
    """Execute the preflight phases for one cycle."""
    if deps is None:
        deps = PrepareCyclePhasesDeps(
            run_deferred_replay_phase=run_deferred_replay_phase,
            load_board_snapshot_phase=load_board_snapshot_phase,
            run_executor_routing_phase=run_executor_routing_phase,
            run_reconciliation_phase=run_reconciliation_phase,
            run_review_queue_phase=run_review_queue_phase,
            run_admission_phase=run_admission_phase,
        )
        result = _execute_prepare_cycle_phases_use_case(
            config,
            db,
            runtime=runtime,
            deps=deps,
            dry_run=dry_run,
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
    )
    return result


def prepare_cycle(
    config: Any,
    db: Any,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
) -> Any:
    """Run control-plane preflight once for a daemon tick."""
    return _prepare_cycle_use_case(
        config,
        db,
        deps=PrepareCycleDeps(
            initialize_cycle_runtime=initialize_cycle_runtime,
            phase_helper_deps=build_phase_helper_deps(),
            begin_runtime_request_stats=begin_runtime_request_stats,
            end_runtime_request_stats=end_runtime_request_stats,
            snapshot_to_issue_ref=_snapshot_to_issue_ref,
            parse_issue_ref=parse_issue_ref,
            record_metric=_support_wiring.record_metric,
            control_key_degraded=CONTROL_KEY_DEGRADED,
            prepare_cycle_phases_deps_factory=PrepareCyclePhasesDeps,
            execute_prepare_cycle_phases=execute_prepare_cycle_phases,
        ),
        prepared_cycle_context_factory=PreparedCycleContext,
        dry_run=dry_run,
        gh_runner=build_gh_runner_port(gh_runner=gh_runner),
    )
