"""Control-plane tick runtime composition."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, cast

from startupai_controller.application.control_plane.tick import GitHubBundle, TickDeps
from startupai_controller.board_automation_config import load_automation_config
from startupai_controller.control_plane_rescue import (
    _drain_review_queue,
    _replay_deferred_actions,
)
from startupai_controller.control_plane_runtime import (
    _apply_automation_runtime,
    _clear_degraded,
    _control_plane_health_summary,
    _current_main_workflows,
    _mark_degraded,
    _persist_admission_summary,
    _record_successful_board_sync,
    _record_successful_github_mutation,
)
from startupai_controller.runtime.wiring import (
    begin_runtime_request_stats,
    build_github_port_bundle,
    build_ready_flow_port,
    build_service_control_port,
    end_runtime_request_stats,
    open_consumer_db,
    runtime_gh_reason_code,
)
from startupai_controller.ports.control_plane_state import ControlPlaneStatePort
from startupai_controller.validate_critical_path_promotion import (
    GhQueryError,
    load_config,
)


@dataclass
class TickRuntime:
    """Runtime resources assembled for one control-plane tick."""

    db: ControlPlaneStatePort
    finalize_payload: Callable[[dict[str, object]], dict[str, object]]
    deps: TickDeps
    cleanup: Callable[[], None]


def build_tick_runtime(*, args: Any, config: Any) -> TickRuntime:
    """Build runtime resources and dependency bundle for one control-plane tick."""
    db = open_consumer_db(config.db_path)
    request_stats_token = begin_runtime_request_stats()
    request_counts_recorded = False

    def finalize_payload(payload: dict[str, object]) -> dict[str, object]:
        nonlocal request_counts_recorded
        if not request_counts_recorded:
            request_stats = end_runtime_request_stats(request_stats_token)
            payload["github_request_counts"] = {
                "graphql": request_stats.graphql,
                "rest": request_stats.rest,
            }
            request_counts_recorded = True
        return payload

    def cleanup() -> None:
        nonlocal request_counts_recorded
        if not request_counts_recorded:
            try:
                end_runtime_request_stats(request_stats_token)
            except Exception:
                pass
        db.close()

    deps = TickDeps(
        load_config=load_config,
        load_automation_config=load_automation_config,
        apply_automation_runtime=_apply_automation_runtime,
        current_main_workflows=_current_main_workflows,
        build_github_port_bundle=cast(
            Callable[..., GitHubBundle],
            build_github_port_bundle,
        ),
        ready_flow_port=build_ready_flow_port(),
        replay_deferred_actions=_replay_deferred_actions,
        drain_review_queue=_drain_review_queue,
        persist_admission_summary=_persist_admission_summary,
        record_successful_github_mutation=_record_successful_github_mutation,
        record_successful_board_sync=_record_successful_board_sync,
        clear_degraded=_clear_degraded,
        mark_degraded=_mark_degraded,
        control_plane_health_summary=_control_plane_health_summary,
        runtime_gh_reason_code=runtime_gh_reason_code,
        service_control_port=build_service_control_port(),
        gh_query_error_type=GhQueryError,
    )

    return TickRuntime(
        db=db,
        finalize_payload=finalize_payload,
        deps=deps,
        cleanup=cleanup,
    )
