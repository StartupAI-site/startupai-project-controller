"""Shell-facing runtime wiring extracted from board_consumer."""

from __future__ import annotations

from typing import Any, Callable

from startupai_controller.application.consumer.status import CollectStatusPayloadDeps


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


def collect_status_payload(
    config: Any,
    *,
    local_only: bool = False,
    collect_status_payload_use_case: Callable[..., dict[str, Any]],
    config_error_type: type[Exception],
    load_automation_config: Callable[[Any], Any],
    apply_automation_runtime: Callable[[Any, Any | None], None],
    current_main_workflows: Callable[..., tuple[dict[str, Any], dict[str, Any], int]],
    read_workflow_snapshot: Callable[[Any], Any | None],
    open_consumer_db: Callable[[Any], Any],
    load_config: Callable[[Any], Any],
    list_project_items_by_status: Callable[..., list[Any]],
    parse_issue_ref: Callable[[str], Any],
    load_admission_summary: Callable[[dict[str, str], Any | None], dict[str, Any]],
    control_plane_health_summary: Callable[..., Any],
    drain_requested: Callable[[Any], bool],
    workflow_status_payload: Callable[[Any], dict[str, Any]],
    session_retry_state: Callable[..., dict[str, Any]],
    parse_iso8601_timestamp: Callable[[str], Any],
    control_keys: dict[str, str],
) -> dict[str, Any]:
    """Collect consumer status as a JSON-serializable payload."""
    return collect_status_payload_use_case(
        config,
        local_only=local_only,
        deps=CollectStatusPayloadDeps(
            config_error_type=config_error_type,
            load_automation_config=load_automation_config,
            apply_automation_runtime=apply_automation_runtime,
            current_main_workflows=current_main_workflows,
            read_workflow_snapshot=read_workflow_snapshot,
            open_consumer_db=open_consumer_db,
            load_config=load_config,
            list_project_items_by_status=list_project_items_by_status,
            parse_issue_ref=parse_issue_ref,
            load_admission_summary=load_admission_summary,
            control_plane_health_summary=control_plane_health_summary,
            drain_requested=drain_requested,
            workflow_status_payload=workflow_status_payload,
            session_retry_state=session_retry_state,
            parse_iso8601_timestamp=parse_iso8601_timestamp,
            control_keys=control_keys,
        ),
    )
