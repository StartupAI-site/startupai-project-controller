#!/usr/bin/env python3
"""Repo-tracked board control-plane wrapper for overnight supervision.

This script replaces ad hoc user-level shepherd logic with repo-owned commands.
It intentionally delegates all stateful decisions to tracked consumer and review
automation code.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


from startupai_controller.application.control_plane.tick import TickDeps, run_tick
from startupai_controller.board_automation_config import (
    DEFAULT_AUTOMATION_CONFIG_PATH,
    DEFAULT_PROJECT_NUMBER,
    DEFAULT_PROJECT_OWNER,
    load_automation_config,
)
from startupai_controller.consumer_config import (
    ConsumerConfig,
    DEFAULT_CONFIG_PATH,
    DEFAULT_DB_PATH,
    DEFAULT_DRAIN_PATH,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SCHEMA_PATH,
    DEFAULT_WORKFLOW_STATE_PATH,
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
from startupai_controller.control_plane_rescue import (
    _drain_review_queue,
    _replay_deferred_actions,
)
from startupai_controller.consumer_workflow import default_repo_roots
from startupai_controller.runtime.wiring import (
    begin_runtime_request_stats,
    build_github_port_bundle,
    build_ready_flow_port,
    build_service_control_port,
    end_runtime_request_stats,
    open_consumer_db,
    runtime_gh_reason_code,
)
from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    GhQueryError,
    load_config,
    parse_issue_ref,
)


def _consumer_config_from_args(args: argparse.Namespace) -> ConsumerConfig:
    """Build a ConsumerConfig for repo-tracked control-plane operations."""
    return ConsumerConfig(
        critical_paths_path=Path(args.file),
        automation_config_path=Path(args.automation_config),
        project_owner=args.project_owner,
        project_number=args.project_number,
        db_path=Path(args.db_path),
        schema_path=Path(args.schema_path),
        output_dir=Path(args.output_dir),
        drain_path=Path(args.drain_path),
        workflow_state_path=Path(args.workflow_state_path),
        repo_roots=default_repo_roots(),
    )


def _consumer_service_active() -> bool:
    """Return True when the user-level consumer service is active."""
    return build_service_control_port().is_active("startupai-consumer.service")


def _review_scope_refs(
    config: ConsumerConfig,
    critical_path_config,
    review_state_port,
) -> list[str]:
    """Return governed Review issue refs for this executor."""
    review_refs: list[str] = []
    for snapshot in review_state_port.list_issues_by_status("Review"):
        issue_ref = snapshot.issue_ref
        if parse_issue_ref(issue_ref).prefix not in config.repo_prefixes:
            continue
        if snapshot.executor.strip().lower() != config.executor:
            continue
        review_refs.append(issue_ref)
    return review_refs


def _tick(args: argparse.Namespace) -> tuple[int, dict[str, object]]:
    """Run one repo-tracked overnight control-plane tick."""
    config = _consumer_config_from_args(args)
    db = open_consumer_db(config.db_path)
    request_stats_token = begin_runtime_request_stats()
    timings_ms: dict[str, int] = {}
    request_counts_recorded = False

    def _finalize_payload(
        payload: dict[str, object],
    ) -> dict[str, object]:
        nonlocal request_counts_recorded
        if not request_counts_recorded:
            request_stats = end_runtime_request_stats(request_stats_token)
            payload["github_request_counts"] = {
                "graphql": request_stats.graphql,
                "rest": request_stats.rest,
            }
            request_counts_recorded = True
        return payload

    try:
        return run_tick(
            args=args,
            config=config,
            db=db,
            finalize_payload=_finalize_payload,
            deps=TickDeps(
                load_config=load_config,
                load_automation_config=load_automation_config,
                apply_automation_runtime=_apply_automation_runtime,
                current_main_workflows=_current_main_workflows,
                build_github_port_bundle=build_github_port_bundle,
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
                consumer_service_active=_consumer_service_active,
                gh_query_error_type=GhQueryError,
            ),
        )
    finally:
        if not request_counts_recorded:
            try:
                end_runtime_request_stats(request_stats_token)
            except Exception:
                pass
        db.close()


def build_parser() -> argparse.ArgumentParser:
    """Build CLI parser."""
    parser = argparse.ArgumentParser(
        description="Repo-tracked board control plane for overnight supervision.",
    )
    parser.add_argument("--file", default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--automation-config", default=DEFAULT_AUTOMATION_CONFIG_PATH)
    parser.add_argument("--project-owner", default=DEFAULT_PROJECT_OWNER)
    parser.add_argument("--project-number", type=int, default=DEFAULT_PROJECT_NUMBER)
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--schema-path", default=str(DEFAULT_SCHEMA_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--drain-path", default=str(DEFAULT_DRAIN_PATH))
    parser.add_argument(
        "--workflow-state-path", default=str(DEFAULT_WORKFLOW_STATE_PATH)
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    tick = subparsers.add_parser("tick", help="Run one control-plane sweep")
    tick.add_argument("--dry-run", action="store_true", default=False)
    tick.add_argument("--json", action="store_true", default=False)

    run = subparsers.add_parser("run", help="Run continuous control-plane sweeps")
    run.add_argument("--interval", type=int, default=180)
    run.add_argument("--dry-run", action="store_true", default=False)
    run.add_argument("--json", action="store_true", default=False)

    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "tick":
        code, payload = _tick(args)
        if args.json:
            print(json.dumps(payload, indent=2))
        else:
            print(json.dumps(payload, indent=2))
        return code

    if args.command == "run":
        while True:
            code, payload = _tick(args)
            if args.json:
                print(json.dumps(payload, indent=2))
            else:
                print(json.dumps(payload, indent=2))
            if code not in {0, 4}:
                return code
            time.sleep(args.interval)

    parser.error(f"Unsupported command: {args.command}")
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
