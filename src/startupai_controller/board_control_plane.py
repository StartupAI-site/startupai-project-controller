#!/usr/bin/env python3
"""Repo-tracked board control-plane wrapper for overnight supervision.

This script replaces ad hoc user-level shepherd logic with repo-owned commands.
It intentionally delegates all stateful decisions to tracked consumer and review
automation code.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path


from startupai_controller.board_automation import (
    admission_summary_payload,
    admit_backlog_items,
    route_protected_queue_executors,
)
from startupai_controller.board_automation_config import (
    DEFAULT_AUTOMATION_CONFIG_PATH,
    DEFAULT_PROJECT_NUMBER,
    DEFAULT_PROJECT_OWNER,
    load_automation_config,
)
from startupai_controller.board_consumer import (
    ConsumerConfig,
    DEFAULT_CONFIG_PATH,
    DEFAULT_DB_PATH,
    DEFAULT_DRAIN_PATH,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SCHEMA_PATH,
    DEFAULT_WORKFLOW_STATE_PATH,
    _apply_automation_runtime,
    _clear_degraded,
    _control_plane_health_summary,
    _current_main_workflows,
    _persist_admission_summary,
    _mark_degraded,
    _record_successful_board_sync,
    _record_successful_github_mutation,
    _replay_deferred_actions,
    _drain_review_queue,
)
from startupai_controller.consumer_workflow import default_repo_roots
from startupai_controller.runtime.wiring import (
    begin_runtime_request_stats,
    build_github_port_bundle,
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
    result = subprocess.run(
        ["systemctl", "--user", "is-active", "startupai-consumer.service"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0 and result.stdout.strip() == "active"


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
        critical_path_config = load_config(config.critical_paths_path)
        automation_config = load_automation_config(config.automation_config_path)
        _apply_automation_runtime(config, automation_config)
        _workflows, statuses, effective_interval = _current_main_workflows(config)
        config.poll_interval_seconds = effective_interval
        github_bundle = build_github_port_bundle(
            config.project_owner,
            config.project_number,
            config=critical_path_config,
        )

        replayed = ()
        if not args.dry_run:
            try:
                phase_started = time.monotonic()
                replayed = _replay_deferred_actions(
                    db,
                    config,
                    critical_path_config,
                )
                timings_ms["deferred_replay"] = int((time.monotonic() - phase_started) * 1000)
            except GhQueryError as error:
                _mark_degraded(
                    db,
                    f"deferred-replay:{runtime_gh_reason_code(error)}:{error}",
                )
                return 4, _finalize_payload({
                    "health": "degraded_recovering",
                    "reason_code": runtime_gh_reason_code(error),
                    "error": f"deferred-replay:{error}",
                    "consumer_service_active": _consumer_service_active(),
                    "timings_ms": timings_ms,
                })

        try:
            phase_started = time.monotonic()
            board_snapshot = None
            timings_ms["board_snapshot"] = int((time.monotonic() - phase_started) * 1000)

            phase_started = time.monotonic()
            routing = route_protected_queue_executors(
                critical_path_config,
                automation_config,
                config.project_owner,
                config.project_number,
                dry_run=args.dry_run,
                board_snapshot=board_snapshot,
            )
            timings_ms["executor_routing"] = int((time.monotonic() - phase_started) * 1000)
        except GhQueryError as error:
            _mark_degraded(
                db,
                f"executor-routing:{runtime_gh_reason_code(error)}:{error}",
            )
            return 4, _finalize_payload({
                "health": "degraded_recovering",
                "reason_code": runtime_gh_reason_code(error),
                "error": f"executor-routing:{error}",
                "consumer_service_active": _consumer_service_active(),
                "timings_ms": timings_ms,
            })
        if routing.routed and not args.dry_run:
            _record_successful_github_mutation(db)

        phase_started = time.monotonic()
        review_queue_summary, board_snapshot = _drain_review_queue(
            config,
            db,
            critical_path_config,
            automation_config,
            board_snapshot=board_snapshot,
            dry_run=args.dry_run,
            github_memo=github_bundle.github_memo,
            pr_port=github_bundle.pull_requests,
        )
        timings_ms["review_queue"] = int((time.monotonic() - phase_started) * 1000)
        if review_queue_summary.error:
            _mark_degraded(
                db,
                f"review-queue:partial-failure:{review_queue_summary.error}",
            )
        elif (
            review_queue_summary.verdict_backfilled
            or review_queue_summary.rerun
            or review_queue_summary.auto_merge_enabled
            or review_queue_summary.requeued
        ) and not args.dry_run:
            _record_successful_github_mutation(db)

        phase_started = time.monotonic()
        admission_decision = admit_backlog_items(
            critical_path_config,
            automation_config,
            config.project_owner,
            config.project_number,
            dispatchable_repo_prefixes=tuple(
                repo_prefix
                for repo_prefix, status in statuses.items()
                if status.available
            ),
            active_lease_issue_refs=tuple(db.active_lease_issue_refs()),
            dry_run=args.dry_run,
            board_snapshot=board_snapshot,
            github_memo=github_bundle.github_memo,
        )
        timings_ms["admission"] = int((time.monotonic() - phase_started) * 1000)
        admission_summary = admission_summary_payload(
            admission_decision,
            enabled=automation_config.admission.enabled,
        )
        if admission_decision.admitted and not args.dry_run:
            _record_successful_github_mutation(db)
        if not args.dry_run:
            _persist_admission_summary(db, admission_summary)

        _record_successful_board_sync(db)
        _clear_degraded(db)
        control_state = db.control_state_snapshot()
        deferred_action_count = db.deferred_action_count()
        oldest_age = db.oldest_deferred_action_age_seconds()
        health = _control_plane_health_summary(
            control_state,
            deferred_action_count=deferred_action_count,
            oldest_deferred_action_age_seconds=oldest_age,
            poll_interval_seconds=effective_interval,
        )
        return 0, _finalize_payload({
            "health": health["health"],
            "reason_code": health["reason_code"],
            "consumer_service_active": _consumer_service_active(),
            "poll_interval_seconds": effective_interval,
            "timings_ms": timings_ms,
            "replayed_actions": list(replayed),
            "deferred_action_count": deferred_action_count,
            "oldest_deferred_action_age_seconds": oldest_age,
            "repo_workflows": {
                repo_prefix: {
                    "available": status.available,
                    "source_path": str(status.source_path),
                    "source_kind": status.source_kind,
                    "disabled_reason": status.disabled_reason,
                }
                for repo_prefix, status in statuses.items()
            },
            "review_rescue": {
                "queued_count": review_queue_summary.queued_count,
                "due_count": review_queue_summary.due_count,
                "seeded": list(review_queue_summary.seeded),
                "removed": list(review_queue_summary.removed),
                "verdict_backfill": list(review_queue_summary.verdict_backfilled),
                "rerun": list(review_queue_summary.rerun),
                "auto_merge_enabled": list(review_queue_summary.auto_merge_enabled),
                "requeued": list(review_queue_summary.requeued),
                "blocked": list(review_queue_summary.blocked),
                "skipped": list(review_queue_summary.skipped),
                "partial_failure": review_queue_summary.partial_failure,
                "error": review_queue_summary.error,
            },
            "verdict_backfill": list(review_queue_summary.verdict_backfilled),
            "admission": admission_summary,
            "executor_routing": {
                "routed": list(routing.routed),
                "unchanged": list(routing.unchanged),
                "skipped": [
                    {"issue_ref": issue_ref, "reason": reason}
                    for issue_ref, reason in routing.skipped
                ],
            },
        })
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
