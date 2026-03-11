"""Tick orchestration for the control-plane application layer."""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Callable


@dataclass(frozen=True)
class TickDeps:
    """Injected seams for control-plane tick orchestration."""

    load_config: Callable[..., Any]
    load_automation_config: Callable[..., Any]
    apply_automation_runtime: Callable[..., None]
    current_main_workflows: Callable[..., tuple[Any, dict[str, Any], int]]
    build_github_port_bundle: Callable[..., Any]
    replay_deferred_actions: Callable[..., tuple[int, ...]]
    drain_review_queue: Callable[..., tuple[Any, Any]]
    route_protected_queue_executors: Callable[..., Any]
    admit_backlog_items: Callable[..., Any]
    admission_summary_payload: Callable[..., dict[str, Any]]
    persist_admission_summary: Callable[..., None]
    record_successful_github_mutation: Callable[..., None]
    record_successful_board_sync: Callable[..., None]
    clear_degraded: Callable[..., None]
    mark_degraded: Callable[..., None]
    control_plane_health_summary: Callable[..., dict[str, Any]]
    runtime_gh_reason_code: Callable[[Exception], str]
    consumer_service_active: Callable[[], bool]
    gh_query_error_type: type[Exception]


def run_tick(
    *,
    args: Any,
    config: Any,
    db: Any,
    finalize_payload: Callable[[dict[str, object]], dict[str, object]],
    deps: TickDeps,
) -> tuple[int, dict[str, object]]:
    """Run one control-plane tick using injected operations."""
    timings_ms: dict[str, int] = {}
    critical_path_config = deps.load_config(config.critical_paths_path)
    automation_config = deps.load_automation_config(config.automation_config_path)
    deps.apply_automation_runtime(config, automation_config)
    _workflows, statuses, effective_interval = deps.current_main_workflows(config)
    config.poll_interval_seconds = effective_interval
    github_bundle = deps.build_github_port_bundle(
        config.project_owner,
        config.project_number,
        config=critical_path_config,
    )

    replayed: tuple[int, ...] = ()
    if not args.dry_run:
        try:
            phase_started = time.monotonic()
            replayed = deps.replay_deferred_actions(
                db,
                config,
                critical_path_config,
            )
            timings_ms["deferred_replay"] = int((time.monotonic() - phase_started) * 1000)
        except deps.gh_query_error_type as error:
            deps.mark_degraded(
                db,
                f"deferred-replay:{deps.runtime_gh_reason_code(error)}:{error}",
            )
            return 4, finalize_payload(
                {
                    "health": "degraded_recovering",
                    "reason_code": deps.runtime_gh_reason_code(error),
                    "error": f"deferred-replay:{error}",
                    "consumer_service_active": deps.consumer_service_active(),
                    "timings_ms": timings_ms,
                }
            )

    try:
        phase_started = time.monotonic()
        board_snapshot = None
        timings_ms["board_snapshot"] = int((time.monotonic() - phase_started) * 1000)

        phase_started = time.monotonic()
        routing = deps.route_protected_queue_executors(
            critical_path_config,
            automation_config,
            config.project_owner,
            config.project_number,
            dry_run=args.dry_run,
            board_snapshot=board_snapshot,
        )
        timings_ms["executor_routing"] = int((time.monotonic() - phase_started) * 1000)
    except deps.gh_query_error_type as error:
        deps.mark_degraded(
            db,
            f"executor-routing:{deps.runtime_gh_reason_code(error)}:{error}",
        )
        return 4, finalize_payload(
            {
                "health": "degraded_recovering",
                "reason_code": deps.runtime_gh_reason_code(error),
                "error": f"executor-routing:{error}",
                "consumer_service_active": deps.consumer_service_active(),
                "timings_ms": timings_ms,
            }
        )
    if routing.routed and not args.dry_run:
        deps.record_successful_github_mutation(db)

    phase_started = time.monotonic()
    review_queue_summary, board_snapshot = deps.drain_review_queue(
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
        deps.mark_degraded(
            db,
            f"review-queue:partial-failure:{review_queue_summary.error}",
        )
    elif (
        review_queue_summary.verdict_backfilled
        or review_queue_summary.rerun
        or review_queue_summary.auto_merge_enabled
        or review_queue_summary.requeued
    ) and not args.dry_run:
        deps.record_successful_github_mutation(db)

    phase_started = time.monotonic()
    admission_decision = deps.admit_backlog_items(
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
    admission_summary = deps.admission_summary_payload(
        admission_decision,
        enabled=automation_config.admission.enabled,
    )
    if admission_decision.admitted and not args.dry_run:
        deps.record_successful_github_mutation(db)
    if not args.dry_run:
        deps.persist_admission_summary(db, admission_summary)

    deps.record_successful_board_sync(db)
    deps.clear_degraded(db)
    control_state = db.control_state_snapshot()
    deferred_action_count = db.deferred_action_count()
    oldest_age = db.oldest_deferred_action_age_seconds()
    health = deps.control_plane_health_summary(
        control_state,
        deferred_action_count=deferred_action_count,
        oldest_deferred_action_age_seconds=oldest_age,
        poll_interval_seconds=effective_interval,
    )
    return 0, finalize_payload(
        {
            "health": health["health"],
            "reason_code": health["reason_code"],
            "consumer_service_active": deps.consumer_service_active(),
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
        }
    )
