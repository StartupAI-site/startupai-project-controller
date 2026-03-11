"""CLI handler implementations for board automation subcommands.

Each ``_cmd_*`` function translates parsed CLI arguments into calls to the
public orchestration surface in ``board_automation``.  A lazy ``_core()``
import avoids circular dependency since ``board_automation`` re-exports these
names.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from startupai_controller.board_automation_config import (
    BoardAutomationConfig,
    ConfigError,
    DEFAULT_AUTOMATION_CONFIG_PATH,
    load_automation_config,
)
from startupai_controller.board_graph import classify_parallelism_snapshot
from startupai_controller.runtime.wiring import build_github_port_bundle
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig


# ---------------------------------------------------------------------------
# Lazy import to break circular dependency with board_automation
# ---------------------------------------------------------------------------


def _core():
    from startupai_controller import board_automation as core

    return core


# ---------------------------------------------------------------------------
# Shared CLI helper
# ---------------------------------------------------------------------------


def _workflow_mutations_enabled(
    automation_config: BoardAutomationConfig,
    workflow_name: str,
) -> bool:
    """Return whether a deprecated workflow is still allowed to mutate state."""
    if automation_config.execution_authority_mode != "single_machine":
        return True
    return automation_config.deprecated_workflow_mutations.get(workflow_name, False)


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------


def _cmd_mark_done(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for mark-done subcommand."""
    core = _core()
    if "/" not in args.pr_repo:
        print(
            f"CONFIG ERROR: --pr-repo must be 'owner/repo', got '{args.pr_repo}'",
            file=sys.stderr,
        )
        return 3

    pr_owner, pr_repo = args.pr_repo.split("/", maxsplit=1)

    issues = core.query_closing_issues(
        pr_owner, pr_repo, args.pr_number, config
    )

    if not issues:
        print("No linked issues found for this PR.")
        return 0

    if args.dry_run:
        refs = [i.ref for i in issues]
        print(f"Would mark Done: {refs}")
        return 0

    marked = core.mark_issues_done(
        issues, config, args.project_owner, args.project_number
    )

    print(f'DONE_ISSUES={json.dumps(marked)}')

    if not marked:
        print("No issues needed status change.")
        return 0

    return 0


def _cmd_auto_promote(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for auto-promote subcommand."""
    core = _core()
    automation_config = load_automation_config(Path(args.automation_config))
    result = core.auto_promote_successors(
        issue_ref=args.issue,
        config=config,
        this_repo_prefix=args.this_repo_prefix,
        project_owner=args.project_owner,
        project_number=args.project_number,
        automation_config=automation_config,
        dry_run=args.dry_run,
    )

    if result.promoted:
        print(f"Promoted: {result.promoted}")
    if result.skipped:
        for ref, reason in result.skipped:
            print(f"Skipped {ref}: {reason}")
    if result.cross_repo_pending:
        print(f"Cross-repo pending: {result.cross_repo_pending}")
    if result.handoff_jobs:
        print(f"Handoff jobs: {result.handoff_jobs}")

    if not result.promoted and not result.cross_repo_pending:
        return 0

    return 0


def _cmd_admit_backlog(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for autonomous backlog admission."""
    core = _core()
    automation_config = load_automation_config(Path(args.automation_config))
    github_bundle = build_github_port_bundle(
        args.project_owner,
        args.project_number,
        config=config,
    )
    decision = core.admit_backlog_items(
        config,
        automation_config,
        args.project_owner,
        args.project_number,
        github_bundle=github_bundle,
        dry_run=args.dry_run,
    )
    payload = core.admission_summary_payload(
        decision,
        enabled=automation_config.admission.enabled,
    )
    if args.issue:
        payload["top_candidates"] = [
            item
            for item in payload["top_candidates"]
            if item["issue_ref"] == args.issue
        ]
        payload["top_skipped"] = [
            item
            for item in payload["top_skipped"]
            if item["issue_ref"] == args.issue
        ]
    if args.limit is not None:
        payload["top_candidates"] = payload["top_candidates"][: args.limit]
        payload["top_skipped"] = payload["top_skipped"][: args.limit]

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0 if not decision.partial_failure else 4

    print(
        "Admission ready count: "
        f"{payload['ready_count']} / floor {payload['ready_floor']} (cap {payload['ready_cap']})"
    )
    print(f"Admission needed: {payload['needed']}")
    print(f"Admission admitted: {payload['admitted']}")
    if payload["top_candidates"]:
        print(f"Top candidates: {payload['top_candidates']}")
    if payload["top_skipped"]:
        print(f"Top skipped: {payload['top_skipped']}")
    if payload["error"]:
        print(f"Admission error: {payload['error']}")
    return 0 if not decision.partial_failure else 4


def _cmd_propagate_blocker(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for propagate-blocker subcommand."""
    core = _core()
    commented = core.propagate_blocker(
        issue_ref=getattr(args, "issue", None),
        config=config,
        this_repo_prefix=getattr(args, "this_repo_prefix", None),
        project_owner=args.project_owner,
        project_number=args.project_number,
        sweep_blocked=args.sweep_blocked,
        all_prefixes=args.all_prefixes,
        dry_run=args.dry_run,
    )

    if commented:
        print(f"Commented on: {commented}")
        return 0

    print("No new advisory comments posted.")
    return 0


def _cmd_reconcile_handoffs(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for reconcile-handoffs subcommand."""
    core = _core()
    github_bundle = build_github_port_bundle(
        args.project_owner,
        args.project_number,
        config=config,
    )
    counters = core.reconcile_handoffs(
        config=config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        ack_timeout_minutes=args.ack_timeout_minutes,
        max_retries=args.max_retries,
        dry_run=args.dry_run,
        github_bundle=github_bundle,
    )

    print(json.dumps(counters, indent=2))

    if counters["escalated"] > 0:
        return 2  # Escalations indicate unresolved handoffs
    return 0


def _cmd_schedule_ready(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for schedule-ready subcommand."""
    core = _core()
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    effective_mode = args.mode
    effective_dry_run = args.dry_run
    if not _workflow_mutations_enabled(automation_config, "ready-scheduler"):
        effective_mode = "advisory"
        effective_dry_run = True

    decision = core.schedule_ready_items(
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        this_repo_prefix=getattr(args, "this_repo_prefix", None),
        all_prefixes=getattr(args, "all_prefixes", False),
        mode=effective_mode,
        per_executor_wip_limit=args.per_executor_wip_limit,
        missing_executor_block_cap=args.missing_executor_block_cap,
        dry_run=effective_dry_run,
    )

    if decision.claimable:
        print(f"Claimable (advisory): {decision.claimable}")
    if decision.claimed:
        print(f"Claimed: {decision.claimed}")
    print(f"CLAIMED_ISSUES_JSON={json.dumps(sorted(decision.claimed))}")
    if decision.deferred_dependency:
        print(f"Deferred (dependency): {decision.deferred_dependency}")
    if decision.deferred_wip:
        print(f"Deferred (WIP limit): {decision.deferred_wip}")
    if decision.blocked_invalid_ready:
        print(f"Blocked (invalid Ready): {decision.blocked_invalid_ready}")
    if decision.blocked_missing_executor:
        print(
            "Blocked (missing/invalid executor): "
            f"{decision.blocked_missing_executor}"
        )
    if decision.skipped_non_graph:
        print(
            "Considered (non-graph, dependency check skipped): "
            f"{decision.skipped_non_graph}"
        )
    if decision.skipped_missing_executor:
        print(
            "Skipped (missing executor over cap): "
            f"{decision.skipped_missing_executor}"
        )

    if not decision.claimed and not decision.claimable:
        print("No claimable Ready items this run.")
        return 0
    return 0


def _cmd_claim_ready(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for claim-ready subcommand."""
    core = _core()
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    result = core.claim_ready_issue(
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        executor=args.executor,
        issue_ref=getattr(args, "issue", None),
        next_issue=getattr(args, "next", False),
        this_repo_prefix=getattr(args, "this_repo_prefix", None),
        all_prefixes=getattr(args, "all_prefixes", False),
        per_executor_wip_limit=args.per_executor_wip_limit,
        dry_run=args.dry_run,
    )

    if result.claimed:
        if args.dry_run:
            print(f"Would claim: {result.claimed}")
        else:
            print(f"Claimed: {result.claimed}")
        return 0

    print(f"Claim rejected: {result.reason}")
    return 2


def _cmd_dispatch_agent(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for dispatch-agent subcommand."""
    core = _core()
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    if not _workflow_mutations_enabled(automation_config, "ready-scheduler"):
        print("Dispatch skipped: local consumer owns execution claims in single_machine mode.")
        return 0

    result = core.dispatch_agent(
        issue_refs=args.issue,
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        dry_run=args.dry_run,
    )
    if result.dispatched:
        print(f"Dispatched: {sorted(result.dispatched)}")
    if result.skipped:
        for ref, reason in result.skipped:
            print(f"Skipped {ref}: {reason}")
    if result.failed:
        for ref, reason in result.failed:
            print(f"Failed {ref}: {reason}", file=sys.stderr)
        return 4
    return 0


def _cmd_rebalance_wip(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for rebalance-wip subcommand."""
    core = _core()
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    effective_dry_run = args.dry_run
    if not _workflow_mutations_enabled(automation_config, "wip-rebalance"):
        effective_dry_run = True

    decision = core.rebalance_wip(
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        this_repo_prefix=getattr(args, "this_repo_prefix", None),
        all_prefixes=getattr(args, "all_prefixes", False),
        cycle_minutes=args.cycle_minutes,
        dry_run=effective_dry_run,
    )
    if decision.kept:
        print(f"Kept: {sorted(decision.kept)}")
    if decision.marked_stale:
        print(f"Marked stale: {sorted(decision.marked_stale)}")
    if decision.moved_ready:
        print(f"Moved to Ready: {sorted(decision.moved_ready)}")
    if decision.moved_blocked:
        print(f"Moved to Blocked: {sorted(decision.moved_blocked)}")
    if decision.skipped:
        for ref, reason in decision.skipped:
            print(f"Skipped {ref}: {reason}")
    return 0


def _cmd_enforce_ready_deps(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for enforce-ready-dependencies subcommand."""
    core = _core()
    corrected = core.enforce_ready_dependency_guard(
        config=config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        this_repo_prefix=getattr(args, "this_repo_prefix", None),
        all_prefixes=getattr(args, "all_prefixes", False),
        dry_run=args.dry_run,
    )

    if corrected:
        print(f"Corrected: {corrected}")
        return 0

    print("No Ready items have unmet predecessors.")
    return 0


def _cmd_audit_in_progress(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for audit-in-progress subcommand."""
    core = _core()
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    effective_dry_run = args.dry_run
    if not _workflow_mutations_enabled(automation_config, "stale-work-guard"):
        effective_dry_run = True

    stale = core.audit_in_progress(
        config=config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        max_age_hours=args.max_age_hours,
        this_repo_prefix=getattr(args, "this_repo_prefix", None),
        all_prefixes=getattr(args, "all_prefixes", False),
        dry_run=effective_dry_run,
    )

    if stale:
        print(f"Escalated stale In Progress: {stale}")
        return 0

    print("No stale In Progress items found.")
    return 0


def _cmd_sync_review_state(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for sync-review-state subcommand."""
    core = _core()
    try:
        automation_config = load_automation_config(
            Path(getattr(args, "automation_config", DEFAULT_AUTOMATION_CONFIG_PATH))
        )
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    github_bundle = build_github_port_bundle(
        args.project_owner,
        args.project_number,
        config=config,
    )

    if args.from_github_event:
        # Read event path from environment variable
        event_path = os.environ.get("GITHUB_EVENT_PATH", "")
        if not event_path:
            print(
                "CONFIG ERROR: $GITHUB_EVENT_PATH not set",
                file=sys.stderr,
            )
            return 3

        # Parse event file and process all linked issues
        pairs = core.resolve_issues_from_event(
            event_path,
            config,
            pr_port=github_bundle.pull_requests,
        )

        if not pairs:
            print("No actionable issue/event pairs found in event.")
            return 0  # No-op success — benign events are not errors

        cli_failed_checks = getattr(args, "failed_checks", None)
        fatal_code = 0
        for issue_ref, event_kind, event_failed_checks in pairs:
            # CLI --failed-checks overrides; else use event-derived names
            effective_failed = cli_failed_checks or event_failed_checks
            code, msg = core.sync_review_state(
                event_kind=event_kind,
                issue_ref=issue_ref,
                config=config,
                project_owner=args.project_owner,
                project_number=args.project_number,
                automation_config=automation_config,
                github_bundle=github_bundle,
                checks_state=args.checks_state,
                failed_checks=effective_failed,
                dry_run=args.dry_run,
            )
            print(msg)
            if code in (3, 4) and code > fatal_code:
                fatal_code = code
        return fatal_code

    # Manual mode with --event-kind
    event_kind = args.event_kind

    if args.resolve_pr:
        pr_repo = args.resolve_pr[0]
        try:
            pr_number = int(args.resolve_pr[1])
        except ValueError:
            print(
                "CONFIG ERROR: PR number must be integer, "
                f"got '{args.resolve_pr[1]}'",
                file=sys.stderr,
            )
            return 3

        issue_refs = core.resolve_pr_to_issues(
            pr_repo,
            pr_number,
            config,
            pr_port=github_bundle.pull_requests,
        )
        if not issue_refs:
            print("No linked issues found for this PR.")
            return 0  # No-op success
    elif args.issue:
        issue_refs = [args.issue]
    else:
        print(
            "CONFIG ERROR: --event-kind requires --issue or --resolve-pr",
            file=sys.stderr,
        )
        return 3

    failed_checks = getattr(args, "failed_checks", None)

    # For bridge path: resolve failed check names from PR when not provided
    if event_kind == "checks_failed" and failed_checks is None and args.resolve_pr:
        pr_owner, pr_repo_name = pr_repo.split("/", maxsplit=1)
        head_sha = core._query_pr_head_sha(
            pr_owner,
            pr_repo_name,
            pr_number,
            pr_port=github_bundle.pull_requests,
        )
        if head_sha:
            failed_checks = core._query_failed_check_runs(
                pr_owner,
                pr_repo_name,
                head_sha,
                pr_port=github_bundle.pull_requests,
            )

    fatal_code = 0
    for issue_ref in issue_refs:
        code, msg = core.sync_review_state(
            event_kind=event_kind,
            issue_ref=issue_ref,
            config=config,
            project_owner=args.project_owner,
            project_number=args.project_number,
            automation_config=automation_config,
            github_bundle=github_bundle,
            checks_state=args.checks_state,
            failed_checks=failed_checks,
            dry_run=args.dry_run,
        )
        print(msg)
        if code in (3, 4) and code > fatal_code:
            fatal_code = code
    return fatal_code


def _cmd_codex_review_gate(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for codex-review-gate subcommand."""
    core = _core()
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    if "/" not in args.pr_repo:
        print(
            f"CONFIG ERROR: --pr-repo must be 'owner/repo', got '{args.pr_repo}'",
            file=sys.stderr,
        )
        return 3

    code, msg = core.codex_review_gate(
        pr_repo=args.pr_repo,
        pr_number=args.pr_number,
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        dry_run=args.dry_run,
        apply_fail_routing=not args.no_fail_routing,
    )
    gate_value = "pass"
    if code != 0:
        if "codex-review=fail" in msg:
            gate_value = "fail"
        elif "missing codex verdict" in msg:
            gate_value = "missing"
        else:
            gate_value = "blocked"
    print(f"CODEX_GATE={gate_value}")
    print(msg)
    return code


def _cmd_automerge_review(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for automerge-review subcommand."""
    core = _core()
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    if "/" not in args.pr_repo:
        print(
            f"CONFIG ERROR: --pr-repo must be 'owner/repo', got '{args.pr_repo}'",
            file=sys.stderr,
        )
        return 3

    code, msg = core.automerge_review(
        pr_repo=args.pr_repo,
        pr_number=args.pr_number,
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        dry_run=args.dry_run,
        update_branch=not args.no_update_branch,
        delete_branch=not args.no_delete_branch,
    )
    print(msg)
    return code


def _cmd_review_rescue(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for review-rescue subcommand."""
    core = _core()
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    if "/" not in args.pr_repo:
        print(
            f"CONFIG ERROR: --pr-repo must be 'owner/repo', got '{args.pr_repo}'",
            file=sys.stderr,
        )
        return 3

    result = core.review_rescue(
        pr_repo=args.pr_repo,
        pr_number=args.pr_number,
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        dry_run=args.dry_run,
    )
    ref = f"{args.pr_repo}#{args.pr_number}"
    if result.rerun_checks:
        print(f"{ref}: reran review checks {list(result.rerun_checks)}")
        return 0
    if result.auto_merge_enabled:
        print(f"{ref}: auto-merge enabled")
        return 0
    if result.requeued_refs:
        print(f"{ref}: re-queued for repair {list(result.requeued_refs)}")
        return 0
    if result.skipped_reason is not None:
        print(f"{ref}: {result.skipped_reason}")
        return 0
    print(f"{ref}: {result.blocked_reason}")
    return 0


def _cmd_review_rescue_all(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for review-rescue-all subcommand."""
    core = _core()
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    sweep = core.review_rescue_all(
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        dry_run=args.dry_run,
    )
    if args.json:
        print(
            json.dumps(
                {
                    "scanned_repos": list(sweep.scanned_repos),
                    "scanned_prs": sweep.scanned_prs,
                    "rerun": list(sweep.rerun),
                    "auto_merge_enabled": list(sweep.auto_merge_enabled),
                    "requeued": list(sweep.requeued),
                    "blocked": list(sweep.blocked),
                    "skipped": list(sweep.skipped),
                },
                indent=2,
            )
        )
        return 0
    print(f"Review rescue repos: {list(sweep.scanned_repos)}")
    print(f"Review rescue PRs scanned: {sweep.scanned_prs}")
    if sweep.rerun:
        print(f"Rerun checks: {list(sweep.rerun)}")
    if sweep.auto_merge_enabled:
        print(f"Auto-merge enabled: {list(sweep.auto_merge_enabled)}")
    if sweep.requeued:
        print(f"Re-queued for repair: {list(sweep.requeued)}")
    if sweep.blocked:
        print(f"Blocked: {list(sweep.blocked)}")
    if sweep.skipped:
        print(f"Skipped: {list(sweep.skipped)}")
    return 0


def _cmd_enforce_execution_policy(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for enforce-execution-policy subcommand."""
    core = _core()
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    if "/" not in args.pr_repo:
        print(
            f"CONFIG ERROR: --pr-repo must be 'owner/repo', got '{args.pr_repo}'",
            file=sys.stderr,
        )
        return 3

    decision = core.enforce_execution_policy(
        pr_repo=args.pr_repo,
        pr_number=args.pr_number,
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        allow_copilot_coding_agent=args.allow_copilot_coding_agent,
        dry_run=args.dry_run,
    )

    if decision.skipped_reason is not None:
        print(f"Execution policy no-op: {decision.skipped_reason}")
        return 0

    if decision.requeued:
        print(f"Re-queued: {sorted(decision.requeued)}")
    if decision.blocked:
        print(f"Blocked: {sorted(decision.blocked)}")
    if decision.copilot_unassigned:
        print(
            "Removed Copilot assignee: "
            f"{sorted(decision.copilot_unassigned)}"
        )
    if decision.pr_closed:
        print(f"Closed PR: {args.pr_repo}#{args.pr_number}")
    else:
        print(f"Execution policy applied: {args.pr_repo}#{args.pr_number}")

    # Policy intervention is a controlled block, not a workflow error.
    return 2


def _cmd_classify_parallelism(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for classify-parallelism subcommand."""
    core = _core()
    review_state_port = core._default_review_state_port(
        args.project_owner,
        args.project_number,
        config,
    )
    snapshot = classify_parallelism_snapshot(
        config=config,
        ready_items=review_state_port.list_issues_by_status("Ready"),
        blocked_items=review_state_port.list_issues_by_status("Blocked"),
        project_owner=args.project_owner,
        project_number=args.project_number,
    )

    print(f'PARALLEL_READY={json.dumps(sorted(snapshot["parallel"]))}')
    print(
        "WAITING_DEPENDENCY="
        f'{json.dumps(sorted(snapshot["waiting_on_dependency"]))}'
    )
    print(f'BLOCKED_POLICY={json.dumps(sorted(snapshot["blocked_policy"]))}')

    if snapshot["non_graph"]:
        print(f'NON_GRAPH={json.dumps(sorted(snapshot["non_graph"]))}')

    return 0
