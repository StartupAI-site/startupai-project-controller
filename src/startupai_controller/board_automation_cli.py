"""CLI surface for board automation orchestration."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from startupai_controller.board_automation_config import (
    DEFAULT_AUTOMATION_CONFIG_PATH,
    DEFAULT_CONFIG_PATH,
    DEFAULT_MISSING_EXECUTOR_BLOCK_CAP,
    DEFAULT_PROJECT_NUMBER,
    DEFAULT_PROJECT_OWNER,
    DEFAULT_REBALANCE_CYCLE_MINUTES,
)


def _core():
    from startupai_controller import board_automation as core

    return core


def build_parser() -> argparse.ArgumentParser:
    """Build the board automation CLI parser."""
    core = _core()
    parser = argparse.ArgumentParser(
        description="Board automation orchestration for GitHub Project boards.",
    )
    parser.add_argument(
        "--file",
        default=DEFAULT_CONFIG_PATH,
        help="Path to critical-paths JSON file",
    )
    parser.add_argument(
        "--automation-config",
        default=DEFAULT_AUTOMATION_CONFIG_PATH,
        help="Path to board-automation policy JSON file",
    )
    parser.add_argument(
        "--project-owner",
        default=DEFAULT_PROJECT_OWNER,
        help="GitHub org/user that owns the Project board",
    )
    parser.add_argument(
        "--project-number",
        type=int,
        default=DEFAULT_PROJECT_NUMBER,
        help="GitHub Project number",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Report what would happen without mutating the board (global)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Subcommand")

    def _add_dry_run_argument(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Report what would happen without mutating the board",
        )

    p_done = subparsers.add_parser("mark-done", help="PR merge -> mark linked issues Done")
    p_done.add_argument("--pr-repo", required=True, help="PR repository (owner/repo)")
    p_done.add_argument("--pr-number", type=int, required=True, help="PR number")
    _add_dry_run_argument(p_done)
    p_done.set_defaults(func=core._cmd_mark_done)

    p_promote = subparsers.add_parser(
        "auto-promote", help="Done issue -> promote eligible successors to Ready"
    )
    p_promote.add_argument("--issue", required=True, help="Issue ref of the Done issue, e.g. crew#88")
    p_promote.add_argument("--this-repo-prefix", required=True, help="Prefix for the current repo, e.g. crew")
    _add_dry_run_argument(p_promote)
    p_promote.set_defaults(func=core._cmd_auto_promote)

    p_admit = subparsers.add_parser(
        "admit-backlog", help="Autonomously fill Ready from governed Backlog items"
    )
    p_admit.add_argument("--json", action="store_true", default=False, help="Emit machine-readable JSON output")
    p_admit.add_argument("--issue", help="Optional issue ref filter for targeted diagnosis")
    p_admit.add_argument("--limit", type=int, help="Optional cap for candidate/skip lists in output")
    _add_dry_run_argument(p_admit)
    p_admit.set_defaults(func=core._cmd_admit_backlog)

    p_blocker = subparsers.add_parser(
        "propagate-blocker", help="Blocked issue -> post advisory comments on successors"
    )
    blocker_group = p_blocker.add_mutually_exclusive_group(required=True)
    blocker_group.add_argument("--issue", help="Issue ref of the Blocked issue")
    blocker_group.add_argument("--sweep-blocked", action="store_true", default=False, help="Sweep all Blocked items")
    p_blocker.add_argument("--this-repo-prefix", help="Prefix for the current repo (required in single-issue mode)")
    p_blocker.add_argument("--all-prefixes", action="store_true", default=False, help="Include all repo prefixes in sweep")
    _add_dry_run_argument(p_blocker)
    p_blocker.set_defaults(func=core._cmd_propagate_blocker)

    p_handoffs = subparsers.add_parser(
        "reconcile-handoffs", help="Retry/escalate stale cross-repo handoffs"
    )
    p_handoffs.add_argument("--ack-timeout-minutes", type=int, default=30, help="Minutes before a handoff is considered stale")
    p_handoffs.add_argument("--max-retries", type=int, default=1, help="Maximum retry attempts before escalation")
    _add_dry_run_argument(p_handoffs)
    p_handoffs.set_defaults(func=core._cmd_reconcile_handoffs)

    p_schedule = subparsers.add_parser("schedule-ready", help="Classify Ready issues; optional claim mode")
    schedule_scope = p_schedule.add_mutually_exclusive_group(required=True)
    schedule_scope.add_argument("--this-repo-prefix", help="Prefix for the current repo")
    schedule_scope.add_argument("--all-prefixes", action="store_true", default=False, help="Include all repo prefixes")
    p_schedule.add_argument("--mode", choices=["advisory", "claim"], default="advisory", help="advisory (default) or claim (mutating)")
    p_schedule.add_argument("--per-executor-wip-limit", type=int, default=3, help="Maximum In Progress items per executor")
    p_schedule.add_argument("--missing-executor-block-cap", type=int, default=DEFAULT_MISSING_EXECUTOR_BLOCK_CAP, help="Max Ready items to auto-block per run for missing/invalid executor")
    _add_dry_run_argument(p_schedule)
    p_schedule.set_defaults(func=core._cmd_schedule_ready)

    p_claim = subparsers.add_parser("claim-ready", help="Explicitly claim one Ready issue into In Progress")
    p_claim.add_argument("--executor", required=True, choices=sorted(core.VALID_EXECUTORS), help="Executor claiming work")
    claim_target = p_claim.add_mutually_exclusive_group(required=True)
    claim_target.add_argument("--next", action="store_true", default=False, help="Claim next eligible Ready item for executor")
    claim_target.add_argument("--issue", help="Claim a specific issue ref (e.g. crew#88)")
    claim_scope = p_claim.add_mutually_exclusive_group(required=False)
    claim_scope.add_argument("--this-repo-prefix", help="Limit --next candidate selection to one prefix")
    claim_scope.add_argument("--all-prefixes", action="store_true", default=False, help="Consider all prefixes for --next")
    p_claim.add_argument("--per-executor-wip-limit", type=int, default=3, help="Maximum In Progress items per executor")
    _add_dry_run_argument(p_claim)
    p_claim.set_defaults(func=core._cmd_claim_ready)

    p_dispatch = subparsers.add_parser("dispatch-agent", help="Dispatch In Progress issues per configured dispatch target")
    p_dispatch.add_argument("--issue", action="append", required=True, help="Issue ref to dispatch (repeatable)")
    _add_dry_run_argument(p_dispatch)
    p_dispatch.set_defaults(func=core._cmd_dispatch_agent)

    p_rebalance = subparsers.add_parser("rebalance-wip", help="Rebalance In Progress lanes with stale demotion and dependency blocking")
    rebalance_scope = p_rebalance.add_mutually_exclusive_group(required=True)
    rebalance_scope.add_argument("--this-repo-prefix", help="Prefix for the current repo")
    rebalance_scope.add_argument("--all-prefixes", action="store_true", default=False, help="Include all repo prefixes")
    p_rebalance.add_argument("--cycle-minutes", type=int, default=DEFAULT_REBALANCE_CYCLE_MINUTES, help="Cadence used for stale-cycle confirmation windows")
    _add_dry_run_argument(p_rebalance)
    p_rebalance.set_defaults(func=core._cmd_rebalance_wip)

    p_enforce = subparsers.add_parser("enforce-ready-dependencies", help="Block Ready issues with unmet predecessors")
    enforce_scope = p_enforce.add_mutually_exclusive_group(required=True)
    enforce_scope.add_argument("--this-repo-prefix", help="Prefix for the current repo")
    enforce_scope.add_argument("--all-prefixes", action="store_true", default=False, help="Include all repo prefixes")
    _add_dry_run_argument(p_enforce)
    p_enforce.set_defaults(func=core._cmd_enforce_ready_deps)

    p_audit = subparsers.add_parser("audit-in-progress", help="Escalate stale In Progress issues with no PR activity")
    audit_scope = p_audit.add_mutually_exclusive_group(required=True)
    audit_scope.add_argument("--this-repo-prefix", help="Prefix for the current repo")
    audit_scope.add_argument("--all-prefixes", action="store_true", default=False, help="Include all repo prefixes")
    p_audit.add_argument("--max-age-hours", type=int, default=24, help="Staleness threshold in hours")
    _add_dry_run_argument(p_audit)
    p_audit.set_defaults(func=core._cmd_audit_in_progress)

    p_sync = subparsers.add_parser("sync-review-state", help="Sync board state with PR/review/check events")
    sync_source = p_sync.add_mutually_exclusive_group(required=True)
    sync_source.add_argument("--from-github-event", action="store_true", default=False, help="Read event from $GITHUB_EVENT_PATH env var")
    sync_source.add_argument("--event-kind", choices=["pr_open", "pr_ready_for_review", "pr_close_merged", "changes_requested", "checks_failed", "checks_passed", "review_submitted"], help="Event kind to process")
    p_sync.add_argument("--issue", help="Issue ref (required with --event-kind unless --resolve-pr is used)")
    p_sync.add_argument("--resolve-pr", nargs=2, metavar=("PR_REPO", "PR_NUMBER"), help="Resolve PR to linked issues: owner/repo number")
    p_sync.add_argument("--checks-state", help="Override checks state (passed/failed)")
    p_sync.add_argument("--failed-checks", nargs="*", default=None, help="Names of failed checks (used to filter by required checks)")
    _add_dry_run_argument(p_sync)
    p_sync.set_defaults(func=core._cmd_sync_review_state)

    p_codex_gate = subparsers.add_parser("codex-review-gate", help="Enforce codex-review verdict contract on PRs")
    p_codex_gate.add_argument("--pr-repo", required=True, help="PR repository (owner/repo)")
    p_codex_gate.add_argument("--pr-number", type=int, required=True, help="PR number")
    p_codex_gate.add_argument("--no-fail-routing", action="store_true", default=False, help="Do not route failed verdicts back to In Progress")
    _add_dry_run_argument(p_codex_gate)
    p_codex_gate.set_defaults(func=core._cmd_codex_review_gate)

    p_automerge = subparsers.add_parser("automerge-review", help="Auto-merge PR when codex + required checks are satisfied")
    p_automerge.add_argument("--pr-repo", required=True, help="PR repository (owner/repo)")
    p_automerge.add_argument("--pr-number", type=int, required=True, help="PR number")
    p_automerge.add_argument("--no-update-branch", action="store_true", default=False, help="Skip branch update when merge state is BEHIND")
    p_automerge.add_argument("--no-delete-branch", action="store_true", default=False, help="Do not delete source branch after merge")
    _add_dry_run_argument(p_automerge)
    p_automerge.set_defaults(func=core._cmd_automerge_review)

    p_review_rescue = subparsers.add_parser("review-rescue", help="Reconcile one PR in Review by rerunning cancelled checks or enabling auto-merge")
    p_review_rescue.add_argument("--pr-repo", required=True, help="PR repository (owner/repo)")
    p_review_rescue.add_argument("--pr-number", type=int, required=True, help="PR number")
    _add_dry_run_argument(p_review_rescue)
    p_review_rescue.set_defaults(func=core._cmd_review_rescue)

    p_review_rescue_all = subparsers.add_parser("review-rescue-all", help="Scan governed repos and reconcile PRs in Review")
    p_review_rescue_all.add_argument("--json", action="store_true", default=False, help="Emit machine-readable JSON output")
    _add_dry_run_argument(p_review_rescue_all)
    p_review_rescue_all.set_defaults(func=core._cmd_review_rescue_all)

    p_policy = subparsers.add_parser("enforce-execution-policy", help="Close Copilot coding-agent PRs and re-queue linked issues")
    p_policy.add_argument("--pr-repo", required=True, help="PR repository (owner/repo)")
    p_policy.add_argument("--pr-number", type=int, required=True, help="PR number")
    p_policy.add_argument("--allow-copilot-coding-agent", action="store_true", default=False, help="Bypass strict execution-lane policy")
    _add_dry_run_argument(p_policy)
    p_policy.set_defaults(func=core._cmd_enforce_execution_policy)

    p_classify = subparsers.add_parser("classify-parallelism", help="Snapshot parallel vs dependency-waiting Ready items")
    _add_dry_run_argument(p_classify)
    p_classify.set_defaults(func=core._cmd_classify_parallelism)

    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for board automation."""
    core = _core()
    parser = build_parser()
    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        parser.print_help()
        return 3

    try:
        config = core.load_config(Path(args.file))
        return args.func(args, config)
    except core.ConfigError as error:
        print(f"CONFIG ERROR: {error}", file=sys.stderr)
        return 3
    except core.GhQueryError as error:
        print(f"GH QUERY ERROR: {error}", file=sys.stderr)
        return 4
