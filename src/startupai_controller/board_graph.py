"""Board graph: selection, dependency reasoning, ranking, and WIP accounting.

Extracted from board_automation.py (ADR-018 step 4). Contains graph-level
helpers that read board state through the canonical adapter surface but never
mutate it directly.

Dependency direction (no cycles):
    validate_critical_path_promotion <- adapters <- board_graph <- board_automation

This module must NEVER import from board_automation or promote_ready.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path


from startupai_controller.adapters.github_cli import (  # canonical adapter surface (ADR-002)
    _ProjectItemSnapshot,  # adapter-internal type — port migration pending
    _list_project_items_by_status,  # partial adapter coverage via ReviewStatePort
    _snapshot_to_issue_ref,  # helper — port migration pending
)
from startupai_controller.domain.models import (
    AdmissionCandidate,
    AdmissionSkip,
    AdmissionDecision,
)
from startupai_controller.domain.scheduling_policy import (
    VALID_EXECUTORS,
    admission_watermarks as admission_watermarks,  # canonical (M5)
    admission_candidate_rank as _domain_admission_candidate_rank,
    has_structured_acceptance_criteria as has_structured_acceptance_criteria,  # canonical (M5)
    normalize_heading as _normalize_heading,  # canonical (M5)
    priority_rank as _priority_rank,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    ConfigError,
    direct_predecessors,
    evaluate_ready_promotion,
    in_any_critical_path,
    parse_issue_ref,
)


# ---------------------------------------------------------------------------
# Pure computation helpers
# ---------------------------------------------------------------------------


# AdmissionCandidate, AdmissionSkip, AdmissionDecision — imported from domain.models (M5)


# _normalize_heading, has_structured_acceptance_criteria, admission_watermarks
# — imported from domain.scheduling_policy (M5)


def _issue_sort_key(issue_ref: str) -> tuple[str, int]:
    """Stable ordering for issue refs like crew#88."""
    parsed = parse_issue_ref(issue_ref)
    return parsed.prefix, parsed.number


def _resolve_issue_coordinates(
    issue_ref: str, config: CriticalPathConfig
) -> tuple[str, str, int]:
    """Map issue ref prefix to owner/repo/number."""
    parsed = parse_issue_ref(issue_ref)
    full_repo = config.issue_prefixes.get(parsed.prefix, "")
    if "/" not in full_repo:
        raise ConfigError(
            f"Unknown issue prefix '{parsed.prefix}' for ref '{issue_ref}'."
        )
    owner, repo = full_repo.split("/", maxsplit=1)
    return owner, repo, parsed.number


def _ready_snapshot_rank(
    snapshot: _ProjectItemSnapshot,
    config: CriticalPathConfig,
) -> tuple[int, tuple[int, str], int]:
    """Return deterministic scheduler ordering for Ready items."""
    ref = _snapshot_to_issue_ref(snapshot, config)
    if ref is None:
        return (2, (99, ""), sys.maxsize)
    parsed = parse_issue_ref(ref)
    critical_rank = 0 if in_any_critical_path(config, ref) else 1
    return critical_rank, _priority_rank(snapshot.priority), parsed.number


def _admission_candidate_rank(
    issue_ref: str,
    *,
    priority: str,
    is_graph_member: bool,
) -> tuple[int, tuple[int, str], int]:
    """Return deterministic scheduler ordering for Backlog admission."""
    parsed = parse_issue_ref(issue_ref)
    return _domain_admission_candidate_rank(
        issue_ref,
        priority=priority,
        is_graph_member=is_graph_member,
        issue_number=parsed.number,
    )


# ---------------------------------------------------------------------------
# Read-via-IO + compute (no mutations)
# ---------------------------------------------------------------------------


def _count_wip_by_executor(
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[str, int]:
    """Count In Progress items per executor. Returns {executor: count}."""
    in_progress = _list_project_items_by_status(
        "In Progress", project_owner, project_number, gh_runner=gh_runner
    )
    counts: dict[str, int] = {}
    for item in in_progress:
        if item.executor:
            counts[item.executor] = counts.get(item.executor, 0) + 1
    return counts


def _count_wip_by_executor_lane(
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[tuple[str, str], int]:
    """Count In Progress items by (executor, repo_prefix)."""
    in_progress = _list_project_items_by_status(
        "In Progress", project_owner, project_number, gh_runner=gh_runner
    )
    counts: dict[tuple[str, str], int] = {}
    for item in in_progress:
        executor = item.executor.strip().lower()
        if executor not in VALID_EXECUTORS:
            continue
        ref = _snapshot_to_issue_ref(item, config)
        if ref is None:
            continue
        lane = parse_issue_ref(ref).prefix
        key = (executor, lane)
        counts[key] = counts.get(key, 0) + 1
    return counts


def classify_parallelism_snapshot(
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = True,
    gh_runner: Callable[..., str] | None = None,
    status_resolver: Callable[..., str] | None = None,
) -> dict[str, list[str]]:
    """Classify Ready items into parallel/dependency/policy buckets."""
    result: dict[str, list[str]] = {
        "parallel": [],
        "waiting_on_dependency": [],
        "blocked_policy": [],
        "non_graph": [],
    }

    ready_items = _list_project_items_by_status(
        "Ready", project_owner, project_number, gh_runner=gh_runner
    )

    for snapshot in ready_items:
        ref = _snapshot_to_issue_ref(snapshot, config)
        if ref is None:
            continue

        if not in_any_critical_path(config, ref):
            result["non_graph"].append(ref)
            continue

        # Check predecessors
        val_code, _val_output = evaluate_ready_promotion(
            issue_ref=ref,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            status_resolver=status_resolver,
            require_in_graph=True,
        )

        if val_code == 0:
            result["parallel"].append(ref)
        else:
            result["waiting_on_dependency"].append(ref)

    # Also check Blocked items for policy blocks
    blocked_items = _list_project_items_by_status(
        "Blocked", project_owner, project_number, gh_runner=gh_runner
    )

    for snapshot in blocked_items:
        ref = _snapshot_to_issue_ref(snapshot, config)
        if ref is None:
            continue

        if in_any_critical_path(config, ref):
            result["blocked_policy"].append(ref)

    return result


# ---------------------------------------------------------------------------
# Dependency analysis (decomposed from enforce_ready_dependency_guard)
# ---------------------------------------------------------------------------


def find_unmet_ready_dependencies(
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    this_repo_prefix: str | None = None,
    all_prefixes: bool = False,
    status_resolver: Callable[..., str] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[tuple[str, str]]:
    """Return (issue_ref, blocked_reason) for Ready graph-member issues with unmet predecessors.

    Pure analysis — no board mutations. The caller
    (``enforce_ready_dependency_guard``) handles the mutation side.
    """
    unmet: list[tuple[str, str]] = []

    ready_items = _list_project_items_by_status(
        "Ready", project_owner, project_number, gh_runner=gh_runner
    )

    for snapshot in ready_items:
        ref = _snapshot_to_issue_ref(snapshot, config)
        if ref is None:
            continue

        # Filter by prefix if not all_prefixes
        if not all_prefixes and this_repo_prefix:
            parsed = parse_issue_ref(ref)
            if parsed.prefix != this_repo_prefix:
                continue

        # Only graph-member issues
        if not in_any_critical_path(config, ref):
            continue

        # Check predecessors
        val_code, _val_output = evaluate_ready_promotion(
            issue_ref=ref,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            status_resolver=status_resolver,
            require_in_graph=True,
        )

        if val_code != 0:
            preds = direct_predecessors(config, ref)
            pred_list = ",".join(sorted(preds))
            reason = f"dependency-unmet:{pred_list}"
            unmet.append((ref, reason))

    return unmet
