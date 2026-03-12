"""Board graph: selection, dependency reasoning, ranking, and WIP accounting.

Extracted from board_automation.py (ADR-018 step 4). Contains graph-level
helpers that operate on typed board state but never mutate it directly.

Dependency direction (no cycles):
    validate_critical_path_promotion <- board_graph <- board_automation

This module must NEVER import from board_automation, promote_ready, adapters,
or shim modules.
"""

from __future__ import annotations

from collections.abc import Callable
from startupai_controller.domain.models import (
    AdmissionCandidate,
    AdmissionSkip,
    AdmissionDecision,
    IssueSnapshot,
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


def _canonical_issue_ref(issue_ref: str, config: CriticalPathConfig) -> str:
    """Normalize legacy owner/repo#N refs to canonical prefix#N refs."""
    try:
        parse_issue_ref(issue_ref)
        return issue_ref
    except ConfigError:
        pass

    if "#" not in issue_ref:
        raise ConfigError(
            f"Invalid issue ref '{issue_ref}'. Expected canonical <prefix>#<n> "
            "or a known owner/repo#<n> form."
        )

    repo_name, number_text = issue_ref.split("#", maxsplit=1)
    prefix = next(
        (
            candidate
            for candidate, repo in config.issue_prefixes.items()
            if repo == repo_name
        ),
        None,
    )
    if prefix is None or not number_text.isdigit():
        raise ConfigError(
            f"Invalid issue ref '{issue_ref}'. Expected canonical <prefix>#<n> "
            "or a known owner/repo#<n> form."
        )
    return f"{prefix}#{number_text}"


def _resolve_issue_coordinates(
    issue_ref: str, config: CriticalPathConfig
) -> tuple[str, str, int]:
    """Map issue ref prefix to owner/repo/number."""
    parsed = parse_issue_ref(_canonical_issue_ref(issue_ref, config))
    full_repo = config.issue_prefixes.get(parsed.prefix, "")
    if "/" not in full_repo:
        raise ConfigError(
            f"Unknown issue prefix '{parsed.prefix}' for ref '{issue_ref}'."
        )
    owner, repo = full_repo.split("/", maxsplit=1)
    return owner, repo, parsed.number


def _ready_snapshot_rank(
    snapshot: IssueSnapshot,
    config: CriticalPathConfig,
) -> tuple[int, tuple[int, str], int]:
    """Return deterministic scheduler ordering for Ready items."""
    issue_ref = _canonical_issue_ref(snapshot.issue_ref, config)
    parsed = parse_issue_ref(issue_ref)
    critical_rank = 0 if in_any_critical_path(config, issue_ref) else 1
    return critical_rank, _priority_rank(snapshot.priority), parsed.number


def _admission_candidate_rank(
    issue_ref: str,
    *,
    priority: str,
    is_graph_member: bool,
) -> tuple[int, tuple[int, str], int]:
    """Return deterministic scheduler ordering for Backlog admission."""
    try:
        parsed = parse_issue_ref(issue_ref)
        canonical_issue_ref = issue_ref
    except ConfigError:
        if "#" not in issue_ref:
            raise
        _repo_name, number_text = issue_ref.split("#", maxsplit=1)
        if not number_text.isdigit():
            raise
        canonical_issue_ref = issue_ref
        parsed = parse_issue_ref(f"crew#{number_text}")
    return _domain_admission_candidate_rank(
        canonical_issue_ref,
        priority=priority,
        is_graph_member=is_graph_member,
        issue_number=parsed.number,
    )


# ---------------------------------------------------------------------------
# Read-via-IO + compute (no mutations)
# ---------------------------------------------------------------------------


def _count_wip_by_executor(
    in_progress_items: list[IssueSnapshot],
) -> dict[str, int]:
    """Count In Progress items per executor. Returns {executor: count}."""
    counts: dict[str, int] = {}
    for item in in_progress_items:
        if item.executor:
            counts[item.executor] = counts.get(item.executor, 0) + 1
    return counts


def _count_wip_by_executor_lane(
    config: CriticalPathConfig,
    in_progress_items: list[IssueSnapshot],
) -> dict[tuple[str, str], int]:
    """Count In Progress items by (executor, repo_prefix)."""
    counts: dict[tuple[str, str], int] = {}
    for item in in_progress_items:
        executor = item.executor.strip().lower()
        if executor not in VALID_EXECUTORS:
            continue
        lane = parse_issue_ref(item.issue_ref).prefix
        key = (executor, lane)
        counts[key] = counts.get(key, 0) + 1
    return counts


def classify_parallelism_snapshot(
    config: CriticalPathConfig,
    ready_items: list[IssueSnapshot],
    blocked_items: list[IssueSnapshot],
    *,
    status_resolver: Callable[..., str] | None = None,
    project_owner: str,
    project_number: int,
) -> dict[str, list[str]]:
    """Classify Ready items into parallel/dependency/policy buckets."""
    result: dict[str, list[str]] = {
        "parallel": [],
        "waiting_on_dependency": [],
        "blocked_policy": [],
        "non_graph": [],
    }

    for snapshot in ready_items:
        ref = snapshot.issue_ref

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
    for snapshot in blocked_items:
        ref = snapshot.issue_ref

        if in_any_critical_path(config, ref):
            result["blocked_policy"].append(ref)

    return result


# ---------------------------------------------------------------------------
# Dependency analysis (decomposed from enforce_ready_dependency_guard)
# ---------------------------------------------------------------------------


def find_unmet_ready_dependencies(
    config: CriticalPathConfig,
    ready_items: list[IssueSnapshot],
    *,
    this_repo_prefix: str | None = None,
    all_prefixes: bool = False,
    status_resolver: Callable[..., str] | None = None,
    project_owner: str,
    project_number: int,
) -> list[tuple[str, str]]:
    """Return (issue_ref, blocked_reason) for Ready graph-member issues with unmet predecessors.

    Pure analysis — no board mutations. The caller
    (``enforce_ready_dependency_guard``) handles the mutation side.
    """
    unmet: list[tuple[str, str]] = []

    for snapshot in ready_items:
        ref = snapshot.issue_ref

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
