"""Scheduling policy — WIP limits, priority ranking, admission watermarks.

Pure policy module: NO GitHub, SQLite, subprocess, or logging imports.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_DISPATCH_TARGETS = {"executor"}
VALID_EXECUTION_AUTHORITY_MODES = {"board", "single_machine"}
PROTECTED_QUEUE_ROUTING_STATUSES = ("Backlog", "Ready")
VALID_EXECUTORS = {"claude", "codex", "human"}


# ---------------------------------------------------------------------------
# WIP limits
# ---------------------------------------------------------------------------


def wip_limit_for_lane(
    wip_limits: dict[str, dict[str, int]] | None,
    executor: str,
    lane: str,
    fallback: int,
) -> int:
    """Resolve WIP limit for an executor/lane pair.

    Accepts primitive dict instead of BoardAutomationConfig.
    """
    if wip_limits is None:
        return fallback
    executor_limits = wip_limits.get(executor, {})
    return executor_limits.get(lane, fallback)


def protected_queue_executor_target(
    execution_authority_mode: str | None,
    execution_authority_executors: tuple[str, ...],
) -> str | None:
    """Return the sole protected execution executor when routing is deterministic.

    Accepts primitive parameters instead of BoardAutomationConfig.
    """
    if execution_authority_mode != "single_machine":
        return None
    executors = tuple(
        executor.strip().lower()
        for executor in execution_authority_executors
        if executor.strip()
    )
    if len(executors) != 1:
        return None
    target = executors[0]
    if target not in VALID_EXECUTORS:
        return None
    return target


def controller_owned_admission(
    issue_ref: str,
    *,
    admission_enabled: bool,
    execution_authority_mode: str | None,
    execution_authority_repos: tuple[str, ...],
) -> bool:
    """Return True when protected Backlog -> Ready is controller-owned."""
    if not admission_enabled:
        return False
    if execution_authority_mode != "single_machine":
        return False
    prefix, sep, number = issue_ref.partition("#")
    if not sep or not number.isdigit():
        return False
    return prefix in execution_authority_repos


# ---------------------------------------------------------------------------
# Priority ranking
# ---------------------------------------------------------------------------


def priority_rank(priority: str) -> tuple[int, str]:
    """Rank single-select priority labels such as P0/P1/P2/P3."""
    normalized = priority.strip()
    match = re.search(r"\bP(\d+)\b", normalized, flags=re.IGNORECASE)
    if match:
        return int(match.group(1)), normalized.lower()
    return 99, normalized.lower()


def repo_prefix_for_slug(
    repo_slug: str,
    issue_prefixes: dict[str, str],
) -> str | None:
    """Return the canonical issue prefix for a repo slug, if configured."""
    for prefix, configured_repo in issue_prefixes.items():
        if configured_repo == repo_slug:
            return prefix
    return None


def snapshot_to_issue_ref(
    issue_ref: str,
    issue_prefixes: dict[str, str],
) -> str | None:
    """Normalize a snapshot issue ref to canonical `<prefix>#<number>` form."""
    normalized = issue_ref.strip()
    left, sep, right = normalized.partition("#")
    if not sep or not right.isdigit():
        return None
    if left in issue_prefixes:
        return f"{left}#{right}"
    prefix = repo_prefix_for_slug(left, issue_prefixes)
    if prefix is None:
        return None
    return f"{prefix}#{right}"


# ---------------------------------------------------------------------------
# Admission watermarks
# ---------------------------------------------------------------------------


def admission_watermarks(
    global_concurrency: int,
    *,
    floor_multiplier: int = 2,
    cap_multiplier: int = 3,
) -> tuple[int, int]:
    """Compute Ready queue floor/cap from global concurrency."""
    concurrency = max(1, int(global_concurrency))
    floor = max(1, concurrency * max(1, int(floor_multiplier)))
    cap = max(floor, concurrency * max(1, int(cap_multiplier)))
    return floor, cap


# ---------------------------------------------------------------------------
# Candidate ranking
# ---------------------------------------------------------------------------


def admission_candidate_rank(
    issue_ref: str,
    *,
    priority: str,
    is_graph_member: bool,
    issue_number: int,
) -> tuple[int, tuple[int, str], int]:
    """Return deterministic scheduler ordering for Backlog admission."""
    graph_rank = 0 if is_graph_member else 1
    return graph_rank, priority_rank(priority), issue_number


# ---------------------------------------------------------------------------
# Acceptance criteria
# ---------------------------------------------------------------------------


def normalize_heading(text: str) -> str:
    """Normalize markdown heading text for acceptance section matching."""
    return re.sub(r"[:\s]+$", "", text.strip().lower())


def has_structured_acceptance_criteria(
    body: str,
    headings: tuple[str, ...] = ("Acceptance Criteria", "Definition of Done"),
) -> bool:
    """Return True when the issue body contains a valid acceptance section."""
    if not body.strip():
        return False

    target_headings = {normalize_heading(item) for item in headings}
    heading_re = re.compile(r"^\s{0,3}#{1,6}\s+(?P<text>.+?)\s*$")
    bullet_re = re.compile(r"^\s*(?:[-*+]\s+|\d+\.\s+|\[[ xX]\]\s+|- \[[ xX]\]\s+).+")
    current_matches = False

    for line in body.splitlines():
        heading_match = heading_re.match(line)
        if heading_match:
            current_matches = normalize_heading(heading_match.group("text")) in target_headings
            continue
        if current_matches and bullet_re.match(line):
            return True

    return False
