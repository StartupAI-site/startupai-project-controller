"""Launch policy — PR candidate classification, session kind determination.

Pure policy module: NO GitHub, SQLite, subprocess, or logging imports.
"""

from __future__ import annotations

import re
from typing import Any

from startupai_controller.domain.models import OpenPullRequestMatch
from startupai_controller.domain.repair_policy import deterministic_branch_pattern


# ---------------------------------------------------------------------------
# PR candidate classification
# ---------------------------------------------------------------------------


def classify_pr_candidates(
    issue_ref: str,
    candidates: list[OpenPullRequestMatch],
    trusted_authors: set[str] | frozenset[str],
    *,
    expected_branch: str | None = None,
    issue_number: int | None = None,
) -> tuple[str, OpenPullRequestMatch | None, str]:
    """Classify open PRs for an issue as adoptable, ambiguous, non-local, or none.

    Pure decision table — accepts pre-fetched candidate data with provenance
    already parsed. Does not call GitHub or any external service.

    Returns (classification, match_or_none, reason_code).
    """
    if not candidates:
        return "none", None, "no-open-pr"

    # Derive issue_number from candidates if not provided
    if issue_number is None:
        # Extract from issue_ref pattern like "crew#88"
        m = re.search(r"#(\d+)$", issue_ref)
        issue_number = int(m.group(1)) if m else 0

    branch_pattern = deterministic_branch_pattern(issue_number)
    adoptable: list[OpenPullRequestMatch] = []
    ambiguous_reasons: list[str] = []

    for candidate in candidates:
        provenance = candidate.provenance
        if provenance is None:
            ambiguous_reasons.append(f"missing-provenance:{candidate.url}")
            continue
        if provenance.get("issue_ref") != issue_ref:
            ambiguous_reasons.append(f"issue-mismatch:{candidate.url}")
            continue
        if provenance.get("executor") != "codex":
            ambiguous_reasons.append(f"executor-mismatch:{candidate.url}")
            continue
        if candidate.author not in trusted_authors:
            ambiguous_reasons.append(f"untrusted-author:{candidate.url}")
            continue
        if expected_branch and candidate.branch_name != expected_branch:
            ambiguous_reasons.append(f"branch-mismatch:{candidate.url}")
            continue
        if not branch_pattern.match(candidate.branch_name):
            ambiguous_reasons.append(f"branch-noncanonical:{candidate.url}")
            continue
        adoptable.append(candidate)

    if len(adoptable) == 1 and not ambiguous_reasons:
        return "adoptable", adoptable[0], "qualifying-open-pr"
    if adoptable and not ambiguous_reasons:
        return "ambiguous", None, "multiple-adoptable-prs"
    if adoptable and ambiguous_reasons:
        return "ambiguous", None, ";".join(ambiguous_reasons)
    return "non-local", None, ";".join(ambiguous_reasons)


# ---------------------------------------------------------------------------
# Session kind determination
# ---------------------------------------------------------------------------


def launch_session_kind(
    classification: str,
    pr_match: OpenPullRequestMatch | None,
) -> str:
    """Determine the session kind for a launch candidate.

    Returns ``"repair"`` when an adoptable PR exists, ``"new_work"`` otherwise.
    Pure decision — no side effects.
    """
    if classification == "adoptable" and pr_match is not None:
        return "repair"
    return "new_work"


# ---------------------------------------------------------------------------
# Board reconciliation truth table
# ---------------------------------------------------------------------------


def reconcile_in_progress_decision(
    classification: str,
    *,
    has_latest_session: bool,
    session_kind: str | None,
    session_status: str | None,
) -> str:
    """Decide where to move an In Progress issue with no active worker.

    Given the PR classification result and the latest session state, returns
    the target status: "ready", "review", or "blocked".

    Pure decision — callers execute the board mutation.
    """
    if classification == "adoptable":
        should_requeue_repair = (
            has_latest_session
            and session_kind == "repair"
            and session_status != "success"
        )
        if should_requeue_repair:
            return "ready"
        return "review"
    if classification == "none":
        return "ready"
    # "ambiguous" or "non-local" — cannot determine ownership
    return "blocked"
