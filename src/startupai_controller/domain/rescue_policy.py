"""Rescue policy — review rescue decisions, check evaluation, gate logic.

Pure policy module: NO GitHub, SQLite, subprocess, or logging imports.
"""

from __future__ import annotations

from typing import Any


# ---------------------------------------------------------------------------
# Configured review checks
# ---------------------------------------------------------------------------


def configured_review_checks(
    pr_repo: str,
    required_checks_by_repo: dict[str, tuple[str, ...]] | None,
) -> tuple[str, ...]:
    """Return repo-specific review checks that should be reconciled.

    Accepts primitive dict instead of BoardAutomationConfig.
    """
    if required_checks_by_repo is None:
        return ()
    return required_checks_by_repo.get(pr_repo.strip().lower(), ())


# ---------------------------------------------------------------------------
# Rescue decision table
# ---------------------------------------------------------------------------


def rescue_decision(
    *,
    review_refs: tuple[str, ...],
    has_cancelled_checks: bool,
    pr_state: str,
    is_draft: bool,
    mergeable: str,
    copilot_review_present: bool,
    codex_gate_code: int,
    codex_gate_message: str,
    required_failed: frozenset[str] | set[str],
    required_pending: frozenset[str] | set[str],
    rescue_failed: frozenset[str] | set[str],
    rescue_pending: frozenset[str] | set[str],
    rescue_missing: frozenset[str] | set[str],
    auto_merge_enabled: bool,
) -> tuple[str, str]:
    """Evaluate rescue action for one PR.

    Returns (action, reason) where action is one of:
    - "rerun_cancelled" — cancelled checks should be rerun
    - "blocked" — PR is blocked (reason describes why)
    - "requeue_conflicting" — conflicting merge state, requeue for fix
    - "requeue_failed" — required checks failed, requeue for fix
    - "skipped" — no action needed (reason describes why)
    - "enable_automerge" — all gates pass, enable auto-merge

    This is a pure decision function — callers execute the action.
    """
    if not review_refs:
        return "skipped", "not-in-review-scope"

    if has_cancelled_checks:
        return "rerun_cancelled", "cancelled-checks-detected"

    if pr_state.upper() != "OPEN":
        return "blocked", f"state={pr_state}"

    if is_draft:
        return "blocked", "draft-pr"

    if mergeable == "CONFLICTING":
        return "requeue_conflicting", "mergeable=CONFLICTING"

    if not copilot_review_present:
        return "blocked", "missing-copilot-review"

    if codex_gate_code != 0:
        return "blocked", codex_gate_message

    if required_failed:
        return "requeue_failed", f"required checks failed {sorted(required_failed)}"

    if required_pending:
        return "blocked", f"required checks pending {sorted(required_pending)}"

    if rescue_failed:
        return "blocked", f"review checks failed {sorted(rescue_failed)}"

    if rescue_pending or rescue_missing:
        waiting = sorted(rescue_pending | rescue_missing)
        return "blocked", f"review checks pending {waiting}"

    if auto_merge_enabled:
        return "skipped", "auto-merge-already-enabled"

    return "enable_automerge", "all-gates-pass"
