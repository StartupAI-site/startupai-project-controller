"""Auto-merge policy — gate evaluation for autonomous merge decisions.

Pure policy module: NO GitHub, SQLite, subprocess, or logging imports.
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# Auto-merge gate evaluation
# ---------------------------------------------------------------------------


def automerge_gate_decision(
    pr_repo: str,
    pr_number: int,
    *,
    review_refs: tuple[str, ...] | list[str],
    copilot_review_present: bool,
    codex_gate_code: int,
    codex_gate_message: str,
    pr_state: str,
    is_draft: bool,
    auto_merge_enabled: bool,
    required_failed: frozenset[str] | set[str],
    required_pending: frozenset[str] | set[str],
    mergeable: str,
    merge_state_status: str,
) -> tuple[int, str, str]:
    """Evaluate whether auto-merge should be enabled for a PR.

    Returns (exit_code, message, action) where action is one of:
    - "no_op" — not in scope or blocked
    - "already_enabled" — auto-merge was already on
    - "enable" — all gates pass, enable auto-merge
    - "update_branch_then_enable" — branch behind, update first then enable

    Exit code 0 = proceed, 2 = blocked/no-op.
    """
    label = f"{pr_repo}#{pr_number}"

    if not review_refs:
        return 2, f"{label}: not in board Review scope; automerge controller no-op", "no_op"

    if not copilot_review_present:
        return 2, f"{label}: missing Copilot review signal (COMMENTED|APPROVED)", "no_op"

    if codex_gate_code != 0:
        return codex_gate_code, codex_gate_message, "no_op"

    if pr_state.upper() != "OPEN":
        return 2, f"{label}: state={pr_state}, not OPEN", "no_op"

    if is_draft:
        return 2, f"{label}: draft PR, skipping auto-merge", "no_op"

    if auto_merge_enabled:
        return 0, f"{label}: auto-merge already enabled", "already_enabled"

    if required_failed:
        return 2, f"{label}: required checks failed {sorted(required_failed)}", "no_op"

    if required_pending:
        return 2, f"{label}: required checks pending {sorted(required_pending)}", "no_op"

    if merge_state_status == "BEHIND":
        return 0, f"{label}: would update branch then enable auto-merge", "update_branch_then_enable"

    if mergeable not in {"MERGEABLE", "UNKNOWN"}:
        return 2, f"{label}: mergeable={mergeable}, cannot auto-merge", "no_op"

    return 0, f"{label}: would enable auto-merge (squash, strict gates)", "enable"
