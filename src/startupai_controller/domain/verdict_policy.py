"""Verdict policy — backfill eligibility, marker format, trust decisions.

Pure policy module: NO GitHub, SQLite, subprocess, or logging imports.
"""

from __future__ import annotations

from startupai_controller.domain.repair_policy import MARKER_PREFIX


# ---------------------------------------------------------------------------
# Verdict marker format
# ---------------------------------------------------------------------------


def verdict_marker_text(session_id: str) -> str:
    """Return the machine-readable HTML comment marker for a codex verdict."""
    return f"<!-- {MARKER_PREFIX}:codex-verdict:session={session_id} -->"


def verdict_comment_body(session_id: str) -> str:
    """Return the full comment body for a codex pass verdict."""
    marker = verdict_marker_text(session_id)
    return "\n".join(
        [
            marker,
            "codex-review: pass",
            "codex-route: none",
            "",
            (
                "Trusted local verdict after successful consumer session "
                f"`{session_id}`."
            ),
        ]
    )


def parse_verdict_marker(text: str) -> str | None:
    """Extract session_id from a verdict marker string, if present."""
    import re

    match = re.search(
        rf"<!--\s*{re.escape(MARKER_PREFIX)}:codex-verdict:session=(\S+)\s*-->",
        text,
    )
    return match.group(1) if match else None


# ---------------------------------------------------------------------------
# Backfill eligibility
# ---------------------------------------------------------------------------


def is_pre_backfill_eligible(
    *,
    last_result: str | None,
    last_reason: str | None,
) -> bool:
    """Return True when a review queue entry should attempt pre-backfill.

    Eligible if verdict-blocked or newly seeded.
    """
    is_verdict_blocked = (
        last_result == "blocked"
        and last_reason is not None
        and "missing codex verdict marker" in last_reason.lower()
    )
    is_newly_seeded = last_result is None
    return is_verdict_blocked or is_newly_seeded


def is_session_verdict_eligible(
    *,
    session_status: str,
    session_phase: str | None,
    session_pr_url: str | None,
    entry_pr_url: str | None = None,
) -> bool:
    """Return True when a session qualifies for verdict posting.

    Optionally enforces that session.pr_url matches entry.pr_url
    (pass entry_pr_url to enable the guard).
    """
    if session_status != "success":
        return False
    if session_phase != "review":
        return False
    if not session_pr_url:
        return False
    if entry_pr_url is not None and session_pr_url != entry_pr_url:
        return False
    return True


def marker_already_present(
    marker: str,
    comment_bodies: frozenset[str] | set[str],
) -> bool:
    """Return True when the marker text is found in any comment body."""
    return any(marker in body for body in comment_bodies)
