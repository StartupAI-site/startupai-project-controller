"""Resolution-proof helpers shared by consumer execution and admission."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

RESOLUTION_COMMENT_KIND = "consumer-resolution"
VALID_RESOLUTION_KINDS = {
    "already_on_main",
    "superseded_by_existing_solution",
    "duplicate",
    "superseded",
    "no_action_needed",
}
AUTO_CLOSE_RESOLUTION_KINDS = {
    "already_on_main",
    "superseded_by_existing_solution",
}
NON_AUTO_CLOSE_RESOLUTION_KINDS = {
    "duplicate",
    "superseded",
    "no_action_needed",
}
VALID_EQUIVALENCE_CLAIMS = {
    "exact_match",
    "strict_superset",
    "unknown",
}
VALID_VERIFICATION_CLASSES = {"strong", "weak", "ambiguous", "failed"}
VALID_RESOLUTION_ACTIONS = {
    "closed_as_already_resolved",
    "blocked_for_resolution_review",
}
_RESOLUTION_MARKER_RE = re.compile(
    r"<!--\s*startupai-board-bot:consumer-resolution:(?P<issue_ref>[^\s]+)\s*-->"
)
_RESOLUTION_DATA_RE = re.compile(
    r"```json\s*(?P<payload>\{.*?\})\s*```",
    flags=re.DOTALL,
)


@dataclass(frozen=True)
class ParsedResolutionComment:
    """Structured resolution metadata parsed from an issue comment."""

    issue_ref: str
    payload: dict[str, Any]


def normalize_resolution_payload(raw: Any) -> dict[str, Any] | None:
    """Return a normalized resolution payload or None when absent/invalid."""
    if not isinstance(raw, dict):
        return None
    kind = str(raw.get("kind") or "").strip()
    if kind not in VALID_RESOLUTION_KINDS:
        return None
    equivalence_claim = str(raw.get("equivalence_claim") or "unknown").strip() or "unknown"
    if equivalence_claim not in VALID_EQUIVALENCE_CLAIMS:
        equivalence_claim = "unknown"

    def _string_list(key: str) -> list[str]:
        value = raw.get(key)
        if not isinstance(value, list):
            return []
        normalized: list[str] = []
        for item in value:
            text = str(item or "").strip()
            if text:
                normalized.append(text)
        return normalized

    return {
        "kind": kind,
        "summary": str(raw.get("summary") or "").strip(),
        "code_refs": _string_list("code_refs"),
        "commit_shas": _string_list("commit_shas"),
        "pr_urls": _string_list("pr_urls"),
        "validated_on_main": bool(raw.get("validated_on_main")),
        "validation_command": (
            str(raw.get("validation_command")).strip()
            if raw.get("validation_command") is not None
            else None
        ),
        "validation_exit_code": raw.get("validation_exit_code"),
        "acceptance_criteria_met": bool(raw.get("acceptance_criteria_met")),
        "acceptance_criteria_notes": str(raw.get("acceptance_criteria_notes") or "").strip(),
        "equivalence_claim": equivalence_claim,
    }


def resolution_has_meaningful_signal(resolution: dict[str, Any] | None) -> bool:
    """Return True when a resolution payload contains actionable signal."""
    if resolution is None:
        return False
    return bool(
        resolution.get("summary")
        or resolution.get("code_refs")
        or resolution.get("commit_shas")
        or resolution.get("pr_urls")
        or resolution.get("acceptance_criteria_met")
        or resolution.get("kind")
    )


def resolution_allows_autoclose(resolution: dict[str, Any] | None) -> bool:
    """Return True when the claimed resolution kind is auto-close eligible."""
    if resolution is None:
        return False
    kind = str(resolution.get("kind") or "")
    equivalence_claim = str(resolution.get("equivalence_claim") or "unknown")
    if kind == "already_on_main":
        return equivalence_claim in {"exact_match", "strict_superset"}
    if kind == "superseded_by_existing_solution":
        return equivalence_claim == "strict_superset"
    return False


def build_resolution_comment(
    *,
    issue_ref: str,
    session_id: str | None,
    resolution_kind: str,
    summary: str,
    verification_class: str,
    final_action: str,
    evidence: dict[str, Any],
) -> str:
    """Render a machine-readable resolution comment body."""
    marker = f"<!-- startupai-board-bot:{RESOLUTION_COMMENT_KIND}:{issue_ref} -->"
    payload = {
        "issue_ref": issue_ref,
        "session_id": session_id,
        "resolution_kind": resolution_kind,
        "summary": summary,
        "verification_class": verification_class,
        "final_action": final_action,
        "evidence": evidence,
    }
    return "\n".join(
        [
            marker,
            f"**Consumer resolution**: `{resolution_kind}`",
            "",
            f"Verification: `{verification_class}`",
            f"Action: `{final_action}`",
            "",
            f"> {summary or 'No summary provided.'}",
            "",
            "```json",
            json.dumps(payload, indent=2, sort_keys=True),
            "```",
        ]
    )


def parse_resolution_comment(body: str) -> ParsedResolutionComment | None:
    """Parse a machine-readable resolution comment, if present."""
    marker_match = _RESOLUTION_MARKER_RE.search(body)
    if marker_match is None:
        return None
    payload_match = _RESOLUTION_DATA_RE.search(body)
    if payload_match is None:
        return None
    try:
        payload = json.loads(payload_match.group("payload"))
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return ParsedResolutionComment(
        issue_ref=marker_match.group("issue_ref"),
        payload=payload,
    )
