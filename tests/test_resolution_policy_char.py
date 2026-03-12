"""Characterization tests for resolution policy functions (M2).

Locks down exact input/output behavior. Imports from
domain/resolution_policy.py directly.
"""

from __future__ import annotations

import json

import pytest

from startupai_controller.domain.resolution_policy import (
    AUTO_CLOSE_RESOLUTION_KINDS,
    NON_AUTO_CLOSE_RESOLUTION_KINDS,
    VALID_EQUIVALENCE_CLAIMS,
    VALID_RESOLUTION_KINDS,
    build_resolution_comment,
    normalize_resolution_payload,
    parse_resolution_comment,
    resolution_allows_autoclose,
    resolution_has_meaningful_signal,
)

# ---------------------------------------------------------------------------
# normalize_resolution_payload
# ---------------------------------------------------------------------------


class TestNormalizeResolutionPayload:
    """Characterize normalize_resolution_payload."""

    def test_none_input(self) -> None:
        assert normalize_resolution_payload(None) is None

    def test_non_dict_input(self) -> None:
        assert normalize_resolution_payload("string") is None
        assert normalize_resolution_payload(42) is None
        assert normalize_resolution_payload([]) is None

    def test_missing_kind(self) -> None:
        assert normalize_resolution_payload({}) is None

    def test_invalid_kind(self) -> None:
        assert normalize_resolution_payload({"kind": "bogus"}) is None

    @pytest.mark.parametrize("kind", sorted(VALID_RESOLUTION_KINDS))
    def test_valid_kinds(self, kind: str) -> None:
        result = normalize_resolution_payload({"kind": kind})
        assert result is not None
        assert result["kind"] == kind

    def test_default_equivalence_claim(self) -> None:
        result = normalize_resolution_payload({"kind": "already_on_main"})
        assert result is not None
        assert result["equivalence_claim"] == "unknown"

    def test_valid_equivalence_claim(self) -> None:
        result = normalize_resolution_payload(
            {"kind": "already_on_main", "equivalence_claim": "exact_match"}
        )
        assert result is not None
        assert result["equivalence_claim"] == "exact_match"

    def test_invalid_equivalence_claim_defaults_to_unknown(self) -> None:
        result = normalize_resolution_payload(
            {"kind": "already_on_main", "equivalence_claim": "bogus"}
        )
        assert result is not None
        assert result["equivalence_claim"] == "unknown"

    def test_string_list_normalization(self) -> None:
        result = normalize_resolution_payload(
            {
                "kind": "already_on_main",
                "code_refs": ["file.py", "", None, "  other.py  "],
            }
        )
        assert result is not None
        # Empty strings and None are stripped
        assert "file.py" in result["code_refs"]
        assert "" not in result["code_refs"]

    def test_non_list_code_refs(self) -> None:
        result = normalize_resolution_payload(
            {"kind": "already_on_main", "code_refs": "not-a-list"}
        )
        assert result is not None
        assert result["code_refs"] == []

    def test_summary_stripped(self) -> None:
        result = normalize_resolution_payload(
            {"kind": "already_on_main", "summary": "  hello  "}
        )
        assert result is not None
        assert result["summary"] == "hello"

    def test_boolean_fields(self) -> None:
        result = normalize_resolution_payload(
            {
                "kind": "already_on_main",
                "validated_on_main": True,
                "acceptance_criteria_met": False,
            }
        )
        assert result is not None
        assert result["validated_on_main"] is True
        assert result["acceptance_criteria_met"] is False

    def test_validation_command_none(self) -> None:
        result = normalize_resolution_payload({"kind": "already_on_main"})
        assert result is not None
        assert result["validation_command"] is None


# ---------------------------------------------------------------------------
# resolution_has_meaningful_signal
# ---------------------------------------------------------------------------


class TestResolutionHasMeaningfulSignal:
    """Characterize resolution_has_meaningful_signal."""

    def test_none(self) -> None:
        assert resolution_has_meaningful_signal(None) is False

    def test_empty_dict(self) -> None:
        assert resolution_has_meaningful_signal({}) is False

    def test_with_summary(self) -> None:
        assert resolution_has_meaningful_signal({"summary": "found duplicate"}) is True

    def test_with_code_refs(self) -> None:
        assert resolution_has_meaningful_signal({"code_refs": ["file.py"]}) is True

    def test_with_commit_shas(self) -> None:
        assert resolution_has_meaningful_signal({"commit_shas": ["abc123"]}) is True

    def test_with_pr_urls(self) -> None:
        assert resolution_has_meaningful_signal({"pr_urls": ["http://x"]}) is True

    def test_with_acceptance_criteria_met(self) -> None:
        assert (
            resolution_has_meaningful_signal({"acceptance_criteria_met": True}) is True
        )

    def test_with_kind(self) -> None:
        assert resolution_has_meaningful_signal({"kind": "already_on_main"}) is True

    def test_falsy_values(self) -> None:
        assert (
            resolution_has_meaningful_signal(
                {"summary": "", "code_refs": [], "kind": ""}
            )
            is False
        )


# ---------------------------------------------------------------------------
# resolution_allows_autoclose
# ---------------------------------------------------------------------------


class TestResolutionAllowsAutoclose:
    """Characterize resolution_allows_autoclose."""

    def test_none(self) -> None:
        assert resolution_allows_autoclose(None) is False

    def test_already_on_main_exact_match(self) -> None:
        assert (
            resolution_allows_autoclose(
                {"kind": "already_on_main", "equivalence_claim": "exact_match"}
            )
            is True
        )

    def test_already_on_main_strict_superset(self) -> None:
        assert (
            resolution_allows_autoclose(
                {"kind": "already_on_main", "equivalence_claim": "strict_superset"}
            )
            is True
        )

    def test_already_on_main_unknown(self) -> None:
        assert (
            resolution_allows_autoclose(
                {"kind": "already_on_main", "equivalence_claim": "unknown"}
            )
            is False
        )

    def test_superseded_strict_superset(self) -> None:
        assert (
            resolution_allows_autoclose(
                {
                    "kind": "superseded_by_existing_solution",
                    "equivalence_claim": "strict_superset",
                }
            )
            is True
        )

    def test_superseded_exact_match(self) -> None:
        assert (
            resolution_allows_autoclose(
                {
                    "kind": "superseded_by_existing_solution",
                    "equivalence_claim": "exact_match",
                }
            )
            is False
        )

    def test_duplicate_not_autoclosable(self) -> None:
        assert (
            resolution_allows_autoclose(
                {"kind": "duplicate", "equivalence_claim": "exact_match"}
            )
            is False
        )

    def test_no_action_needed_not_autoclosable(self) -> None:
        assert (
            resolution_allows_autoclose(
                {"kind": "no_action_needed", "equivalence_claim": "exact_match"}
            )
            is False
        )

    def test_auto_close_vs_non_close_kinds(self) -> None:
        """Verify the constant sets are correct."""
        assert AUTO_CLOSE_RESOLUTION_KINDS == {
            "already_on_main",
            "superseded_by_existing_solution",
        }
        assert NON_AUTO_CLOSE_RESOLUTION_KINDS == {
            "duplicate",
            "superseded",
            "no_action_needed",
        }


# ---------------------------------------------------------------------------
# build_resolution_comment / parse_resolution_comment roundtrip
# ---------------------------------------------------------------------------


class TestResolutionCommentRoundtrip:
    """Characterize build/parse resolution comment roundtrip."""

    def test_roundtrip(self) -> None:
        body = build_resolution_comment(
            issue_ref="crew#42",
            session_id="sess-123",
            resolution_kind="already_on_main",
            summary="Already fixed in main",
            verification_class="strong",
            final_action="closed_as_already_resolved",
            evidence={"commit_shas": ["abc123"]},
        )
        parsed = parse_resolution_comment(body)
        assert parsed is not None
        assert parsed.issue_ref == "crew#42"
        assert parsed.payload["resolution_kind"] == "already_on_main"
        assert parsed.payload["session_id"] == "sess-123"
        assert parsed.payload["verification_class"] == "strong"
        assert parsed.payload["final_action"] == "closed_as_already_resolved"
        assert parsed.payload["evidence"]["commit_shas"] == ["abc123"]

    def test_comment_contains_marker(self) -> None:
        body = build_resolution_comment(
            issue_ref="crew#42",
            session_id=None,
            resolution_kind="duplicate",
            summary="Duplicate of #1",
            verification_class="weak",
            final_action="blocked_for_resolution_review",
            evidence={},
        )
        assert "<!-- startupai-board-bot:consumer-resolution:crew#42 -->" in body

    def test_parse_returns_none_for_no_marker(self) -> None:
        assert parse_resolution_comment("no marker here") is None

    def test_parse_returns_none_for_bad_json(self) -> None:
        body = "<!-- startupai-board-bot:consumer-resolution:crew#1 -->\n```json\n{bad}\n```"
        assert parse_resolution_comment(body) is None

    def test_parse_returns_none_for_no_json_block(self) -> None:
        body = "<!-- startupai-board-bot:consumer-resolution:crew#1 -->\nNo json block."
        assert parse_resolution_comment(body) is None
