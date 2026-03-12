"""Characterization tests for scheduling policy functions (M1).

Locks down exact input/output behavior of pure scheduling functions.
Imports from domain/scheduling_policy.py directly.
"""

from __future__ import annotations

import pytest

from startupai_controller.domain.scheduling_policy import (
    wip_limit_for_lane as _wip_limit_for_lane,
    protected_queue_executor_target as _protected_queue_executor_target,
    priority_rank as _priority_rank,
    repo_prefix_for_slug as _repo_prefix_for_slug,
    snapshot_to_issue_ref as _snapshot_to_issue_ref,
    admission_candidate_rank as _admission_candidate_rank,
    normalize_heading as _normalize_heading,
    admission_watermarks,
    has_structured_acceptance_criteria,
)

# ---------------------------------------------------------------------------
# _wip_limit_for_lane
# ---------------------------------------------------------------------------


class TestWipLimitForLane:
    """Characterize _wip_limit_for_lane."""

    def test_none_config_returns_fallback(self) -> None:
        assert _wip_limit_for_lane(None, "codex", "crew", 3) == 3

    def test_matching_executor_and_lane(self) -> None:
        assert _wip_limit_for_lane({"codex": {"crew": 5}}, "codex", "crew", 3) == 5

    def test_missing_executor_returns_fallback(self) -> None:
        assert _wip_limit_for_lane({"claude": {"crew": 5}}, "codex", "crew", 3) == 3

    def test_missing_lane_returns_fallback(self) -> None:
        assert _wip_limit_for_lane({"codex": {"app": 5}}, "codex", "crew", 3) == 3

    def test_empty_wip_limits(self) -> None:
        assert _wip_limit_for_lane({}, "codex", "crew", 3) == 3


# ---------------------------------------------------------------------------
# _protected_queue_executor_target
# ---------------------------------------------------------------------------


class TestProtectedQueueExecutorTarget:
    """Characterize _protected_queue_executor_target."""

    def test_none_mode(self) -> None:
        assert _protected_queue_executor_target(None, ()) is None

    def test_board_authority_mode(self) -> None:
        assert _protected_queue_executor_target("board", ("codex",)) is None

    def test_single_machine_single_executor(self) -> None:
        assert _protected_queue_executor_target("single_machine", ("codex",)) == "codex"

    def test_single_machine_multiple_executors(self) -> None:
        assert (
            _protected_queue_executor_target("single_machine", ("codex", "claude"))
            is None
        )

    def test_single_machine_invalid_executor(self) -> None:
        assert _protected_queue_executor_target("single_machine", ("bogus",)) is None

    def test_single_machine_empty_executors(self) -> None:
        assert _protected_queue_executor_target("single_machine", ()) is None

    def test_executor_whitespace_stripped(self) -> None:
        assert (
            _protected_queue_executor_target("single_machine", ("  codex  ",))
            == "codex"
        )


# ---------------------------------------------------------------------------
# _priority_rank
# ---------------------------------------------------------------------------


class TestPriorityRank:
    """Characterize _priority_rank."""

    def test_p0(self) -> None:
        assert _priority_rank("P0") == (0, "p0")

    def test_p1(self) -> None:
        assert _priority_rank("P1") == (1, "p1")

    def test_p2(self) -> None:
        assert _priority_rank("P2") == (2, "p2")

    def test_p3(self) -> None:
        assert _priority_rank("P3") == (3, "p3")

    def test_case_insensitive(self) -> None:
        assert _priority_rank("p1") == (1, "p1")

    def test_with_label(self) -> None:
        rank, _ = _priority_rank("P1 - High")
        assert rank == 1

    def test_non_priority(self) -> None:
        rank, _ = _priority_rank("Unknown")
        assert rank == 99

    def test_empty_string(self) -> None:
        rank, _ = _priority_rank("")
        assert rank == 99

    def test_ordering(self) -> None:
        """P0 < P1 < P2 < P3 < Unknown."""
        assert _priority_rank("P0") < _priority_rank("P1")
        assert _priority_rank("P1") < _priority_rank("P2")
        assert _priority_rank("P2") < _priority_rank("P3")
        assert _priority_rank("P3") < _priority_rank("Unknown")


class TestSnapshotIssueRef:
    """Characterize snapshot_to_issue_ref and repo_prefix_for_slug."""

    ISSUE_PREFIXES = {
        "app": "StartupAI-site/app.startupai-site",
        "crew": "StartupAI-site/startupai-project-controller",
        "site": "StartupAI-site/startupai.site",
    }

    def test_repo_prefix_for_slug(self) -> None:
        assert (
            _repo_prefix_for_slug(
                "StartupAI-site/startupai.site",
                self.ISSUE_PREFIXES,
            )
            == "site"
        )

    def test_snapshot_issue_ref_preserves_canonical_ref(self) -> None:
        assert _snapshot_to_issue_ref("app#46", self.ISSUE_PREFIXES) == "app#46"

    def test_snapshot_issue_ref_normalizes_repo_scoped_ref(self) -> None:
        assert (
            _snapshot_to_issue_ref(
                "StartupAI-site/startupai.site#37",
                self.ISSUE_PREFIXES,
            )
            == "site#37"
        )

    def test_snapshot_issue_ref_rejects_unknown_repo(self) -> None:
        assert (
            _snapshot_to_issue_ref(
                "StartupAI-site/unknown#37",
                self.ISSUE_PREFIXES,
            )
            is None
        )

    def test_snapshot_issue_ref_rejects_non_numeric_issue(self) -> None:
        assert _snapshot_to_issue_ref("site#x", self.ISSUE_PREFIXES) is None


# ---------------------------------------------------------------------------
# admission_watermarks
# ---------------------------------------------------------------------------


class TestAdmissionWatermarks:
    """Characterize admission_watermarks."""

    def test_default_multipliers(self) -> None:
        floor, cap = admission_watermarks(1)
        assert floor == 2
        assert cap == 3

    def test_concurrency_2(self) -> None:
        floor, cap = admission_watermarks(2)
        assert floor == 4
        assert cap == 6

    def test_custom_multipliers(self) -> None:
        floor, cap = admission_watermarks(3, floor_multiplier=3, cap_multiplier=5)
        assert floor == 9
        assert cap == 15

    def test_zero_concurrency_floors_at_1(self) -> None:
        floor, cap = admission_watermarks(0)
        assert floor >= 1
        assert cap >= floor

    def test_negative_concurrency(self) -> None:
        floor, cap = admission_watermarks(-5)
        assert floor >= 1
        assert cap >= floor

    def test_cap_at_least_floor(self) -> None:
        floor, cap = admission_watermarks(1, floor_multiplier=5, cap_multiplier=2)
        assert cap >= floor


# ---------------------------------------------------------------------------
# _admission_candidate_rank
# ---------------------------------------------------------------------------


class TestAdmissionCandidateRank:
    """Characterize _admission_candidate_rank."""

    def test_graph_member_higher_priority(self) -> None:
        graph = _admission_candidate_rank(
            "crew#1", priority="P0", is_graph_member=True, issue_number=1
        )
        non_graph = _admission_candidate_rank(
            "crew#1", priority="P0", is_graph_member=False, issue_number=1
        )
        assert graph < non_graph

    def test_higher_priority_ranks_first(self) -> None:
        p0 = _admission_candidate_rank(
            "crew#1", priority="P0", is_graph_member=True, issue_number=1
        )
        p2 = _admission_candidate_rank(
            "crew#1", priority="P2", is_graph_member=True, issue_number=1
        )
        assert p0 < p2

    def test_lower_issue_number_first(self) -> None:
        low = _admission_candidate_rank(
            "crew#1", priority="P1", is_graph_member=True, issue_number=1
        )
        high = _admission_candidate_rank(
            "crew#99", priority="P1", is_graph_member=True, issue_number=99
        )
        assert low < high


# ---------------------------------------------------------------------------
# has_structured_acceptance_criteria
# ---------------------------------------------------------------------------


class TestHasStructuredAcceptanceCriteria:
    """Characterize has_structured_acceptance_criteria."""

    def test_empty_body(self) -> None:
        assert has_structured_acceptance_criteria("") is False

    def test_whitespace_only(self) -> None:
        assert has_structured_acceptance_criteria("   ") is False

    def test_standard_heading_with_bullet(self) -> None:
        body = "## Acceptance Criteria\n- item one\n- item two"
        assert has_structured_acceptance_criteria(body) is True

    def test_definition_of_done_heading(self) -> None:
        body = "## Definition of Done\n- item one"
        assert has_structured_acceptance_criteria(body) is True

    def test_heading_without_bullets(self) -> None:
        body = "## Acceptance Criteria\nJust text, no bullets."
        assert has_structured_acceptance_criteria(body) is False

    def test_checkbox_format(self) -> None:
        body = "## Acceptance Criteria\n- [ ] task one\n- [x] task two"
        assert has_structured_acceptance_criteria(body) is True

    def test_numbered_list(self) -> None:
        body = "## Acceptance Criteria\n1. first item"
        assert has_structured_acceptance_criteria(body) is True

    def test_no_heading_no_match(self) -> None:
        body = "Some text\n- bullet without heading"
        assert has_structured_acceptance_criteria(body) is False


# ---------------------------------------------------------------------------
# _normalize_heading
# ---------------------------------------------------------------------------


class TestNormalizeHeading:
    """Characterize _normalize_heading."""

    def test_strips_trailing_colon(self) -> None:
        assert _normalize_heading("Acceptance Criteria:") == "acceptance criteria"

    def test_strips_trailing_whitespace(self) -> None:
        assert _normalize_heading("  Acceptance Criteria  ") == "acceptance criteria"

    def test_lowercases(self) -> None:
        assert _normalize_heading("ACCEPTANCE CRITERIA") == "acceptance criteria"
