"""Characterization tests for scheduling policy functions (M1).

Locks down exact input/output behavior of pure scheduling functions before
extraction to domain/scheduling_policy.py.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from startupai_controller.board_automation import (
    _wip_limit_for_lane,
    _protected_queue_executor_target,
)
from startupai_controller.board_io import _priority_rank
from startupai_controller.board_graph import (
    _admission_candidate_rank,
    _normalize_heading,
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
        config = SimpleNamespace(wip_limits={"codex": {"crew": 5}})
        assert _wip_limit_for_lane(config, "codex", "crew", 3) == 5

    def test_missing_executor_returns_fallback(self) -> None:
        config = SimpleNamespace(wip_limits={"claude": {"crew": 5}})
        assert _wip_limit_for_lane(config, "codex", "crew", 3) == 3

    def test_missing_lane_returns_fallback(self) -> None:
        config = SimpleNamespace(wip_limits={"codex": {"app": 5}})
        assert _wip_limit_for_lane(config, "codex", "crew", 3) == 3

    def test_empty_wip_limits(self) -> None:
        config = SimpleNamespace(wip_limits={})
        assert _wip_limit_for_lane(config, "codex", "crew", 3) == 3


# ---------------------------------------------------------------------------
# _protected_queue_executor_target
# ---------------------------------------------------------------------------


class TestProtectedQueueExecutorTarget:
    """Characterize _protected_queue_executor_target."""

    def test_none_config(self) -> None:
        assert _protected_queue_executor_target(None) is None

    def test_board_authority_mode(self) -> None:
        config = SimpleNamespace(
            execution_authority_mode="board",
            execution_authority_executors=["codex"],
        )
        assert _protected_queue_executor_target(config) is None

    def test_single_machine_single_executor(self) -> None:
        config = SimpleNamespace(
            execution_authority_mode="single_machine",
            execution_authority_executors=["codex"],
        )
        assert _protected_queue_executor_target(config) == "codex"

    def test_single_machine_multiple_executors(self) -> None:
        config = SimpleNamespace(
            execution_authority_mode="single_machine",
            execution_authority_executors=["codex", "claude"],
        )
        assert _protected_queue_executor_target(config) is None

    def test_single_machine_invalid_executor(self) -> None:
        config = SimpleNamespace(
            execution_authority_mode="single_machine",
            execution_authority_executors=["bogus"],
        )
        assert _protected_queue_executor_target(config) is None

    def test_single_machine_empty_executors(self) -> None:
        config = SimpleNamespace(
            execution_authority_mode="single_machine",
            execution_authority_executors=[],
        )
        assert _protected_queue_executor_target(config) is None

    def test_executor_whitespace_stripped(self) -> None:
        config = SimpleNamespace(
            execution_authority_mode="single_machine",
            execution_authority_executors=["  codex  "],
        )
        assert _protected_queue_executor_target(config) == "codex"


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
        graph = _admission_candidate_rank("crew#1", priority="P0", is_graph_member=True)
        non_graph = _admission_candidate_rank("crew#1", priority="P0", is_graph_member=False)
        assert graph < non_graph

    def test_higher_priority_ranks_first(self) -> None:
        p0 = _admission_candidate_rank("crew#1", priority="P0", is_graph_member=True)
        p2 = _admission_candidate_rank("crew#1", priority="P2", is_graph_member=True)
        assert p0 < p2

    def test_lower_issue_number_first(self) -> None:
        low = _admission_candidate_rank("crew#1", priority="P1", is_graph_member=True)
        high = _admission_candidate_rank("crew#99", priority="P1", is_graph_member=True)
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
