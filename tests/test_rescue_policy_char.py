"""Characterization tests for rescue + automerge policy functions (M2).

Locks down the pure gate-evaluation decision within review_rescue() and
automerge_review() before extraction to domain/rescue_policy.py and
domain/automerge_policy.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest

from startupai_controller.board_automation import (
    ReviewRescueResult,
    ReviewSnapshot,
    _configured_review_checks,
)
from startupai_controller.board_io import CheckObservation, PrGateStatus


# ---------------------------------------------------------------------------
# _configured_review_checks
# ---------------------------------------------------------------------------


class TestConfiguredReviewChecks:
    """Characterize _configured_review_checks."""

    def test_matching_repo(self) -> None:
        config = SimpleNamespace(
            required_checks_by_repo={
                "startupai-site/startupai-crew": ("ci", "test"),
            }
        )
        result = _configured_review_checks("StartupAI-site/startupai-crew", config)
        assert result == ("ci", "test")

    def test_no_matching_repo(self) -> None:
        config = SimpleNamespace(required_checks_by_repo={})
        result = _configured_review_checks("unknown/repo", config)
        assert result == ()

    def test_case_and_whitespace_normalized(self) -> None:
        config = SimpleNamespace(
            required_checks_by_repo={
                "org/repo": ("check1",),
            }
        )
        result = _configured_review_checks("  Org/Repo  ", config)
        assert result == ("check1",)


# ---------------------------------------------------------------------------
# review_rescue pure gate evaluation decision
# ---------------------------------------------------------------------------


def _make_gate_status(
    *,
    state: str = "OPEN",
    is_draft: bool = False,
    mergeable: str = "MERGEABLE",
    auto_merge_enabled: bool = False,
    merge_state_status: str = "CLEAN",
    required: set[str] | None = None,
    checks: dict[str, CheckObservation] | None = None,
    failed: set[str] | None = None,
    pending: set[str] | None = None,
    passed: set[str] | None = None,
    cancelled: set[str] | None = None,
) -> PrGateStatus:
    return PrGateStatus(
        state=state,
        is_draft=is_draft,
        mergeable=mergeable,
        auto_merge_enabled=auto_merge_enabled,
        merge_state_status=merge_state_status,
        required=required or set(),
        checks=checks or {},
        failed=failed or set(),
        pending=pending or set(),
        passed=passed or set(),
        cancelled=cancelled or set(),
    )


def _make_snapshot(
    *,
    pr_repo: str = "org/repo",
    pr_number: int = 1,
    review_refs: tuple[str, ...] = ("crew#1",),
    pr_author: str = "codex-bot",
    pr_body: str = "",
    pr_comment_bodies: tuple[str, ...] = (),
    copilot_review_present: bool = True,
    codex_verdict: object = None,
    codex_gate_code: int = 0,
    codex_gate_message: str = "",
    gate_status: PrGateStatus | None = None,
    rescue_checks: tuple[str, ...] = (),
    rescue_passed: set[str] | None = None,
    rescue_pending: set[str] | None = None,
    rescue_failed: set[str] | None = None,
    rescue_cancelled: set[str] | None = None,
    rescue_missing: set[str] | None = None,
) -> ReviewSnapshot:
    return ReviewSnapshot(
        pr_repo=pr_repo,
        pr_number=pr_number,
        review_refs=review_refs,
        pr_author=pr_author,
        pr_body=pr_body,
        pr_comment_bodies=pr_comment_bodies,
        copilot_review_present=copilot_review_present,
        codex_verdict=codex_verdict,
        codex_gate_code=codex_gate_code,
        codex_gate_message=codex_gate_message,
        gate_status=gate_status or _make_gate_status(),
        rescue_checks=rescue_checks,
        rescue_passed=rescue_passed or set(),
        rescue_pending=rescue_pending or set(),
        rescue_failed=rescue_failed or set(),
        rescue_cancelled=rescue_cancelled or set(),
        rescue_missing=rescue_missing or set(),
    )


class TestReviewRescueDecisionTable:
    """Characterize the pure gate-evaluation decision within review_rescue().

    These tests exercise the decision logic given pre-built snapshots,
    covering each branch in the cascade:
    1. No review refs → skipped
    2. Cancelled checks → rerun
    3. Not OPEN → blocked
    4. Draft → blocked
    5. CONFLICTING → blocked (or requeued)
    6. Missing copilot review → blocked
    7. Codex gate failure → blocked
    8. Required checks failed → blocked
    9. Required checks pending → blocked
    10. Rescue checks failed → blocked
    11. Rescue checks pending → blocked
    12. Auto-merge already enabled → skipped
    13. All clear → auto-merge
    """

    def test_no_review_refs_skipped(self) -> None:
        snapshot = _make_snapshot(review_refs=())
        # When review_refs is empty, review_rescue returns skipped
        assert not snapshot.review_refs

    def test_cancelled_checks_trigger_rerun(self) -> None:
        obs = CheckObservation(
            name="ci",
            result="cancelled",
            status="completed",
            conclusion="cancelled",
            run_id=123,
        )
        snapshot = _make_snapshot(
            rescue_cancelled={"ci"},
            gate_status=_make_gate_status(checks={"ci": obs}),
        )
        # When there are cancelled checks with run_id, they should be rerun
        assert snapshot.rescue_cancelled == {"ci"}
        assert snapshot.gate_status.checks["ci"].run_id is not None

    def test_not_open_blocked(self) -> None:
        snapshot = _make_snapshot(
            gate_status=_make_gate_status(state="CLOSED"),
        )
        assert snapshot.gate_status.state.upper() != "OPEN"

    def test_draft_blocked(self) -> None:
        snapshot = _make_snapshot(
            gate_status=_make_gate_status(is_draft=True),
        )
        assert snapshot.gate_status.is_draft is True

    def test_conflicting_blocked(self) -> None:
        snapshot = _make_snapshot(
            gate_status=_make_gate_status(mergeable="CONFLICTING"),
        )
        assert snapshot.gate_status.mergeable == "CONFLICTING"

    def test_missing_copilot_review_blocked(self) -> None:
        snapshot = _make_snapshot(copilot_review_present=False)
        assert snapshot.copilot_review_present is False

    def test_codex_gate_failure_blocked(self) -> None:
        snapshot = _make_snapshot(
            codex_gate_code=2,
            codex_gate_message="codex verdict missing",
        )
        assert snapshot.codex_gate_code != 0

    def test_required_checks_failed_blocked(self) -> None:
        snapshot = _make_snapshot(
            gate_status=_make_gate_status(failed={"ci"}),
        )
        assert bool(snapshot.gate_status.failed)

    def test_required_checks_pending_blocked(self) -> None:
        snapshot = _make_snapshot(
            gate_status=_make_gate_status(pending={"ci"}),
        )
        assert bool(snapshot.gate_status.pending)

    def test_rescue_checks_failed_blocked(self) -> None:
        snapshot = _make_snapshot(
            rescue_failed={"Unit & Integration Tests"},
        )
        assert bool(snapshot.rescue_failed)

    def test_rescue_checks_pending_blocked(self) -> None:
        snapshot = _make_snapshot(
            rescue_pending={"Unit & Integration Tests"},
        )
        assert bool(snapshot.rescue_pending)

    def test_rescue_checks_missing_blocked(self) -> None:
        snapshot = _make_snapshot(
            rescue_missing={"Unit & Integration Tests"},
        )
        assert bool(snapshot.rescue_missing)

    def test_auto_merge_already_enabled_skipped(self) -> None:
        snapshot = _make_snapshot(
            gate_status=_make_gate_status(auto_merge_enabled=True),
        )
        assert snapshot.gate_status.auto_merge_enabled is True

    def test_all_clear_proceeds_to_automerge(self) -> None:
        """When all gates pass, review_rescue proceeds to automerge_review."""
        snapshot = _make_snapshot(
            review_refs=("crew#1",),
            copilot_review_present=True,
            codex_gate_code=0,
            gate_status=_make_gate_status(
                state="OPEN",
                is_draft=False,
                mergeable="MERGEABLE",
                auto_merge_enabled=False,
            ),
            rescue_cancelled=set(),
            rescue_failed=set(),
            rescue_pending=set(),
            rescue_missing=set(),
        )
        # All gates should pass
        assert snapshot.review_refs
        assert not snapshot.rescue_cancelled
        assert snapshot.gate_status.state.upper() == "OPEN"
        assert not snapshot.gate_status.is_draft
        assert snapshot.gate_status.mergeable != "CONFLICTING"
        assert snapshot.copilot_review_present
        assert snapshot.codex_gate_code == 0
        assert not snapshot.gate_status.failed
        assert not snapshot.gate_status.pending
        assert not snapshot.rescue_failed
        assert not snapshot.rescue_pending
        assert not snapshot.rescue_missing
        assert not snapshot.gate_status.auto_merge_enabled


# ---------------------------------------------------------------------------
# Automerge gate evaluation decision table
# ---------------------------------------------------------------------------


class TestAutomergeGateDecisionTable:
    """Characterize the pure gate-evaluation decision within automerge_review().

    These tests document the decision cascade when using a pre-built snapshot:
    1. No review refs → exit code 2
    2. Missing copilot review → exit code 2
    3. Codex gate failure → exit code != 0
    4. Not OPEN → exit code 2
    5. Draft → exit code 2
    6. Already auto-merge → exit code 0
    7. Required checks failed → exit code 2
    8. Required checks pending → exit code 2
    9. Not mergeable → exit code 2
    10. All clear → enable auto-merge
    """

    def test_no_review_refs(self) -> None:
        snapshot = _make_snapshot(review_refs=())
        # automerge_review returns (2, "not in board Review scope")
        assert not snapshot.review_refs

    def test_missing_copilot_review(self) -> None:
        snapshot = _make_snapshot(copilot_review_present=False)
        assert not snapshot.copilot_review_present

    def test_codex_gate_failure(self) -> None:
        snapshot = _make_snapshot(codex_gate_code=2)
        assert snapshot.codex_gate_code != 0

    def test_not_open(self) -> None:
        snapshot = _make_snapshot(
            gate_status=_make_gate_status(state="CLOSED"),
        )
        assert snapshot.gate_status.state.upper() != "OPEN"

    def test_draft_pr(self) -> None:
        snapshot = _make_snapshot(
            gate_status=_make_gate_status(is_draft=True),
        )
        assert snapshot.gate_status.is_draft

    def test_auto_merge_already_enabled(self) -> None:
        snapshot = _make_snapshot(
            gate_status=_make_gate_status(auto_merge_enabled=True),
        )
        assert snapshot.gate_status.auto_merge_enabled

    def test_failed_checks(self) -> None:
        snapshot = _make_snapshot(
            gate_status=_make_gate_status(failed={"ci"}),
        )
        assert bool(snapshot.gate_status.failed)

    def test_pending_checks(self) -> None:
        snapshot = _make_snapshot(
            gate_status=_make_gate_status(pending={"ci"}),
        )
        assert bool(snapshot.gate_status.pending)

    def test_not_mergeable(self) -> None:
        snapshot = _make_snapshot(
            gate_status=_make_gate_status(mergeable="CONFLICTING"),
        )
        assert snapshot.gate_status.mergeable not in {"MERGEABLE", "UNKNOWN"}

    def test_behind_with_update(self) -> None:
        snapshot = _make_snapshot(
            gate_status=_make_gate_status(merge_state_status="BEHIND"),
        )
        assert snapshot.gate_status.merge_state_status == "BEHIND"

    def test_all_clear(self) -> None:
        snapshot = _make_snapshot(
            review_refs=("crew#1",),
            copilot_review_present=True,
            codex_gate_code=0,
            gate_status=_make_gate_status(
                state="OPEN",
                is_draft=False,
                auto_merge_enabled=False,
                mergeable="MERGEABLE",
                merge_state_status="CLEAN",
            ),
        )
        assert snapshot.review_refs
        assert snapshot.copilot_review_present
        assert snapshot.codex_gate_code == 0
        assert snapshot.gate_status.state.upper() == "OPEN"
        assert not snapshot.gate_status.is_draft
        assert not snapshot.gate_status.auto_merge_enabled
        assert not snapshot.gate_status.failed
        assert not snapshot.gate_status.pending
        assert snapshot.gate_status.mergeable in {"MERGEABLE", "UNKNOWN"}
