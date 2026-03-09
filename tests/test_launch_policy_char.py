"""Characterization tests for launch policy functions (M1).

Locks down exact input/output behavior of candidate selection and PR
classification. Imports from domain modules directly.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from startupai_controller.domain.models import OpenPullRequestMatch
from startupai_controller.domain.repair_policy import (
    MARKER_PREFIX,
    consumer_provenance_marker as _consumer_provenance_marker,
    deterministic_branch_pattern as _deterministic_branch_pattern,
    parse_consumer_provenance as _parse_consumer_provenance,
)
from startupai_controller.domain.launch_policy import (
    classify_pr_candidates,
    launch_session_kind,
    reconcile_in_progress_decision,
)


# ---------------------------------------------------------------------------
# _classify_open_pr_candidates decision table
# (Testing the pure classification logic via domain function)
# ---------------------------------------------------------------------------


def _classify_candidates_pure(
    issue_ref: str,
    candidates: list[OpenPullRequestMatch],
    trusted_authors: set[str],
    expected_branch: str | None = None,
) -> tuple[str, OpenPullRequestMatch | None, str]:
    """Delegate to domain.launch_policy.classify_pr_candidates."""
    return classify_pr_candidates(
        issue_ref,
        candidates,
        trusted_authors,
        expected_branch=expected_branch,
    )


def _make_candidate(
    issue_ref: str = "crew#42",
    url: str = "https://github.com/org/repo/pull/1",
    number: int = 1,
    author: str = "codex-bot",
    branch_name: str = "feat/42-do-thing",
    executor: str = "codex",
    repo_prefix: str = "crew",
    session_id: str = "sess-1",
) -> OpenPullRequestMatch:
    """Create a candidate with valid provenance."""
    marker = _consumer_provenance_marker(
        session_id=session_id,
        issue_ref=issue_ref,
        repo_prefix=repo_prefix,
        branch_name=branch_name,
        executor=executor,
    )
    body = f"PR body\n{marker}\nMore text"
    provenance = _parse_consumer_provenance(body)
    return OpenPullRequestMatch(
        url=url,
        number=number,
        author=author,
        body=body,
        branch_name=branch_name,
        provenance=provenance,
    )


class TestClassifyOpenPrCandidates:
    """Characterize the pure PR classification decision table."""

    TRUSTED_AUTHORS = {"codex-bot", "github-actions[bot]"}

    def test_no_candidates(self) -> None:
        cls, match, reason = _classify_candidates_pure(
            "crew#42", [], self.TRUSTED_AUTHORS
        )
        assert cls == "none"
        assert match is None
        assert reason == "no-open-pr"

    def test_single_adoptable(self) -> None:
        candidate = _make_candidate(issue_ref="crew#42", author="codex-bot")
        cls, match, reason = _classify_candidates_pure(
            "crew#42", [candidate], self.TRUSTED_AUTHORS
        )
        assert cls == "adoptable"
        assert match is candidate
        assert reason == "qualifying-open-pr"

    def test_missing_provenance(self) -> None:
        candidate = OpenPullRequestMatch(
            url="https://github.com/org/repo/pull/1",
            number=1,
            author="codex-bot",
            body="no provenance",
            branch_name="feat/42-do-thing",
            provenance=None,
        )
        cls, match, reason = _classify_candidates_pure(
            "crew#42", [candidate], self.TRUSTED_AUTHORS
        )
        assert cls == "non-local"
        assert match is None
        assert "missing-provenance" in reason

    def test_issue_mismatch(self) -> None:
        candidate = _make_candidate(issue_ref="crew#99", author="codex-bot")
        cls, match, reason = _classify_candidates_pure(
            "crew#42", [candidate], self.TRUSTED_AUTHORS
        )
        assert cls == "non-local"
        assert "issue-mismatch" in reason

    def test_executor_mismatch(self) -> None:
        candidate = _make_candidate(
            issue_ref="crew#42", executor="claude", author="codex-bot"
        )
        cls, match, reason = _classify_candidates_pure(
            "crew#42", [candidate], self.TRUSTED_AUTHORS
        )
        assert cls == "non-local"
        assert "executor-mismatch" in reason

    def test_untrusted_author(self) -> None:
        candidate = _make_candidate(
            issue_ref="crew#42", author="random-user"
        )
        cls, match, reason = _classify_candidates_pure(
            "crew#42", [candidate], self.TRUSTED_AUTHORS
        )
        assert cls == "non-local"
        assert "untrusted-author" in reason

    def test_branch_mismatch_with_expected(self) -> None:
        candidate = _make_candidate(
            issue_ref="crew#42",
            branch_name="feat/42-do-thing",
            author="codex-bot",
        )
        cls, match, reason = _classify_candidates_pure(
            "crew#42",
            [candidate],
            self.TRUSTED_AUTHORS,
            expected_branch="feat/42-other-branch",
        )
        assert cls == "non-local"
        assert "branch-mismatch" in reason

    def test_noncanonical_branch(self) -> None:
        # Branch doesn't match feat/{number}-{slug} pattern
        candidate = _make_candidate(
            issue_ref="crew#42",
            branch_name="fix/42-some-thing",
            author="codex-bot",
        )
        cls, match, reason = _classify_candidates_pure(
            "crew#42", [candidate], self.TRUSTED_AUTHORS
        )
        assert cls == "non-local"
        assert "branch-noncanonical" in reason

    def test_multiple_adoptable_is_ambiguous(self) -> None:
        c1 = _make_candidate(
            issue_ref="crew#42",
            url="https://github.com/org/repo/pull/1",
            branch_name="feat/42-first",
            author="codex-bot",
            session_id="s1",
        )
        c2 = _make_candidate(
            issue_ref="crew#42",
            url="https://github.com/org/repo/pull/2",
            branch_name="feat/42-second",
            author="codex-bot",
            session_id="s2",
        )
        cls, match, reason = _classify_candidates_pure(
            "crew#42", [c1, c2], self.TRUSTED_AUTHORS
        )
        assert cls == "ambiguous"
        assert match is None
        assert reason == "multiple-adoptable-prs"

    def test_adoptable_plus_ambiguous_is_ambiguous(self) -> None:
        good = _make_candidate(
            issue_ref="crew#42",
            url="https://github.com/org/repo/pull/1",
            branch_name="feat/42-good",
            author="codex-bot",
        )
        bad = OpenPullRequestMatch(
            url="https://github.com/org/repo/pull/2",
            number=2,
            author="codex-bot",
            body="no provenance",
            branch_name="feat/42-bad",
            provenance=None,
        )
        cls, match, reason = _classify_candidates_pure(
            "crew#42", [good, bad], self.TRUSTED_AUTHORS
        )
        assert cls == "ambiguous"


# ---------------------------------------------------------------------------
# reconcile_in_progress_decision — domain function direct tests
# ---------------------------------------------------------------------------


class TestReconcileInProgressDecision:
    """Characterize reconcile_in_progress_decision truth table."""

    def test_adoptable_repair_not_success_returns_ready(self) -> None:
        result = reconcile_in_progress_decision(
            "adoptable",
            has_latest_session=True,
            session_kind="repair",
            session_status="failed",
        )
        assert result == "ready"

    def test_adoptable_repair_success_returns_review(self) -> None:
        result = reconcile_in_progress_decision(
            "adoptable",
            has_latest_session=True,
            session_kind="repair",
            session_status="success",
        )
        assert result == "review"

    def test_adoptable_fresh_session_returns_review(self) -> None:
        result = reconcile_in_progress_decision(
            "adoptable",
            has_latest_session=True,
            session_kind="fresh",
            session_status="success",
        )
        assert result == "review"

    def test_adoptable_no_session_returns_review(self) -> None:
        result = reconcile_in_progress_decision(
            "adoptable",
            has_latest_session=False,
            session_kind=None,
            session_status=None,
        )
        assert result == "review"

    def test_none_classification_returns_ready(self) -> None:
        result = reconcile_in_progress_decision(
            "none",
            has_latest_session=True,
            session_kind="fresh",
            session_status="success",
        )
        assert result == "ready"

    def test_ambiguous_returns_blocked(self) -> None:
        result = reconcile_in_progress_decision(
            "ambiguous",
            has_latest_session=True,
            session_kind="fresh",
            session_status="success",
        )
        assert result == "blocked"

    def test_non_local_returns_blocked(self) -> None:
        result = reconcile_in_progress_decision(
            "non-local",
            has_latest_session=True,
            session_kind="fresh",
            session_status="success",
        )
        assert result == "blocked"


# ---------------------------------------------------------------------------
# launch_session_kind
# ---------------------------------------------------------------------------


class TestLaunchSessionKind:
    """Characterize launch_session_kind domain function."""

    def test_adoptable_with_match_returns_repair(self) -> None:
        pr_match = _make_candidate(issue_ref="crew#42")
        assert launch_session_kind("adoptable", pr_match) == "repair"

    def test_adoptable_without_match_returns_new_work(self) -> None:
        assert launch_session_kind("adoptable", None) == "new_work"

    def test_none_classification_returns_new_work(self) -> None:
        assert launch_session_kind("none", None) == "new_work"

    def test_ambiguous_returns_new_work(self) -> None:
        assert launch_session_kind("ambiguous", None) == "new_work"

    def test_non_local_returns_new_work(self) -> None:
        assert launch_session_kind("non-local", None) == "new_work"
