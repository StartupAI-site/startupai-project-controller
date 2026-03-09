"""Characterization tests for launch policy functions (M1).

Locks down exact input/output behavior of candidate selection and PR
classification before extraction to domain/launch_policy.py.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from startupai_controller.board_consumer import (
    OpenPullRequestMatch,
    _consumer_provenance_marker,
    _deterministic_branch_pattern,
    _parse_consumer_provenance,
)
from startupai_controller.board_io import MARKER_PREFIX


# ---------------------------------------------------------------------------
# _classify_open_pr_candidates decision table
# (Testing the pure classification logic, not the GitHub query)
# ---------------------------------------------------------------------------


def _classify_candidates_pure(
    issue_ref: str,
    candidates: list[OpenPullRequestMatch],
    trusted_authors: set[str],
    expected_branch: str | None = None,
) -> tuple[str, OpenPullRequestMatch | None, str]:
    """Reproduce the pure classification decision from _classify_open_pr_candidates.

    This function encodes the exact decision table, separated from the GitHub
    query that fetches candidates.
    """
    if not candidates:
        return "none", None, "no-open-pr"

    adoptable: list[OpenPullRequestMatch] = []
    ambiguous_reasons: list[str] = []
    branch_pattern = _deterministic_branch_pattern(issue_ref)

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
