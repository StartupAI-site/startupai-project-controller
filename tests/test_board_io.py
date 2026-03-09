"""Unit tests for board_io module (GitHub API interaction layer).

Tests IO functions directly without going through orchestration.
All tests use dependency injection -- NO real GitHub API calls.
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess

import pytest

import startupai_controller.board_io as board_io
from startupai_controller.board_io import (
    LinkedIssue,
    _marker_for,
    _comment_exists,
    _repo_to_prefix,
    _query_failed_check_runs,
    clear_cycle_board_snapshot_cache,
    clear_required_status_checks_cache,
    build_cycle_board_snapshot,
    query_closing_issues,
    query_latest_codex_verdict,
    query_required_status_checks,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    load_config,
)


# -- Fixtures -----------------------------------------------------------------


def _valid_payload() -> dict:
    return {
        "issue_prefixes": {
            "app": "StartupAI-site/app.startupai-site",
            "crew": "StartupAI-site/startupai-crew",
            "site": "StartupAI-site/startupai.site",
        },
        "critical_paths": {
            "planning-surface": {
                "goal": "Founder reviews and approves experiment plan",
                "first_value_at": "app#149",
                "edges": [
                    ["app#149", "crew#84"],
                    ["crew#84", "crew#85"],
                    ["crew#85", "crew#87"],
                    ["crew#87", "crew#88"],
                    ["crew#88", "app#153"],
                    ["app#153", "app#154"],
                ],
            }
        },
    }


def _write_config(tmp_path: Path, payload: dict | None = None) -> Path:
    config_path = tmp_path / "critical-paths.json"
    data = payload if payload is not None else _valid_payload()
    config_path.write_text(json.dumps(data), encoding="utf-8")
    return config_path


def _load(tmp_path: Path) -> CriticalPathConfig:
    return load_config(_write_config(tmp_path))


# -- Marker & comment tests ---------------------------------------------------


def test_marker_uniqueness() -> None:
    """Different refs produce different markers."""
    m1 = _marker_for("promote-bridge", "crew#88")
    m2 = _marker_for("promote-bridge", "crew#89")
    assert m1 != m2
    assert "crew#88" in m1
    assert "crew#89" in m2


def test_comment_exists_true() -> None:
    """Response containing marker returns True."""
    marker = _marker_for("promote-bridge", "crew#88")

    def fake_gh(args):
        return f"some text\n{marker}\nmore text"

    assert _comment_exists(
        "StartupAI-site", "startupai-crew", 88, marker, gh_runner=fake_gh
    )


def test_comment_exists_false() -> None:
    """Empty response returns False."""
    marker = _marker_for("promote-bridge", "crew#88")

    def fake_gh(args):
        return ""

    assert not _comment_exists(
        "StartupAI-site", "startupai-crew", 88, marker, gh_runner=fake_gh
    )


# -- Repo prefix tests --------------------------------------------------------


def test_repo_to_prefix(tmp_path: Path) -> None:
    """Reverse lookup works for both app and crew."""
    config = _load(tmp_path)
    assert _repo_to_prefix("StartupAI-site/startupai-crew", config) == "crew"
    assert _repo_to_prefix("StartupAI-site/app.startupai-site", config) == "app"
    assert _repo_to_prefix("StartupAI-site/startupai.site", config) == "site"
    assert _repo_to_prefix("StartupAI-site/unknown", config) is None


# -- query_closing_issues tests ------------------------------------------------


def test_query_closing_issues_parses_response(tmp_path: Path) -> None:
    """Injected GraphQL response -> correct LinkedIssue list."""
    config = _load(tmp_path)
    graphql_response = json.dumps(
        {
            "data": {
                "repository": {
                    "pullRequest": {
                        "closingIssuesReferences": {
                            "nodes": [
                                {
                                    "number": 88,
                                    "repository": {
                                        "nameWithOwner": "StartupAI-site/startupai-crew",
                                    },
                                }
                            ]
                        }
                    }
                }
            }
        }
    )

    def fake_gh(args):
        return graphql_response

    issues = query_closing_issues(
        "StartupAI-site", "startupai-crew", 97, config, gh_runner=fake_gh
    )
    assert len(issues) == 1
    assert issues[0].ref == "crew#88"
    assert issues[0].number == 88


# -- Check-run query tests -----------------------------------------------------


def test_query_failed_check_runs_parses_response() -> None:
    """_query_failed_check_runs returns names of failed checks."""
    response = json.dumps(
        {
            "check_runs": [
                {"name": "Tests", "conclusion": "failure"},
                {"name": "Lint", "conclusion": "success"},
                {"name": "Build", "conclusion": "failure"},
            ]
        }
    )
    result = _query_failed_check_runs(
        "owner", "repo", "sha123", gh_runner=lambda args: response
    )
    assert result == ["Tests", "Build"]


def test_query_failed_check_runs_returns_none_on_api_error() -> None:
    """_query_failed_check_runs returns None when API fails."""
    from startupai_controller.validate_critical_path_promotion import GhQueryError as _GhQueryError

    def failing_gh(args):
        raise _GhQueryError("rate limited")

    result = _query_failed_check_runs(
        "owner", "repo", "sha123", gh_runner=failing_gh
    )
    assert result is None


def test_query_pr_gate_status_normalizes_state_to_uppercase() -> None:
    """PR gate status should normalize GitHub's lowercase state values."""
    pr_payload = json.dumps(
        {
            "state": "open",
            "isDraft": False,
            "mergeStateStatus": "CLEAN",
            "mergeable": "MERGEABLE",
            "baseRefName": "main",
            "statusCheckRollup": [],
            "autoMergeRequest": None,
        }
    )
    branch_payload = json.dumps({"contexts": [], "checks": []})

    def fake_gh(args):
        if args[:2] == ["pr", "view"]:
            return pr_payload
        if args[:2] == ["api", "repos/owner/repo/branches/main/protection/required_status_checks"]:
            return branch_payload
        raise AssertionError(args)

    result = board_io._query_pr_gate_status(
        "owner/repo",
        123,
        gh_runner=fake_gh,
    )

    assert result.state == "OPEN"


def test_query_pull_request_view_payloads_batches_graphql_aliases() -> None:
    """Multi-PR payload queries should parse aliased GraphQL responses."""
    graphql_payload = json.dumps(
        {
            "data": {
                "repository": {
                    "pr_210": {
                        "number": 210,
                        "state": "OPEN",
                        "isDraft": False,
                        "mergeStateStatus": "CLEAN",
                        "mergeable": "MERGEABLE",
                        "baseRefName": "main",
                        "autoMergeRequest": None,
                        "body": "Closes #84",
                        "author": {"login": "codex-bot"},
                        "reviews": {
                            "nodes": [
                                {
                                    "body": "codex-review: pass",
                                    "submittedAt": "2026-03-09T12:00:00Z",
                                    "state": "COMMENTED",
                                    "author": {"login": "codex-bot"},
                                }
                            ]
                        },
                        "comments": {
                            "nodes": [
                                {
                                    "body": "first comment",
                                    "createdAt": "2026-03-09T11:00:00Z",
                                    "author": {"login": "maintainer"},
                                }
                            ]
                        },
                        "commits": {
                            "nodes": [
                                {
                                    "commit": {
                                        "statusCheckRollup": {
                                            "contexts": {
                                                "nodes": [
                                                    {
                                                        "__typename": "CheckRun",
                                                        "name": "ci",
                                                        "status": "COMPLETED",
                                                        "conclusion": "SUCCESS",
                                                        "detailsUrl": "https://github.com/run/1",
                                                        "completedAt": "2026-03-09T12:05:00Z",
                                                        "startedAt": "2026-03-09T12:04:00Z",
                                                        "workflowName": "CI",
                                                    },
                                                    {
                                                        "__typename": "StatusContext",
                                                        "context": "lint",
                                                        "state": "SUCCESS",
                                                        "targetUrl": "https://github.com/status/1",
                                                        "createdAt": "2026-03-09T12:03:00Z",
                                                    },
                                                ]
                                            }
                                        }
                                    }
                                }
                            ]
                        },
                    },
                    "pr_211": {
                        "number": 211,
                        "state": "OPEN",
                        "isDraft": True,
                        "mergeStateStatus": "BLOCKED",
                        "mergeable": "CONFLICTING",
                        "baseRefName": "release",
                        "autoMergeRequest": {"enabledAt": "2026-03-09T12:06:00Z"},
                        "body": "Closes #85",
                        "author": {"login": "codex-bot"},
                        "reviews": {"nodes": []},
                        "comments": {"nodes": []},
                        "commits": {"nodes": []},
                    },
                }
            }
        }
    )

    def fake_gh(args):
        assert args[:2] == ["api", "graphql"]
        joined = "\n".join(args)
        assert "pr_210: pullRequest(number: 210)" in joined
        assert "pr_211: pullRequest(number: 211)" in joined
        return graphql_payload

    payloads = board_io.query_pull_request_view_payloads(
        "StartupAI-site/startupai-crew",
        (211, 210),
        gh_runner=fake_gh,
    )

    assert sorted(payloads) == [210, 211]
    first = payloads[210]
    assert first.author == "codex-bot"
    assert first.body == "Closes #84"
    assert first.base_ref_name == "main"
    assert first.auto_merge_enabled is False
    assert len(first.comments) == 1
    assert len(first.reviews) == 1
    assert len(first.status_check_rollup) == 2
    assert first.status_check_rollup[0]["__typename"] == "CheckRun"
    assert first.status_check_rollup[1]["__typename"] == "StatusContext"
    assert first.status_check_rollup[1]["startedAt"] == "2026-03-09T12:03:00Z"

    second = payloads[211]
    assert second.is_draft is True
    assert second.merge_state_status == "BLOCKED"
    assert second.mergeable == "CONFLICTING"
    assert second.base_ref_name == "release"
    assert second.auto_merge_enabled is True


def test_run_gh_retries_transient_connection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unsupported commands still use subprocess retry behavior."""
    calls = {"count": 0}

    def fake_check_output(args, text=True, stderr=None, timeout=None):
        calls["count"] += 1
        if calls["count"] == 1:
            raise subprocess.CalledProcessError(
                returncode=1,
                cmd=args,
                output="error connecting to api.github.com",
            )
        return '{"ok": true}'

    monkeypatch.setattr(board_io.subprocess, "check_output", fake_check_output)
    monkeypatch.setattr(board_io.time, "sleep", lambda *_: None)

    result = board_io._run_gh(["unsupported", "command"])

    assert result == '{"ok": true}'
    assert calls["count"] == 2


def test_run_gh_fails_fast_on_timeout_expired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unsupported subprocess fallbacks still fail fast on timeout."""
    calls = {"count": 0, "timeout": None}

    def fake_check_output(args, text=True, stderr=None, timeout=None):
        calls["count"] += 1
        calls["timeout"] = timeout
        raise subprocess.TimeoutExpired(cmd=args, timeout=timeout)

    monkeypatch.setattr(board_io.subprocess, "check_output", fake_check_output)
    monkeypatch.setattr(board_io.time, "sleep", lambda *_: None)

    with pytest.raises(board_io.GhCommandError, match="timed out after"):
        board_io._run_gh(["unsupported", "command"])

    assert calls["count"] == 1
    assert calls["timeout"] == board_io._GH_COMMAND_TIMEOUT_SECONDS


def test_query_required_status_checks_uses_ttl_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Required checks are cached per repo/base branch within the process TTL."""
    clear_required_status_checks_cache()
    calls = {"count": 0}

    def fake_run_gh(args, **kwargs):
        calls["count"] += 1
        return json.dumps({"contexts": ["ci"], "checks": [{"context": "gate"}]})

    monkeypatch.setattr(board_io, "_run_gh", fake_run_gh)

    first = query_required_status_checks("StartupAI-site/startupai-crew", "main")
    second = query_required_status_checks("StartupAI-site/startupai-crew", "main")

    assert first == {"ci", "gate"}
    assert second == {"ci", "gate"}
    assert calls["count"] == 1


def test_query_required_status_checks_returns_stale_cache_on_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cached required-check policy is reused if a refresh fails."""
    clear_required_status_checks_cache()
    monkeypatch.setattr(
        board_io,
        "_run_gh",
        lambda *args, **kwargs: json.dumps({"contexts": ["ci"]}),
    )
    first = query_required_status_checks("StartupAI-site/startupai-crew", "main")

    monkeypatch.setattr(
        board_io,
        "_run_gh",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            board_io.GhCommandError(
                operation_type="query",
                failure_kind="rate_limit",
                command_excerpt="api repos/x/y",
                detail="API rate limit exceeded",
            )
        ),
    )
    board_io._required_status_checks_ttl_cache[
        ("StartupAI-site/startupai-crew", "main")
    ] = (0.0, {"ci"})

    second = query_required_status_checks("StartupAI-site/startupai-crew", "main")

    assert first == {"ci"}
    assert second == {"ci"}


def test_build_cycle_board_snapshot_uses_short_ttl_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Back-to-back snapshot reads reuse the same thin board payload."""
    clear_cycle_board_snapshot_cache()
    calls = {"count": 0}

    payload = {
        "data": {
            "organization": {
                "projectV2": {
                    "id": "PVT_x",
                    "items": {
                        "pageInfo": {"hasNextPage": False, "endCursor": ""},
                        "nodes": [
                            {
                                "id": "PVTI_x",
                                "statusField": {"name": "Ready"},
                                "executorField": {"name": "codex"},
                                "handoffField": {"name": "none"},
                                "priorityField": {"name": "P1"},
                                "sprintField": {"name": "S1"},
                                "agentField": {"name": "frontend-dev"},
                                "ownerField": {"text": "codex:local-consumer"},
                                "content": {
                                    "number": 84,
                                    "title": "Test issue",
                                    "updatedAt": "2026-03-09T10:00:00Z",
                                    "repository": {
                                        "name": "startupai-crew",
                                        "nameWithOwner": "StartupAI-site/startupai-crew",
                                        "owner": {"login": "StartupAI-site"},
                                    },
                                },
                            }
                        ],
                    },
                }
            }
        }
    }

    def fake_run_gh(args, **kwargs):
        calls["count"] += 1
        return json.dumps(payload)

    monkeypatch.setattr(board_io, "_run_gh", fake_run_gh)

    first = build_cycle_board_snapshot("StartupAI-site", 1)
    second = build_cycle_board_snapshot("StartupAI-site", 1)

    assert len(first.items) == 1
    assert len(second.items) == 1
    assert calls["count"] == 1

    clear_cycle_board_snapshot_cache()
    third = build_cycle_board_snapshot("StartupAI-site", 1)
    assert len(third.items) == 1
    assert calls["count"] == 2


# -- Codex verdict tests -------------------------------------------------------


def test_query_latest_codex_verdict_prefers_latest_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Latest verdict marker (comment/review) wins by timestamp."""
    payload = {
        "comments": [
            {
                "body": "codex-review: fail\ncodex-route: executor",
                "createdAt": "2026-03-05T10:00:00Z",
                "author": {"login": "codex-bot"},
            },
            {
                "body": "codex-review: pass",
                "createdAt": "2026-03-05T11:00:00Z",
                "author": {"login": "codex-bot"},
            },
        ],
        "reviews": [],
    }
    monkeypatch.setattr(board_io, "_run_gh", lambda *a, **k: json.dumps(payload))
    verdict = query_latest_codex_verdict(
        "StartupAI-site/app.startupai-site",
        158,
        trusted_actors={"codex-bot"},
    )
    assert verdict is not None
    assert verdict.decision == "pass"
    assert verdict.actor == "codex-bot"


# -- enable_pull_request_automerge (M3: bounded verification) ----------------


from startupai_controller.board_io import enable_pull_request_automerge, GhCommandError


def test_enable_automerge_confirmed_after_retries() -> None:
    """Verification reads succeed on second attempt → "confirmed"."""
    call_count = {"enable": 0, "view": 0}

    def fake_gh(args):
        if "merge" in args and "--auto" in args:
            call_count["enable"] += 1
            return ""
        if "view" in args and "--json" in args:
            call_count["view"] += 1
            if call_count["view"] >= 2:
                return json.dumps({"autoMergeRequest": {"enabledAt": "2026-03-09"}})
            return json.dumps({"autoMergeRequest": None})
        return ""

    result = enable_pull_request_automerge(
        "o/r", 42, confirm_retries=3, confirm_delay_seconds=0, gh_runner=fake_gh,
    )
    assert result == "confirmed"
    assert call_count["enable"] == 1
    assert call_count["view"] >= 2


def test_enable_automerge_pending_when_never_confirmed() -> None:
    """All verify reads return null → "pending"."""
    def fake_gh(args):
        if "merge" in args and "--auto" in args:
            return ""
        if "view" in args:
            return json.dumps({"autoMergeRequest": None})
        return ""

    result = enable_pull_request_automerge(
        "o/r", 42, confirm_retries=3, confirm_delay_seconds=0, gh_runner=fake_gh,
    )
    assert result == "pending"


def test_enable_automerge_propagates_gh_error_on_enable() -> None:
    """Enable call raises GhQueryError → exception propagates (NOT caught)."""
    from startupai_controller.validate_critical_path_promotion import GhQueryError

    def fake_gh(args):
        if "merge" in args and "--auto" in args:
            raise GhQueryError("transport error")
        return ""

    with pytest.raises(GhQueryError, match="transport error"):
        enable_pull_request_automerge(
            "o/r", 42, confirm_retries=3, confirm_delay_seconds=0, gh_runner=fake_gh,
        )
