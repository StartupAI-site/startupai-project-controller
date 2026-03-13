"""Unit tests for GitHub CLI compatibility helpers and adapter entrypoints."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import startupai_controller.adapters.github_cli as github_cli
import startupai_controller.adapters.pull_request_compat as pull_request_compat
from startupai_controller.adapters.github_cli import (
    GitHubCliAdapter,
    _query_failed_check_runs,
    _query_issue_board_info,
    _query_project_item_field,
    _query_status_field_option,
    _set_board_status,
    clear_required_status_checks_cache,
    enable_pull_request_automerge,
    query_closing_issues,
    query_latest_codex_verdict,
    query_pull_request_view_payloads,
    query_required_status_checks,
)
from startupai_controller.adapters.github_types import PullRequestViewPayload
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
    load_config,
)


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


def test_query_closing_issues_parses_response(tmp_path: Path) -> None:
    """Injected GraphQL response yields the expected linked issue list."""
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
        "StartupAI-site",
        "startupai-crew",
        97,
        config,
        gh_runner=fake_gh,
    )

    assert len(issues) == 1
    assert issues[0].ref == "crew#88"
    assert issues[0].number == 88


def test_query_failed_check_runs_parses_response() -> None:
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
        "owner",
        "repo",
        "sha123",
        gh_runner=lambda args: response,
    )

    assert result == ["Tests", "Build"]


def test_query_failed_check_runs_returns_none_on_api_error() -> None:
    def failing_gh(args):
        raise GhQueryError("rate limited")

    result = _query_failed_check_runs(
        "owner",
        "repo",
        "sha123",
        gh_runner=failing_gh,
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
        if args[:2] == [
            "api",
            "repos/owner/repo/branches/main/protection/required_status_checks",
        ]:
            return branch_payload
        raise AssertionError(args)

    result = GitHubCliAdapter(
        project_owner="",
        project_number=0,
        gh_runner=fake_gh,
    ).get_gate_status(
        "owner/repo",
        123,
    )

    assert result.state == "OPEN"


def test_query_project_item_field_uses_github_cli_runner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _load(tmp_path)

    def fake_gh(args, gh_runner=None, operation_type="query"):
        return json.dumps(
            {
                "data": {
                    "repository": {
                        "issue": {
                            "projectItems": {
                                "nodes": [
                                    {
                                        "project": {
                                            "owner": {"login": "StartupAI-site"},
                                            "number": 1,
                                        },
                                        "fieldByName": {"name": "codex"},
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        )

    monkeypatch.setattr(github_cli, "_run_gh", fake_gh)

    value = _query_project_item_field(
        "crew#84",
        "Executor",
        config,
        "StartupAI-site",
        1,
    )

    assert value == "codex"


def test_query_issue_board_info_uses_github_cli_runner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _load(tmp_path)

    def fake_gh(args, gh_runner=None, operation_type="query"):
        return json.dumps(
            {
                "data": {
                    "repository": {
                        "issue": {
                            "projectItems": {
                                "nodes": [
                                    {
                                        "id": "ITEM1",
                                        "project": {
                                            "id": "PROJ1",
                                            "owner": {"login": "StartupAI-site"},
                                            "number": 1,
                                        },
                                        "statusField": {"name": "Ready"},
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        )

    monkeypatch.setattr(github_cli, "_run_gh", fake_gh)

    info = _query_issue_board_info(
        "crew#84",
        config,
        "StartupAI-site",
        1,
    )

    assert info.status == "Ready"
    assert info.item_id == "ITEM1"
    assert info.project_id == "PROJ1"


def test_query_status_field_option_uses_github_cli_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_gh(args, gh_runner=None, operation_type="query"):
        return json.dumps(
            {
                "data": {
                    "node": {
                        "field": {
                            "id": "FIELD1",
                            "options": [
                                {"id": "OPT1", "name": "Review"},
                            ],
                        }
                    }
                }
            }
        )

    monkeypatch.setattr(github_cli, "_run_gh", fake_gh)

    field_id, option_id = _query_status_field_option("PROJ1", "Review")

    assert field_id == "FIELD1"
    assert option_id == "OPT1"


def test_set_board_status_uses_github_cli_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_gh(args, gh_runner=None, operation_type="query"):
        calls.append(list(args))
        return json.dumps(
            {
                "data": {
                    "updateProjectV2ItemFieldValue": {"projectV2Item": {"id": "ITEM1"}}
                }
            }
        )

    monkeypatch.setattr(github_cli, "_run_gh", fake_gh)

    _set_board_status("PROJ1", "ITEM1", "FIELD1", "OPT1")

    assert calls
    assert "updateProjectV2ItemFieldValue" in " ".join(calls[0])


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

    payloads = query_pull_request_view_payloads(
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


def test_query_required_status_checks_uses_ttl_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Required checks are cached per repo/base branch within the process TTL."""
    clear_required_status_checks_cache()
    calls = {"count": 0}

    class _Adapter:
        def _query_required_status_checks(self, pr_repo, base_ref_name):
            calls["count"] += 1
            return {"ci", "gate"}

    monkeypatch.setattr(
        pull_request_compat,
        "_build_pull_request_adapter",
        lambda **kwargs: _Adapter(),
    )

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

    class _SuccessAdapter:
        def _query_required_status_checks(self, pr_repo, base_ref_name):
            return {"ci"}

    class _FailingAdapter:
        def _query_required_status_checks(self, pr_repo, base_ref_name):
            raise GhQueryError("API rate limit exceeded")

    monkeypatch.setattr(
        pull_request_compat,
        "_build_pull_request_adapter",
        lambda **kwargs: _SuccessAdapter(),
    )
    first = query_required_status_checks("StartupAI-site/startupai-crew", "main")

    monkeypatch.setattr(
        pull_request_compat,
        "_build_pull_request_adapter",
        lambda **kwargs: _FailingAdapter(),
    )
    pull_request_compat._required_status_checks_ttl_cache[
        ("StartupAI-site/startupai-crew", "main")
    ] = (0.0, {"ci"})

    second = query_required_status_checks("StartupAI-site/startupai-crew", "main")

    assert first == {"ci"}
    assert second == {"ci"}


def test_query_latest_codex_verdict_prefers_latest_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Latest verdict marker (comment/review) wins by timestamp."""
    payload = PullRequestViewPayload(
        pr_repo="StartupAI-site/app.startupai-site",
        pr_number=158,
        url="https://github.com/StartupAI-site/app.startupai-site/pull/158",
        head_ref_name="feat/test",
        author="codex-bot",
        body="",
        state="OPEN",
        is_draft=False,
        merge_state_status="CLEAN",
        mergeable="MERGEABLE",
        base_ref_name="main",
        merged_at="",
        auto_merge_enabled=False,
        comments=[
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
        reviews=[],
        status_check_rollup=(),
    )
    monkeypatch.setattr(
        pull_request_compat,
        "query_pull_request_view_payload",
        lambda *args, **kwargs: payload,
    )

    verdict = query_latest_codex_verdict(
        "StartupAI-site/app.startupai-site",
        158,
        trusted_actors={"codex-bot"},
    )

    assert verdict is not None
    assert verdict.decision == "pass"
    assert verdict.actor == "codex-bot"


def test_enable_automerge_confirmed_after_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verification reads succeed on second attempt -> confirmed."""
    call_count = {"enable": 0, "view": 0}

    class _Adapter:
        def _gh_json(self, args, *, error_message):
            call_count["view"] += 1
            if call_count["view"] >= 2:
                return {"autoMergeRequest": {"enabledAt": "2026-03-09"}}
            return {"autoMergeRequest": None}

    def fake_run_gh(args, **kwargs):
        if "merge" in args and "--auto" in args:
            call_count["enable"] += 1
        return ""

    monkeypatch.setattr(pull_request_compat, "_run_gh", fake_run_gh)
    monkeypatch.setattr(
        pull_request_compat,
        "_build_pull_request_adapter",
        lambda **kwargs: _Adapter(),
    )
    monkeypatch.setattr(pull_request_compat.time, "sleep", lambda *_: None)

    result = enable_pull_request_automerge(
        "o/r",
        42,
        confirm_retries=3,
        confirm_delay_seconds=0,
    )

    assert result == "confirmed"
    assert call_count["enable"] == 1
    assert call_count["view"] == 2


def test_enable_automerge_pending_when_never_confirmed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """All verify reads return null -> pending."""

    class _Adapter:
        def _gh_json(self, args, *, error_message):
            return {"autoMergeRequest": None}

    monkeypatch.setattr(pull_request_compat, "_run_gh", lambda *args, **kwargs: "")
    monkeypatch.setattr(
        pull_request_compat,
        "_build_pull_request_adapter",
        lambda **kwargs: _Adapter(),
    )
    monkeypatch.setattr(pull_request_compat.time, "sleep", lambda *_: None)

    result = enable_pull_request_automerge(
        "o/r",
        42,
        confirm_retries=3,
        confirm_delay_seconds=0,
    )

    assert result == "pending"


def test_enable_automerge_propagates_gh_error_on_enable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Enable call raises GhQueryError -> exception propagates."""

    def fake_run_gh(args, **kwargs):
        raise GhQueryError("transport error")

    monkeypatch.setattr(pull_request_compat, "_run_gh", fake_run_gh)

    with pytest.raises(GhQueryError, match="transport error"):
        enable_pull_request_automerge(
            "o/r",
            42,
            confirm_retries=3,
            confirm_delay_seconds=0,
        )
