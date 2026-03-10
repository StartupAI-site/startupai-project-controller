from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

from startupai_controller.adapters.review_state import (
    GitHubReviewStateAdapter,
    _query_latest_non_automation_comment_timestamp,
    build_cycle_board_snapshot,
    clear_cycle_board_snapshot_cache,
)
from startupai_controller.runtime.wiring import build_github_port_bundle


def _config() -> SimpleNamespace:
    return SimpleNamespace(issue_prefixes={"crew": "StartupAI-site/startupai-crew"})


def test_runtime_wiring_uses_dedicated_review_state_adapter() -> None:
    bundle = build_github_port_bundle(
        "StartupAI-site",
        1,
        config=_config(),
    )

    assert isinstance(bundle.review_state, GitHubReviewStateAdapter)


def test_search_open_issue_numbers_with_comment_marker(monkeypatch) -> None:
    recorded: list[list[str]] = []

    def fake_run_gh(args, gh_runner=None, operation_type="query"):
        recorded.append(args)
        return json.dumps({"items": [{"number": 84}, {"number": 85}]})

    monkeypatch.setattr("startupai_controller.adapters.github_base._run_gh", fake_run_gh)
    adapter = GitHubReviewStateAdapter(project_owner="StartupAI-site", project_number=1)

    numbers = adapter.search_open_issue_numbers_with_comment_marker(
        "StartupAI-site/startupai-crew",
        "startupai-board-bot:handoff:job=",
    )

    assert numbers == (84, 85)
    assert recorded[0][0:4] == ["api", "search/issues", "-X", "GET"]


def test_list_issue_comment_bodies_reads_bodies_from_issue_comments(monkeypatch) -> None:
    monkeypatch.setattr(
        "startupai_controller.adapters.github_base._run_gh",
        lambda args, gh_runner=None, operation_type="query": json.dumps(
            [
                {"body": "first"},
                {"body": "second"},
                {"body": ""},
            ]
        ),
    )
    adapter = GitHubReviewStateAdapter(project_owner="StartupAI-site", project_number=1)

    comments = adapter.list_issue_comment_bodies(
        "StartupAI-site/startupai-crew",
        84,
    )

    assert comments == ("first", "second", "")


def test_latest_matching_comment_timestamp_delegates_to_query_helper(monkeypatch) -> None:
    expected = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)
    recorded: list[tuple[str, str, int, tuple[str, ...]]] = []

    def fake_latest_matching(owner, repo, number, markers, gh_runner=None):
        recorded.append((owner, repo, number, markers))
        return expected

    monkeypatch.setattr(
        "startupai_controller.adapters.review_state._query_latest_matching_comment_timestamp",
        fake_latest_matching,
    )
    adapter = GitHubReviewStateAdapter(project_owner="StartupAI-site", project_number=1)

    result = adapter.latest_matching_comment_timestamp(
        "StartupAI-site/startupai-crew",
        84,
        ("marker-a", "marker-b"),
    )

    assert result == expected
    assert recorded == [
        (
            "StartupAI-site",
            "startupai-crew",
            84,
            ("marker-a", "marker-b"),
        )
    ]


def test_build_cycle_board_snapshot_uses_adapter_owned_cache(monkeypatch) -> None:
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

    def fake_run_gh(args, *, gh_runner=None, operation_type="query"):
        calls["count"] += 1
        return json.dumps(payload)

    monkeypatch.setattr("startupai_controller.adapters.github_base._run_gh", fake_run_gh)

    first = build_cycle_board_snapshot("StartupAI-site", 1)
    second = build_cycle_board_snapshot("StartupAI-site", 1)

    assert len(first.items) == 1
    assert len(second.items) == 1
    assert calls["count"] == 1

    clear_cycle_board_snapshot_cache()
    third = build_cycle_board_snapshot("StartupAI-site", 1)
    assert len(third.items) == 1
    assert calls["count"] == 2


def test_query_latest_non_automation_comment_timestamp_filters_markers_and_bots(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "startupai_controller.adapters.review_state._query_issue_comments",
        lambda owner, repo, number, gh_runner=None: [
            {
                "body": "<!-- startupai-board-bot:marker -->",
                "updated_at": "2026-03-10T10:00:00Z",
                "user": {"login": "codex-bot"},
            },
            {
                "body": "Human note",
                "updated_at": "2026-03-10T11:00:00Z",
                "user": {"login": "chris"},
            },
        ],
    )

    latest = _query_latest_non_automation_comment_timestamp(
        "StartupAI-site",
        "startupai-crew",
        42,
    )

    assert latest == datetime(2026, 3, 10, 11, 0, tzinfo=timezone.utc)
