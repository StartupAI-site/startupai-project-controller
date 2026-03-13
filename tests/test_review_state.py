"""Unit tests for review-state adapter helpers."""

from __future__ import annotations

import json

from startupai_controller.adapters.review_state import (
    _comment_exists,
    build_cycle_board_snapshot,
    clear_cycle_board_snapshot_cache,
)


def test_comment_exists_true() -> None:
    """Response containing marker returns True."""
    marker = "<!-- startupai-board-bot:promote-bridge:crew#88 -->"

    def fake_gh(args):
        return json.dumps([{"body": f"some text\n{marker}\nmore text"}])

    assert _comment_exists(
        "StartupAI-site",
        "startupai-crew",
        88,
        marker,
        gh_runner=fake_gh,
    )


def test_comment_exists_false() -> None:
    """Empty response returns False."""
    marker = "<!-- startupai-board-bot:promote-bridge:crew#88 -->"

    def fake_gh(args):
        return "[]"

    assert not _comment_exists(
        "StartupAI-site",
        "startupai-crew",
        88,
        marker,
        gh_runner=fake_gh,
    )


def test_build_cycle_board_snapshot_uses_short_ttl_cache() -> None:
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

    def fake_gh(args, **kwargs):
        calls["count"] += 1
        return json.dumps(payload)

    first = build_cycle_board_snapshot("StartupAI-site", 1, gh_runner=fake_gh)
    second = build_cycle_board_snapshot("StartupAI-site", 1, gh_runner=fake_gh)

    assert len(first.items) == 1
    assert len(second.items) == 1
    assert calls["count"] == 1

    clear_cycle_board_snapshot_cache()
    third = build_cycle_board_snapshot("StartupAI-site", 1, gh_runner=fake_gh)
    assert len(third.items) == 1
    assert calls["count"] == 2
