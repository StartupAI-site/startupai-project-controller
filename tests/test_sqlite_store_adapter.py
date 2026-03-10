from __future__ import annotations

from datetime import datetime, timezone

from startupai_controller.adapters.sqlite_store import SqliteSessionStore


class _FakeDB:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def list_due_review_queue_items(self, *, now=None, limit=None):
        self.calls.append(("list_due_review_queue_items", {"now": now, "limit": limit}))
        return ["due"]

    def enqueue_review_item(self, **kwargs):
        self.calls.append(("enqueue_review_item", kwargs))


def test_list_due_review_items_uses_keyword_only_now() -> None:
    db = _FakeDB()
    adapter = SqliteSessionStore(db)
    now = datetime(2026, 3, 9, tzinfo=timezone.utc)

    result = adapter.list_due_review_items(now)

    assert result == ["due"]
    assert db.calls == [
        ("list_due_review_queue_items", {"now": now, "limit": None})
    ]


def test_enqueue_review_item_serializes_next_attempt_datetime() -> None:
    db = _FakeDB()
    adapter = SqliteSessionStore(db)
    next_attempt = datetime(2026, 3, 9, 12, 30, tzinfo=timezone.utc)

    adapter.enqueue_review_item(
        "crew#42",
        "https://github.com/StartupAI-site/startupai-crew/pull/42",
        "StartupAI-site/startupai-crew",
        42,
        "sess-42",
        next_attempt,
    )

    assert db.calls == [
        (
            "enqueue_review_item",
            {
                "issue_ref": "crew#42",
                "pr_url": "https://github.com/StartupAI-site/startupai-crew/pull/42",
                "pr_repo": "StartupAI-site/startupai-crew",
                "pr_number": 42,
                "source_session_id": "sess-42",
                "next_attempt_at": next_attempt.isoformat(),
            },
        )
    ]
