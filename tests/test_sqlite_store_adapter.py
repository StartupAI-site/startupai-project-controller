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

    def get_session(self, session_id):
        self.calls.append(("get_session", session_id))
        return {"id": session_id}

    def reschedule_review_queue_item(self, issue_ref, *, next_attempt_at, now=None):
        self.calls.append(
            (
                "reschedule_review_queue_item",
                {
                    "issue_ref": issue_ref,
                    "next_attempt_at": next_attempt_at,
                    "now": now,
                },
            )
        )


def test_list_due_review_items_uses_keyword_only_now() -> None:
    db = _FakeDB()
    adapter = SqliteSessionStore(db)
    now = datetime(2026, 3, 9, tzinfo=timezone.utc)

    result = adapter.list_due_review_items(now)

    assert result == ["due"]
    assert db.calls == [("list_due_review_queue_items", {"now": now, "limit": None})]


def test_enqueue_review_item_passes_keyword_args() -> None:
    db = _FakeDB()
    adapter = SqliteSessionStore(db)
    now = datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc)
    next_attempt = "2026-03-09T12:30:00+00:00"

    adapter.enqueue_review_item(
        "crew#42",
        "https://github.com/StartupAI-site/startupai-crew/pull/42",
        "StartupAI-site/startupai-crew",
        42,
        "sess-42",
        next_attempt,
        now=now,
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
                "next_attempt_at": next_attempt,
                "now": now,
            },
        )
    ]


def test_get_session_delegates_to_underlying_db() -> None:
    db = _FakeDB()
    adapter = SqliteSessionStore(db)

    result = adapter.get_session("sess-99")

    assert result == {"id": "sess-99"}
    assert db.calls == [("get_session", "sess-99")]


def test_reschedule_review_queue_item_passes_keyword_args() -> None:
    db = _FakeDB()
    adapter = SqliteSessionStore(db)
    now = datetime(2026, 3, 9, 15, 0, tzinfo=timezone.utc)

    adapter.reschedule_review_queue_item(
        "crew#42",
        next_attempt_at="2026-03-09T15:10:00+00:00",
        now=now,
    )

    assert db.calls == [
        (
            "reschedule_review_queue_item",
            {
                "issue_ref": "crew#42",
                "next_attempt_at": "2026-03-09T15:10:00+00:00",
                "now": now,
            },
        )
    ]
