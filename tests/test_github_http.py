"""Contract tests for direct GitHub HTTP transport."""

from __future__ import annotations

import ast
import json
import socket
from pathlib import Path
from urllib import error as urllib_error

import pytest

import startupai_controller.adapters.github_http_transport as github_http


class _FakeResponse:
    def __init__(
        self, payload: object, *, headers: dict[str, str] | None = None
    ) -> None:
        if isinstance(payload, str):
            raw = payload
        else:
            raw = json.dumps(payload)
        self._raw = raw.encode("utf-8")
        self.headers = headers or {}

    def read(self) -> bytes:
        return self._raw

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_run_github_command_retries_transient_network_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")
    monkeypatch.setattr(github_http.time, "sleep", lambda *_: None)
    calls = {"count": 0}

    def fake_urlopen(request, timeout=None):
        calls["count"] += 1
        if calls["count"] == 1:
            raise urllib_error.URLError(OSError("error connecting to api.github.com"))
        assert request.full_url == github_http.GRAPHQL_URL
        assert timeout == 8.0
        return _FakeResponse({"ok": True})

    monkeypatch.setattr(github_http.urllib_request, "urlopen", fake_urlopen)

    result = github_http.run_github_command(
        ["api", "graphql", "-f", "query=query { viewer { login } }"],
        operation_type="query",
        timeout_seconds=8.0,
        retry_delays=(1.0, 2.0, 4.0),
    )

    assert result == '{"ok": true}'
    assert calls["count"] == 2


def test_run_github_command_fails_fast_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")
    monkeypatch.setattr(github_http.time, "sleep", lambda *_: None)
    calls = {"count": 0}

    def fake_urlopen(request, timeout=None):
        calls["count"] += 1
        raise urllib_error.URLError(socket.timeout("timed out"))

    monkeypatch.setattr(github_http.urllib_request, "urlopen", fake_urlopen)

    with pytest.raises(github_http.GitHubTransportError, match="timed out after 8.0s"):
        github_http.run_github_command(
            ["api", "graphql", "-f", "query=query { viewer { login } }"],
            operation_type="query",
            timeout_seconds=8.0,
            retry_delays=(1.0, 2.0, 4.0),
        )

    assert calls["count"] == 1


def test_run_github_command_records_request_stats(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")
    monkeypatch.setattr(github_http.time, "sleep", lambda *_: None)
    calls = {"count": 0}

    def fake_urlopen(request, timeout=None):
        calls["count"] += 1
        if calls["count"] == 1:
            raise urllib_error.URLError(OSError("error connecting to api.github.com"))
        return _FakeResponse({"ok": True})

    monkeypatch.setattr(github_http.urllib_request, "urlopen", fake_urlopen)
    token = github_http.begin_request_stats()
    try:
        result = github_http.run_github_command(
            ["api", "graphql", "-f", "query=query { viewer { login } }"],
            operation_type="query",
            timeout_seconds=8.0,
            retry_delays=(1.0, 2.0, 4.0),
        )
    finally:
        stats = github_http.end_request_stats(token)

    assert result == '{"ok": true}'
    assert stats.graphql == 2
    assert stats.rest == 0
    assert stats.retries == 1
    assert stats.error_counts == {"network": 1}
    assert stats.latency_le_250_ms >= 1


def test_run_github_command_paginated_body_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")

    def fake_urlopen(request, timeout=None):
        if "page=2" in request.full_url:
            return _FakeResponse([{"body": "second"}])
        return _FakeResponse(
            [{"body": "first"}],
            headers={
                "Link": '<https://api.github.com/repos/o/r/issues/1/comments?page=2>; rel="next"'
            },
        )

    monkeypatch.setattr(github_http.urllib_request, "urlopen", fake_urlopen)

    result = github_http.run_github_command(
        [
            "api",
            "repos/o/r/issues/1/comments",
            "--paginate",
            "-q",
            ".[].body",
        ],
        operation_type="query",
        timeout_seconds=8.0,
        retry_delays=(1.0, 2.0, 4.0),
    )

    assert result == "first\nsecond"


def test_run_github_command_single_body_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")

    def fake_urlopen(request, timeout=None):
        assert request.full_url.endswith("/repos/o/r/issues/1")
        return _FakeResponse({"body": "issue body"})

    monkeypatch.setattr(github_http.urllib_request, "urlopen", fake_urlopen)

    result = github_http.run_github_command(
        [
            "api",
            "repos/o/r/issues/1",
            "--jq",
            ".body",
        ],
        operation_type="query",
        timeout_seconds=8.0,
        retry_delays=(1.0, 2.0, 4.0),
    )

    assert result == "issue body"


def test_run_github_command_issue_context_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")

    def fake_urlopen(request, timeout=None):
        assert request.full_url.endswith("/repos/o/r/issues/1")
        return _FakeResponse(
            {
                "title": "Issue title",
                "body": "Issue body",
                "updated_at": "2026-03-09T02:00:00Z",
                "labels": [{"name": "bug"}, {"name": "board"}],
            }
        )

    monkeypatch.setattr(github_http.urllib_request, "urlopen", fake_urlopen)

    result = github_http.run_github_command(
        [
            "api",
            "repos/o/r/issues/1",
            "--jq",
            "{title: .title, body: .body, labels: [.labels[].name], updated_at: .updated_at}",
        ],
        operation_type="query",
        timeout_seconds=8.0,
        retry_delays=(1.0, 2.0, 4.0),
    )

    assert json.loads(result) == {
        "title": "Issue title",
        "body": "Issue body",
        "labels": ["bug", "board"],
        "updated_at": "2026-03-09T02:00:00Z",
    }


def test_supported_output_filters_cover_live_call_surface() -> None:
    root = Path(__file__).resolve().parents[2] / "scripts"
    discovered: set[str] = set()

    for path in root.rglob("*.py"):
        src = path.read_text(encoding="utf-8")
        tree = ast.parse(src, filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            name = None
            if isinstance(fn, ast.Name):
                name = fn.id
            elif isinstance(fn, ast.Attribute):
                name = fn.attr
            if name not in {"_run_gh", "run_github_command"}:
                continue
            if not node.args or not isinstance(node.args[0], (ast.List, ast.Tuple)):
                continue
            items: list[str] = []
            for elt in node.args[0].elts:
                if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                    items.append(elt.value)
                else:
                    items.append("<expr>")
            for flag in ("--jq", "-q"):
                if flag in items:
                    idx = items.index(flag)
                    if idx + 1 < len(items) and items[idx + 1] != "<expr>":
                        discovered.add(items[idx + 1])

    assert discovered <= github_http.SUPPORTED_OUTPUT_FILTERS


def test_pr_view_returns_normalized_comments_and_reviews(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")

    def fake_urlopen(request, timeout=None):
        if request.full_url.endswith("/repos/owner/repo/pulls/42"):
            return _FakeResponse({"number": 42, "html_url": "https://example/pr/42"})
        if "/issues/42/comments" in request.full_url:
            return _FakeResponse(
                [
                    {
                        "body": "comment body",
                        "created_at": "2026-03-08T20:00:00Z",
                        "user": {"login": "codex"},
                    }
                ]
            )
        if "/pulls/42/reviews" in request.full_url:
            return _FakeResponse(
                [
                    {
                        "body": "review body",
                        "submitted_at": "2026-03-08T20:05:00Z",
                        "state": "COMMENTED",
                        "user": {"login": "reviewer"},
                    }
                ]
            )
        raise AssertionError(f"unexpected url: {request.full_url}")

    monkeypatch.setattr(github_http.urllib_request, "urlopen", fake_urlopen)

    result = github_http.run_github_command(
        ["pr", "view", "42", "--repo", "owner/repo", "--json", "comments,reviews"],
        operation_type="query",
        timeout_seconds=8.0,
        retry_delays=(1.0, 2.0, 4.0),
    )

    payload = json.loads(result or "{}")
    assert payload["comments"][0]["author"]["login"] == "codex"
    assert payload["comments"][0]["createdAt"] == "2026-03-08T20:00:00Z"
    assert payload["reviews"][0]["author"]["login"] == "reviewer"
    assert payload["reviews"][0]["submittedAt"] == "2026-03-08T20:05:00Z"


def test_pr_create_posts_directly_to_rest_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")
    captured: dict[str, object] = {}

    def fake_urlopen(request, timeout=None):
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        captured["body"] = json.loads((request.data or b"{}").decode("utf-8"))
        return _FakeResponse({"html_url": "https://example/pr/123"})

    monkeypatch.setattr(github_http.urllib_request, "urlopen", fake_urlopen)

    result = github_http.run_github_command(
        [
            "pr",
            "create",
            "--repo",
            "owner/repo",
            "--head",
            "feat/test-branch",
            "--title",
            "Test PR",
            "--body",
            "Body text",
        ],
        operation_type="mutation",
        timeout_seconds=8.0,
        retry_delays=(1.0, 2.0, 4.0),
    )

    assert result == "https://example/pr/123"
    assert captured["url"].endswith("/repos/owner/repo/pulls")
    assert captured["method"] == "POST"
    assert captured["body"] == {
        "head": "feat/test-branch",
        "base": "main",
        "title": "Test PR",
        "body": "Body text",
    }
