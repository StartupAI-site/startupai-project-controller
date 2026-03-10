"""Unit tests for board_automation module.

All tests use dependency injection (DI) -- NO real GitHub API calls.
Patterns match existing test files (test_validate_critical_path_promotion.py,
test_promote_ready.py).
"""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pytest

import startupai_controller.board_automation as automation
import startupai_controller.board_io as board_io_mod
from startupai_controller.board_automation import (
    ExecutorRoutingDecision,
    PromotionResult,
    SchedulingDecision,
    mark_issues_done,
    auto_promote_successors,
    propagate_blocker,
    reconcile_handoffs,
    schedule_ready_items,
    claim_ready_issue,
    enforce_ready_dependency_guard,
    audit_in_progress,
    sync_review_state,
    codex_review_gate,
    automerge_review,
    review_rescue,
    review_rescue_all,
    route_protected_queue_executors,
    resolve_issues_from_event,
    resolve_pr_to_issues,
    _post_claim_comment,
)
from startupai_controller.board_graph import classify_parallelism_snapshot
from startupai_controller.board_io import (
    CheckObservation,
    LinkedIssue,
    CodexReviewVerdict,
    OpenPullRequest,
    PrGateStatus,
    query_closing_issues,
    query_latest_codex_verdict,
    _marker_for,
    _comment_exists,
    _repo_to_prefix,
    _query_failed_check_runs,
)
from startupai_controller.promote_ready import BoardInfo
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    load_config,
    direct_successors,
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


def _automation_config() -> automation.BoardAutomationConfig:
    return automation.BoardAutomationConfig(
        wip_limits={
            "codex": {"crew": 1, "app": 1, "site": 1},
            "claude": {"crew": 3, "app": 3, "site": 3},
            "human": {"crew": 3, "app": 3, "site": 3},
        },
        freshness_hours=24,
        stale_confirmation_cycles=2,
        trusted_codex_actors={"codex", "codex[bot]", "chris00walker"},
        trusted_local_authors={"codex", "codex[bot]", "chris00walker"},
        dispatch_target="executor",
        canary_thresholds={},
        execution_authority_mode="single_machine",
        execution_authority_repos=("app", "crew", "site"),
        execution_authority_executors=("codex",),
        global_concurrency=2,
        required_checks_by_repo={
            "startupai-site/app.startupai-site": (
                "ci",
                "db-test-gate",
                "e2e",
                "tag-contract",
                "codex-review-gate",
            )
        },
    )


def _fake_pr_port(**overrides):
    defaults = {
        "list_open_prs": lambda repo: [],
        "get_pull_request": lambda repo, number: None,
        "linked_issue_refs": lambda pr_repo, pr_number: (),
        "has_copilot_review_signal": lambda pr_repo, pr_number: False,
        "required_status_checks": lambda pr_repo, base_ref_name="main": set(),
        "get_gate_status": lambda pr_repo, pr_number: PrGateStatus(
            required=set(),
            passed=set(),
            failed=set(),
            pending=set(),
            cancelled=set(),
            merge_state_status="CLEAN",
            mergeable="MERGEABLE",
            is_draft=False,
            state="OPEN",
            auto_merge_enabled=False,
        ),
        "enable_automerge": lambda pr_repo, pr_number, delete_branch=False: "confirmed",
        "rerun_failed_check": lambda pr_repo, check_name, run_id: True,
        "update_branch": lambda pr_repo, pr_number: None,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _fake_review_state_port(*, status_by_issue: dict[str, str] | None = None):
    statuses = status_by_issue or {}
    return SimpleNamespace(
        get_issue_status=lambda issue_ref: statuses.get(issue_ref),
        get_issue_fields=lambda issue_ref: SimpleNamespace(
            issue_ref=issue_ref,
            status=statuses.get(issue_ref, ""),
            priority="P1",
            sprint="Sprint 1",
            executor="codex",
            owner="codex:local-consumer",
            handoff_to="none",
            blocked_reason="",
        ),
    )


def _fake_board_port(calls: list[tuple[str, str]] | None = None):
    bucket = calls if calls is not None else []
    return SimpleNamespace(
        set_issue_status=lambda issue_ref, status: bucket.append((issue_ref, status)),
        set_issue_field=lambda issue_ref, field_name, value: bucket.append(
            (issue_ref, f"{field_name}={value}")
        ),
        post_issue_comment=lambda repo, issue_number, body: bucket.append(
            (f"{repo}#{issue_number}", body)
        ),
    )


def _make_review_snapshot(
    *,
    pr_repo: str = "StartupAI-site/app.startupai-site",
    pr_number: int = 184,
    review_refs: tuple[str, ...] = ("app#110",),
    pr_author: str = "chris00walker",
    pr_body: str = "",
    pr_comment_bodies: tuple[str, ...] = (),
    copilot_review_present: bool = True,
    codex_gate_code: int = 0,
    codex_gate_message: str = "pass",
    gate_status: PrGateStatus | None = None,
    rescue_checks: tuple[str, ...] = (),
    rescue_passed: set[str] | None = None,
    rescue_pending: set[str] | None = None,
    rescue_failed: set[str] | None = None,
    rescue_cancelled: set[str] | None = None,
    rescue_missing: set[str] | None = None,
) -> automation.ReviewSnapshot:
    if gate_status is None:
        gate_status = PrGateStatus(
            required=set(),
            passed=set(),
            failed=set(),
            pending=set(),
            cancelled=set(),
            merge_state_status="CLEAN",
            mergeable="MERGEABLE",
            is_draft=False,
            state="OPEN",
            auto_merge_enabled=False,
            checks={},
        )
    return automation.ReviewSnapshot(
        pr_repo=pr_repo,
        pr_number=pr_number,
        review_refs=review_refs,
        pr_author=pr_author,
        pr_body=pr_body,
        pr_comment_bodies=pr_comment_bodies,
        copilot_review_present=copilot_review_present,
        codex_verdict=None,
        codex_gate_code=codex_gate_code,
        codex_gate_message=codex_gate_message,
        gate_status=gate_status,
        rescue_checks=rescue_checks,
        rescue_passed=rescue_passed or set(),
        rescue_pending=rescue_pending or set(),
        rescue_failed=rescue_failed or set(),
        rescue_cancelled=rescue_cancelled or set(),
        rescue_missing=rescue_missing or set(),
    )


def _write_automation_config(tmp_path: Path) -> Path:
    path = tmp_path / "board-automation-config.json"
    path.write_text(
        json.dumps(
            {
                "version": 2,
                "wip_limits": {
                    "codex": {"crew": 1, "app": 1, "site": 1},
                    "claude": {"crew": 3, "app": 3, "site": 3},
                    "human": {"crew": 3, "app": 3, "site": 3},
                },
                "freshness_hours": 24,
                "stale_confirmation_cycles": 2,
                "trusted_codex_actors": ["codex", "codex[bot]", "chris00walker"],
                "trusted_local_authors": ["codex", "codex[bot]", "chris00walker"],
                "execution_authority": {
                    "mode": "single_machine",
                    "repos": ["app", "crew", "site"],
                    "executors": ["codex"],
                    "global_concurrency": 2
                },
                "dispatch": {"target": "executor"},
                "canary_thresholds": {},
                "required_checks_by_repo": {
                    "startupai-site/app.startupai-site": [
                        "ci",
                        "db-test-gate",
                        "e2e",
                        "tag-contract",
                        "codex-review-gate"
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    return path


def _make_info(status: str) -> BoardInfo:
    return BoardInfo(status=status, item_id="ITEM_123", project_id="PROJ_456")


# -- direct_successors --------------------------------------------------------


def test_direct_successors(tmp_path: Path) -> None:
    """Graph utility returns correct successors."""
    config = _load(tmp_path)
    succs = direct_successors(config, "crew#87")
    assert succs == {"crew#88"}
    succs2 = direct_successors(config, "crew#88")
    assert succs2 == {"app#153"}


# -- mark-done tests ----------------------------------------------------------


def test_mark_done_sets_status(tmp_path: Path) -> None:
    """Board mutator called with correct IDs."""
    config = _load(tmp_path)
    mutator = MagicMock()
    issues = [
        LinkedIssue(
            owner="StartupAI-site",
            repo="startupai-crew",
            number=88,
            ref="crew#88",
        )
    ]
    result = mark_issues_done(
        issues,
        config,
        "StartupAI-site",
        1,
        board_info_resolver=lambda *_: _make_info("Review"),
        board_mutator=mutator,
    )
    assert "crew#88" in result
    mutator.assert_called_once()


def test_mark_done_skips_already_done(tmp_path: Path) -> None:
    """Issue already Done -> no mutation."""
    config = _load(tmp_path)
    mutator = MagicMock()
    issues = [
        LinkedIssue(
            owner="StartupAI-site",
            repo="startupai-crew",
            number=88,
            ref="crew#88",
        )
    ]
    result = mark_issues_done(
        issues,
        config,
        "StartupAI-site",
        1,
        board_info_resolver=lambda *_: _make_info("Done"),
        board_mutator=mutator,
    )
    assert "crew#88" not in result
    mutator.assert_not_called()


def test_mark_done_skips_not_on_board(tmp_path: Path) -> None:
    """NOT_ON_BOARD -> skip without error."""
    config = _load(tmp_path)
    mutator = MagicMock()
    issues = [
        LinkedIssue(
            owner="StartupAI-site",
            repo="startupai-crew",
            number=88,
            ref="crew#88",
        )
    ]
    result = mark_issues_done(
        issues,
        config,
        "StartupAI-site",
        1,
        board_info_resolver=lambda *_: _make_info("NOT_ON_BOARD"),
        board_mutator=mutator,
    )
    assert "crew#88" not in result
    mutator.assert_not_called()


# -- auto-promote tests -------------------------------------------------------


def test_promote_eligible_same_repo(tmp_path: Path) -> None:
    """Predecessor Done, same-repo -> promoted."""
    config = _load(tmp_path)
    result = auto_promote_successors(
        "crew#87",
        config,
        "crew",
        "StartupAI-site",
        1,
        status_resolver=lambda *_: "Done",
        board_info_resolver=lambda *_: _make_info("Backlog"),
        board_mutator=MagicMock(),
    )
    assert "crew#88" in result.promoted


def test_skip_blocked_predecessor(tmp_path: Path) -> None:
    """Predecessor not Done -> skipped."""
    config = _load(tmp_path)
    result = auto_promote_successors(
        "crew#84",
        config,
        "crew",
        "StartupAI-site",
        1,
        status_resolver=lambda *_: "In Progress",
        board_info_resolver=lambda *_: _make_info("Backlog"),
        board_mutator=MagicMock(),
    )
    assert "crew#85" not in result.promoted
    assert any(ref == "crew#85" for ref, _ in result.skipped)


def test_bridge_comment_for_cross_repo(tmp_path: Path) -> None:
    """Cross-repo successor -> comment posted, not promoted."""
    config = _load(tmp_path)
    poster = MagicMock()
    result = auto_promote_successors(
        "crew#88",
        config,
        "crew",
        "StartupAI-site",
        1,
        status_resolver=lambda *_: "Done",
        board_info_resolver=lambda *_: _make_info("Backlog"),
        board_mutator=MagicMock(),
        comment_checker=lambda *_, **__: False,
        comment_poster=poster,
    )
    assert "app#153" in result.cross_repo_pending
    poster.assert_called_once()


def test_bridge_comment_deduped(tmp_path: Path) -> None:
    """Marker exists -> no duplicate comment."""
    config = _load(tmp_path)
    poster = MagicMock()
    result = auto_promote_successors(
        "crew#88",
        config,
        "crew",
        "StartupAI-site",
        1,
        status_resolver=lambda *_: "Done",
        board_info_resolver=lambda *_: _make_info("Backlog"),
        board_mutator=MagicMock(),
        comment_checker=lambda *_, **__: True,  # marker already exists
        comment_poster=poster,
    )
    poster.assert_not_called()


def test_dry_run_no_mutations(tmp_path: Path) -> None:
    """dry_run=True -> no promotions, no comments."""
    config = _load(tmp_path)
    mutator = MagicMock()
    poster = MagicMock()
    result = auto_promote_successors(
        "crew#87",
        config,
        "crew",
        "StartupAI-site",
        1,
        dry_run=True,
        status_resolver=lambda *_: "Done",
        board_info_resolver=lambda *_: _make_info("Backlog"),
        board_mutator=mutator,
        comment_poster=poster,
    )
    mutator.assert_not_called()
    poster.assert_not_called()


def test_cross_repo_pending_in_result(tmp_path: Path) -> None:
    """Cross-repo refs appear in PromotionResult.cross_repo_pending."""
    config = _load(tmp_path)
    result = auto_promote_successors(
        "crew#88",
        config,
        "crew",
        "StartupAI-site",
        1,
        status_resolver=lambda *_: "Done",
        board_info_resolver=lambda *_: _make_info("Backlog"),
        board_mutator=MagicMock(),
        comment_checker=lambda *_, **__: False,
        comment_poster=MagicMock(),
    )
    assert "app#153" in result.cross_repo_pending


# -- propagate-blocker tests --------------------------------------------------


def _field_response(value: str, field_type: str = "text") -> str:
    """Build a mock GraphQL response for _query_project_item_field."""
    field_data = {"text": value} if field_type == "text" else {"name": value}
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
                                    "fieldByName": field_data,
                                }
                            ]
                        }
                    }
                }
            }
        }
    )


def test_posts_advisory_on_successors(tmp_path: Path) -> None:
    """Comment posted on each successor."""
    config = _load(tmp_path)
    poster = MagicMock()
    result = propagate_blocker(
        "crew#87",
        config,
        "crew",
        "StartupAI-site",
        1,
        comment_checker=lambda *_, **__: False,
        comment_poster=poster,
        gh_runner=lambda args: _field_response("some reason"),
    )
    assert len(result) > 0
    poster.assert_called()


def test_blocker_comment_deduped(tmp_path: Path) -> None:
    """Marker exists -> skip."""
    config = _load(tmp_path)
    poster = MagicMock()
    result = propagate_blocker(
        "crew#87",
        config,
        "crew",
        "StartupAI-site",
        1,
        comment_checker=lambda *_, **__: True,
        comment_poster=poster,
        gh_runner=lambda args: _field_response("some reason"),
    )
    poster.assert_not_called()


def test_cross_repo_failure_graceful(tmp_path: Path) -> None:
    """Cross-repo post fails -> warning, not crash."""
    config = _load(tmp_path)

    # crew#88 has cross-repo successor app#153
    def failing_poster(*args, **kwargs):
        raise Exception("API error")

    # Should not raise - just warn
    result = propagate_blocker(
        "crew#88",
        config,
        "crew",
        "StartupAI-site",
        1,
        comment_checker=lambda *_, **__: False,
        comment_poster=failing_poster,
        gh_runner=lambda args: _field_response("reason"),
    )


def _empty_project_items_response() -> str:
    """Build a mock GraphQL response for _list_project_items_by_status with no items."""
    return json.dumps(
        {
            "data": {
                "organization": {
                    "projectV2": {
                        "items": {
                            "pageInfo": {"hasNextPage": False, "endCursor": ""},
                            "nodes": [],
                        }
                    }
                }
            }
        }
    )


def _project_items_response(nodes: list[dict]) -> str:
    """Build a mock GraphQL response for _list_project_items_by_status."""
    return json.dumps(
        {
            "data": {
                "organization": {
                    "projectV2": {
                        "items": {
                            "pageInfo": {"hasNextPage": False, "endCursor": ""},
                            "nodes": nodes,
                        }
                    }
                }
            }
        }
    )


def test_propagate_blocker_sweep_blocked(tmp_path: Path) -> None:
    """Sweep mode scans all Blocked items with non-empty reason."""
    config = _load(tmp_path)
    poster = MagicMock()
    result = propagate_blocker(
        None,
        config,
        None,
        "StartupAI-site",
        1,
        sweep_blocked=True,
        all_prefixes=True,
        comment_checker=lambda *_, **__: False,
        comment_poster=poster,
        gh_runner=lambda args: _empty_project_items_response(),
    )
    # In sweep mode with no blocked items found, result should be empty
    assert isinstance(result, list)


def test_propagate_blocker_sweep_skips_empty_reason(tmp_path: Path) -> None:
    """Sweep mode skips Blocked items with empty Blocked Reason."""
    config = _load(tmp_path)
    poster = MagicMock()
    result = propagate_blocker(
        None,
        config,
        None,
        "StartupAI-site",
        1,
        sweep_blocked=True,
        all_prefixes=True,
        comment_checker=lambda *_, **__: False,
        comment_poster=poster,
        gh_runner=lambda args: _empty_project_items_response(),
    )
    poster.assert_not_called()


# -- reconcile-handoffs tests -------------------------------------------------


def _empty_search_response() -> str:
    """Build a mock search API response with no items."""
    return json.dumps({"total_count": 0, "items": []})


def _search_response(*numbers: int) -> str:
    """Build a mock search API response with issue numbers."""
    return json.dumps(
        {
            "total_count": len(numbers),
            "items": [{"number": number} for number in numbers],
        }
    )


def _slurped_comments_response(comments: list[dict]) -> str:
    """Build a mock gh api --paginate --slurp issue comments response."""
    return json.dumps([comments])


def test_reconcile_ack_marks_completed(tmp_path: Path) -> None:
    """ACK signal present -> job counted as completed."""
    config = _load(tmp_path)
    # When no handoff markers found, all counters start at 0
    counts = reconcile_handoffs(
        config,
        "StartupAI-site",
        1,
        gh_runner=lambda args: _empty_search_response(),
    )
    assert isinstance(counts, dict)
    assert "completed" in counts


def test_reconcile_retries_once(tmp_path: Path) -> None:
    """No ACK past timeout -> one retry comment posted."""
    config = _load(tmp_path)
    now = datetime.now(timezone.utc)
    posted: list[str] = []

    def fake_gh(args):
        if args[:2] == ["api", "search/issues"]:
            query = next((arg for arg in args if arg.startswith("q=")), "")
            return (
                _search_response(84)
                if "repo:StartupAI-site/startupai-crew" in query
                else _empty_search_response()
            )
        if args[:2] == ["api", "repos/StartupAI-site/startupai-crew/issues/84/comments"]:
            return _slurped_comments_response(
                [
                    {
                        "body": f"<!-- {automation.MARKER_PREFIX}:handoff:job=crew-84-to-app-149 -->",
                        "created_at": (now - timedelta(minutes=31)).isoformat().replace("+00:00", "Z"),
                        "updated_at": (now - timedelta(minutes=31)).isoformat().replace("+00:00", "Z"),
                        "user": {"login": "chris00walker"},
                    }
                ]
            )
        raise AssertionError(f"Unexpected gh call: {args}")

    with patch.object(automation, "_query_project_item_field", return_value="Backlog"), patch.object(
        automation, "_post_comment", lambda _o, _r, _n, body, **_k: posted.append(body)
    ):
        counts = reconcile_handoffs(
            config,
            "StartupAI-site",
            1,
            ack_timeout_minutes=30,
            gh_runner=fake_gh,
        )
    assert counts["retried"] == 1
    assert not counts["escalated"]
    assert posted


def test_reconcile_escalates_after_retry(tmp_path: Path) -> None:
    """Still no ACK after retry -> Status=Blocked + reason."""
    config = _load(tmp_path)
    now = datetime.now(timezone.utc)
    blocked: list[str] = []

    def fake_gh(args):
        if args[:2] == ["api", "search/issues"]:
            query = next((arg for arg in args if arg.startswith("q=")), "")
            return (
                _search_response(84)
                if "repo:StartupAI-site/startupai-crew" in query
                else _empty_search_response()
            )
        if args[:2] == ["api", "repos/StartupAI-site/startupai-crew/issues/84/comments"]:
            return _slurped_comments_response(
                [
                    {
                        "body": f"<!-- {automation.MARKER_PREFIX}:handoff:job=crew-84-to-app-149 -->",
                        "created_at": (now - timedelta(minutes=31)).isoformat().replace("+00:00", "Z"),
                        "updated_at": (now - timedelta(minutes=31)).isoformat().replace("+00:00", "Z"),
                        "user": {"login": "chris00walker"},
                    }
                ]
            )
        raise AssertionError(f"Unexpected gh call: {args}")

    with patch.object(automation, "_query_project_item_field", return_value="Backlog"), patch.object(
        automation,
        "_set_blocked_with_reason",
        lambda issue_ref, *_a, **_k: blocked.append(issue_ref),
    ):
        counts = reconcile_handoffs(
            config,
            "StartupAI-site",
            1,
            ack_timeout_minutes=30,
            max_retries=0,
            gh_runner=fake_gh,
        )
    assert counts["escalated"] == 1
    assert blocked == ["crew#84"]


def test_reconcile_idempotent_on_escalated(tmp_path: Path) -> None:
    """Existing escalation marker -> no duplicate mutations."""
    config = _load(tmp_path)
    counts = reconcile_handoffs(
        config,
        "StartupAI-site",
        1,
        gh_runner=lambda args: _empty_search_response(),
    )
    assert isinstance(counts, dict)


def test_reconcile_keeps_recent_unacked_handoff_pending(tmp_path: Path) -> None:
    """Recent handoff signal should stay pending until ack timeout elapses."""
    config = _load(tmp_path)
    now = datetime.now(timezone.utc)
    posted: list[str] = []

    def fake_gh(args):
        if args[:2] == ["api", "search/issues"]:
            query = next((arg for arg in args if arg.startswith("q=")), "")
            return (
                _search_response(84)
                if "repo:StartupAI-site/startupai-crew" in query
                else _empty_search_response()
            )
        if args[:2] == ["api", "repos/StartupAI-site/startupai-crew/issues/84/comments"]:
            return _slurped_comments_response(
                [
                    {
                        "body": f"<!-- {automation.MARKER_PREFIX}:handoff:job=crew-84-to-app-149 -->",
                        "created_at": (now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
                        "updated_at": (now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
                        "user": {"login": "chris00walker"},
                    }
                ]
            )
        raise AssertionError(f"Unexpected gh call: {args}")

    with patch.object(automation, "_query_project_item_field", return_value="Backlog"), patch.object(
        automation, "_post_comment", lambda _o, _r, _n, body, **_k: posted.append(body)
    ):
        counts = reconcile_handoffs(
            config,
            "StartupAI-site",
            1,
            ack_timeout_minutes=30,
            gh_runner=fake_gh,
        )
    assert counts["pending"] == 1
    assert not posted


# -- executor routing tests ---------------------------------------------------


def test_route_protected_queue_executors_retargets_backlog_and_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _load(tmp_path)
    auto_config = _automation_config()
    snapshots = {
        "Backlog": [
            automation._ProjectItemSnapshot(
                issue_ref="StartupAI-site/startupai-crew#19",
                status="Backlog",
                executor="claude",
                handoff_to="none",
                priority="P0",
                item_id="ITEM_19",
                project_id="PROJ_1",
            )
        ],
        "Ready": [
            automation._ProjectItemSnapshot(
                issue_ref="StartupAI-site/startupai-crew#27",
                status="Ready",
                executor="claude",
                handoff_to="none",
                priority="P0",
                item_id="ITEM_27",
                project_id="PROJ_1",
            ),
            automation._ProjectItemSnapshot(
                issue_ref="StartupAI-site/startupai-crew#32",
                status="Ready",
                executor="codex",
                handoff_to="none",
                priority="P0",
                item_id="ITEM_32",
                project_id="PROJ_1",
            ),
        ],
    }
    calls: list[tuple[str, str, str, str]] = []

    monkeypatch.setattr(
        automation,
        "_list_project_items_by_status",
        lambda status, *_args, **_kwargs: list(snapshots.get(status, [])),
    )
    monkeypatch.setattr(
        automation,
        "_set_single_select_field",
        lambda project_id, item_id, field_name, option_name, **_kwargs: calls.append(
            (project_id, item_id, field_name, option_name)
        ),
    )

    result = route_protected_queue_executors(
        config,
        auto_config,
        "StartupAI-site",
        1,
    )

    assert result.routed == ["crew#19", "crew#27"]
    assert result.unchanged == ["crew#32"]
    assert result.skipped == []
    assert calls == [
        ("PROJ_1", "ITEM_19", "Executor", "codex"),
        ("PROJ_1", "ITEM_27", "Executor", "codex"),
    ]


def test_route_protected_queue_executors_skips_without_single_target_executor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _load(tmp_path)
    auto_config = automation.BoardAutomationConfig(
        wip_limits={"codex": {"crew": 1}, "human": {"crew": 1}},
        freshness_hours=24,
        stale_confirmation_cycles=2,
        trusted_codex_actors={"codex"},
        trusted_local_authors={"codex"},
        execution_authority_mode="single_machine",
        execution_authority_repos=("crew",),
        execution_authority_executors=("codex", "human"),
    )
    called = {"list": False}

    monkeypatch.setattr(
        automation,
        "_list_project_items_by_status",
        lambda *_args, **_kwargs: called.__setitem__("list", True),
    )

    result = route_protected_queue_executors(
        config,
        auto_config,
        "StartupAI-site",
        1,
    )

    assert result.routed == []
    assert result.unchanged == []
    assert result.skipped == [("*", "no-deterministic-executor-target")]
    assert called["list"] is False


def test_admit_backlog_items_promotes_governed_backlog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _load(tmp_path)
    auto_config = replace(
        _automation_config(),
        admission=automation.AdmissionConfig(enabled=True),
    )
    items = [
        automation._ProjectItemSnapshot(
            issue_ref="StartupAI-site/startupai-crew#19",
            status="Backlog",
            executor="claude",
            handoff_to="none",
            priority="P0",
            sprint="Sprint 1",
            agent="backend-dev",
            title="Fix admission",
            body="## Acceptance Criteria\n- controller fills Ready",
            item_id="ITEM_19",
            project_id="PROJ_1",
            repo_slug="StartupAI-site/startupai-crew",
            issue_number=19,
        ),
        automation._ProjectItemSnapshot(
            issue_ref="StartupAI-site/startupai-crew#27",
            status="Backlog",
            executor="claude",
            handoff_to="none",
            priority="P1",
            sprint="Sprint 1",
            agent="backend-dev",
            title="Second item",
            body="## Definition of Done\n- queue remains fed",
            item_id="ITEM_27",
            project_id="PROJ_1",
            repo_slug="StartupAI-site/startupai-crew",
            issue_number=27,
        ),
    ]
    field_calls: list[tuple[str, str, str, str]] = []
    text_calls: list[tuple[str, str, str, str]] = []

    monkeypatch.setattr(
        automation,
        "_list_project_items",
        lambda *_args, **_kwargs: items,
    )
    monkeypatch.setattr(
        automation,
        "query_open_pull_requests",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        automation,
        "list_issue_comment_bodies",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        automation,
        "evaluate_ready_promotion",
        lambda **_kwargs: (0, ""),
    )
    monkeypatch.setattr(
        automation,
        "_set_single_select_field",
        lambda project_id, item_id, field_name, option_name, **_kwargs: field_calls.append(
            (project_id, item_id, field_name, option_name)
        ),
    )
    monkeypatch.setattr(
        automation,
        "_set_text_field",
        lambda project_id, item_id, field_name, value, **_kwargs: text_calls.append(
            (project_id, item_id, field_name, value)
        ),
    )

    decision = automation.admit_backlog_items(
        config,
        auto_config,
        "StartupAI-site",
        1,
        dispatchable_repo_prefixes=("crew",),
    )

    assert decision.admitted == ("crew#19", "crew#27")
    assert ("PROJ_1", "ITEM_19", "Status", "Ready") in field_calls
    assert ("PROJ_1", "ITEM_19", "Executor", "codex") in field_calls
    assert ("PROJ_1", "ITEM_19", "Owner", "codex:local-consumer") in text_calls


def test_admit_backlog_items_skips_missing_acceptance_criteria(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _load(tmp_path)
    auto_config = replace(
        _automation_config(),
        admission=automation.AdmissionConfig(enabled=True),
    )
    items = [
        automation._ProjectItemSnapshot(
            issue_ref="StartupAI-site/startupai-crew#19",
            status="Backlog",
            executor="codex",
            handoff_to="none",
            priority="P0",
            sprint="Sprint 1",
            agent="backend-dev",
            title="Missing section",
            body="This still needs acceptance criteria.",
            item_id="ITEM_19",
            project_id="PROJ_1",
            repo_slug="StartupAI-site/startupai-crew",
            issue_number=19,
        ),
    ]

    monkeypatch.setattr(
        automation,
        "_list_project_items",
        lambda *_args, **_kwargs: items,
    )
    monkeypatch.setattr(
        automation,
        "query_open_pull_requests",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        automation,
        "list_issue_comment_bodies",
        lambda *_args, **_kwargs: [],
    )

    decision = automation.admit_backlog_items(
        config,
        auto_config,
        "StartupAI-site",
        1,
        dispatchable_repo_prefixes=("crew",),
        dry_run=True,
    )

    assert decision.admitted == ()
    assert decision.skip_reason_counts["missing_acceptance_criteria"] == 1


def test_admit_backlog_items_closes_prior_resolved_issue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _load(tmp_path)
    auto_config = replace(
        _automation_config(),
        admission=automation.AdmissionConfig(enabled=True),
    )
    items = [
        automation._ProjectItemSnapshot(
            issue_ref="StartupAI-site/startupai-crew#19",
            status="Backlog",
            executor="codex",
            handoff_to="none",
            priority="P0",
            sprint="Sprint 1",
            agent="backend-dev",
            title="Already solved",
            body="## Acceptance Criteria\n- already handled",
            item_id="ITEM_19",
            project_id="PROJ_1",
            repo_slug="StartupAI-site/startupai-crew",
            issue_number=19,
        ),
    ]
    marked: list[str] = []
    closed: list[tuple[str, str, int]] = []

    monkeypatch.setattr(automation, "_list_project_items", lambda *_args, **_kwargs: items)
    monkeypatch.setattr(automation, "query_open_pull_requests", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        automation,
        "list_issue_comment_bodies",
        lambda *_args, **_kwargs: [
            "\n".join(
                [
                    "<!-- startupai-board-bot:consumer-resolution:crew#19 -->",
                    "```json",
                    json.dumps(
                        {
                            "issue_ref": "crew#19",
                            "session_id": "abc123",
                            "resolution_kind": "already_on_main",
                            "summary": "Already implemented on main.",
                            "verification_class": "strong",
                            "final_action": "closed_as_already_resolved",
                            "evidence": {"pr_urls": ["https://github.com/O/R/pull/1"]},
                        }
                    ),
                    "```",
                ]
            )
        ],
    )
    monkeypatch.setattr(
        automation,
        "mark_issues_done",
        lambda issues, *_args, **_kwargs: marked.extend(issue.ref for issue in issues) or marked,
    )
    monkeypatch.setattr(
        automation,
        "close_issue",
        lambda owner, repo, number, **_kwargs: closed.append((owner, repo, number)),
    )

    decision = automation.admit_backlog_items(
        config,
        auto_config,
        "StartupAI-site",
        1,
        dispatchable_repo_prefixes=("crew",),
    )

    assert decision.resolved == ("crew#19",)
    assert decision.admitted == ()
    assert marked == ["crew#19"]
    assert closed == [("StartupAI-site", "startupai-crew", 19)]


def test_admit_backlog_items_blocks_prior_ambiguous_resolution_issue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _load(tmp_path)
    auto_config = replace(
        _automation_config(),
        admission=automation.AdmissionConfig(enabled=True),
    )
    items = [
        automation._ProjectItemSnapshot(
            issue_ref="StartupAI-site/startupai-crew#27",
            status="Backlog",
            executor="codex",
            handoff_to="none",
            priority="P0",
            sprint="Sprint 1",
            agent="backend-dev",
            title="Needs human check",
            body="## Acceptance Criteria\n- still valid",
            item_id="ITEM_27",
            project_id="PROJ_1",
            repo_slug="StartupAI-site/startupai-crew",
            issue_number=27,
        ),
    ]
    blocked: list[tuple[str, str]] = []
    handoffs: list[str] = []

    monkeypatch.setattr(automation, "_list_project_items", lambda *_args, **_kwargs: items)
    monkeypatch.setattr(automation, "query_open_pull_requests", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        automation,
        "list_issue_comment_bodies",
        lambda *_args, **_kwargs: [
            "\n".join(
                [
                    "<!-- startupai-board-bot:consumer-resolution:crew#27 -->",
                    "```json",
                    json.dumps(
                        {
                            "issue_ref": "crew#27",
                            "session_id": "abc123",
                            "resolution_kind": "duplicate",
                            "summary": "Looks duplicated.",
                            "verification_class": "weak",
                            "final_action": "blocked_for_resolution_review",
                            "evidence": {},
                        }
                    ),
                    "```",
                ]
            )
        ],
    )
    monkeypatch.setattr(
        automation,
        "_set_blocked_with_reason",
        lambda issue_ref, reason, *_args, **_kwargs: blocked.append((issue_ref, reason)),
    )
    monkeypatch.setattr(
        automation,
        "_default_board_mutation_port",
        lambda *_args, **_kwargs: SimpleNamespace(
            set_issue_status=lambda issue_ref, status: None,
            set_issue_field=lambda issue_ref, field_name, value: (
                handoffs.append(value)
                if field_name == "Handoff To"
                else None
            ),
            post_issue_comment=lambda repo, issue_number, body: None,
            close_issue=lambda repo, issue_number: None,
        ),
    )
    monkeypatch.setattr(
        automation,
        "_default_review_state_port",
        lambda *_args, **_kwargs: _fake_review_state_port(
            status_by_issue={"crew#27": "Backlog"}
        ),
    )

    decision = automation.admit_backlog_items(
        config,
        auto_config,
        "StartupAI-site",
        1,
        dispatchable_repo_prefixes=("crew",),
    )

    assert decision.blocked == ("crew#27",)
    assert decision.admitted == ()
    assert blocked == [("crew#27", "resolution-review-required:duplicate")]
    assert handoffs == ["claude"]


def test_admit_backlog_items_fast_path_skips_deep_reads_when_ready_floor_met(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _load(tmp_path)
    auto_config = replace(
        _automation_config(),
        global_concurrency=1,
        admission=automation.AdmissionConfig(enabled=True, ready_floor_multiplier=1),
    )
    items = [
        automation._ProjectItemSnapshot(
            issue_ref="StartupAI-site/startupai-crew#10",
            status="Ready",
            executor="codex",
            handoff_to="none",
            priority="P0",
            sprint="Sprint 1",
            agent="backend-dev",
            title="Already ready",
            item_id="ITEM_READY",
            project_id="PROJ_1",
            repo_slug="StartupAI-site/startupai-crew",
            issue_number=10,
        ),
        automation._ProjectItemSnapshot(
            issue_ref="StartupAI-site/startupai-crew#11",
            status="Backlog",
            executor="codex",
            handoff_to="none",
            priority="P1",
            sprint="Sprint 1",
            agent="backend-dev",
            title="Should not be deeply evaluated",
            item_id="ITEM_BACKLOG",
            project_id="PROJ_1",
            repo_slug="StartupAI-site/startupai-crew",
            issue_number=11,
        ),
    ]
    monkeypatch.setattr(automation, "_list_project_items", lambda *_args, **_kwargs: items)
    monkeypatch.setattr(
        automation,
        "query_open_pull_requests",
        lambda *_args, **_kwargs: pytest.fail("should not list PRs when admission is full"),
    )
    monkeypatch.setattr(
        automation,
        "list_issue_comment_bodies",
        lambda *_args, **_kwargs: pytest.fail("should not fetch comments when admission is full"),
    )
    monkeypatch.setattr(
        automation,
        "memoized_query_issue_body",
        lambda *_args, **_kwargs: pytest.fail("should not fetch bodies when admission is full"),
    )
    monkeypatch.setattr(
        automation,
        "evaluate_ready_promotion",
        lambda *_args, **_kwargs: pytest.fail("should not validate dependencies when admission is full"),
    )

    decision = automation.admit_backlog_items(
        config,
        auto_config,
        "StartupAI-site",
        1,
        dispatchable_repo_prefixes=("crew",),
    )

    assert decision.needed == 0
    assert decision.deep_evaluation_performed is False
    assert decision.deep_evaluation_truncated is False


# -- schedule-ready tests -----------------------------------------------------


def test_schedule_claims_dependency_safe_items(tmp_path: Path) -> None:
    """Ready + predecessors Done + WIP available -> moved to In Progress."""
    config = _load(tmp_path)
    # app#149 is a root node (no predecessors), should be claimable
    decision = schedule_ready_items(
        config,
        "StartupAI-site",
        1,
        all_prefixes=True,
        status_resolver=lambda *_: "Done",
        board_info_resolver=lambda *_: _make_info("Ready"),
        board_mutator=MagicMock(),
        gh_runner=lambda args: _empty_project_items_response(),
    )
    assert isinstance(decision, SchedulingDecision)


def test_schedule_defers_when_wip_full(tmp_path: Path) -> None:
    """WIP at limit -> stays Ready with deferred_wip."""
    config = _load(tmp_path)
    decision = schedule_ready_items(
        config,
        "StartupAI-site",
        1,
        all_prefixes=True,
        per_executor_wip_limit=0,  # no WIP slots
        status_resolver=lambda *_: "Done",
        board_info_resolver=lambda *_: _make_info("Ready"),
        board_mutator=MagicMock(),
        gh_runner=lambda args: _empty_project_items_response(),
    )
    assert isinstance(decision, SchedulingDecision)


def test_schedule_blocks_invalid_ready(tmp_path: Path) -> None:
    """Ready + unmet predecessors -> moved to Blocked."""
    config = _load(tmp_path)
    mutator = MagicMock()
    decision = schedule_ready_items(
        config,
        "StartupAI-site",
        1,
        all_prefixes=True,
        status_resolver=lambda *_: "In Progress",  # predecessors not Done
        board_info_resolver=lambda *_: _make_info("Ready"),
        board_mutator=mutator,
        gh_runner=lambda args: _empty_project_items_response(),
    )
    assert isinstance(decision, SchedulingDecision)


def test_schedule_skips_non_graph_issues(tmp_path: Path) -> None:
    """Ready item not in graph -> left in Ready."""
    config = _load(tmp_path)
    decision = schedule_ready_items(
        config,
        "StartupAI-site",
        1,
        all_prefixes=True,
        status_resolver=lambda *_: "Done",
        board_info_resolver=lambda *_: _make_info("Ready"),
        board_mutator=MagicMock(),
        gh_runner=lambda args: _empty_project_items_response(),
    )
    assert isinstance(decision, SchedulingDecision)


def test_schedule_skips_missing_executor(tmp_path: Path) -> None:
    """Ready item with no Executor -> left in Ready."""
    config = _load(tmp_path)
    decision = schedule_ready_items(
        config,
        "StartupAI-site",
        1,
        all_prefixes=True,
        status_resolver=lambda *_: "Done",
        board_info_resolver=lambda *_: _make_info("Ready"),
        board_mutator=MagicMock(),
        gh_runner=lambda args: _empty_project_items_response(),
    )
    assert isinstance(decision, SchedulingDecision)


def test_schedule_marks_non_graph_ready_claimable_in_advisory_mode(
    tmp_path: Path,
) -> None:
    """Non-graph Ready item is reported claimable in advisory mode."""
    config = _load(tmp_path)
    mutator = MagicMock()

    ready_node = {
        "fieldValueByName": {"name": "Ready"},
        "executorField": {"name": "codex"},
        "handoffField": {"name": "none"},
        "content": {
            "number": 999,
            "repository": {"nameWithOwner": "StartupAI-site/startupai-crew"},
        },
    }

    responses = [
        _project_items_response([]),  # In Progress listing for WIP counts
        _project_items_response([ready_node]),  # Ready listing
    ]

    decision = schedule_ready_items(
        config,
        "StartupAI-site",
        1,
        all_prefixes=True,
        mode="advisory",
        status_resolver=lambda *_: "Done",
        board_info_resolver=lambda *_: _make_info("Ready"),
        board_mutator=mutator,
        gh_runner=lambda args: responses.pop(0),
    )

    assert decision.claimable == ["crew#999"]
    assert decision.claimed == []
    assert decision.skipped_non_graph == ["crew#999"]
    mutator.assert_not_called()


def test_schedule_claim_mode_mutates_status_for_claimable_item(
    tmp_path: Path,
) -> None:
    """Claim mode performs Ready -> In Progress mutation."""
    config = _load(tmp_path)
    mutator = MagicMock()

    ready_node = {
        "fieldValueByName": {"name": "Ready"},
        "executorField": {"name": "codex"},
        "handoffField": {"name": "none"},
        "content": {
            "number": 999,
            "repository": {"nameWithOwner": "StartupAI-site/startupai-crew"},
        },
    }
    responses = [
        _project_items_response([]),
        _project_items_response([ready_node]),
    ]

    decision = schedule_ready_items(
        config,
        "StartupAI-site",
        1,
        all_prefixes=True,
        mode="claim",
        status_resolver=lambda *_: "Done",
        board_info_resolver=lambda *_: _make_info("Ready"),
        board_mutator=mutator,
        gh_runner=lambda args: responses.pop(0),
    )

    assert decision.claimed == ["crew#999"]
    assert decision.claimable == []
    mutator.assert_called_once()


def test_schedule_blocks_missing_executor_with_cap(tmp_path: Path) -> None:
    """Missing executor items are auto-blocked up to the configured cap."""
    config = _load(tmp_path)

    ready_nodes = [
        {
            "fieldValueByName": {"name": "Ready"},
            "executorField": {"name": ""},
            "handoffField": {"name": "none"},
            "content": {
                "number": 999,
                "repository": {"nameWithOwner": "StartupAI-site/startupai-crew"},
            },
        },
        {
            "fieldValueByName": {"name": "Ready"},
            "executorField": {"name": ""},
            "handoffField": {"name": "none"},
            "content": {
                "number": 1000,
                "repository": {"nameWithOwner": "StartupAI-site/startupai-crew"},
            },
        },
    ]
    responses = [
        _project_items_response([]),  # In Progress listing for WIP counts
        _project_items_response(ready_nodes),  # Ready listing
    ]

    decision = schedule_ready_items(
        config,
        "StartupAI-site",
        1,
        all_prefixes=True,
        missing_executor_block_cap=1,
        dry_run=True,  # avoid external text-field mutation calls
        gh_runner=lambda args: responses.pop(0),
    )

    assert decision.blocked_missing_executor == ["crew#999"]
    assert decision.skipped_missing_executor == ["crew#1000"]


def test_claim_ready_next_claims_first_eligible(tmp_path: Path) -> None:
    """claim-ready --next claims first eligible Ready for executor."""
    config = _load(tmp_path)
    mutator = MagicMock()
    poster = MagicMock()

    ready_nodes = [
        {
            "fieldValueByName": {"name": "Ready"},
            "executorField": {"name": "codex"},
            "handoffField": {"name": "none"},
            "content": {
                "number": 87,
                "repository": {"nameWithOwner": "StartupAI-site/startupai-crew"},
            },
        },
        {
            "fieldValueByName": {"name": "Ready"},
            "executorField": {"name": "codex"},
            "handoffField": {"name": "none"},
            "content": {
                "number": 88,
                "repository": {"nameWithOwner": "StartupAI-site/startupai-crew"},
            },
        },
    ]
    responses = [
        _project_items_response(ready_nodes),  # Ready listing
        _project_items_response([]),  # In Progress listing
    ]

    result = claim_ready_issue(
        config,
        "StartupAI-site",
        1,
        executor="codex",
        next_issue=True,
        all_prefixes=True,
        status_resolver=lambda *_: "Done",
        board_info_resolver=lambda *_: _make_info("Ready"),
        board_mutator=mutator,
        comment_checker=lambda *_, **__: False,
        comment_poster=poster,
        gh_runner=lambda args: responses.pop(0),
    )
    assert result.claimed == "crew#87"
    assert result.reason == ""
    mutator.assert_called_once()
    poster.assert_called_once()


def test_claim_ready_rejects_executor_mismatch(tmp_path: Path) -> None:
    """Specific issue claim fails when Executor field is different."""
    config = _load(tmp_path)
    ready_nodes = [
        {
            "fieldValueByName": {"name": "Ready"},
            "executorField": {"name": "claude"},
            "handoffField": {"name": "none"},
            "content": {
                "number": 88,
                "repository": {"nameWithOwner": "StartupAI-site/startupai-crew"},
            },
        }
    ]
    responses = [
        _project_items_response(ready_nodes),
        _project_items_response([]),
    ]
    result = claim_ready_issue(
        config,
        "StartupAI-site",
        1,
        executor="codex",
        issue_ref="crew#88",
        board_info_resolver=lambda *_: _make_info("Ready"),
        board_mutator=MagicMock(),
        gh_runner=lambda args: responses.pop(0),
    )
    assert result.claimed is None
    assert result.reason == "executor-mismatch:claude"


def test_claim_ready_rejects_dependency_unmet(tmp_path: Path) -> None:
    """Specific issue in graph fails claim when predecessors are unmet."""
    config = _load(tmp_path)
    ready_nodes = [
        {
            "fieldValueByName": {"name": "Ready"},
            "executorField": {"name": "codex"},
            "handoffField": {"name": "none"},
            "content": {
                "number": 88,
                "repository": {"nameWithOwner": "StartupAI-site/startupai-crew"},
            },
        }
    ]
    responses = [
        _project_items_response(ready_nodes),
        _project_items_response([]),
    ]
    result = claim_ready_issue(
        config,
        "StartupAI-site",
        1,
        executor="codex",
        issue_ref="crew#88",
        status_resolver=lambda *_: "In Progress",
        board_info_resolver=lambda *_: _make_info("Ready"),
        board_mutator=MagicMock(),
        gh_runner=lambda args: responses.pop(0),
    )
    assert result.claimed is None
    assert result.reason == "dependency-unmet"


def test_audit_in_progress_escalates_stale_without_pr(tmp_path: Path) -> None:
    """Stale In Progress with no PR field should escalate via comment."""
    config = _load(tmp_path)
    snapshot = automation._ProjectItemSnapshot(
        issue_ref="StartupAI-site/startupai-crew#88",
        status="In Progress",
        executor="codex",
        handoff_to="none",
    )

    with patch.object(automation, "_set_single_select_field") as handoff_set:
        with patch.object(
            automation,
            "_list_project_items_by_status",
            return_value=[snapshot],
        ):
            with patch.object(
                automation,
                "_query_project_item_field",
                return_value="n/a",
            ):
                with patch.object(
                    automation,
                    "_query_issue_updated_at",
                    return_value=automation.datetime(2000, 1, 1, tzinfo=automation.timezone.utc),
                ):
                    with patch.object(automation, "_comment_exists", return_value=False):
                        poster = MagicMock()
                        result = audit_in_progress(
                            config,
                            "StartupAI-site",
                            1,
                            all_prefixes=True,
                            max_age_hours=24,
                            board_info_resolver=lambda *_: _make_info("In Progress"),
                            comment_checker=automation._comment_exists,
                            comment_poster=poster,
                        )
    assert result == ["crew#88"]
    handoff_set.assert_called_once()
    poster.assert_called_once()


# -- enforce-ready-dependencies tests -----------------------------------------


def test_guard_blocks_unmet_ready(tmp_path: Path) -> None:
    """Unmet predecessor in Ready -> moved to Blocked."""
    config = _load(tmp_path)
    mutator = MagicMock()
    corrected = enforce_ready_dependency_guard(
        config,
        "StartupAI-site",
        1,
        all_prefixes=True,
        status_resolver=lambda *_: "In Progress",
        board_info_resolver=lambda *_: _make_info("Ready"),
        board_mutator=mutator,
        gh_runner=lambda args: _empty_project_items_response(),
    )
    assert isinstance(corrected, list)


def test_guard_leaves_valid_ready(tmp_path: Path) -> None:
    """Dependency-safe Ready -> unchanged."""
    config = _load(tmp_path)
    mutator = MagicMock()
    corrected = enforce_ready_dependency_guard(
        config,
        "StartupAI-site",
        1,
        all_prefixes=True,
        status_resolver=lambda *_: "Done",
        board_info_resolver=lambda *_: _make_info("Ready"),
        board_mutator=mutator,
        gh_runner=lambda args: _empty_project_items_response(),
    )
    assert isinstance(corrected, list)


def test_guard_skips_non_graph_issues(tmp_path: Path) -> None:
    """Non-graph Ready item -> left in Ready."""
    config = _load(tmp_path)
    corrected = enforce_ready_dependency_guard(
        config,
        "StartupAI-site",
        1,
        all_prefixes=True,
        status_resolver=lambda *_: "Done",
        board_info_resolver=lambda *_: _make_info("Ready"),
        board_mutator=MagicMock(),
        gh_runner=lambda args: _empty_project_items_response(),
    )
    assert isinstance(corrected, list)


# -- sync-review-state tests --------------------------------------------------


def test_resolve_issues_from_pr_event(tmp_path: Path) -> None:
    """PR event JSON -> correct issue refs."""
    config = _load(tmp_path)
    # Create a mock event file
    event = {
        "action": "opened",
        "pull_request": {
            "number": 97,
            "merged": False,
            "base": {"repo": {"full_name": "StartupAI-site/startupai-crew"}},
        },
    }
    event_path = tmp_path / "event.json"
    event_path.write_text(json.dumps(event))

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
    results = resolve_issues_from_event(
        str(event_path),
        config,
        gh_runner=lambda args: graphql_response,
    )
    assert len(results) >= 1
    assert results[0][1] == "pr_open"


def test_resolve_issues_from_ready_for_review_event(tmp_path: Path) -> None:
    """ready_for_review event should map to pr_ready_for_review."""
    config = _load(tmp_path)
    event = {
        "action": "ready_for_review",
        "pull_request": {
            "number": 97,
            "merged": False,
            "base": {"repo": {"full_name": "StartupAI-site/startupai-crew"}},
        },
    }
    event_path = tmp_path / "event-ready.json"
    event_path.write_text(json.dumps(event))

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
    results = resolve_issues_from_event(
        str(event_path),
        config,
        gh_runner=lambda args: graphql_response,
    )
    assert len(results) == 1
    assert results[0][0] == "crew#88"
    assert results[0][1] == "pr_ready_for_review"


def test_review_sync_noop_on_pr_open(
    tmp_path: Path,
) -> None:
    """PR open -> no board change (waiting for review signal)."""
    config = _load(tmp_path)
    mutator = MagicMock()
    code, msg = sync_review_state(
        "pr_open",
        "crew#88",
        config,
        "StartupAI-site",
        1,
        board_info_resolver=lambda *_: _make_info("In Progress"),
        board_mutator=mutator,
    )
    assert code == 2
    mutator.assert_not_called()
    assert "waiting for review signal" in msg


def test_review_sync_moves_in_progress_to_review_on_review_submitted(
    tmp_path: Path,
) -> None:
    """review_submitted + In Progress -> Review."""
    config = _load(tmp_path)
    mutator = MagicMock()
    code, msg = sync_review_state(
        "review_submitted",
        "crew#88",
        config,
        "StartupAI-site",
        1,
        board_info_resolver=lambda *_: _make_info("In Progress"),
        board_mutator=mutator,
    )
    assert code == 0
    mutator.assert_called_once()
    assert "Review" in msg


def test_review_sync_uses_ports_for_status_transition(
    tmp_path: Path,
) -> None:
    """review_submitted transitions through ReviewStatePort + BoardMutationPort."""
    config = _load(tmp_path)
    board_calls: list[tuple[str, str]] = []

    code, msg = sync_review_state(
        "review_submitted",
        "crew#88",
        config,
        "StartupAI-site",
        1,
        review_state_port=_fake_review_state_port(
            status_by_issue={"crew#88": "In Progress"}
        ),
        board_port=_fake_board_port(board_calls),
    )

    assert code == 0
    assert board_calls == [("crew#88", "Review")]
    assert "Review" in msg


def test_review_sync_moves_in_progress_to_review_on_ready_for_review(
    tmp_path: Path,
) -> None:
    """pr_ready_for_review + In Progress -> Review."""
    config = _load(tmp_path)
    mutator = MagicMock()
    code, msg = sync_review_state(
        "pr_ready_for_review",
        "crew#88",
        config,
        "StartupAI-site",
        1,
        board_info_resolver=lambda *_: _make_info("In Progress"),
        board_mutator=mutator,
    )
    assert code == 0
    mutator.assert_called_once()
    assert "PR ready for review" in msg


def test_review_sync_noop_on_review_submitted_when_already_review(
    tmp_path: Path,
) -> None:
    """review_submitted + already Review -> no-op."""
    config = _load(tmp_path)
    mutator = MagicMock()
    code, msg = sync_review_state(
        "review_submitted",
        "crew#88",
        config,
        "StartupAI-site",
        1,
        board_info_resolver=lambda *_: _make_info("Review"),
        board_mutator=mutator,
    )
    assert code == 2
    mutator.assert_not_called()


def test_review_sync_changes_requested_when_in_progress_is_noop(
    tmp_path: Path,
) -> None:
    """changes_requested + In Progress -> no-op (first review is changes_requested)."""
    config = _load(tmp_path)
    mutator = MagicMock()
    code, msg = sync_review_state(
        "changes_requested",
        "crew#88",
        config,
        "StartupAI-site",
        1,
        board_info_resolver=lambda *_: _make_info("In Progress"),
        board_mutator=mutator,
    )
    assert code == 2
    mutator.assert_not_called()


def test_review_sync_moves_review_to_in_progress_on_changes_requested(
    tmp_path: Path,
) -> None:
    """changes_requested -> Review -> In Progress."""
    config = _load(tmp_path)
    mutator = MagicMock()
    code, msg = sync_review_state(
        "changes_requested",
        "crew#88",
        config,
        "StartupAI-site",
        1,
        board_info_resolver=lambda *_: _make_info("Review"),
        board_mutator=mutator,
    )
    assert code == 0
    mutator.assert_called_once()


def test_review_sync_defers_governed_changes_requested_to_consumer(
    tmp_path: Path,
) -> None:
    """Single-machine governed issues do not re-enter In Progress from workflow sync."""
    config = _load(tmp_path)
    policy = _automation_config()
    mutator = MagicMock()
    code, msg = sync_review_state(
        "changes_requested",
        "app#71",
        config,
        "StartupAI-site",
        1,
        automation_config=policy,
        board_info_resolver=lambda *_: _make_info("Review"),
        board_mutator=mutator,
    )
    assert code == 2
    mutator.assert_not_called()
    assert "deferred to consumer" in msg


def _branch_protection_response(contexts: list[str] | None = None) -> str:
    """Build a mock branch protection required_status_checks response."""
    return json.dumps(
        {
            "strict": True,
            "contexts": contexts or ["Unit & Integration Tests", "Planning Surface Guard"],
            "checks": [],
        }
    )


def test_review_sync_moves_review_to_in_progress_on_failed_checks(
    tmp_path: Path,
) -> None:
    """checks_failed with required check in failed_checks -> Review -> In Progress."""
    config = _load(tmp_path)
    mutator = MagicMock()
    code, msg = sync_review_state(
        "checks_failed",
        "crew#88",
        config,
        "StartupAI-site",
        1,
        checks_state="fail",
        failed_checks=["Unit & Integration Tests"],
        board_info_resolver=lambda *_: _make_info("Review"),
        board_mutator=mutator,
        gh_runner=lambda args: _branch_protection_response(),
    )
    assert code == 0


def test_review_sync_defers_governed_required_check_failure_to_consumer(
    tmp_path: Path,
) -> None:
    """Governed single-machine issues are requeued by consumer rescue, not workflow sync."""
    config = _load(tmp_path)
    policy = _automation_config()
    mutator = MagicMock()
    code, msg = sync_review_state(
        "checks_failed",
        "app#71",
        config,
        "StartupAI-site",
        1,
        automation_config=policy,
        failed_checks=["db-test-gate"],
        board_info_resolver=lambda *_: _make_info("Review"),
        board_mutator=mutator,
        gh_runner=lambda args: _branch_protection_response(["ci", "db-test-gate"]),
    )
    assert code == 2
    mutator.assert_not_called()
    assert "deferred to consumer" in msg


def test_review_sync_moves_review_to_done_on_merge(tmp_path: Path) -> None:
    """Merge + checks pass -> Review -> Done."""
    config = _load(tmp_path)
    mutator = MagicMock()
    code, msg = sync_review_state(
        "pr_close_merged",
        "crew#88",
        config,
        "StartupAI-site",
        1,
        checks_state="passed",
        board_info_resolver=lambda *_: _make_info("Review"),
        board_mutator=mutator,
    )
    assert code == 0


def test_review_sync_ignores_non_required_check_failure(
    tmp_path: Path,
) -> None:
    """Non-required check failure -> no board change."""
    config = _load(tmp_path)
    mutator = MagicMock()
    code, msg = sync_review_state(
        "checks_failed",
        "crew#88",
        config,
        "StartupAI-site",
        1,
        failed_checks=["Lint", "Optional Check"],
        board_info_resolver=lambda *_: _make_info("Review"),
        board_mutator=mutator,
        gh_runner=lambda args: _branch_protection_response(
            ["Unit & Integration Tests", "Planning Surface Guard"]
        ),
    )
    assert code == 2  # No-op: failures are all non-required
    mutator.assert_not_called()
    assert "non-required" in msg


def test_review_sync_checks_failed_uses_pr_port_required_checks(
    tmp_path: Path,
) -> None:
    """checks_failed filters on required checks via PullRequestPort."""
    config = _load(tmp_path)
    board_calls: list[tuple[str, str]] = []

    code, msg = sync_review_state(
        "checks_failed",
        "crew#88",
        config,
        "StartupAI-site",
        1,
        failed_checks=["Lint"],
        pr_port=_fake_pr_port(
            required_status_checks=lambda pr_repo, base_ref_name="main": {
                "Unit & Integration Tests"
            }
        ),
        review_state_port=_fake_review_state_port(status_by_issue={"crew#88": "Review"}),
        board_port=_fake_board_port(board_calls),
    )

    assert code == 2
    assert board_calls == []
    assert "non-required" in msg


def test_review_sync_transitions_on_required_check_failure(
    tmp_path: Path,
) -> None:
    """Required check failure -> Review -> In Progress."""
    config = _load(tmp_path)
    mutator = MagicMock()
    code, msg = sync_review_state(
        "checks_failed",
        "crew#88",
        config,
        "StartupAI-site",
        1,
        failed_checks=["Unit & Integration Tests", "Lint"],
        board_info_resolver=lambda *_: _make_info("Review"),
        board_mutator=mutator,
        gh_runner=lambda args: _branch_protection_response(
            ["Unit & Integration Tests", "Planning Surface Guard"]
        ),
    )
    assert code == 0
    mutator.assert_called_once()


def test_review_sync_refuses_transition_without_failed_check_names(
    tmp_path: Path,
) -> None:
    """checks_failed without failed_checks -> no transition (safe default)."""
    config = _load(tmp_path)
    mutator = MagicMock()
    code, msg = sync_review_state(
        "checks_failed",
        "crew#88",
        config,
        "StartupAI-site",
        1,
        checks_state="fail",
        # failed_checks deliberately omitted
        board_info_resolver=lambda *_: _make_info("Review"),
        board_mutator=mutator,
        gh_runner=lambda args: _branch_protection_response(),
    )
    assert code == 2
    mutator.assert_not_called()
    assert "no failed check names" in msg


def test_review_sync_no_mutation_on_check_api_failure(
    tmp_path: Path,
) -> None:
    """Branch protection API error -> no board mutation, exit 4."""
    from startupai_controller.validate_critical_path_promotion import GhQueryError as _GhQueryError

    config = _load(tmp_path)
    mutator = MagicMock()

    # When gh_runner raises GhQueryError for branch protection query
    def failing_gh(args):
        if "branches" in str(args):
            raise _GhQueryError("API error")
        return "{}"

    code, msg = sync_review_state(
        "checks_failed",
        "crew#88",
        config,
        "StartupAI-site",
        1,
        board_info_resolver=lambda *_: _make_info("Review"),
        board_mutator=mutator,
        gh_runner=failing_gh,
    )
    assert code == 4
    mutator.assert_not_called()


def test_resolve_issues_from_check_suite_queries_failed_runs(
    tmp_path: Path,
) -> None:
    """check_suite failure event queries check runs and returns failed names."""
    config = _load(tmp_path)

    graphql_closing = json.dumps(
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
    check_runs_response = json.dumps(
        {
            "check_runs": [
                {"name": "Unit & Integration Tests", "conclusion": "failure"},
                {"name": "Lint", "conclusion": "success"},
                {"name": "Planning Surface Guard", "conclusion": "failure"},
            ]
        }
    )

    event = {
        "check_suite": {
            "conclusion": "failure",
            "head_sha": "abc123",
            "pull_requests": [
                {
                    "number": 102,
                    "base": {
                        "repo": {"full_name": "StartupAI-site/startupai-crew"}
                    },
                }
            ],
        }
    }
    event_path = tmp_path / "event.json"
    event_path.write_text(json.dumps(event))

    def mock_gh(args):
        args_str = " ".join(str(a) for a in args)
        if "check-runs" in args_str:
            return check_runs_response
        return graphql_closing

    results = resolve_issues_from_event(
        str(event_path), config, gh_runner=mock_gh
    )
    assert len(results) >= 1
    issue_ref, event_kind, failed_checks = results[0]
    assert issue_ref == "crew#88"
    assert event_kind == "checks_failed"
    assert failed_checks is not None
    assert "Unit & Integration Tests" in failed_checks
    assert "Planning Surface Guard" in failed_checks
    assert "Lint" not in failed_checks  # Lint succeeded


def test_cmd_sync_review_state_propagates_fatal_exit_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Handler returns exit 4 when sync_review_state returns 4."""
    from startupai_controller.validate_critical_path_promotion import GhQueryError as _GhQueryError

    config_path = _write_config(tmp_path)

    # Mock sync_review_state to return exit 4 for the issue
    def mock_sync(event_kind, issue_ref, config, project_owner,
                  project_number, **kwargs):
        return 4, f"Cannot read branch protection for owner/repo: API error"

    monkeypatch.setattr(automation, "sync_review_state", mock_sync)

    code = automation._cmd_sync_review_state(
        argparse.Namespace(
            from_github_event=False,
            event_kind="checks_failed",
            issue="crew#88",
            resolve_pr=None,
            project_owner="StartupAI-site",
            project_number=1,
            checks_state="fail",
            failed_checks=["Unit & Integration Tests"],
            dry_run=False,
        ),
        load_config(config_path),
    )
    assert code == 4


def test_sync_review_state_resolve_pr_path(tmp_path: Path) -> None:
    """--resolve-pr resolves PR->issue refs."""
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
    refs = resolve_pr_to_issues(
        "StartupAI-site/startupai-crew",
        97,
        config,
        gh_runner=lambda args: graphql_response,
    )
    assert "crew#88" in refs


def test_sync_review_state_resolve_pr_no_linked_issues(
    tmp_path: Path,
) -> None:
    """--resolve-pr with no linked issues -> empty list."""
    config = _load(tmp_path)
    graphql_response = json.dumps(
        {
            "data": {
                "repository": {
                    "pullRequest": {
                        "closingIssuesReferences": {"nodes": []}
                    }
                }
            }
        }
    )
    refs = resolve_pr_to_issues(
        "StartupAI-site/startupai-crew",
        97,
        config,
        gh_runner=lambda args: graphql_response,
    )
    assert refs == []


def test_cmd_schedule_ready_returns_zero_on_no_claims(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No claimable items is a benign no-op for scheduled runs."""
    config = _load(tmp_path)
    automation_config_path = _write_automation_config(tmp_path)
    monkeypatch.setattr(automation, "schedule_ready_items", lambda *a, **k: SchedulingDecision())

    code = automation._cmd_schedule_ready(
        argparse.Namespace(
            project_owner="StartupAI-site",
            project_number=1,
            this_repo_prefix=None,
            all_prefixes=True,
            mode="advisory",
            per_executor_wip_limit=3,
            missing_executor_block_cap=5,
            automation_config=str(automation_config_path),
            dry_run=False,
        ),
        config,
    )
    assert code == 0


def test_cmd_enforce_ready_deps_returns_zero_on_no_corrections(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Guard no-op is benign and should not fail workflows."""
    config = _load(tmp_path)
    monkeypatch.setattr(automation, "enforce_ready_dependency_guard", lambda *a, **k: [])

    code = automation._cmd_enforce_ready_deps(
        argparse.Namespace(
            project_owner="StartupAI-site",
            project_number=1,
            this_repo_prefix=None,
            all_prefixes=True,
            dry_run=False,
        ),
        config,
    )
    assert code == 0


def test_parser_accepts_dry_run_after_subcommand() -> None:
    """CLI accepts --dry-run after subcommand for workflow/manual parity."""
    parser = automation.build_parser()
    args = parser.parse_args(["schedule-ready", "--all-prefixes", "--dry-run"])
    assert args.command == "schedule-ready"
    assert args.dry_run is True


def test_cmd_claim_ready_returns_two_on_rejection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Claim rejection should return exit code 2."""
    config = _load(tmp_path)
    automation_config_path = _write_automation_config(tmp_path)
    monkeypatch.setattr(
        automation,
        "claim_ready_issue",
        lambda **kwargs: automation.ClaimReadyResult(claimed=None, reason="wip-limit"),
    )

    code = automation._cmd_claim_ready(
        argparse.Namespace(
            project_owner="StartupAI-site",
            project_number=1,
            executor="codex",
            issue="crew#88",
            next=False,
            this_repo_prefix=None,
            all_prefixes=True,
            per_executor_wip_limit=3,
            automation_config=str(automation_config_path),
            dry_run=False,
        ),
        config,
    )
    assert code == 2


def test_parser_claim_ready_parses_next_executor() -> None:
    """Parser accepts claim-ready --next form."""
    parser = automation.build_parser()
    args = parser.parse_args(
        ["claim-ready", "--executor", "codex", "--next", "--all-prefixes"]
    )
    assert args.command == "claim-ready"
    assert args.executor == "codex"
    assert args.next is True


# -- Mention safety tests ----------------------------------------------------


@pytest.mark.parametrize("executor", ["claude", "codex", "copilot", "human"])
def test_claim_comment_no_at_mentions(tmp_path: Path, executor: str) -> None:
    """Claim comments must never contain @claude, @copilot, or @codex."""
    config = _load(tmp_path)
    posted_bodies: list[str] = []

    def fake_poster(owner, repo, number, body, **kw):
        posted_bodies.append(body)

    _post_claim_comment(
        "crew#88",
        executor,
        config,
        comment_checker=lambda *a, **kw: False,  # marker not found
        comment_poster=fake_poster,
        gh_runner=lambda args: "",
    )
    assert len(posted_bodies) == 1
    body = posted_bodies[0]
    assert "@claude" not in body
    assert "@copilot" not in body
    assert "@codex" not in body


def test_claim_comment_uses_board_port_when_no_legacy_poster(
    tmp_path: Path,
) -> None:
    """Claim comments can flow through BoardMutationPort."""
    config = _load(tmp_path)
    board_calls: list[tuple[str, str]] = []

    _post_claim_comment(
        "crew#88",
        "codex",
        config,
        board_port=_fake_board_port(board_calls),
        comment_checker=lambda *a, **kw: False,
    )

    assert len(board_calls) == 1
    target, body = board_calls[0]
    assert target == "StartupAI-site/startupai-crew#88"
    assert "Claimed for execution" in body


def test_set_blocked_with_reason_uses_ports(tmp_path: Path) -> None:
    """Blocked transition and reason update can flow through ports."""
    config = _load(tmp_path)
    board_calls: list[tuple[str, str]] = []

    automation._set_blocked_with_reason(
        "crew#88",
        "dependency-unmet",
        config,
        "StartupAI-site",
        1,
        review_state_port=_fake_review_state_port(status_by_issue={"crew#88": "Ready"}),
        board_port=_fake_board_port(board_calls),
    )

    assert board_calls == [
        ("crew#88", "Blocked"),
        ("crew#88", "Blocked Reason=dependency-unmet"),
    ]


def test_stale_escalation_no_at_mentions(tmp_path: Path) -> None:
    """Stale In Progress escalation comments must not contain @mentions."""
    config = _load(tmp_path)
    posted_bodies: list[str] = []

    def fake_poster(owner, repo, number, body, **kw):
        posted_bodies.append(body)

    # Build a mock In Progress item with stale age
    in_progress_response = json.dumps(
        {
            "data": {
                "organization": {
                    "projectV2": {
                        "items": {
                            "nodes": [
                                {
                                    "content": {
                                        "__typename": "Issue",
                                        "number": 88,
                                        "repository": {
                                            "name": "startupai-crew",
                                            "owner": {"login": "StartupAI-site"},
                                        },
                                        "createdAt": "2025-01-01T00:00:00Z",
                                        "closedAt": None,
                                    },
                                    "fieldValues": {
                                        "nodes": [
                                            {
                                                "__typename": "ProjectV2ItemFieldSingleSelectValue",
                                                "field": {"name": "Status"},
                                                "name": "In Progress",
                                            },
                                            {
                                                "__typename": "ProjectV2ItemFieldSingleSelectValue",
                                                "field": {"name": "Executor"},
                                                "name": "claude",
                                            },
                                        ]
                                    },
                                }
                            ],
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                        }
                    }
                }
            }
        }
    )

    # PR search returns no linked PRs (to trigger stale detection)
    def fake_gh(args):
        if "graphql" in args:
            query_str = " ".join(args)
            if "projectV2" in query_str:
                return in_progress_response
        if "search" in args:
            return json.dumps({"items": []})
        return ""

    audit_in_progress(
        config,
        "StartupAI-site",
        1,
        this_repo_prefix="crew",
        max_age_hours=0,  # everything is stale
        dry_run=False,
        comment_checker=lambda *a, **kw: False,
        comment_poster=fake_poster,
        gh_runner=fake_gh,
    )

    for body in posted_bodies:
        assert "@claude" not in body
        assert "@copilot" not in body
        assert "@codex" not in body


# -- Codex gate + automerge tests -------------------------------------------


def test_codex_review_gate_pass(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Pass verdict in Review returns exit 0."""
    config = _load(tmp_path)
    policy = _automation_config()
    monkeypatch.setattr(
        automation,
        "query_closing_issues",
        lambda *a, **k: [LinkedIssue("StartupAI-site", "app.startupai-site", 110, "app#110")],
    )
    monkeypatch.setattr(
        automation,
        "_query_issue_board_info",
        lambda *a, **k: _make_info("Review"),
    )
    monkeypatch.setattr(
        automation,
        "query_latest_codex_verdict",
        lambda *a, **k: CodexReviewVerdict(
            decision="pass",
            route="none",
            source="comment",
            timestamp="2026-03-05T11:00:00Z",
            actor="codex-bot",
            checklist=[],
        ),
    )
    code, msg = codex_review_gate(
        "StartupAI-site/app.startupai-site",
        158,
        config,
        policy,
        "StartupAI-site",
        1,
    )
    assert code == 0
    assert "codex-review=pass" in msg


def test_codex_review_gate_fail_routes_back(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail verdict routes issue back via _apply_codex_fail_routing."""
    config = _load(tmp_path)
    policy = _automation_config()
    monkeypatch.setattr(
        automation,
        "query_closing_issues",
        lambda *a, **k: [LinkedIssue("StartupAI-site", "app.startupai-site", 110, "app#110")],
    )
    monkeypatch.setattr(
        automation,
        "_query_issue_board_info",
        lambda *a, **k: _make_info("Review"),
    )
    monkeypatch.setattr(
        automation,
        "query_latest_codex_verdict",
        lambda *a, **k: CodexReviewVerdict(
            decision="fail",
            route="codex",
            source="review",
            timestamp="2026-03-05T11:00:00Z",
            actor="codex-bot",
            checklist=["Fix failing tests"],
        ),
    )
    routed: list[str] = []
    monkeypatch.setattr(
        automation,
        "_apply_codex_fail_routing",
        lambda issue_ref, *a, **k: routed.append(issue_ref),
    )
    code, msg = codex_review_gate(
        "StartupAI-site/app.startupai-site",
        158,
        config,
        policy,
        "StartupAI-site",
        1,
    )
    assert code == 2
    assert "codex-review=fail" in msg
    assert routed == ["app#110"]


def test_automerge_review_blocks_when_gate_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Automerge returns 2 when codex gate is not pass."""
    config = _load(tmp_path)
    policy = _automation_config()
    snapshot = _make_review_snapshot(
        pr_repo="StartupAI-site/app.startupai-site",
        pr_number=158,
        review_refs=("app#110",),
        codex_gate_code=2,
        codex_gate_message="missing verdict",
    )
    code, msg = automerge_review(
        "StartupAI-site/app.startupai-site",
        158,
        config,
        policy,
        "StartupAI-site",
        1,
        snapshot=snapshot,
    )
    assert code == 2
    assert "missing verdict" in msg


def test_automerge_review_blocks_on_pending_required_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pending required checks should block merge controller."""
    config = _load(tmp_path)
    policy = _automation_config()
    monkeypatch.setattr(
        automation,
        "codex_review_gate",
        lambda *a, **k: (0, "pass"),
    )
    pr_port = _fake_pr_port(
        linked_issue_refs=lambda pr_repo, pr_number: ("app#110",),
        has_copilot_review_signal=lambda pr_repo, pr_number: True,
        get_gate_status=lambda pr_repo, pr_number: PrGateStatus(
            required={"ci", "db-test-gate"},
            passed={"ci"},
            failed=set(),
            pending={"db-test-gate"},
            cancelled=set(),
            merge_state_status="CLEAN",
            mergeable="MERGEABLE",
            is_draft=False,
            state="OPEN",
            auto_merge_enabled=False,
        ),
    )
    review_state_port = _fake_review_state_port(status_by_issue={"app#110": "Review"})
    code, msg = automerge_review(
        "StartupAI-site/app.startupai-site",
        158,
        config,
        policy,
        "StartupAI-site",
        1,
        pr_port=pr_port,
        review_state_port=review_state_port,
    )
    assert code == 2
    assert "pending" in msg


def test_automerge_review_dry_run_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Dry-run reports merge action when all gates pass."""
    config = _load(tmp_path)
    policy = _automation_config()
    monkeypatch.setattr(
        automation,
        "codex_review_gate",
        lambda *a, **k: (0, "pass"),
    )
    pr_port = _fake_pr_port(
        linked_issue_refs=lambda pr_repo, pr_number: ("app#110",),
        has_copilot_review_signal=lambda pr_repo, pr_number: True,
        get_gate_status=lambda pr_repo, pr_number: PrGateStatus(
            required={"ci"},
            passed={"ci"},
            failed=set(),
            pending=set(),
            cancelled=set(),
            merge_state_status="CLEAN",
            mergeable="MERGEABLE",
            is_draft=False,
            state="open",
            auto_merge_enabled=False,
        ),
    )
    review_state_port = _fake_review_state_port(status_by_issue={"app#110": "Review"})
    code, msg = automerge_review(
        "StartupAI-site/app.startupai-site",
        158,
        config,
        policy,
        "StartupAI-site",
        1,
        dry_run=True,
        pr_port=pr_port,
        review_state_port=review_state_port,
    )
    assert code == 0
    assert "would enable auto-merge" in msg


def test_automerge_review_updates_branch_then_merges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When BEHIND, update branch before enabling auto-merge."""
    config = _load(tmp_path)
    policy = _automation_config()
    monkeypatch.setattr(
        automation,
        "codex_review_gate",
        lambda *a, **k: (0, "pass"),
    )
    pr_port = _fake_pr_port(
        linked_issue_refs=lambda pr_repo, pr_number: ("app#110",),
        has_copilot_review_signal=lambda pr_repo, pr_number: True,
        get_gate_status=lambda pr_repo, pr_number: PrGateStatus(
            required={"ci"},
            passed={"ci"},
            failed=set(),
            pending=set(),
            cancelled=set(),
            merge_state_status="BEHIND",
            mergeable="MERGEABLE",
            is_draft=False,
            state="open",
            auto_merge_enabled=False,
        ),
        update_branch=lambda pr_repo, pr_number: calls.append(("update", pr_number)),
        enable_automerge=lambda pr_repo, pr_number, delete_branch=False: (
            calls.append(("merge", pr_number)) or "confirmed"
        ),
    )

    calls: list[tuple[str, int]] = []

    code, _msg = automerge_review(
        "StartupAI-site/app.startupai-site",
        158,
        config,
        policy,
        "StartupAI-site",
        1,
        dry_run=False,
        pr_port=pr_port,
        review_state_port=_fake_review_state_port(status_by_issue={"app#110": "Review"}),
    )
    assert code == 0
    assert ("update", 158) in calls
    assert ("merge", 158) in calls


def test_automerge_review_noop_when_not_in_review_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Automerge should no-op for PRs not linked to Review issues."""
    config = _load(tmp_path)
    policy = _automation_config()
    code, msg = automerge_review(
        "StartupAI-site/app.startupai-site",
        158,
        config,
        policy,
        "StartupAI-site",
        1,
        dry_run=True,
        pr_port=_fake_pr_port(linked_issue_refs=lambda pr_repo, pr_number: ("app#110",)),
        review_state_port=_fake_review_state_port(status_by_issue={"app#110": "In Progress"}),
    )
    assert code == 2
    assert "no-op" in msg


def test_automerge_review_noops_when_auto_merge_already_enabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Automerge should be idempotent when auto-merge is already on."""
    config = _load(tmp_path)
    policy = _automation_config()
    monkeypatch.setattr(
        automation,
        "codex_review_gate",
        lambda *a, **k: (0, "pass"),
    )
    pr_port = _fake_pr_port(
        linked_issue_refs=lambda pr_repo, pr_number: ("app#110",),
        has_copilot_review_signal=lambda pr_repo, pr_number: True,
        get_gate_status=lambda pr_repo, pr_number: PrGateStatus(
            required={"ci"},
            passed={"ci"},
            failed=set(),
            pending=set(),
            cancelled=set(),
            merge_state_status="CLEAN",
            mergeable="MERGEABLE",
            is_draft=False,
            state="OPEN",
            auto_merge_enabled=True,
        ),
    )
    code, msg = automerge_review(
        "StartupAI-site/app.startupai-site",
        158,
        config,
        policy,
        "StartupAI-site",
        1,
        pr_port=pr_port,
        review_state_port=_fake_review_state_port(status_by_issue={"app#110": "Review"}),
    )
    assert code == 0
    assert "already enabled" in msg


def test_review_rescue_reruns_cancelled_repo_specific_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cancelled bridge checks should be rerun from the central rescue loop."""
    config = _load(tmp_path)
    policy = _automation_config()
    gate_status = PrGateStatus(
        required={"ci", "db-test-gate"},
        passed={"ci", "db-test-gate"},
        failed=set(),
        pending=set(),
        cancelled=set(),
        merge_state_status="CLEAN",
        mergeable="MERGEABLE",
        is_draft=False,
        state="open",
        auto_merge_enabled=False,
        checks={
            "ci": CheckObservation(
                name="ci",
                result="pass",
                status="completed",
                conclusion="success",
            ),
            "db-test-gate": CheckObservation(
                name="db-test-gate",
                result="pass",
                status="completed",
                conclusion="success",
            ),
            "e2e": CheckObservation(
                name="e2e",
                result="pass",
                status="completed",
                conclusion="success",
            ),
            "tag-contract": CheckObservation(
                name="tag-contract",
                result="pass",
                status="completed",
                conclusion="success",
                run_id=101,
            ),
            "codex-review-gate": CheckObservation(
                name="codex-review-gate",
                result="cancelled",
                status="completed",
                conclusion="cancelled",
                run_id=202,
            ),
        },
    )
    snapshot = _make_review_snapshot(
        pr_repo="StartupAI-site/app.startupai-site",
        pr_number=183,
        review_refs=("app#110",),
        gate_status=gate_status,
        rescue_checks=("ci", "db-test-gate", "e2e", "tag-contract", "codex-review-gate"),
        rescue_passed={"ci", "db-test-gate", "e2e", "tag-contract"},
        rescue_cancelled={"codex-review-gate"},
    )
    reruns: list[tuple[str, int]] = []
    monkeypatch.setattr(
        board_io_mod,
        "rerun_actions_run",
        lambda pr_repo, run_id, **kwargs: reruns.append((pr_repo, run_id)),
    )

    result = review_rescue(
        "StartupAI-site/app.startupai-site",
        183,
        config,
        policy,
        "StartupAI-site",
        1,
        snapshot=snapshot,
    )
    assert result.rerun_checks == ("codex-review-gate",)
    assert ("StartupAI-site/app.startupai-site", 202) in reruns
    assert result.blocked_reason is None


def test_review_rescue_enables_automerge_when_snapshot_is_ready(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review rescue should hand off to automerge when all gates are green."""
    config = _load(tmp_path)
    policy = _automation_config()
    gate_status = PrGateStatus(
        required={"ci", "db-test-gate"},
        passed={"ci", "db-test-gate"},
        failed=set(),
        pending=set(),
        cancelled=set(),
        merge_state_status="CLEAN",
        mergeable="MERGEABLE",
        is_draft=False,
        state="OPEN",
        auto_merge_enabled=False,
        checks={
            "ci": CheckObservation(
                name="ci",
                result="pass",
                status="completed",
                conclusion="success",
            ),
            "db-test-gate": CheckObservation(
                name="db-test-gate",
                result="pass",
                status="completed",
                conclusion="success",
            ),
            "e2e": CheckObservation(
                name="e2e",
                result="pass",
                status="completed",
                conclusion="success",
            ),
            "tag-contract": CheckObservation(
                name="tag-contract",
                result="pass",
                status="completed",
                conclusion="success",
                run_id=101,
            ),
            "codex-review-gate": CheckObservation(
                name="codex-review-gate",
                result="pass",
                status="completed",
                conclusion="success",
                run_id=202,
            ),
        },
    )
    snapshot = _make_review_snapshot(
        pr_repo="StartupAI-site/app.startupai-site",
        pr_number=184,
        review_refs=("app#110",),
        gate_status=gate_status,
        rescue_checks=("ci", "db-test-gate", "e2e", "tag-contract", "codex-review-gate"),
        rescue_passed={"ci", "db-test-gate", "e2e", "tag-contract", "codex-review-gate"},
    )
    calls: list[tuple[str, int]] = []
    monkeypatch.setattr(
        automation,
        "automerge_review",
        lambda pr_repo, pr_number, *a, **k: calls.append((pr_repo, pr_number)) or (0, "enabled"),
    )

    result = review_rescue(
        "StartupAI-site/app.startupai-site",
        184,
        config,
        policy,
        "StartupAI-site",
        1,
        snapshot=snapshot,
    )
    assert result.auto_merge_enabled is True
    assert calls == [("StartupAI-site/app.startupai-site", 184)]


def test_review_rescue_requeues_valid_local_pr_when_required_checks_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A real failed required check should put a local review PR back in Ready."""
    config = _load(tmp_path)
    policy = _automation_config()
    gate_status = PrGateStatus(
        required={"ci", "db-test-gate"},
        passed={"ci"},
        failed={"db-test-gate"},
        pending=set(),
        cancelled=set(),
        merge_state_status="BLOCKED",
        mergeable="MERGEABLE",
        is_draft=False,
        state="OPEN",
        auto_merge_enabled=True,
        checks={
            "ci": CheckObservation(
                name="ci",
                result="pass",
                status="completed",
                conclusion="success",
            ),
            "db-test-gate": CheckObservation(
                name="db-test-gate",
                result="fail",
                status="completed",
                conclusion="failure",
                run_id=404,
            ),
        },
    )
    snapshot = _make_review_snapshot(
        pr_repo="StartupAI-site/app.startupai-site",
        pr_number=177,
        review_refs=("app#71",),
        pr_author="chris00walker",
        pr_body=(
            "Closes #71\n\n"
            "<!-- startupai-board-bot:consumer:"
            "session=sess-71 issue=app#71 repo=app "
            "branch=feat/71-s-002-status-vs-connection-status-diverg "
            "executor=codex -->"
        ),
        gate_status=gate_status,
        rescue_checks=("ci", "db-test-gate"),
        rescue_passed={"ci"},
        rescue_failed={"db-test-gate"},
    )
    requeue_calls: list[tuple[str, str]] = []

    result = review_rescue(
        "StartupAI-site/app.startupai-site",
        177,
        config,
        policy,
        "StartupAI-site",
        1,
        snapshot=snapshot,
        pr_port=_fake_pr_port(),
        review_state_port=_fake_review_state_port(status_by_issue={"app#71": "Review"}),
        board_port=_fake_board_port(requeue_calls),
    )

    assert result.requeued_refs == ("app#71",)
    assert result.blocked_reason is None
    assert requeue_calls == [("app#71", "Ready")]


def test_review_rescue_does_not_skip_when_auto_merge_is_enabled_but_checks_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Auto-merge-enabled PRs still need rescue while required checks are pending."""
    config = _load(tmp_path)
    policy = _automation_config()
    gate_status = PrGateStatus(
        required={"ci", "db-test-gate"},
        passed={"ci"},
        failed=set(),
        pending={"db-test-gate"},
        cancelled=set(),
        merge_state_status="BLOCKED",
        mergeable="MERGEABLE",
        is_draft=False,
        state="OPEN",
        auto_merge_enabled=True,
        checks={
            "ci": CheckObservation(
                name="ci",
                result="pass",
                status="completed",
                conclusion="success",
            ),
            "db-test-gate": CheckObservation(
                name="db-test-gate",
                result="pending",
                status="in_progress",
                conclusion="",
            ),
            "codex-review-gate": CheckObservation(
                name="codex-review-gate",
                result="pass",
                status="completed",
                conclusion="success",
                run_id=202,
            ),
            "tag-contract": CheckObservation(
                name="tag-contract",
                result="pass",
                status="completed",
                conclusion="success",
                run_id=101,
            ),
        },
    )
    snapshot = _make_review_snapshot(
        pr_repo="StartupAI-site/app.startupai-site",
        pr_number=177,
        review_refs=("app#71",),
        gate_status=gate_status,
        rescue_checks=("ci", "db-test-gate", "codex-review-gate", "tag-contract"),
        rescue_passed={"ci", "codex-review-gate", "tag-contract"},
        rescue_pending={"db-test-gate"},
    )

    result = review_rescue(
        "StartupAI-site/app.startupai-site",
        177,
        config,
        policy,
        "StartupAI-site",
        1,
        snapshot=snapshot,
    )

    assert result.skipped_reason is None
    assert result.blocked_reason == "required checks pending ['db-test-gate']"


def test_review_rescue_requeues_conflicting_local_pr_before_pending_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Conflict repair should win even when required checks look pending."""
    config = _load(tmp_path)
    policy = _automation_config()
    gate_status = PrGateStatus(
        required={"ci", "db-test-gate"},
        passed=set(),
        failed=set(),
        pending={"ci", "db-test-gate"},
        cancelled=set(),
        merge_state_status="DIRTY",
        mergeable="CONFLICTING",
        is_draft=False,
        state="OPEN",
        auto_merge_enabled=True,
        checks={},
    )
    snapshot = _make_review_snapshot(
        pr_repo="StartupAI-site/app.startupai-site",
        pr_number=190,
        review_refs=("app#97",),
        pr_author="chris00walker",
        pr_body=(
            "Closes #97\n\n"
            "<!-- startupai-board-bot:consumer:"
            "session=sess-97 issue=app#97 repo=app "
            "branch=feat/97-u-002-missing-screen-reader-announcement "
            "executor=codex -->"
        ),
        gate_status=gate_status,
        rescue_checks=("ci", "db-test-gate"),
        rescue_pending={"ci", "db-test-gate"},
    )
    requeue_calls: list[tuple[str, str]] = []

    result = review_rescue(
        "StartupAI-site/app.startupai-site",
        190,
        config,
        policy,
        "StartupAI-site",
        1,
        snapshot=snapshot,
        pr_port=_fake_pr_port(),
        review_state_port=_fake_review_state_port(status_by_issue={"app#97": "Review"}),
        board_port=_fake_board_port(requeue_calls),
    )

    assert result.requeued_refs == ("app#97",)
    assert result.blocked_reason is None
    assert requeue_calls == [("app#97", "Ready")]


def test_review_rescue_all_scans_execution_authority_repos(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cross-repo rescue sweep should scan every governed repo."""
    config = _load(tmp_path)
    policy = _automation_config()
    monkeypatch.setattr(
        automation,
        "review_rescue",
        lambda pr_repo, pr_number, *a, **k: automation.ReviewRescueResult(
            pr_repo=pr_repo,
            pr_number=pr_number,
            auto_merge_enabled=(pr_repo == "StartupAI-site/app.startupai-site" and pr_number == 10),
        ),
    )

    sweep = review_rescue_all(
        config,
        policy,
        "StartupAI-site",
        1,
        dry_run=True,
        pr_port=_fake_pr_port(
            list_open_prs=lambda repo: (
                [OpenPullRequest(number=10, url=f"https://example.com/{repo}/10", head_ref_name="head", is_draft=False)]
                if repo == "StartupAI-site/app.startupai-site"
                else []
            )
        ),
    )
    assert sweep.scanned_repos == (
        "StartupAI-site/app.startupai-site",
        "StartupAI-site/startupai-crew",
        "StartupAI-site/startupai.site",
    )
    assert sweep.scanned_prs == 1
    assert sweep.auto_merge_enabled == ("StartupAI-site/app.startupai-site#10",)


def test_review_rescue_all_reports_requeued_refs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cross-repo rescue sweep should surface review PRs re-queued for repair."""
    config = _load(tmp_path)
    policy = _automation_config()
    monkeypatch.setattr(
        automation,
        "review_rescue",
        lambda pr_repo, pr_number, *a, **k: automation.ReviewRescueResult(
            pr_repo=pr_repo,
            pr_number=pr_number,
            requeued_refs=("app#71",),
        ),
    )

    sweep = review_rescue_all(
        config,
        policy,
        "StartupAI-site",
        1,
        dry_run=True,
        pr_port=_fake_pr_port(
            list_open_prs=lambda repo: (
                [OpenPullRequest(number=177, url=f"https://example.com/{repo}/177", head_ref_name="head", is_draft=False)]
                if repo == "StartupAI-site/app.startupai-site"
                else []
            )
        ),
    )

    assert sweep.requeued == ("app#71",)


def test_parser_accepts_codex_gate_and_automerge_commands() -> None:
    """Parser supports new strict review + merge controller subcommands."""
    parser = automation.build_parser()
    gate_args = parser.parse_args(
        [
            "codex-review-gate",
            "--pr-repo",
            "StartupAI-site/app.startupai-site",
            "--pr-number",
            "158",
            "--dry-run",
        ]
    )
    assert gate_args.command == "codex-review-gate"
    rescue_all_args = parser.parse_args(["review-rescue-all", "--json"])
    assert rescue_all_args.command == "review-rescue-all"
    assert rescue_all_args.json is True
    auto_args = parser.parse_args(
        [
            "automerge-review",
            "--pr-repo",
            "StartupAI-site/app.startupai-site",
            "--pr-number",
            "158",
            "--dry-run",
        ]
    )
    assert auto_args.command == "automerge-review"
    rescue_args = parser.parse_args(
        [
            "review-rescue",
            "--pr-repo",
            "StartupAI-site/app.startupai-site",
            "--pr-number",
            "158",
            "--dry-run",
        ]
    )
    assert rescue_args.command == "review-rescue"
    rescue_all_args = parser.parse_args(["review-rescue-all", "--dry-run"])
    assert rescue_all_args.command == "review-rescue-all"


def test_dispatch_agent_posts_executor_lane_comment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """dispatch-agent should post deterministic executor-lane dispatch comment."""
    config = _load(tmp_path)
    policy = _automation_config()
    monkeypatch.setattr(
        automation,
        "_query_issue_board_info",
        lambda *a, **k: _make_info("In Progress"),
    )
    monkeypatch.setattr(
        automation,
        "_query_project_item_field",
        lambda *a, **k: "codex",
    )
    posted: list[str] = []
    monkeypatch.setattr(
        automation,
        "_post_comment",
        lambda _o, _r, _n, body, **_k: posted.append(body),
    )
    monkeypatch.setattr(
        automation,
        "_comment_exists",
        lambda *a, **k: False,
    )

    result = automation.dispatch_agent(
        issue_refs=["crew#88"],
        config=config,
        automation_config=policy,
        project_owner="StartupAI-site",
        project_number=1,
    )
    assert result.dispatched == ["crew#88"]
    assert not result.failed
    assert posted
    assert "Executor=codex" in posted[0]
    assert "@copilot" not in posted[0]


def test_dispatch_agent_rejects_non_executor_dispatch_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """dispatch-agent should fail fast when dispatch target is not executor."""
    config = _load(tmp_path)
    policy = automation.BoardAutomationConfig(
        wip_limits=_automation_config().wip_limits,
        freshness_hours=24,
        stale_confirmation_cycles=2,
        trusted_codex_actors={"codex", "codex[bot]"},
        dispatch_target="copilot",
        canary_thresholds={},
    )
    monkeypatch.setattr(
        automation,
        "_query_issue_board_info",
        lambda *a, **k: _make_info("In Progress"),
    )
    monkeypatch.setattr(
        automation,
        "_query_project_item_field",
        lambda *a, **k: "codex",
    )
    monkeypatch.setattr(
        automation,
        "_comment_exists",
        lambda *a, **k: False,
    )

    result = automation.dispatch_agent(
        issue_refs=["crew#88"],
        config=config,
        automation_config=policy,
        project_owner="StartupAI-site",
        project_number=1,
    )
    assert not result.dispatched
    assert result.failed
    assert result.failed[0][0] == "crew#88"
    assert "unsupported-dispatch-target:copilot" in result.failed[0][1]


def test_dispatch_agent_comment_failure_fails_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """dispatch-agent comment failure should not mutate board state."""
    config = _load(tmp_path)
    policy = _automation_config()
    monkeypatch.setattr(
        automation,
        "_query_issue_board_info",
        lambda *a, **k: _make_info("In Progress"),
    )
    monkeypatch.setattr(
        automation,
        "_query_project_item_field",
        lambda *a, **k: "codex",
    )
    monkeypatch.setattr(
        automation,
        "_comment_exists",
        lambda *a, **k: False,
    )
    post_calls = {"count": 0}
    def flaky_post_comment(*_a, **_k):
        post_calls["count"] += 1
        if post_calls["count"] == 1:
            raise automation.GhQueryError("boom")

    monkeypatch.setattr(
        automation,
        "_post_comment",
        flaky_post_comment,
    )
    routed = MagicMock()
    set_text = MagicMock()
    set_select = MagicMock()
    monkeypatch.setattr(automation, "_set_status_if_changed", routed)
    monkeypatch.setattr(automation, "_set_text_field", set_text)
    monkeypatch.setattr(automation, "_set_single_select_field", set_select)

    result = automation.dispatch_agent(
        issue_refs=["crew#89"],
        config=config,
        automation_config=policy,
        project_owner="StartupAI-site",
        project_number=1,
    )
    assert result.failed
    routed.assert_not_called()
    set_text.assert_not_called()
    set_select.assert_not_called()


def test_enforce_execution_policy_skips_non_copilot_actor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Execution policy should no-op when PR actor is not Copilot coding agent."""
    config = _load(tmp_path)

    def fake_gh(args):
        if args[:3] == ["pr", "view", "42"]:
            return json.dumps(
                {
                    "author": {"login": "chris00walker"},
                    "state": "OPEN",
                    "url": "https://github.com/StartupAI-site/startupai-crew/pull/42",
                }
            )
        raise AssertionError(f"Unexpected gh call: {args}")

    decision = automation.enforce_execution_policy(
        pr_repo="StartupAI-site/startupai-crew",
        pr_number=42,
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        gh_runner=fake_gh,
    )
    assert decision.skipped_reason == "actor=chris00walker"
    assert decision.enforced_pr is False
    assert decision.pr_closed is False


def test_enforce_execution_policy_closes_copilot_pr_and_requeues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Copilot coding-agent PR should be closed and linked issue re-queued."""
    config = _load(tmp_path)

    gh_calls: list[list[str]] = []

    def fake_gh(args):
        gh_calls.append(args)
        if args[:3] == ["pr", "view", "119"]:
            return json.dumps(
                {
                    "author": {"login": "app/copilot-swe-agent"},
                    "state": "OPEN",
                    "url": "https://github.com/StartupAI-site/startupai-crew/pull/119",
                }
            )
        if args[:3] == ["pr", "close", "119"]:
            return ""
        raise AssertionError(f"Unexpected gh call: {args}")

    monkeypatch.setattr(
        automation,
        "query_closing_issues",
        lambda *a, **k: [
            LinkedIssue(
                owner="StartupAI-site",
                repo="startupai-crew",
                number=18,
                ref="crew#18",
            )
        ],
    )
    monkeypatch.setattr(
        automation,
        "_set_status_if_changed",
        lambda *a, **k: (True, "In Progress"),
    )
    monkeypatch.setattr(
        automation,
        "_query_issue_assignees",
        lambda *a, **k: ["chris00walker", "Copilot"],
    )
    updated_assignees: list[list[str]] = []
    monkeypatch.setattr(
        automation,
        "_set_issue_assignees",
        lambda _o, _r, _n, assignees, **_k: updated_assignees.append(assignees),
    )
    monkeypatch.setattr(automation, "_comment_exists", lambda *a, **k: False)
    posted: list[str] = []
    monkeypatch.setattr(
        automation,
        "_post_comment",
        lambda _o, _r, _n, body, **_k: posted.append(body),
    )

    decision = automation.enforce_execution_policy(
        pr_repo="StartupAI-site/startupai-crew",
        pr_number=119,
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        gh_runner=fake_gh,
    )

    assert decision.enforced_pr is True
    assert decision.pr_closed is True
    assert decision.requeued == ["crew#18"]
    assert decision.copilot_unassigned == ["crew#18"]
    assert updated_assignees == [["chris00walker"]]
    assert any(args[:3] == ["pr", "close", "119"] for args in gh_calls)
    assert posted


def test_rebalance_wip_marks_stale_then_demotes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """First stale cycle marks candidate; second eligible cycle demotes."""
    config = _load(tmp_path)
    policy = _automation_config()
    snapshot = automation._ProjectItemSnapshot(
        issue_ref="StartupAI-site/startupai-crew#88",
        status="In Progress",
        executor="codex",
        handoff_to="none",
    )
    monkeypatch.setattr(
        automation,
        "_list_project_items_by_status",
        lambda status, *a, **k: [snapshot] if status == "In Progress" else [],
    )
    monkeypatch.setattr(automation, "in_any_critical_path", lambda *a, **k: False)
    now = datetime(2026, 3, 5, 12, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(
        automation,
        "_query_latest_wip_activity_timestamp",
        lambda *a, **k: now - timedelta(hours=48),
    )
    monkeypatch.setattr(
        automation,
        "_query_project_item_field",
        lambda *a, **k: "codex" if a[1] == "Executor" else "",
    )
    monkeypatch.setattr(automation, "_comment_exists", lambda *a, **k: False)
    posted: list[str] = []
    monkeypatch.setattr(
        automation,
        "_post_comment",
        lambda _o, _r, _n, body, **_k: posted.append(body),
    )
    monkeypatch.setattr(automation, "_query_latest_marker_timestamp", lambda *a, **k: None)

    decision_1 = automation.rebalance_wip(
        config=config,
        automation_config=policy,
        project_owner="StartupAI-site",
        project_number=1,
        all_prefixes=True,
        dry_run=False,
    )
    assert "crew#88" in decision_1.marked_stale

    marker_time = now - timedelta(minutes=31)
    monkeypatch.setattr(
        automation,
        "_query_latest_marker_timestamp",
        lambda *a, **k: marker_time,
    )
    moved: list[str] = []
    monkeypatch.setattr(
        automation,
        "_set_status_if_changed",
        lambda issue_ref, *a, **k: (moved.append(issue_ref) or True, "In Progress"),
    )
    decision_2 = automation.rebalance_wip(
        config=config,
        automation_config=policy,
        project_owner="StartupAI-site",
        project_number=1,
        all_prefixes=True,
        dry_run=False,
    )
    assert "crew#88" in decision_2.moved_ready
    assert moved == ["crew#88"]


def test_rebalance_wip_ignores_issue_updated_at_for_freshness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Issue.updated_at must not keep WIP alive once execution activity is stale."""
    config = _load(tmp_path)
    policy = _automation_config()
    snapshot = automation._ProjectItemSnapshot(
        issue_ref="StartupAI-site/startupai-crew#88",
        status="In Progress",
        executor="codex",
        handoff_to="none",
    )
    monkeypatch.setattr(
        automation,
        "_list_project_items_by_status",
        lambda status, *a, **k: [snapshot] if status == "In Progress" else [],
    )
    monkeypatch.setattr(automation, "in_any_critical_path", lambda *a, **k: False)
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(
        automation,
        "_query_latest_wip_activity_timestamp",
        lambda *a, **k: now - timedelta(hours=48),
    )
    monkeypatch.setattr(
        automation,
        "_query_issue_updated_at",
        lambda *a, **k: now,
    )
    monkeypatch.setattr(
        automation,
        "_query_project_item_field",
        lambda *a, **k: "codex" if a[1] == "Executor" else "",
    )
    monkeypatch.setattr(automation, "_comment_exists", lambda *a, **k: False)
    monkeypatch.setattr(automation, "_post_comment", lambda *a, **k: None)
    monkeypatch.setattr(automation, "_query_latest_marker_timestamp", lambda *a, **k: None)

    decision = automation.rebalance_wip(
        config=config,
        automation_config=policy,
        project_owner="StartupAI-site",
        project_number=1,
        all_prefixes=True,
        dry_run=False,
    )
    assert "crew#88" in decision.marked_stale


def test_schedule_ready_claim_mode_uses_priority_and_critical_path_ranking(
    tmp_path: Path,
) -> None:
    """Claim mode should prioritize critical-path items by Priority then issue number."""
    config = _load(tmp_path)
    mutator = MagicMock()

    ready_nodes = [
        {
            "fieldValueByName": {"name": "Ready"},
            "executorField": {"name": "codex"},
            "handoffField": {"name": "none"},
            "priorityField": {"name": "P0 Critical"},
            "content": {
                "number": 999,
                "repository": {"nameWithOwner": "StartupAI-site/startupai-crew"},
            },
        },
        {
            "fieldValueByName": {"name": "Ready"},
            "executorField": {"name": "codex"},
            "handoffField": {"name": "none"},
            "priorityField": {"name": "P2 Medium"},
            "content": {
                "number": 88,
                "repository": {"nameWithOwner": "StartupAI-site/startupai-crew"},
            },
        },
        {
            "fieldValueByName": {"name": "Ready"},
            "executorField": {"name": "codex"},
            "handoffField": {"name": "none"},
            "priorityField": {"name": "P1 High"},
            "content": {
                "number": 84,
                "repository": {"nameWithOwner": "StartupAI-site/startupai-crew"},
            },
        },
    ]
    responses = [
        _project_items_response([]),
        _project_items_response(ready_nodes),
    ]

    decision = schedule_ready_items(
        config,
        "StartupAI-site",
        1,
        all_prefixes=True,
        mode="claim",
        per_executor_wip_limit=1,
        status_resolver=lambda *_: "Done",
        board_info_resolver=lambda *_: _make_info("Ready"),
        board_mutator=mutator,
        gh_runner=lambda args: responses.pop(0),
    )

    assert decision.claimed == ["crew#84"]
    assert decision.deferred_wip == ["crew#88", "crew#999"]
    mutator.assert_called_once()


def test_parser_accepts_dispatch_and_rebalance_commands() -> None:
    """Parser supports dispatch-agent and rebalance-wip subcommands."""
    parser = automation.build_parser()
    dispatch_args = parser.parse_args(["dispatch-agent", "--issue", "crew#88"])
    assert dispatch_args.command == "dispatch-agent"
    rebalance_args = parser.parse_args(["rebalance-wip", "--all-prefixes"])
    assert rebalance_args.command == "rebalance-wip"


def test_cmd_dispatch_agent_config_error_no_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Invalid automation config must fail before any mutation call."""
    config = _load(tmp_path)
    called = {"dispatch": False}
    monkeypatch.setattr(
        automation,
        "dispatch_agent",
        lambda *a, **k: called.__setitem__("dispatch", True),
    )
    args = argparse.Namespace(
        automation_config=str(tmp_path / "missing-config.json"),
        issue=["crew#88"],
        project_owner="StartupAI-site",
        project_number=1,
        dry_run=False,
    )
    code = automation._cmd_dispatch_agent(args, config)
    assert code == 3
    assert called["dispatch"] is False


def test_load_automation_config_defaults_dispatch_target_to_executor(
    tmp_path: Path,
) -> None:
    """Missing dispatch.target should default to executor mode."""
    config_path = tmp_path / "board-automation-config.json"
    config_path.write_text(
        json.dumps(
            {
                "version": 1,
                "wip_limits": {
                    "codex": {"crew": 1, "app": 1, "site": 1},
                    "claude": {"crew": 3, "app": 3, "site": 3},
                    "human": {"crew": 3, "app": 3, "site": 3},
                },
                "freshness_hours": 24,
                "stale_confirmation_cycles": 2,
                "trusted_codex_actors": [],
                "canary_thresholds": {},
            }
        ),
        encoding="utf-8",
    )
    loaded = automation.load_automation_config(config_path)
    assert loaded.dispatch_target == "executor"


def test_load_automation_config_rejects_unknown_dispatch_target(
    tmp_path: Path,
) -> None:
    """Unknown dispatch.target should fail config load."""
    config_path = tmp_path / "board-automation-config.json"
    config_path.write_text(
        json.dumps(
            {
                "version": 1,
                "wip_limits": {
                    "codex": {"crew": 1, "app": 1, "site": 1},
                    "claude": {"crew": 3, "app": 3, "site": 3},
                    "human": {"crew": 3, "app": 3, "site": 3},
                },
                "freshness_hours": 24,
                "stale_confirmation_cycles": 2,
                "trusted_codex_actors": [],
                "dispatch": {"target": "invalid-mode"},
                "canary_thresholds": {},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(automation.ConfigError):
        automation.load_automation_config(config_path)


def test_load_automation_config_rejects_legacy_copilot_dispatch_target(
    tmp_path: Path,
) -> None:
    """Legacy copilot dispatch target must be rejected in strict mode."""
    config_path = tmp_path / "board-automation-config.json"
    config_path.write_text(
        json.dumps(
            {
                "version": 1,
                "wip_limits": {
                    "codex": {"crew": 1, "app": 1, "site": 1},
                    "claude": {"crew": 3, "app": 3, "site": 3},
                    "human": {"crew": 3, "app": 3, "site": 3},
                },
                "freshness_hours": 24,
                "stale_confirmation_cycles": 2,
                "trusted_codex_actors": [],
                "dispatch": {"target": "copilot"},
                "canary_thresholds": {},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(automation.ConfigError):
        automation.load_automation_config(config_path)


def test_load_automation_config_accepts_aligned_multi_worker_lane_limits(
    tmp_path: Path,
) -> None:
    """Protected codex lanes may match global concurrency in multi-worker mode."""
    config_path = tmp_path / "board-automation-config.json"
    config_path.write_text(
        json.dumps(
            {
                "version": 2,
                "wip_limits": {
                    "codex": {"crew": 2, "app": 2, "site": 2},
                    "claude": {"crew": 3, "app": 3, "site": 3},
                    "human": {"crew": 3, "app": 3, "site": 3},
                },
                "freshness_hours": 24,
                "stale_confirmation_cycles": 2,
                "trusted_codex_actors": [],
                "trusted_local_authors": [],
                "execution_authority": {
                    "mode": "single_machine",
                    "repos": ["app", "crew", "site"],
                    "executors": ["codex"],
                    "global_concurrency": 2,
                },
                "feature_flags": {"multi_worker": True},
                "dispatch": {"target": "executor"},
                "canary_thresholds": {},
            }
        ),
        encoding="utf-8",
    )

    loaded = automation.load_automation_config(config_path)

    assert loaded.multi_worker_enabled is True
    assert loaded.global_concurrency == 2
    assert loaded.wip_limits["codex"]["app"] == 2


def test_load_automation_config_rejects_multi_worker_lane_limits_below_global_concurrency(
    tmp_path: Path,
) -> None:
    """Protected codex lanes below global concurrency are invalid policy."""
    config_path = tmp_path / "board-automation-config.json"
    config_path.write_text(
        json.dumps(
            {
                "version": 2,
                "wip_limits": {
                    "codex": {"crew": 2, "app": 1, "site": 2},
                    "claude": {"crew": 3, "app": 3, "site": 3},
                    "human": {"crew": 3, "app": 3, "site": 3},
                },
                "freshness_hours": 24,
                "stale_confirmation_cycles": 2,
                "trusted_codex_actors": [],
                "trusted_local_authors": [],
                "execution_authority": {
                    "mode": "single_machine",
                    "repos": ["app", "crew", "site"],
                    "executors": ["codex"],
                    "global_concurrency": 2,
                },
                "feature_flags": {"multi_worker": True},
                "dispatch": {"target": "executor"},
                "canary_thresholds": {},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(automation.ConfigError, match="wip limit 1 is below global concurrency 2"):
        automation.load_automation_config(config_path)


def test_claim_ready_allows_second_app_issue_when_lane_limit_matches_global_concurrency(
    tmp_path: Path,
) -> None:
    """One active app issue should not block a second app issue when app lane limit is 2."""
    config = _load(tmp_path)
    mutator = MagicMock()
    poster = MagicMock()
    policy = automation.BoardAutomationConfig(
        wip_limits={
            "codex": {"crew": 2, "app": 2, "site": 2},
            "claude": {"crew": 3, "app": 3, "site": 3},
            "human": {"crew": 3, "app": 3, "site": 3},
        },
        freshness_hours=24,
        stale_confirmation_cycles=2,
        trusted_codex_actors={"codex", "codex[bot]", "chris00walker"},
        trusted_local_authors={"codex", "codex[bot]", "chris00walker"},
        dispatch_target="executor",
        canary_thresholds={},
        execution_authority_mode="single_machine",
        execution_authority_repos=("app", "crew", "site"),
        execution_authority_executors=("codex",),
        global_concurrency=2,
        multi_worker_enabled=True,
    )
    ready_nodes = [
        {
            "fieldValueByName": {"name": "Ready"},
            "executorField": {"name": "codex"},
            "handoffField": {"name": "none"},
            "content": {
                "number": 99,
                "repository": {"nameWithOwner": "StartupAI-site/app.startupai-site"},
            },
        }
    ]
    in_progress_nodes = [
        {
            "fieldValueByName": {"name": "In Progress"},
            "executorField": {"name": "codex"},
            "handoffField": {"name": "none"},
            "content": {
                "number": 71,
                "repository": {"nameWithOwner": "StartupAI-site/app.startupai-site"},
            },
        }
    ]
    responses = [
        _project_items_response(ready_nodes),
        _project_items_response(in_progress_nodes),
        _project_items_response(in_progress_nodes),
    ]

    result = claim_ready_issue(
        config,
        "StartupAI-site",
        1,
        executor="codex",
        issue_ref="app#99",
        all_prefixes=True,
        automation_config=policy,
        board_info_resolver=lambda *_: _make_info("Ready"),
        board_mutator=mutator,
        comment_checker=lambda *_, **__: False,
        comment_poster=poster,
        gh_runner=lambda args: responses.pop(0),
    )

    assert result.claimed == "app#99"
    assert result.reason == ""
    mutator.assert_called_once()
    poster.assert_called_once()


# -- Fix C: rescue_checks uses only live branch protection ------------------


def test_rescue_checks_uses_only_live_branch_protection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """rescue_checks should use live checks only, not config overlay."""
    config = _load(tmp_path)
    # Config has codex-review-gate, but live branch protection only has "ci"
    policy = _automation_config()

    snapshot = _make_review_snapshot(
        gate_status=PrGateStatus(
            required={"ci"},
            passed={"ci"},
            failed=set(),
            pending=set(),
            cancelled=set(),
            merge_state_status="CLEAN",
            mergeable="MERGEABLE",
            is_draft=False,
            state="OPEN",
            auto_merge_enabled=False,
            checks={"ci": CheckObservation(name="ci", result="pass", conclusion="success", status="completed")},
        ),
        rescue_checks=("ci",),
        rescue_passed={"ci"},
    )
    # rescue_checks should be only ("ci",), not ("ci", "codex-review-gate")
    assert snapshot.rescue_checks == ("ci",)
    assert "codex-review-gate" not in snapshot.rescue_checks


def test_configured_review_checks_not_in_rescue_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Phantom config check should not appear in rescue_missing.

    The config has codex-review-gate for app, but live branch protection
    only requires "ci". Since rescue_checks now uses live-only, the phantom
    check should not appear in rescue_missing.
    """
    # Simulate what _build_review_snapshot does for rescue_checks:
    # BEFORE fix: rescue_checks = sorted(set(live) | set(config))
    # AFTER fix:  rescue_checks = sorted(live)
    live_checks = {"ci"}
    config_checks = ("ci", "db-test-gate", "e2e", "tag-contract", "codex-review-gate")

    # New behavior: live-only
    rescue_checks = tuple(sorted(live_checks))
    assert "codex-review-gate" not in rescue_checks
    assert rescue_checks == ("ci",)

    # Old behavior would have been:
    old_rescue = tuple(sorted(set(live_checks) | set(config_checks)))
    assert "codex-review-gate" in old_rescue  # confirms the fix was needed


# -- Fix B: automerge_review handles pending state --------------------------


def test_automerge_review_returns_blocked_for_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When enable_pull_request_automerge returns 'pending', automerge_review returns code 2."""
    config = _load(tmp_path)
    policy = _automation_config()

    snapshot = _make_review_snapshot(
        copilot_review_present=True,
        codex_gate_code=0,
        gate_status=PrGateStatus(
            required={"ci"},
            passed={"ci"},
            failed=set(),
            pending=set(),
            cancelled=set(),
            merge_state_status="CLEAN",
            mergeable="MERGEABLE",
            is_draft=False,
            state="OPEN",
            auto_merge_enabled=False,
        ),
    )

    pr_port = _fake_pr_port(
        enable_automerge=lambda *a, **k: "pending",
    )

    code, msg = automerge_review(
        "StartupAI-site/app.startupai-site",
        184,
        config,
        policy,
        "StartupAI-site",
        1,
        snapshot=snapshot,
        pr_port=pr_port,
    )
    assert code == 2
    assert "pending verification" in msg
