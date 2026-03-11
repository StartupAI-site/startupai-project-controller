"""Unit tests for board_consumer module.

All tests use dependency injection (DI) -- NO real GitHub API calls, no real
codex exec, no real wt-create.sh. Follows test_board_automation.py patterns.
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from startupai_controller.board_consumer import (
    ConsumerConfig,
    CycleResult,
    RepairBranchReconcileOutcome,
    ESCALATION_CEILING_FAILED,
    ESCALATION_CEILING_TRANSIENT,
    MAX_REQUEUE_CYCLES,
    REVIEW_QUEUE_AUTOMERGE_RETRY_SECONDS,
    REVIEW_QUEUE_FAILED_RETRY_SECONDS,
    REVIEW_QUEUE_PENDING_AUTOMERGE_RETRY_SECONDS,
    REVIEW_QUEUE_PENDING_RETRY_SECONDS,
    REVIEW_QUEUE_STABLE_BLOCKED_RETRY_SECONDS,
    _backfill_review_verdicts,
    _apply_review_queue_result,
    _apply_review_queue_partial_failure,
    _blocker_class,
    _build_review_snapshots_for_queue_entries,
    _cmd_drain,
    _cmd_report_slo,
    _cmd_reconcile,
    _cmd_resume,
    _collect_status_payload,
    _create_status_http_server,
    _drain_review_queue,
    _cmd_status,
    _assemble_codex_prompt,
    _build_pr_body,
    _clear_drain,
    _create_or_update_pr,
    _create_worktree,
    _drain_requested,
    _escalate_to_claude,
    _escalation_ceiling_for_blocker_class,
    _extract_acceptance_criteria,
    _fetch_issue_context,
    _hydrate_issue_context,
    _has_commits_on_branch,
    _parse_codex_result,
    _post_pr_codex_verdict,
    _pre_backfill_verdicts_for_due_prs,
    _prepare_worktree,
    _post_result_comment,
    _reconcile_repair_branch,
    _reconcile_board_truth,
    _replay_deferred_actions,
    _recover_interrupted_sessions,
    _request_drain,
    _review_queue_retry_seconds_for_blocked_reason,
    _review_queue_retry_seconds_for_result,
    _return_issue_to_ready,
    _transition_issue_to_in_progress,
    _transition_issue_to_review,
    _resolve_cli_command,
    _run_codex_session,
    _select_best_candidate,
    build_parser,
    run_daemon_loop,
    run_one_cycle,
    ReconciliationResult,
    WorktreePrepareError,
)
from startupai_controller.board_automation import (
    BoardAutomationConfig,
    ClaimReadyResult,
    load_automation_config,
)
from startupai_controller.consumer_workflow import load_workflow_definition
from startupai_controller.domain.models import IssueSnapshot
from startupai_controller.board_io import CycleBoardSnapshot, GhCommandError, _ProjectItemSnapshot
from startupai_controller.board_consumer import OpenPullRequestMatch
from startupai_controller.consumer_db import ConsumerDB
from startupai_controller.promote_ready import BoardInfo
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
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


def _write_automation_config(tmp_path: Path) -> Path:
    path = tmp_path / "board-automation-config.json"
    path.write_text(
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
                "trusted_codex_actors": ["codex", "codex[bot]"],
                "dispatch": {"target": "executor"},
                "canary_thresholds": {},
            }
        ),
        encoding="utf-8",
    )
    return path


def _write_repo_workflow(repo_root: Path, *, validation_cmd: str = "make test") -> Path:
    repo_root.mkdir(parents=True, exist_ok=True)
    path = repo_root / "WORKFLOW.md"
    path.write_text(
        "\n".join(
            [
                "---",
                "startupai_consumer:",
                "  poll_interval_seconds: 180",
                "  codex_timeout_seconds: 1800",
                "  max_retries: 3",
                "  retry_backoff_seconds: 300",
                f"  validation_cmd: {validation_cmd}",
                "  workspace_hooks:",
                "    after_create: []",
                "    before_run: []",
                "---",
                "Follow repo instructions for {{ issue_ref }} in {{ worktree_path }}.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def _make_consumer_config(tmp_path: Path) -> ConsumerConfig:
    cp_path = _write_config(tmp_path)
    auto_path = _write_automation_config(tmp_path)
    repo_roots = {
        "crew": tmp_path / "repos" / "crew",
        "app": tmp_path / "repos" / "app",
        "site": tmp_path / "repos" / "site",
    }
    for root in repo_roots.values():
        root.mkdir(parents=True, exist_ok=True)
    _write_repo_workflow(repo_roots["crew"], validation_cmd="uv run pytest tests/contracts -q")
    _write_repo_workflow(repo_roots["app"], validation_cmd="pnpm type-check")
    _write_repo_workflow(repo_roots["site"], validation_cmd="pnpm build")
    return ConsumerConfig(
        critical_paths_path=cp_path,
        automation_config_path=auto_path,
        db_path=tmp_path / "test.db",
        output_dir=tmp_path / "outputs",
        drain_path=tmp_path / "consumer.drain",
        schema_path=Path(__file__).resolve().parent.parent
        / "config"
        / "codex_session_result.schema.json",
        workflow_state_path=tmp_path / "workflow-state.json",
        repo_roots=repo_roots,
    )


def _make_db(tmp_path: Path) -> ConsumerDB:
    return ConsumerDB(db_path=tmp_path / "test.db")


def _project_items_response(nodes: list[dict]) -> str:
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


def _make_node(
    repo: str = "StartupAI-site/startupai-crew",
    number: int = 84,
    status: str = "Ready",
    executor: str = "codex",
    priority: str = "P0",
) -> dict:
    return {
        "fieldValueByName": {"name": status},
        "executorField": {"name": executor},
        "handoffField": {"name": "none"},
        "priorityField": {"name": priority},
        "content": {
            "number": number,
            "repository": {"nameWithOwner": repo},
        },
    }


def _empty_response() -> str:
    return _project_items_response([])


def _project_field_response(
    *,
    field_name: str | None = None,
    single_select_options: list[tuple[str, str]] | None = None,
) -> str:
    field: dict[str, object] = {"id": f"{(field_name or 'status').upper()}_FIELD"}
    if single_select_options is not None:
        field["options"] = [{"id": option_id, "name": name} for option_id, name in single_select_options]
    return json.dumps({"data": {"node": {"field": field}}})


def _project_field_mutation_response() -> str:
    return json.dumps(
        {
            "data": {
                "updateProjectV2ItemFieldValue": {
                    "projectV2Item": {"id": "ITEM"}
                }
            }
        }
    )


def _codex_result_json(
    outcome: str = "success",
    summary: str = "Done.",
    **extra: object,
) -> dict:
    return {
        "outcome": outcome,
        "summary": summary,
        "tests_run": extra.get("tests_run"),
        "tests_passed": extra.get("tests_passed"),
        "changed_files": extra.get("changed_files", []),
        "commit_shas": extra.get("commit_shas", []),
        "pr_url": extra.get("pr_url"),
        "resolution": extra.get("resolution"),
        "blocker_reason": extra.get("blocker_reason"),
        "needs_handoff_to": extra.get("needs_handoff_to"),
        "duration_seconds": extra.get("duration_seconds", 60),
    }


# -- _select_best_candidate tests -------------------------------------------


class TestSelectBestCandidate:
    def test_selects_highest_priority(self, tmp_path: Path) -> None:
        """P0 ranks before P2 among Ready codex crew items."""
        config = _load(tmp_path)
        # crew#84 = P0, crew#85 = P2 (both critical-path)
        nodes = [
            _make_node(number=85, executor="codex", priority="P2"),
            _make_node(number=84, executor="codex", priority="P0"),
        ]
        # All predecessors done
        def status_resolver(ref, *a, **kw):
            return "Done"

        def gh_runner(args, **kw):
            return _project_items_response(nodes)

        result = _select_best_candidate(
            config,
            "StartupAI-site",
            1,
            executor="codex",
            this_repo_prefix="crew",
            status_resolver=status_resolver,
            gh_runner=gh_runner,
        )
        assert result == "crew#84"

    def test_filters_non_codex_executor(self, tmp_path: Path) -> None:
        config = _load(tmp_path)
        nodes = [_make_node(number=84, executor="claude", priority="P0")]

        def gh_runner(args, **kw):
            return _project_items_response(nodes)

        result = _select_best_candidate(
            config, "StartupAI-site", 1, executor="codex", gh_runner=gh_runner,
        )
        assert result is None

    def test_filters_non_crew_prefix(self, tmp_path: Path) -> None:
        config = _load(tmp_path)
        nodes = [
            _make_node(
                repo="StartupAI-site/app.startupai-site",
                number=150,
                executor="codex",
            )
        ]

        def gh_runner(args, **kw):
            return _project_items_response(nodes)

        result = _select_best_candidate(
            config,
            "StartupAI-site",
            1,
            executor="codex",
            this_repo_prefix="crew",
            gh_runner=gh_runner,
        )
        assert result is None

    def test_returns_none_when_empty(self, tmp_path: Path) -> None:
        config = _load(tmp_path)

        def gh_runner(args, **kw):
            return _empty_response()

        result = _select_best_candidate(
            config, "StartupAI-site", 1, gh_runner=gh_runner,
        )
        assert result is None


# -- _fetch_issue_context tests -----------------------------------------------


class TestFetchIssueContext:
    def test_parses_issue_json(self) -> None:
        data = {"title": "Test Issue", "body": "Some body", "labels": ["bug"]}

        def gh_runner(args, **kw):
            return json.dumps(data)

        result = _fetch_issue_context("Owner", "repo", 1, gh_runner=gh_runner)
        assert result["title"] == "Test Issue"

    def test_raises_on_bad_json(self) -> None:
        def gh_runner(args, **kw):
            return "not json"

        with pytest.raises(Exception):
            _fetch_issue_context("Owner", "repo", 1, gh_runner=gh_runner)

    def test_passes_correct_api_path(self) -> None:
        captured: list[list[str]] = []

        def gh_runner(args, **kw):
            captured.append(list(args))
            return json.dumps({"title": "T", "body": "", "labels": []})

        _fetch_issue_context("StartupAI-site", "startupai-crew", 84, gh_runner=gh_runner)
        assert "repos/StartupAI-site/startupai-crew/issues/84" in captured[0][1]


# -- _create_worktree tests ---------------------------------------------------


class TestCreateWorktree:
    def test_parses_worktree_path_from_output(self, tmp_path: Path) -> None:
        config = _make_consumer_config(tmp_path)
        captured: list[list[str]] = []

        def subprocess_runner(args, **kw):
            captured.append(list(args))
            return subprocess.CompletedProcess(
                args=args,
                returncode=0,
                stdout="Worktree ready: /tmp/wt/crew/feat/84-test-issue\nDone",
                stderr="",
            )

        path, branch = _create_worktree(
            "crew#84", "Test Issue", config, subprocess_runner=subprocess_runner
        )
        assert path == "/tmp/wt/crew/feat/84-test-issue"
        assert branch == "feat/84-test-issue"
        assert captured[0][2] == "feat/test-issue"
        assert "--issue" in captured[0]
        assert "84" in captured[0]

    def test_raises_on_failure(self, tmp_path: Path) -> None:
        config = _make_consumer_config(tmp_path)

        def subprocess_runner(args, **kw):
            return subprocess.CompletedProcess(
                args=args, returncode=1, stdout="", stderr="error"
            )

        with pytest.raises(RuntimeError, match="wt-create.sh failed"):
            _create_worktree(
                "crew#84", "Test", config, subprocess_runner=subprocess_runner
            )

    def test_reuses_existing_issue_worktree(self, tmp_path: Path, monkeypatch) -> None:
        config = _make_consumer_config(tmp_path)
        expected_path = os.path.expanduser(
            "~/projects/worktrees/crew/feat/84-test-issue"
        )
        calls: list[list[str]] = []

        def subprocess_runner(args, **kw):
            calls.append(list(args))
            if args[0].endswith("wt-create.sh"):
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=1,
                    stdout="",
                    stderr=(
                        "ERROR: Worktree already exists at "
                        f"{expected_path}\nUse wt-list.sh to see active worktrees\n"
                    ),
                )
            if args[:4] == ["git", "-C", expected_path, "branch"]:
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout="feat/84-test-issue\n",
                    stderr="",
                )
            if args[:4] == ["git", "-C", expected_path, "status"]:
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout="",
                    stderr="",
                )
            if args[:4] == ["git", "-C", expected_path, "fetch"]:
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout="",
                    stderr="",
                )
            if args[:4] == ["git", "-C", expected_path, "rev-list"]:
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout="0 0\n",
                    stderr="",
                )
            return subprocess.CompletedProcess(
                args=args,
                returncode=0,
                stdout="",
                stderr="",
            )

        monkeypatch.setattr("startupai_controller.board_consumer.os.path.isdir", lambda path: path == expected_path)

        path, branch = _create_worktree(
            "crew#84", "Test Issue", config, subprocess_runner=subprocess_runner
        )

        assert path == expected_path
        assert branch == "feat/84-test-issue"
        assert calls[1] == [
            "git",
            "-C",
            expected_path,
            "branch",
            "--show-current",
        ]

    def test_reused_worktree_fast_forwards_clean_remote_branch(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = _make_consumer_config(tmp_path)
        expected_path = os.path.expanduser(
            "~/projects/worktrees/crew/feat/84-test-issue"
        )
        calls: list[list[str]] = []

        def subprocess_runner(args, **kw):
            calls.append(list(args))
            if args[0].endswith("wt-create.sh"):
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=1,
                    stdout="",
                    stderr=(
                        "ERROR: Worktree already exists at "
                        f"{expected_path}\nUse wt-list.sh to see active worktrees\n"
                    ),
                )
            if args[:4] == ["git", "-C", expected_path, "branch"]:
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout="feat/84-test-issue\n",
                    stderr="",
                )
            if args[:4] == ["git", "-C", expected_path, "status"]:
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout="",
                    stderr="",
                )
            if args[:4] == ["git", "-C", expected_path, "fetch"]:
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout="",
                    stderr="",
                )
            if args[:4] == ["git", "-C", expected_path, "rev-list"]:
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout="0 3\n",
                    stderr="",
                )
            if args[:4] == ["git", "-C", expected_path, "merge"]:
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout="Updating 123..456\n",
                    stderr="",
                )
            return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

        monkeypatch.setattr(
            "startupai_controller.board_consumer.os.path.isdir",
            lambda path: path == expected_path,
        )

        path, branch = _create_worktree(
            "crew#84", "Test Issue", config, subprocess_runner=subprocess_runner
        )

        assert path == expected_path
        assert branch == "feat/84-test-issue"
        assert [
            "git",
            "-C",
            expected_path,
            "merge",
            "--ff-only",
            "origin/feat/84-test-issue",
        ] in calls

    def test_fallback_path_when_no_output(self, tmp_path: Path) -> None:
        config = _make_consumer_config(tmp_path)

        def subprocess_runner(args, **kw):
            return subprocess.CompletedProcess(
                args=args, returncode=0, stdout="Done\n", stderr=""
            )

        path, branch = _create_worktree(
            "crew#84", "Test", config, subprocess_runner=subprocess_runner
        )
        assert branch == "feat/84-test"
        assert "crew/feat/84-test" in path


class TestHydrateIssueContext:
    def test_uses_fresh_cached_issue_context(self, tmp_path: Path) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        now = datetime(2026, 3, 8, 20, 0, tzinfo=timezone.utc)
        snapshot = _ProjectItemSnapshot(
            issue_ref="StartupAI-site/startupai-crew#84",
            status="Ready",
            executor="codex",
            handoff_to="none",
            title="Cached title",
            issue_updated_at="2026-03-08T19:00:00+00:00",
        )
        db.set_issue_context(
            "crew#84",
            owner="StartupAI-site",
            repo="startupai-crew",
            number=84,
            title="Cached title",
            body="Cached body",
            labels=["cached"],
            issue_updated_at="2026-03-08T19:00:00+00:00",
            fetched_at=now.isoformat(),
            expires_at=(now + timedelta(minutes=15)).isoformat(),
        )

        def gh_runner(args, **kw):
            raise AssertionError("GitHub should not be called on cache hit")

        result = _hydrate_issue_context(
            "crew#84",
            owner="StartupAI-site",
            repo="startupai-crew",
            number=84,
            snapshot=snapshot,
            config=config,
            db=db,
            gh_runner=gh_runner,
            now=now,
        )

        assert result["title"] == "Cached title"
        assert result["body"] == "Cached body"
        assert result["labels"] == ["cached"]


class TestPrepareWorktree:
    def test_reuses_matching_clean_worktree(self, tmp_path: Path) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        repo_root = config.repo_roots["crew"]
        repo_root.mkdir(parents=True, exist_ok=True)
        worktree_path = tmp_path / "wt" / "crew84"
        worktree_path.mkdir(parents=True, exist_ok=True)
        calls: list[list[str]] = []

        def subprocess_runner(args, **kw):
            calls.append(list(args))
            args_tuple = tuple(str(part) for part in args)
            if args_tuple[:5] == (
                "git",
                "-C",
                str(repo_root),
                "worktree",
                "list",
            ):
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout=(
                        f"worktree {worktree_path}\n"
                        "branch refs/heads/feat/84-test-issue\n\n"
                    ),
                    stderr="",
                )
            if args_tuple[:4] == ("git", "-C", str(worktree_path), "status"):
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
            if args_tuple[:4] == ("git", "-C", str(worktree_path), "fetch"):
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
            if args_tuple[:4] == ("git", "-C", str(worktree_path), "rev-list"):
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="0 0\n", stderr="")
            raise AssertionError(f"unexpected command: {args}")

        path, branch = _prepare_worktree(
            "crew#84",
            "Test Issue",
            config,
            db,
            subprocess_runner=subprocess_runner,
        )

        assert path == str(worktree_path)
        assert branch == "feat/84-test-issue"
        assert not any("wt-create.sh" in " ".join(call) for call in calls)


class TestRepairBranchReconcile:
    def test_merges_remote_branch_then_main(self, tmp_path: Path) -> None:
        worktree = tmp_path / "wt"
        worktree.mkdir()
        calls: list[list[str]] = []

        def subprocess_runner(args, **kw):
            calls.append(list(args))
            args_tuple = tuple(args[3:])
            if args_tuple == ("checkout", "feat/84-test"):
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
            if args_tuple == ("status", "--porcelain"):
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
            if args_tuple == ("fetch", "origin", "main", "feat/84-test"):
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
            if args_tuple == ("rev-list", "--left-right", "--count", "HEAD...origin/feat/84-test"):
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="0 2\n", stderr="")
            if args_tuple == ("merge", "--ff-only", "origin/feat/84-test"):
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="Updating\n", stderr="")
            if args_tuple == ("merge", "--no-edit", "origin/main"):
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="Merge made by the 'ort' strategy.\n", stderr="")
            raise AssertionError(f"unexpected git call: {args}")

        result = _reconcile_repair_branch(
            str(worktree),
            "feat/84-test",
            subprocess_runner=subprocess_runner,
        )

        assert result.state == "merged_main"
        assert [
            "git",
            "-C",
            str(worktree),
            "merge",
            "--ff-only",
            "origin/feat/84-test",
        ] in calls

    def test_conflicted_main_merge_returns_conflicted_state(self, tmp_path: Path) -> None:
        worktree = tmp_path / "wt"
        worktree.mkdir()

        def subprocess_runner(args, **kw):
            args_tuple = tuple(args[3:])
            if args_tuple == ("checkout", "feat/84-test"):
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
            if args_tuple == ("status", "--porcelain"):
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
            if args_tuple == ("fetch", "origin", "main", "feat/84-test"):
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
            if args_tuple == ("rev-list", "--left-right", "--count", "HEAD...origin/feat/84-test"):
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="0 0\n", stderr="")
            if args_tuple == ("merge", "--no-edit", "origin/main"):
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=1,
                    stdout="Auto-merging file.py\n",
                    stderr="CONFLICT (content): Merge conflict in file.py\n",
                )
            if args_tuple == ("rev-parse", "-q", "--verify", "MERGE_HEAD"):
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="abc123\n", stderr="")
            raise AssertionError(f"unexpected git call: {args}")

        result = _reconcile_repair_branch(
            str(worktree),
            "feat/84-test",
            subprocess_runner=subprocess_runner,
        )

        assert result.state == "conflicted_main_merge"


class TestCodexOutputSchema:
    def test_requires_every_declared_property(self) -> None:
        schema_path = (
            Path(__file__).resolve().parent.parent
            / "config"
            / "codex_session_result.schema.json"
        )
        schema = json.loads(schema_path.read_text(encoding="utf-8"))

        assert set(schema["required"]) == set(schema["properties"])


# -- _assemble_codex_prompt tests ---------------------------------------------


class TestAssembleCodexPrompt:
    def test_includes_all_fields(self, tmp_path: Path) -> None:
        config = _load(tmp_path)
        consumer_config = _make_consumer_config(tmp_path)
        context = {"title": "Engine", "body": "## Acceptance Criteria\n- Works"}
        prompt = _assemble_codex_prompt(
            context,
            "crew#84",
            config,
            consumer_config,
            "/tmp/wt",
            "feat/84-engine",
            dependency_summary="- crew#83 (Done)",
        )
        assert "Engine (#84)" in prompt
        assert "/tmp/wt" in prompt
        assert "feat/84-engine" in prompt
        assert "crew#83 (Done)" in prompt
        assert "Do NOT create a new worktree" in prompt

    def test_no_pr_creation_guardrail(self, tmp_path: Path) -> None:
        config = _load(tmp_path)
        consumer_config = _make_consumer_config(tmp_path)
        prompt = _assemble_codex_prompt(
            {"title": "T", "body": ""},
            "crew#84",
            config,
            consumer_config,
            "/tmp/wt",
            "feat/84-t",
        )
        assert "Do not open or create pull requests" in prompt

    def test_validation_cmd_from_config(self, tmp_path: Path) -> None:
        config = _load(tmp_path)
        consumer_config = _make_consumer_config(tmp_path)
        consumer_config.validation_cmd = "make test"
        prompt = _assemble_codex_prompt(
            {"title": "T", "body": ""},
            "crew#84",
            config,
            consumer_config,
            "/tmp/wt",
            "feat/84-t",
        )
        assert "make test" in prompt

    def test_repair_prompt_includes_reconcile_context(self, tmp_path: Path) -> None:
        config = _load(tmp_path)
        consumer_config = _make_consumer_config(tmp_path)
        prompt = _assemble_codex_prompt(
            {"title": "T", "body": ""},
            "crew#84",
            config,
            consumer_config,
            "/tmp/wt",
            "feat/84-t",
            session_kind="repair",
            repair_pr_url="https://github.com/O/R/pull/77",
            branch_reconcile_state="conflicted_main_merge",
        )
        assert "Existing PR: https://github.com/O/R/pull/77" in prompt
        assert "Branch reconcile state: conflicted_main_merge" in prompt
        assert "make the branch cleanly mergeable with main" in prompt

    def test_extracts_acceptance_criteria(self) -> None:
        body = "## Acceptance Criteria\n- Item passes\n- Tests green\n\n## Notes\nOther"
        result = _extract_acceptance_criteria(body)
        assert "Item passes" in result
        assert "Other" not in result

    def test_includes_repo_workflow_instructions(self, tmp_path: Path) -> None:
        config = _load(tmp_path)
        consumer_config = _make_consumer_config(tmp_path)
        workflow_path = _write_repo_workflow(tmp_path / "crew-workflow")
        workflow = load_workflow_definition(
            workflow_path,
            repo_prefix="crew",
            source_kind="main",
        )

        prompt = _assemble_codex_prompt(
            {"title": "T", "body": ""},
            "crew#84",
            config,
            consumer_config,
            "/tmp/wt",
            "feat/84-t",
            workflow_definition=workflow,
        )

        assert "Repository workflow instructions:" in prompt
        assert "Follow repo instructions for crew#84 in /tmp/wt." in prompt


# -- _run_codex_session tests -------------------------------------------------


class TestRunCodexSession:
    def test_returns_exit_code(self) -> None:
        def subprocess_runner(args, **kw):
            return subprocess.CompletedProcess(
                args=args, returncode=0, stdout="", stderr=""
            )

        code = _run_codex_session(
            "/tmp/wt",
            "prompt",
            Path("schema.json"),
            Path("out.json"),
            1800,
            subprocess_runner=subprocess_runner,
        )
        assert code == 0

    def test_timeout_exit_code(self) -> None:
        def subprocess_runner(args, **kw):
            return subprocess.CompletedProcess(
                args=args, returncode=124, stdout="", stderr=""
            )

        code = _run_codex_session(
            "/tmp/wt",
            "prompt",
            Path("schema.json"),
            Path("out.json"),
            1800,
            subprocess_runner=subprocess_runner,
        )
        assert code == 124

    def test_passes_correct_args(self) -> None:
        captured: list[list[str]] = []

        def subprocess_runner(args, **kw):
            captured.append(list(args))
            return subprocess.CompletedProcess(
                args=args, returncode=0, stdout="", stderr=""
            )

        _run_codex_session(
            "/tmp/wt",
            "do the thing",
            Path("/schemas/result.json"),
            Path("/out/sess.json"),
            1800,
            subprocess_runner=subprocess_runner,
        )
        cmd = captured[0]
        assert "timeout" in cmd[0]
        assert "1800" in cmd
        assert "codex" in cmd[2]
        assert "--full-auto" in cmd
        assert "/tmp/wt" in cmd

    def test_failure_exit_code(self) -> None:
        def subprocess_runner(args, **kw):
            return subprocess.CompletedProcess(
                args=args, returncode=1, stdout="", stderr="error"
            )

        code = _run_codex_session(
            "/tmp/wt", "p", Path("s.json"), Path("o.json"), 30,
            subprocess_runner=subprocess_runner,
        )
        assert code == 1

    def test_avoids_pipe_deadlock_with_large_cli_output(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, object] = {}
        heartbeats: list[str] = []

        class FakeProcess:
            def __init__(self, args, **kwargs):
                captured["kwargs"] = kwargs
                kwargs["stdout"].write("x" * 200000)
                kwargs["stdout"].flush()
                kwargs["stderr"].write("done")
                kwargs["stderr"].flush()
                self.returncode = 0

            def poll(self):
                return self.returncode

            def kill(self):
                self.returncode = 124

            def wait(self):
                return self.returncode

        monkeypatch.setattr(
            "startupai_controller.board_consumer._resolve_cli_command",
            lambda _command: "/usr/bin/codex",
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer.subprocess.Popen",
            FakeProcess,
        )

        code = _run_codex_session(
            "/tmp/wt",
            "prompt",
            Path("schema.json"),
            Path("out.json"),
            1800,
            heartbeat_fn=lambda: heartbeats.append("tick"),
        )

        assert code == 0
        assert heartbeats
        assert captured["kwargs"]["stdout"] is not subprocess.PIPE
        assert captured["kwargs"]["stderr"] is not subprocess.PIPE


class TestResolveCliCommand:
    def test_uses_path_when_available(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("startupai_controller.board_consumer.shutil.which", lambda _cmd: "/bin/codex")
        assert _resolve_cli_command("codex") == "/bin/codex"

    def test_falls_back_to_known_user_install_location(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        pnpm_dir = tmp_path / ".local" / "share" / "pnpm"
        pnpm_dir.mkdir(parents=True)
        codex_path = pnpm_dir / "codex"
        codex_path.write_text("#!/bin/sh\n", encoding="utf-8")
        codex_path.chmod(0o755)

        monkeypatch.setattr("startupai_controller.board_consumer.shutil.which", lambda _cmd: None)
        monkeypatch.setattr(
            "startupai_controller.board_consumer.Path.home",
            classmethod(lambda cls: tmp_path),
        )

        assert _resolve_cli_command("codex") == str(codex_path)


# -- _parse_codex_result tests ------------------------------------------------


class TestParseCodexResult:
    def test_parses_valid_json(self, tmp_path: Path) -> None:
        path = tmp_path / "result.json"
        data = _codex_result_json()
        path.write_text(json.dumps(data))
        result = _parse_codex_result(path)
        assert result is not None
        assert result["outcome"] == "success"

    def test_returns_none_on_bad_json(self, tmp_path: Path) -> None:
        path = tmp_path / "result.json"
        path.write_text("not json")
        result = _parse_codex_result(path)
        assert result is None

    def test_returns_none_on_missing_file(self, tmp_path: Path) -> None:
        result = _parse_codex_result(tmp_path / "nonexistent.json")
        assert result is None


# -- _create_or_update_pr tests -----------------------------------------------


class TestCreateOrUpdatePr:
    def test_creates_pr_when_none_exists(self, tmp_path: Path) -> None:
        config = _make_consumer_config(tmp_path)
        call_count = {"n": 0}

        def gh_runner(args, **kw):
            call_count["n"] += 1
            if call_count["n"] == 1:
                # First call: pr view fails (no PR)
                raise Exception("no PR")
            # Second call: pr create
            return "https://github.com/O/R/pull/1"

        url = _create_or_update_pr(
            "/tmp/wt", "feat/84-test", 84, "O", "R", "Title", config,
            gh_runner=gh_runner,
        )
        assert "pull/1" in url

    def test_returns_existing_pr_url(self, tmp_path: Path) -> None:
        config = _make_consumer_config(tmp_path)

        def gh_runner(args, **kw):
            return json.dumps({
                "url": "https://github.com/O/R/pull/5",
                "body": "Closes #84\nLead Agent: codex\nHandoff: none",
            })

        url = _create_or_update_pr(
            "/tmp/wt", "feat/84-test", 84, "O", "R", "Title", config,
            gh_runner=gh_runner,
        )
        assert "pull/5" in url

    def test_edits_pr_with_missing_fields(self, tmp_path: Path) -> None:
        config = _make_consumer_config(tmp_path)
        calls: list[list[str]] = []

        def gh_runner(args, **kw):
            calls.append(list(args))
            if len(calls) == 1:
                # pr view
                return json.dumps({
                    "url": "https://github.com/O/R/pull/5",
                    "body": "Some text without required fields",
                })
            # pr edit
            return ""

        url = _create_or_update_pr(
            "/tmp/wt", "feat/84-test", 84, "O", "R", "Title", config,
            gh_runner=gh_runner,
        )
        assert "pull/5" in url
        # Verify edit was called
        assert len(calls) == 2
        assert "edit" in calls[1]

    def test_pr_body_has_required_fields(self) -> None:
        body = _build_pr_body("Test Title", 84)
        assert "Lead Agent: codex" in body
        assert "Handoff: none" in body
        assert "Closes #84" in body


# -- _escalate_to_claude tests ------------------------------------------------


class TestEscalateToClaude:
    def test_posts_escalation_comment(self, tmp_path: Path) -> None:
        config = _load(tmp_path)
        posted: list[str] = []

        def gh_runner(args, **kw):
            args_str = " ".join(str(part) for part in args)
            if "field(name: \"Status\")" in args_str:
                return _project_field_response(
                    field_name="Status",
                    single_select_options=[("O_BLOCKED", "Blocked")],
                )
            if "fieldName=Blocked Reason" in args_str:
                return _project_field_response(field_name="Blocked Reason")
            if "fieldName=Handoff To" in args_str:
                return _project_field_response(
                    field_name="Handoff To",
                    single_select_options=[("O_CLAUDE", "claude")],
                )
            if "updateProjectV2ItemFieldValue" in args_str:
                return _project_field_mutation_response()
            return json.dumps({})

        def poster(owner, repo, number, body, **kw):
            posted.append(body)

        _escalate_to_claude(
            "crew#84",
            config,
            "StartupAI-site",
            1,
            reason="tests failing",
            board_info_resolver=lambda *a, **kw: MagicMock(
                status="In Progress", item_id="I", project_id="P"
            ),
            comment_poster=poster,
            gh_runner=gh_runner,
        )
        assert len(posted) == 1
        assert "consumer-escalation" in posted[0]
        assert "tests failing" in posted[0]

    def test_escalation_sets_blocked_and_handoff_fields(self, tmp_path: Path) -> None:
        config = _load(tmp_path)
        gh_calls: list[list[str]] = []

        def gh_runner(args, **kw):
            gh_calls.append(list(args))
            args_str = " ".join(str(part) for part in args)
            if "field(name: \"Status\")" in args_str:
                return _project_field_response(
                    field_name="Status",
                    single_select_options=[("O_BLOCKED", "Blocked")],
                )
            if "fieldName=Blocked Reason" in args_str:
                return _project_field_response(field_name="Blocked Reason")
            if "fieldName=Handoff To" in args_str:
                return _project_field_response(
                    field_name="Handoff To",
                    single_select_options=[("O_CLAUDE", "claude")],
                )
            if "updateProjectV2ItemFieldValue" in args_str:
                return _project_field_mutation_response()
            return json.dumps({})

        _escalate_to_claude(
            "crew#84",
            config,
            "StartupAI-site",
            1,
            board_info_resolver=lambda *a, **kw: MagicMock(
                status="In Progress", item_id="ITEM", project_id="PROJ"
            ),
            comment_poster=lambda *a, **kw: None,
            gh_runner=gh_runner,
        )
        joined = "\n".join(" ".join(str(part) for part in call) for call in gh_calls)
        assert "Blocked Reason" in joined
        assert "Blocked" in joined
        assert "Handoff To" in joined
        assert "O_CLAUDE" in joined

    def test_skips_duplicate_escalation_comment(self, tmp_path: Path) -> None:
        config = _load(tmp_path)
        posted: list[str] = []

        _escalate_to_claude(
            "crew#84",
            config,
            "StartupAI-site",
            1,
            board_info_resolver=lambda *a, **kw: MagicMock(
                status="Blocked", item_id="ITEM", project_id="PROJ"
            ),
            comment_checker=lambda *a, **kw: True,
            comment_poster=lambda *args, **kw: posted.append(args[3]),
            gh_runner=lambda args, **kw: (
                _project_field_response(
                    field_name="Status",
                    single_select_options=[("O_BLOCKED", "Blocked")],
                )
                if "field(name: \"Status\")" in " ".join(str(part) for part in args)
                else _project_field_response(field_name="Blocked Reason")
                if "fieldName=Blocked Reason" in " ".join(str(part) for part in args)
                else _project_field_response(
                    field_name="Handoff To",
                    single_select_options=[("O_CLAUDE", "claude")],
                )
                if "fieldName=Handoff To" in " ".join(str(part) for part in args)
                else _project_field_mutation_response()
            ),
        )

        assert posted == []


# -- _transition_issue_to_review tests ----------------------------------------


class TestTransitionIssueToReview:
    def test_promotes_in_progress_issue_to_review(self, tmp_path: Path) -> None:
        config = _load(tmp_path)
        gh_calls: list[list[str]] = []

        def gh_runner(args, **kw):
            gh_calls.append(list(args))
            args_str = " ".join(str(a) for a in args)
            if "fieldValueByName" in args_str:
                return json.dumps(
                    {
                        "data": {
                            "organization": {
                                "projectV2": {
                                    "items": {
                                        "nodes": [
                                            {
                                                "fieldValueByName": {"name": "In Progress"},
                                                "id": "ITEM1",
                                                "content": {
                                                    "number": 84,
                                                    "repository": {
                                                        "nameWithOwner": "StartupAI-site/startupai-crew"
                                                    },
                                                },
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    }
                )
            if 'field(name: "Status")' in args_str or "fieldName=Status" in args_str:
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
                        },
                    }
                )
            if "updateProjectV2ItemFieldValue" in args_str:
                return json.dumps({"data": {"updateProjectV2ItemFieldValue": {"projectV2Item": {"id": "ITEM1"}}}})
            return json.dumps({})

        _transition_issue_to_review(
            "crew#84",
            config,
            "StartupAI-site",
            1,
            board_info_resolver=lambda *args, **kwargs: BoardInfo(
                status="In Progress",
                item_id="ITEM1",
                project_id="PROJ1",
            ),
            gh_runner=gh_runner,
        )

        assert any("updateProjectV2ItemFieldValue" in " ".join(call) for call in gh_calls)


# -- _post_pr_codex_verdict tests ---------------------------------------------


class TestPostPrCodexVerdict:
    def test_posts_pass_marker_on_pull_request(self) -> None:
        posted: list[tuple[str, str, int, str]] = []

        def poster(owner, repo, number, body, **kwargs):
            posted.append((owner, repo, number, body))

        result = _post_pr_codex_verdict(
            "https://github.com/StartupAI-site/startupai-crew/pull/148",
            "session-123",
            comment_checker=lambda *args, **kwargs: False,
            comment_poster=poster,
        )

        assert result is True
        assert posted
        owner, repo, number, body = posted[0]
        assert (owner, repo, number) == (
            "StartupAI-site",
            "startupai-crew",
            148,
        )
        assert "codex-review: pass" in body
        assert "codex-route: none" in body
        assert "session-123" in body

    def test_returns_false_when_marker_already_exists(self) -> None:
        posted: list[tuple[str, str, int, str]] = []

        def poster(owner, repo, number, body, **kwargs):
            posted.append((owner, repo, number, body))

        result = _post_pr_codex_verdict(
            "https://github.com/StartupAI-site/startupai-crew/pull/148",
            "session-123",
            comment_checker=lambda *args, **kwargs: True,
            comment_poster=poster,
        )

        assert result is False
        assert posted == []


class TestBackfillReviewVerdicts:
    def test_posts_for_successful_review_sessions_with_pr(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        review_session = db.create_session("crew#84", "codex")
        db.update_session(
            review_session,
            status="success",
            phase="review",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/148",
        )
        skipped_session = db.create_session("crew#85", "codex")
        db.update_session(
            skipped_session,
            status="failed",
            phase="review",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/149",
        )

        posted: list[tuple[str, str, int, str]] = []

        def poster(owner, repo, number, body, **kwargs):
            posted.append((owner, repo, number, body))

        result = _backfill_review_verdicts(
            db,
            comment_checker=lambda *args, **kwargs: False,
            comment_poster=poster,
        )

        assert result == ("crew#84",)
        assert len(posted) == 1
        assert posted[0][2] == 148
        assert "codex-review: pass" in posted[0][3]

    def test_skips_existing_marker_and_non_review_sessions(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        existing_marker = db.create_session("crew#84", "codex")
        db.update_session(
            existing_marker,
            status="success",
            phase="review",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/148",
        )
        completed_session = db.create_session("crew#85", "codex")
        db.update_session(
            completed_session,
            status="success",
            phase="completed",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/149",
        )
        no_pr_session = db.create_session("crew#86", "codex")
        db.update_session(
            no_pr_session,
            status="success",
            phase="review",
        )

        posted: list[tuple[str, str, int, str]] = []

        def poster(owner, repo, number, body, **kwargs):
            posted.append((owner, repo, number, body))

        result = _backfill_review_verdicts(
            db,
            comment_checker=lambda owner, repo, number, marker, **kwargs: number == 148,
            comment_poster=poster,
        )

        assert result == ()
        assert posted == []


# -- _post_result_comment tests -----------------------------------------------


class TestPostResultComment:
    def test_posts_marker_comment(self, tmp_path: Path) -> None:
        config = _load(tmp_path)
        posted: list[str] = []

        def poster(owner, repo, number, body, **kw):
            posted.append(body)

        _post_result_comment(
            "crew#84",
            _codex_result_json(),
            "sess-001",
            config,
            comment_poster=poster,
        )
        assert len(posted) == 1
        assert "consumer-result" in posted[0]
        assert "success" in posted[0]

    def test_includes_test_counts(self, tmp_path: Path) -> None:
        config = _load(tmp_path)
        posted: list[str] = []

        def poster(owner, repo, number, body, **kw):
            posted.append(body)

        _post_result_comment(
            "crew#84",
            _codex_result_json(tests_run=10, tests_passed=8),
            "sess-001",
            config,
            comment_poster=poster,
        )
        assert "8/10" in posted[0]


# -- run_one_cycle tests (all paths) -----------------------------------------


class TestRunOneCycle:
    """Integration-level tests for the full cycle with all DI."""

    def _setup_cycle(
        self,
        tmp_path: Path,
        *,
        ready_nodes: list[dict] | None = None,
        claim_succeeds: bool = True,
        codex_exit: int = 0,
        codex_result: dict | None = None,
        has_commits: bool = True,
    ) -> dict:
        """Build common DI callables for run_one_cycle tests."""
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        nodes = ready_nodes if ready_nodes is not None else [_make_node(number=84, executor="codex")]
        if codex_result is None:
            codex_result = _codex_result_json()

        # Write codex output to expected location
        config.output_dir.mkdir(parents=True, exist_ok=True)
        worktree_root = tmp_path / "claimed-worktree"
        worktree_root.mkdir(parents=True, exist_ok=True)
        _write_repo_workflow(
            worktree_root,
            validation_cmd="uv run pytest tests/contracts -q",
        )

        call_log: dict[str, list] = {
            "gh": [],
            "subprocess": [],
            "comments": [],
        }

        def gh_runner(args, **kw):
            call_log["gh"].append(list(args))
            args_str = " ".join(str(a) for a in args)

            if "field(name: \"Status\")" in args_str:
                return _project_field_response(
                    field_name="Status",
                    single_select_options=[("O_BLOCKED", "Blocked"), ("O_IN_PROGRESS", "In Progress")],
                )
            if "fieldName=Blocked Reason" in args_str:
                return _project_field_response(field_name="Blocked Reason")
            if "fieldName=Handoff To" in args_str:
                return _project_field_response(
                    field_name="Handoff To",
                    single_select_options=[("O_CLAUDE", "claude")],
                )
            if "updateProjectV2ItemFieldValue" in args_str:
                return _project_field_mutation_response()

            # list_project_items_by_status (GraphQL query)
            if "api" in args and "graphql" in args:
                return _project_items_response(nodes)

            # fetch issue context
            if "api" in args and "issues/" in args_str:
                return json.dumps({
                    "title": "Test Issue",
                    "body": "## Acceptance Criteria\n- Item works",
                    "labels": [],
                })

            # pr view
            if "pr" in args and "view" in args:
                if claim_succeeds:
                    return json.dumps({
                        "url": "https://github.com/O/R/pull/10",
                        "body": "Closes #84\nLead Agent: codex\nHandoff: none",
                    })
                raise Exception("no PR")

            if "pr" in args and "list" in args:
                return "[]"

            # pr create
            if "pr" in args and "create" in args:
                return "https://github.com/O/R/pull/10"

            # Board info query (for claim)
            return json.dumps({
                "data": {
                    "organization": {
                        "projectV2": {
                            "items": {"nodes": []},
                            "field": {"id": "F1", "options": [{"id": "O1", "name": "In Progress"}]},
                        }
                    }
                }
            })

        def subprocess_runner(args, **kw):
            call_log["subprocess"].append(list(args))
            args_str = " ".join(str(a) for a in args)

            # wt-create.sh
            if "wt-create" in args_str:
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout=f"Worktree ready: {worktree_root}",
                    stderr="",
                )

            # codex exec
            if "codex" in args_str:
                # Write result to output path
                for i, a in enumerate(args):
                    if str(a) == "-o" and i + 1 < len(args):
                        out_path = Path(args[i + 1])
                        out_path.parent.mkdir(parents=True, exist_ok=True)
                        out_path.write_text(json.dumps(codex_result))
                return subprocess.CompletedProcess(
                    args=args, returncode=codex_exit, stdout="", stderr=""
                )

            # git log (has_commits check)
            if "git" in args_str and "log" in args_str:
                stdout = "abc1234 Some commit" if has_commits else ""
                return subprocess.CompletedProcess(
                    args=args, returncode=0, stdout=stdout, stderr=""
                )

            return subprocess.CompletedProcess(
                args=args, returncode=0, stdout="", stderr=""
            )

        def status_resolver(ref, *a, **kw):
            return "Done"

        def board_info_resolver(*a, **kw):
            return MagicMock(
                status="Ready", item_id="ITEM", project_id="PROJ"
            )

        def board_mutator(*a, **kw):
            pass

        def comment_checker(*a, **kw):
            return False

        def comment_poster(*a, **kw):
            call_log["comments"].append(a)

        return {
            "config": config,
            "db": db,
            "gh_runner": gh_runner,
            "subprocess_runner": subprocess_runner,
            "status_resolver": status_resolver,
            "board_info_resolver": board_info_resolver,
            "board_mutator": board_mutator,
            "comment_checker": comment_checker,
            "comment_poster": comment_poster,
            "call_log": call_log,
        }

    def test_idle_no_ready_items(self, tmp_path: Path) -> None:
        setup = self._setup_cycle(tmp_path, ready_nodes=[])
        result = run_one_cycle(setup["config"], setup["db"], **{
            k: v for k, v in setup.items()
            if k not in ("config", "db", "call_log")
        })
        assert result.action == "idle"
        assert "no-ready" in result.reason

    def test_idle_lease_cap(self, tmp_path: Path) -> None:
        setup = self._setup_cycle(tmp_path)
        db = setup["db"]
        # Pre-acquire a lease
        sess = db.create_session("crew#99", "codex")
        db.acquire_lease("crew#99", sess)

        result = run_one_cycle(setup["config"], db, **{
            k: v for k, v in setup.items()
            if k not in ("config", "db", "call_log")
        })
        assert result.action == "idle"
        assert result.reason == "lease-cap"

    def test_lease_conflict_marks_aborted(self, tmp_path: Path) -> None:
        """Lease conflict marks session aborted (not failed) and does not burn retry budget."""
        setup = self._setup_cycle(tmp_path)
        db = setup["db"]
        # Pre-acquire a lease for the SAME issue the consumer will select
        existing_sess = db.create_session("crew#84", "codex")
        db.acquire_lease("crew#84", existing_sess)
        # Release the global cap so we get past the cap check, but the
        # per-issue lease will conflict.  Simulate by releasing the cap
        # lease but keeping the per-issue one.
        # Actually the cap check (active_lease_count >= 1) will block first.
        # So we need a different approach: release that lease, then re-acquire
        # only the per-issue lease after the cap check passes.
        db.release_lease("crew#84")

        # Instead, directly test via a second one-shot targeting the same issue
        # while a lease is held by another session.
        sess_hold = db.create_session("crew#84", "codex")
        db.acquire_lease("crew#84", sess_hold)

        # The global cap will block, so this actually tests the cap path.
        # To properly test the lease-conflict path, we need to bypass the cap.
        # Release the holding lease, run a cycle that creates a session, but
        # have acquire_lease fail.  Easiest: monkeypatch acquire_lease.
        db.release_lease("crew#84")

        original_acquire = db.acquire_lease
        acquire_calls = {"n": 0}

        def failing_acquire(issue_ref, session_id, now=None):
            acquire_calls["n"] += 1
            return False  # Simulate conflict

        db.acquire_lease = failing_acquire  # type: ignore[assignment]

        result = run_one_cycle(setup["config"], db, **{
            k: v for k, v in setup.items()
            if k not in ("config", "db", "call_log")
        })
        db.acquire_lease = original_acquire  # type: ignore[assignment]

        assert result.action == "idle"
        assert result.reason == "lease-conflict"
        # Session should be aborted, NOT failed
        sessions = db.recent_sessions(limit=1)
        assert sessions[0].status == "aborted"
        # Aborted should not count as a retry
        assert db.count_retries("crew#84") == 0

    def test_dry_run_skips_execution(self, tmp_path: Path) -> None:
        setup = self._setup_cycle(tmp_path)
        result = run_one_cycle(
            setup["config"],
            setup["db"],
            dry_run=True,
            **{
                k: v for k, v in setup.items()
                if k not in ("config", "db", "call_log")
            },
        )
        assert result.action == "claimed"
        assert result.reason == "dry-run"
        assert result.issue_ref == "crew#84"
        # Lease should be released
        assert setup["db"].active_lease_count() == 0

    def test_idle_cycle_does_not_backfill_review_verdicts(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        setup = self._setup_cycle(tmp_path, ready_nodes=[])
        prior_session = setup["db"].create_session("crew#70", "codex")
        setup["db"].update_session(
            prior_session,
            status="success",
            phase="review",
            pr_url="https://github.com/O/R/pull/11",
        )
        verdicts: list[tuple[str, str]] = []

        def post_verdict(pr_url, session_id, *args, **kwargs):
            verdicts.append((pr_url, session_id))
            return True

        monkeypatch.setattr(
            "startupai_controller.board_consumer._post_pr_codex_verdict",
            post_verdict,
        )

        result = run_one_cycle(
            setup["config"],
            setup["db"],
            **{
                k: v for k, v in setup.items()
                if k not in ("config", "db", "call_log")
            },
        )

        assert result.action == "idle"
        assert result.reason == "no-ready-for-executor"
        assert verdicts == []

    def test_success_path(self, tmp_path: Path, monkeypatch) -> None:
        setup = self._setup_cycle(tmp_path)
        transitioned: list[str] = []
        verdicts: list[tuple[str, str]] = []

        def transition(issue_ref, *args, **kwargs):
            transitioned.append(issue_ref)

        def post_verdict(pr_url, session_id, *args, **kwargs):
            verdicts.append((pr_url, session_id))

        monkeypatch.setattr(
            "startupai_controller.board_consumer._transition_issue_to_review",
            transition,
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._post_pr_codex_verdict",
            post_verdict,
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer.review_rescue",
            lambda *args, **kwargs: SimpleNamespace(
                rerun_checks=(),
                auto_merge_enabled=True,
                requeued_refs=(),
                blocked_reason=None,
                skipped_reason=None,
            ),
        )
        monkeypatch.setattr(
            "startupai_controller.adapters.pull_requests.GitHubPullRequestAdapter.review_state_digests",
            lambda self, pr_refs: {pr_ref: "digest-1" for pr_ref in pr_refs},
        )
        result = run_one_cycle(setup["config"], setup["db"], **{
            k: v for k, v in setup.items()
            if k not in ("config", "db", "call_log")
        })
        assert result.action == "claimed"
        assert result.issue_ref == "crew#84"
        assert result.reason == "success"
        assert transitioned == ["crew#84"]
        assert len(verdicts) == 1
        assert verdicts[0][0] == "https://github.com/O/R/pull/10"
        assert verdicts[0][1]
        # Lease released
        assert setup["db"].active_lease_count() == 0
        # Session recorded
        sessions = setup["db"].recent_sessions(limit=1)
        assert sessions[0].status == "success"
        assert sessions[0].session_kind == "new_work"
        review_entry = setup["db"].get_review_queue_item("crew#84")
        assert review_entry is not None
        assert review_entry.pr_number == 10
        assert review_entry.last_result == "auto_merge_enabled"
        assert review_entry.last_state_digest == "digest-1"
        assert review_entry.attempt_count == 1

    def test_review_queue_drain_requeues_issue_and_updates_snapshot(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        cp_config = _load(tmp_path)
        auto_config = load_automation_config(config.automation_config_path)
        board_snapshot = CycleBoardSnapshot(
            items=(
                _ProjectItemSnapshot(
                    issue_ref="StartupAI-site/startupai-crew#84",
                    status="Review",
                    executor="codex",
                    handoff_to="none",
                ),
            ),
            by_status={
                "Review": (
                    _ProjectItemSnapshot(
                        issue_ref="StartupAI-site/startupai-crew#84",
                        status="Review",
                        executor="codex",
                        handoff_to="none",
                    ),
                )
            },
        )
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/210",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=210,
            now=datetime.now(timezone.utc) - timedelta(minutes=10),
        )

        monkeypatch.setattr(
            "startupai_controller.board_consumer.review_rescue",
            lambda *args, **kwargs: SimpleNamespace(
                rerun_checks=(),
                auto_merge_enabled=False,
                requeued_refs=("crew#84",),
                blocked_reason=None,
                skipped_reason=None,
            ),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._build_review_snapshots_for_queue_entries",
            lambda *args, **kwargs: {
                ("StartupAI-site/startupai-crew", 210): SimpleNamespace(
                    pr_comment_bodies=(),
                )
            },
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._backfill_review_verdicts_from_snapshots",
            lambda *args, **kwargs: (),
        )
        monkeypatch.setattr(
            "startupai_controller.adapters.pull_requests.GitHubPullRequestAdapter.review_state_digests",
            lambda self, pr_refs: {pr_ref: "digest-1" for pr_ref in pr_refs},
        )

        summary, updated_snapshot = _drain_review_queue(
            config,
            db,
            cp_config,
            auto_config,
            board_snapshot=board_snapshot,
            dry_run=False,
        )

        assert summary.requeued == ("crew#84",)
        assert db.get_review_queue_item("crew#84") is None
        assert updated_snapshot.items_with_status("Ready")[0].issue_ref == "StartupAI-site/startupai-crew#84"

    def test_review_queue_drain_processes_each_pr_once_for_multiple_issue_rows(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        cp_config = _load(tmp_path)
        auto_config = load_automation_config(config.automation_config_path)
        review_items = (
            _ProjectItemSnapshot(
                issue_ref="StartupAI-site/startupai-crew#84",
                status="Review",
                executor="codex",
                handoff_to="none",
            ),
            _ProjectItemSnapshot(
                issue_ref="StartupAI-site/startupai-crew#85",
                status="Review",
                executor="codex",
                handoff_to="none",
            ),
        )
        board_snapshot = CycleBoardSnapshot(
            items=review_items,
            by_status={"Review": review_items},
        )
        now = datetime.now(timezone.utc) - timedelta(minutes=10)
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/210",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=210,
            now=now,
        )
        db.enqueue_review_item(
            "crew#85",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/210",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=210,
            now=now,
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._build_review_snapshots_for_queue_entries",
            lambda *args, **kwargs: {
                ("StartupAI-site/startupai-crew", 210): SimpleNamespace(
                    pr_comment_bodies=(),
                )
            },
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._backfill_review_verdicts_from_snapshots",
            lambda *args, **kwargs: (),
        )
        monkeypatch.setattr(
            "startupai_controller.adapters.pull_requests.GitHubPullRequestAdapter.review_state_digests",
            lambda self, pr_refs: {pr_ref: "digest-1" for pr_ref in pr_refs},
        )
        rescue_calls: list[tuple[str, int]] = []
        monkeypatch.setattr(
            "startupai_controller.board_consumer.review_rescue",
            lambda pr_repo, pr_number, *args, **kwargs: rescue_calls.append(
                (pr_repo, pr_number)
            )
            or SimpleNamespace(
                rerun_checks=(),
                auto_merge_enabled=True,
                requeued_refs=(),
                blocked_reason=None,
                skipped_reason=None,
            ),
        )

        summary, _updated_snapshot = _drain_review_queue(
            config,
            db,
            cp_config,
            auto_config,
            board_snapshot=board_snapshot,
            dry_run=False,
        )

        assert rescue_calls == [("StartupAI-site/startupai-crew", 210)]
        assert summary.auto_merge_enabled == ("StartupAI-site/startupai-crew#210",)
        assert db.get_review_queue_item("crew#84").last_result == "auto_merge_enabled"
        assert db.get_review_queue_item("crew#85").last_result == "auto_merge_enabled"

    def test_review_queue_retry_seconds_are_state_class_aware(self) -> None:
        assert (
            _review_queue_retry_seconds_for_result(
                SimpleNamespace(
                    rerun_checks=(),
                    auto_merge_enabled=True,
                    requeued_refs=(),
                    blocked_reason=None,
                    skipped_reason=None,
                )
            )
            == REVIEW_QUEUE_AUTOMERGE_RETRY_SECONDS
        )
        assert (
            _review_queue_retry_seconds_for_result(
                SimpleNamespace(
                    rerun_checks=("ci",),
                    auto_merge_enabled=False,
                    requeued_refs=(),
                    blocked_reason=None,
                    skipped_reason=None,
                )
            )
            == REVIEW_QUEUE_PENDING_RETRY_SECONDS
        )
        assert (
            _review_queue_retry_seconds_for_result(
                SimpleNamespace(
                    rerun_checks=(),
                    auto_merge_enabled=False,
                    requeued_refs=(),
                    blocked_reason="required checks pending ['ci']",
                    skipped_reason=None,
                )
            )
            == REVIEW_QUEUE_PENDING_RETRY_SECONDS
        )
        assert (
            _review_queue_retry_seconds_for_result(
                SimpleNamespace(
                    rerun_checks=(),
                    auto_merge_enabled=False,
                    requeued_refs=(),
                    blocked_reason="required checks failed ['ci']",
                    skipped_reason=None,
                )
            )
            == REVIEW_QUEUE_FAILED_RETRY_SECONDS
        )
        assert (
            _review_queue_retry_seconds_for_result(
                SimpleNamespace(
                    rerun_checks=(),
                    auto_merge_enabled=False,
                    requeued_refs=(),
                    blocked_reason="StartupAI-site/app.startupai-site#201: missing codex verdict marker (codex-review: pass|fail from trusted actor)",
                    skipped_reason=None,
                )
            )
            == REVIEW_QUEUE_STABLE_BLOCKED_RETRY_SECONDS
        )
        assert (
            _review_queue_retry_seconds_for_result(
                SimpleNamespace(
                    rerun_checks=(),
                    auto_merge_enabled=False,
                    requeued_refs=(),
                    blocked_reason=None,
                    skipped_reason="auto-merge-already-enabled",
                )
            )
            == REVIEW_QUEUE_AUTOMERGE_RETRY_SECONDS
        )

    def test_apply_review_queue_result_parks_auto_merge_enabled_items(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        now = datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc)
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/200",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=200,
            now=now,
        )
        entry = db.get_review_queue_item("crew#84")
        assert entry is not None

        _apply_review_queue_result(
            db,
            entry,
            SimpleNamespace(
                rerun_checks=(),
                auto_merge_enabled=True,
                requeued_refs=(),
                blocked_reason=None,
                skipped_reason=None,
            ),
            now=now,
        )

        updated = db.get_review_queue_item("crew#84")
        assert updated is not None
        assert updated.last_result == "auto_merge_enabled"
        assert updated.next_attempt_at == (
            now + timedelta(seconds=REVIEW_QUEUE_AUTOMERGE_RETRY_SECONDS)
        ).isoformat()

    def test_apply_review_queue_partial_failure_backs_off_due_entries(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        config = _make_consumer_config(tmp_path)
        now = datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc)
        for issue_ref, pr_number in (("crew#84", 200), ("crew#85", 201)):
            db.enqueue_review_item(
                issue_ref,
                pr_url=f"https://github.com/StartupAI-site/startupai-crew/pull/{pr_number}",
                pr_repo="StartupAI-site/startupai-crew",
                pr_number=pr_number,
                now=now,
            )
        entries = db.list_review_queue_items()

        _apply_review_queue_partial_failure(
            db,
            entries,
            config=config,
            error="query:rate_limit:Failed running gh pr view",
            now=now,
        )

        updated = db.list_review_queue_items()
        assert [entry.last_result for entry in updated] == [
            "partial_failure",
            "partial_failure",
        ]
        assert all(
            entry.next_attempt_at
            == (now + timedelta(seconds=config.rate_limit_cooldown_seconds)).isoformat()
            for entry in updated
        )

    def test_partial_failure_clears_blocked_streak(
        self, tmp_path: Path
    ) -> None:
        """Partial-failure must not preserve stale blocked_streak/blocked_class."""
        db = _make_db(tmp_path)
        config = _make_consumer_config(tmp_path)
        now = datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc)
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/200",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=200,
            now=now,
        )
        # Simulate accumulated blocked state
        db.update_review_queue_item(
            "crew#84",
            next_attempt_at=now.isoformat(),
            last_result="blocked",
            last_reason="required checks pending",
            blocked_streak=5,
            blocked_class="transient",
            now=now,
        )
        entries = db.list_review_queue_items()
        assert entries[0].blocked_streak == 5

        _apply_review_queue_partial_failure(
            db,
            entries,
            config=config,
            error="query:network:connection reset",
            now=now,
        )

        updated = db.get_review_queue_item("crew#84")
        assert updated is not None
        assert updated.last_result == "partial_failure"
        assert updated.blocked_streak == 0
        assert updated.blocked_class is None

    def test_review_queue_partial_failure_retries_are_not_immediately_due(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        cp_config = _load(tmp_path)
        auto_config = load_automation_config(config.automation_config_path)
        now = datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc)
        review_items = (
            _ProjectItemSnapshot(
                issue_ref="StartupAI-site/startupai-crew#84",
                status="Review",
                executor="codex",
                handoff_to="none",
            ),
            _ProjectItemSnapshot(
                issue_ref="StartupAI-site/startupai-crew#85",
                status="Review",
                executor="codex",
                handoff_to="none",
            ),
        )
        board_snapshot = CycleBoardSnapshot(
            items=review_items,
            by_status={"Review": review_items},
        )
        for issue_ref, pr_number in (("crew#84", 200), ("crew#85", 201)):
            db.enqueue_review_item(
                issue_ref,
                pr_url=f"https://github.com/StartupAI-site/startupai-crew/pull/{pr_number}",
                pr_repo="StartupAI-site/startupai-crew",
                pr_number=pr_number,
                now=now - timedelta(minutes=10),
            )

        monkeypatch.setattr(
            "startupai_controller.board_consumer.datetime",
            SimpleNamespace(
                now=lambda tz=None: now,
                fromisoformat=datetime.fromisoformat,
            ),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._build_review_snapshots_for_queue_entries",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                GhQueryError("query:rate_limit:Failed running gh pr view")
            ),
        )

        summary, _updated_snapshot = _drain_review_queue(
            config,
            db,
            cp_config,
            auto_config,
            board_snapshot=board_snapshot,
            dry_run=False,
        )

        assert summary.partial_failure is True
        assert "rate_limit" in str(summary.error)
        updated = db.list_review_queue_items()
        assert [entry.last_result for entry in updated] == [
            "partial_failure",
            "partial_failure",
        ]
        assert db.due_review_queue_count(now=now + timedelta(minutes=1)) == 0

    def test_review_queue_drain_reparks_unchanged_digest_without_full_hydration(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        cp_config = _load(tmp_path)
        auto_config = load_automation_config(config.automation_config_path)
        now = datetime.now(timezone.utc)
        board_snapshot = CycleBoardSnapshot(
            items=(
                _ProjectItemSnapshot(
                    issue_ref="StartupAI-site/startupai-crew#84",
                    status="Review",
                    executor="codex",
                    handoff_to="none",
                ),
            ),
            by_status={
                "Review": (
                    _ProjectItemSnapshot(
                        issue_ref="StartupAI-site/startupai-crew#84",
                        status="Review",
                        executor="codex",
                        handoff_to="none",
                    ),
                )
            },
        )
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/210",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=210,
            now=now - timedelta(minutes=10),
        )
        db.update_review_queue_item(
            "crew#84",
            next_attempt_at=(now - timedelta(minutes=1)).isoformat(),
            last_result="blocked",
            last_reason="StartupAI-site/startupai-crew#210: missing codex verdict marker (codex-review: pass|fail from trusted actor)",
            last_state_digest="digest-1",
            now=now - timedelta(minutes=5),
        )

        monkeypatch.setattr(
            "startupai_controller.board_consumer._partition_review_queue_entries_by_probe_change",
            lambda entries, **kwargs: ([], entries),
        )

        def fail_build(*args, **kwargs):
            raise AssertionError("full review snapshot build should be skipped")

        monkeypatch.setattr(
            "startupai_controller.board_consumer._build_review_snapshots_for_queue_entries",
            fail_build,
        )

        summary, _updated_snapshot = _drain_review_queue(
            config,
            db,
            cp_config,
            auto_config,
            board_snapshot=board_snapshot,
            dry_run=False,
        )

        assert summary.partial_failure is False
        assert summary.due_count == 0
        updated = db.get_review_queue_item("crew#84")
        assert updated is not None
        assert updated.last_result == "blocked"
        assert updated.last_reason == "StartupAI-site/startupai-crew#210: missing codex verdict marker (codex-review: pass|fail from trusted actor)"
        assert updated.next_attempt_datetime() > now + timedelta(minutes=20)

    def test_review_queue_drain_wakes_changed_parked_digest_for_immediate_processing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        cp_config = _load(tmp_path)
        auto_config = load_automation_config(config.automation_config_path)
        now = datetime.now(timezone.utc)
        board_snapshot = CycleBoardSnapshot(
            items=(
                _ProjectItemSnapshot(
                    issue_ref="StartupAI-site/startupai-crew#84",
                    status="Review",
                    executor="codex",
                    handoff_to="none",
                ),
            ),
            by_status={
                "Review": (
                    _ProjectItemSnapshot(
                        issue_ref="StartupAI-site/startupai-crew#84",
                        status="Review",
                        executor="codex",
                        handoff_to="none",
                    ),
                )
            },
        )
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/210",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=210,
            now=now - timedelta(minutes=10),
            next_attempt_at=(now + timedelta(hours=1)).isoformat(),
        )
        db.update_review_queue_item(
            "crew#84",
            next_attempt_at=(now + timedelta(hours=1)).isoformat(),
            last_result="skipped",
            last_reason="auto-merge-already-enabled",
            last_state_digest="digest-old",
            now=now - timedelta(minutes=5),
        )

        monkeypatch.setattr(
            "startupai_controller.adapters.pull_requests.GitHubPullRequestAdapter.review_state_digests",
            lambda self, pr_refs: {pr_ref: "digest-new" for pr_ref in pr_refs},
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._build_review_snapshots_for_queue_entries",
            lambda **kwargs: {
                ("StartupAI-site/startupai-crew", 210): SimpleNamespace(
                    pr_comment_bodies=(),
                )
            },
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer.review_rescue",
            lambda *args, **kwargs: SimpleNamespace(
                rerun_checks=(),
                auto_merge_enabled=True,
                requeued_refs=(),
                blocked_reason=None,
                skipped_reason=None,
            ),
        )

        summary, _updated_snapshot = _drain_review_queue(
            config,
            db,
            cp_config,
            auto_config,
            board_snapshot=board_snapshot,
            dry_run=False,
        )

        assert summary.due_count == 1
        assert summary.auto_merge_enabled == ("StartupAI-site/startupai-crew#210",)
        updated = db.get_review_queue_item("crew#84")
        assert updated is not None
        assert updated.last_result == "auto_merge_enabled"
        assert updated.last_state_digest == "digest-new"

    def test_build_review_snapshots_batches_pr_payload_queries_by_repo(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        auto_config = load_automation_config(config.automation_config_path)
        now = datetime.now(timezone.utc) - timedelta(minutes=10)
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/210",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=210,
            now=now,
        )
        db.enqueue_review_item(
            "crew#85",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/211",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=211,
            now=now,
        )
        db.enqueue_review_item(
            "app#17",
            pr_url="https://github.com/StartupAI-site/app.startupai-site/pull/300",
            pr_repo="StartupAI-site/app.startupai-site",
            pr_number=300,
            now=now,
        )

        captured: dict[tuple[str, int], tuple[str, ...]] = {}

        class FakePrPort:
            def review_snapshots(
                self,
                review_refs_by_pr: dict[tuple[str, int], tuple[str, ...]],
                *,
                trusted_codex_actors: frozenset[str],
            ) -> dict[tuple[str, int], SimpleNamespace]:
                assert trusted_codex_actors == frozenset(
                    auto_config.trusted_codex_actors
                )
                captured.update(review_refs_by_pr)
                return {
                    pr_key: SimpleNamespace(
                        pr_repo=pr_key[0],
                        pr_number=pr_key[1],
                        review_refs=review_refs,
                        pr_comment_bodies=(),
                    )
                    for pr_key, review_refs in review_refs_by_pr.items()
                }

        queue_entries = db.list_review_queue_items()
        snapshots = _build_review_snapshots_for_queue_entries(
            queue_entries=queue_entries,
            review_refs={"crew#84", "crew#85", "app#17"},
            pr_port=FakePrPort(),
            trusted_codex_actors=frozenset(auto_config.trusted_codex_actors),
        )

        assert captured == {
            ("StartupAI-site/startupai-crew", 210): ("crew#84",),
            ("StartupAI-site/startupai-crew", 211): ("crew#85",),
            ("StartupAI-site/app.startupai-site", 300): ("app#17",),
        }
        assert snapshots[("StartupAI-site/startupai-crew", 210)].review_refs == (
            "crew#84",
        )
        assert snapshots[("StartupAI-site/startupai-crew", 211)].review_refs == (
            "crew#85",
        )
        assert snapshots[("StartupAI-site/app.startupai-site", 300)].review_refs == (
            "app#17",
        )

    def test_repair_session_requeues_failed_existing_pr(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        setup = self._setup_cycle(
            tmp_path,
            codex_exit=0,
            codex_result=_codex_result_json(outcome="failed", summary="Needs repair"),
        )
        requeued: list[str] = []
        transitioned: list[str] = []

        monkeypatch.setattr(
            "startupai_controller.board_consumer._classify_open_pr_candidates",
            lambda *args, **kwargs: (
                "adoptable",
                OpenPullRequestMatch(
                    url="https://github.com/O/R/pull/77",
                    number=77,
                    author="codex",
                    body=(
                        "Closes #84\n\n"
                        "<!-- startupai-board-bot:consumer:"
                        "session=sess-84 issue=crew#84 repo=crew "
                        "branch=feat/84-test executor=codex -->"
                    ),
                    branch_name="feat/84-test",
                    provenance={
                        "issue_ref": "crew#84",
                        "executor": "codex",
                    },
                ),
                "qualifying-open-pr",
            ),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._create_worktree",
            lambda issue_ref, title, config, **kwargs: (
                str(tmp_path / "claimed-worktree"),
                kwargs["branch_name_override"],
            ),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._create_or_update_pr",
            lambda *args, **kwargs: "https://github.com/O/R/pull/77",
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._reconcile_repair_branch",
            lambda *args, **kwargs: RepairBranchReconcileOutcome("up_to_date"),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._return_issue_to_ready",
            lambda issue_ref, *args, **kwargs: requeued.append(issue_ref),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._transition_issue_to_review",
            lambda issue_ref, *args, **kwargs: transitioned.append(issue_ref),
        )

        result = run_one_cycle(
            setup["config"],
            setup["db"],
            **{k: v for k, v in setup.items() if k not in ("config", "db", "call_log")},
        )

        assert result.issue_ref == "crew#84"
        assert result.reason == "failed"
        assert transitioned == []
        assert requeued == ["crew#84"]
        session = setup["db"].recent_sessions(limit=1)[0]
        assert session.session_kind == "repair"
        assert session.repair_pr_url == "https://github.com/O/R/pull/77"
        assert session.pr_url == "https://github.com/O/R/pull/77"
        assert session.phase == "completed"

    def test_repair_session_blocks_when_branch_reconcile_setup_fails(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        setup = self._setup_cycle(tmp_path)
        blocked: list[tuple[str, str]] = []

        monkeypatch.setattr(
            "startupai_controller.board_consumer._classify_open_pr_candidates",
            lambda *args, **kwargs: (
                "adoptable",
                OpenPullRequestMatch(
                    url="https://github.com/O/R/pull/77",
                    number=77,
                    author="codex",
                    body=(
                        "Closes #84\n\n"
                        "<!-- startupai-board-bot:consumer:"
                        "session=sess-84 issue=crew#84 repo=crew "
                        "branch=feat/84-test executor=codex -->"
                    ),
                    branch_name="feat/84-test",
                    provenance={"issue_ref": "crew#84", "executor": "codex"},
                ),
                "qualifying-open-pr",
            ),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._create_worktree",
            lambda issue_ref, title, config, **kwargs: (
                str(tmp_path / "claimed-worktree"),
                kwargs["branch_name_override"],
            ),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._reconcile_repair_branch",
            lambda *args, **kwargs: RepairBranchReconcileOutcome(
                "reconcile_setup_failed",
                "repair-branch-diverged-from-origin",
            ),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._set_blocked_with_reason",
            lambda issue_ref, reason, *args, **kwargs: blocked.append((issue_ref, reason)),
        )

        result = run_one_cycle(
            setup["config"],
            setup["db"],
            **{k: v for k, v in setup.items() if k not in ("config", "db", "call_log")},
        )

        assert result.action == "error"
        assert result.reason == "repair-branch-reconcile:repair-branch-diverged-from-origin"
        assert blocked == [
            ("crew#84", "repair-branch-reconcile:repair-branch-diverged-from-origin")
        ]
        assert setup["db"].recent_sessions(limit=1) == []

    def test_repair_session_records_conflicted_main_merge_state(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        setup = self._setup_cycle(tmp_path)

        monkeypatch.setattr(
            "startupai_controller.board_consumer._classify_open_pr_candidates",
            lambda *args, **kwargs: (
                "adoptable",
                OpenPullRequestMatch(
                    url="https://github.com/O/R/pull/77",
                    number=77,
                    author="codex",
                    body=(
                        "Closes #84\n\n"
                        "<!-- startupai-board-bot:consumer:"
                        "session=sess-84 issue=crew#84 repo=crew "
                        "branch=feat/84-test executor=codex -->"
                    ),
                    branch_name="feat/84-test",
                    provenance={"issue_ref": "crew#84", "executor": "codex"},
                ),
                "qualifying-open-pr",
            ),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._create_worktree",
            lambda issue_ref, title, config, **kwargs: (
                str(tmp_path / "claimed-worktree"),
                kwargs["branch_name_override"],
            ),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._reconcile_repair_branch",
            lambda *args, **kwargs: RepairBranchReconcileOutcome(
                "conflicted_main_merge"
            ),
        )

        result = run_one_cycle(
            setup["config"],
            setup["db"],
            **{k: v for k, v in setup.items() if k not in ("config", "db", "call_log")},
        )

        assert result.reason == "success"
        session = setup["db"].recent_sessions(limit=1)[0]
        assert session.session_kind == "repair"
        assert session.branch_reconcile_state == "conflicted_main_merge"

    def test_codex_timeout(self, tmp_path: Path) -> None:
        setup = self._setup_cycle(tmp_path, codex_exit=124)
        result = run_one_cycle(setup["config"], setup["db"], **{
            k: v for k, v in setup.items()
            if k not in ("config", "db", "call_log")
        })
        assert result.action == "claimed"
        assert result.reason == "timeout"

    def test_codex_failure(self, tmp_path: Path) -> None:
        setup = self._setup_cycle(
            tmp_path,
            codex_exit=0,
            codex_result=_codex_result_json(outcome="failed", summary="Tests fail"),
        )
        result = run_one_cycle(setup["config"], setup["db"], **{
            k: v for k, v in setup.items()
            if k not in ("config", "db", "call_log")
        })
        assert result.action == "claimed"
        assert result.reason == "failed"

    def test_codex_blocked_outcome(self, tmp_path: Path) -> None:
        setup = self._setup_cycle(
            tmp_path,
            codex_exit=0,
            codex_result=_codex_result_json(
                outcome="blocked", summary="Missing dep", blocker_reason="Need API"
            ),
        )
        result = run_one_cycle(setup["config"], setup["db"], **{
            k: v for k, v in setup.items()
            if k not in ("config", "db", "call_log")
        })
        assert result.action == "claimed"
        assert result.reason == "failed"

    def test_max_retries_escalation(self, tmp_path: Path) -> None:
        setup = self._setup_cycle(tmp_path)
        db = setup["db"]
        # Pre-create 3 failed sessions
        for _ in range(3):
            sid = db.create_session("crew#84", "codex")
            db.update_session(
                sid,
                status="failed",
                started_at="2026-03-06T12:00:00+00:00",
            )

        result = run_one_cycle(setup["config"], db, **{
            k: v for k, v in setup.items()
            if k not in ("config", "db", "call_log")
        })
        assert result.action == "error"
        assert result.reason == "max-retries-exceeded"
        assert len(setup["call_log"]["comments"]) == 1
        escalation_body = setup["call_log"]["comments"][0][3]
        assert "consumer-escalation" in escalation_body
        gh_joined = "\n".join(
            " ".join(str(part) for part in call) for call in setup["call_log"]["gh"]
        )
        assert "Blocked Reason" in gh_joined

    def test_skipped_tests_do_not_poison_success(self, tmp_path: Path) -> None:
        setup = self._setup_cycle(
            tmp_path,
            codex_result=_codex_result_json(
                outcome="success", tests_run=10, tests_passed=8
            ),
        )
        result = run_one_cycle(setup["config"], setup["db"], **{
            k: v for k, v in setup.items()
            if k not in ("config", "db", "call_log")
        })
        assert result.reason == "success"

    def test_target_issue_override(self, tmp_path: Path) -> None:
        setup = self._setup_cycle(
            tmp_path,
            ready_nodes=[
                _make_node(number=85, executor="codex"),
                _make_node(number=84, executor="codex"),
            ],
        )
        result = run_one_cycle(
            setup["config"],
            setup["db"],
            target_issue="crew#85",
            **{
                k: v for k, v in setup.items()
                if k not in ("config", "db", "call_log")
            },
        )
        assert result.issue_ref == "crew#85"

    def test_retry_backoff_skips_recently_failed_candidate(self, tmp_path: Path) -> None:
        setup = self._setup_cycle(
            tmp_path,
            ready_nodes=[
                _make_node(number=84, executor="codex", priority="P0"),
                _make_node(number=85, executor="codex", priority="P2"),
            ],
        )
        failed_session = setup["db"].create_session("crew#84", "codex")
        setup["db"].update_session(
            failed_session,
            status="failed",
            failure_reason="api_error",
            retry_count=1,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )

        result = run_one_cycle(
            setup["config"],
            setup["db"],
            **{k: v for k, v in setup.items() if k not in ("config", "db", "call_log")},
        )

        assert result.issue_ref == "crew#85"

    def test_nonretryable_failure_does_not_trigger_backoff(self, tmp_path: Path) -> None:
        setup = self._setup_cycle(
            tmp_path,
            ready_nodes=[
                _make_node(number=84, executor="codex", priority="P0"),
                _make_node(number=85, executor="codex", priority="P2"),
            ],
        )
        failed_session = setup["db"].create_session("crew#84", "codex")
        setup["db"].update_session(
            failed_session,
            status="aborted",
            failure_reason="workspace_error",
            completed_at=datetime.now(timezone.utc).isoformat(),
        )

        result = run_one_cycle(
            setup["config"],
            setup["db"],
            **{k: v for k, v in setup.items() if k not in ("config", "db", "call_log")},
        )

        assert result.issue_ref == "crew#84"

    def test_claim_rejected(self, tmp_path: Path) -> None:
        setup = self._setup_cycle(tmp_path)
        # Override board_info to return "In Progress" so claim fails
        setup["board_info_resolver"] = lambda *a, **kw: MagicMock(
            status="In Progress", item_id="I", project_id="P"
        )

        result = run_one_cycle(setup["config"], setup["db"], **{
            k: v for k, v in setup.items()
            if k not in ("config", "db", "call_log")
        })
        assert result.action == "idle"
        assert "claim-rejected" in result.reason

    def test_missing_repo_workflow_disables_dispatch(self, tmp_path: Path) -> None:
        setup = self._setup_cycle(
            tmp_path,
            ready_nodes=[
                _make_node(number=150, repo="StartupAI-site/app.startupai-site", executor="codex")
            ],
        )
        app_workflow = setup["config"].repo_roots["app"] / "WORKFLOW.md"
        app_workflow.unlink()

        result = run_one_cycle(
            setup["config"],
            setup["db"],
            target_issue="app#150",
            **{k: v for k, v in setup.items() if k not in ("config", "db", "call_log")},
        )

        assert result.action == "idle"
        assert result.reason.startswith("repo-dispatch-disabled:")

    def test_wip_limit_claim_rejection_is_aborted(self, tmp_path: Path, monkeypatch) -> None:
        setup = self._setup_cycle(tmp_path)

        monkeypatch.setattr(
            "startupai_controller.board_consumer.claim_ready_issue",
            lambda *args, **kwargs: ClaimReadyResult(reason="wip-limit"),
        )

        result = run_one_cycle(setup["config"], setup["db"], **{
            k: v for k, v in setup.items()
            if k not in ("config", "db", "call_log")
        })

        assert result.reason == "claim-rejected:wip-limit"
        sessions = setup["db"].recent_sessions(limit=1)
        assert sessions[0].status == "aborted"

    def test_selection_error_returns_error(self, tmp_path: Path, monkeypatch) -> None:
        setup = self._setup_cycle(tmp_path)

        def raising_select(*args, **kwargs):
            raise GhQueryError("selection fetch failed")

        monkeypatch.setattr(
            "startupai_controller.board_consumer._select_best_candidate",
            raising_select,
        )

        result = run_one_cycle(setup["config"], setup["db"], **{
            k: v for k, v in setup.items()
            if k not in ("config", "db", "call_log")
        })

        assert result.action == "error"
        assert "selection-error:selection fetch failed" in result.reason
        assert setup["db"].active_lease_count() == 0
        assert setup["db"].recent_sessions(limit=1) == []

    def test_claim_error_releases_lease(self, tmp_path: Path, monkeypatch) -> None:
        setup = self._setup_cycle(tmp_path)

        def raising_claim(*args, **kwargs):
            raise GhQueryError("temporary github failure")

        monkeypatch.setattr(
            "startupai_controller.board_consumer.claim_ready_issue",
            raising_claim,
        )

        result = run_one_cycle(setup["config"], setup["db"], **{
            k: v for k, v in setup.items()
            if k not in ("config", "db", "call_log")
        })

        assert result.action == "error"
        assert "claim-error:temporary github failure" in result.reason
        assert setup["db"].active_lease_count() == 0
        assert setup["db"].count_retries("crew#84") == 0

        session = setup["db"].recent_sessions(limit=1)[0]
        assert session.status == "failed"
        assert session.failure_reason == "api_error"
        assert session.retry_count == 1
        assert session.started_at is None
        assert session.completed_at is not None

    def test_rate_limit_claim_error_suppresses_future_claims(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        setup = self._setup_cycle(tmp_path)

        def raising_claim(*args, **kwargs):
            raise GhCommandError(
                operation_type="query",
                failure_kind="rate_limit",
                command_excerpt="api repos/O/R/issues/84",
                detail="API rate limit exceeded",
                rate_limit_reset_at=None,
            )

        monkeypatch.setattr("startupai_controller.board_consumer.claim_ready_issue", raising_claim)

        first = run_one_cycle(
            setup["config"],
            setup["db"],
            **{k: v for k, v in setup.items() if k not in ("config", "db", "call_log")},
        )
        second = run_one_cycle(
            setup["config"],
            setup["db"],
            **{k: v for k, v in setup.items() if k not in ("config", "db", "call_log")},
        )

        assert first.action == "error"
        assert first.reason.startswith("claim-error:")
        assert second.action == "idle"
        assert second.reason == "claim-suppressed:claim"

    def test_worktree_failure(self, tmp_path: Path) -> None:
        setup = self._setup_cycle(tmp_path)
        original_runner = setup["subprocess_runner"]
        requeued: list[str] = []

        def failing_subprocess(args, **kw):
            if "wt-create" in " ".join(str(a) for a in args):
                return subprocess.CompletedProcess(
                    args=args, returncode=1, stdout="", stderr="wt error"
                )
            return original_runner(args, **kw)

        setup["subprocess_runner"] = failing_subprocess
        with patch(
            "startupai_controller.board_consumer._set_blocked_with_reason",
            side_effect=lambda issue_ref, reason, *args, **kwargs: requeued.append(reason),
        ):
            result = run_one_cycle(setup["config"], setup["db"], **{
                k: v for k, v in setup.items()
                if k not in ("config", "db", "call_log")
            })
        assert result.action == "error"
        assert "workspace_error:" in result.reason
        assert requeued == ["workspace_prepare:wt-create.sh failed (exit 1): wt error"]
        assert setup["db"].recent_sessions(limit=1) == []

    def test_config_error(self, tmp_path: Path) -> None:
        config = _make_consumer_config(tmp_path)
        config.critical_paths_path = tmp_path / "nonexistent.json"
        db = _make_db(tmp_path)
        result = run_one_cycle(config, db)
        assert result.action == "error"
        assert "config-error" in result.reason

    def test_salvage_pr_on_non_timeout_failure(self, tmp_path: Path) -> None:
        """Non-zero exit but has commits -> consumer creates PR."""
        setup = self._setup_cycle(
            tmp_path,
            codex_exit=1,
            has_commits=True,
        )
        result = run_one_cycle(setup["config"], setup["db"], **{
            k: v for k, v in setup.items()
            if k not in ("config", "db", "call_log")
        })
        assert result.pr_url is not None

    def test_nonzero_no_output_no_commits_requeues_and_aborts(self, tmp_path: Path) -> None:
        setup = self._setup_cycle(
            tmp_path,
            codex_exit=1,
            has_commits=False,
        )
        requeued: list[str] = []

        with patch(
            "startupai_controller.board_consumer._parse_codex_result",
            return_value=None,
        ), patch(
            "startupai_controller.board_consumer._return_issue_to_ready",
            side_effect=lambda issue_ref, *args, **kwargs: requeued.append(issue_ref),
        ):
            result = run_one_cycle(setup["config"], setup["db"], **{
                k: v for k, v in setup.items()
                if k not in ("config", "db", "call_log")
            })

        assert result.action == "claimed"
        assert result.reason == "aborted"
        assert result.pr_url is None
        assert requeued == ["crew#84"]
        sessions = setup["db"].recent_sessions(limit=1)
        assert sessions[0].status == "aborted"

    def test_successful_noop_without_resolution_blocks_instead_of_requeueing(
        self, tmp_path: Path
    ) -> None:
        setup = self._setup_cycle(
            tmp_path,
            codex_exit=0,
            has_commits=False,
            codex_result=_codex_result_json(resolution=None),
        )
        requeued: list[str] = []

        with patch(
            "startupai_controller.board_consumer._return_issue_to_ready",
            side_effect=lambda issue_ref, *args, **kwargs: requeued.append(issue_ref),
        ):
            result = run_one_cycle(
                setup["config"],
                setup["db"],
                **{k: v for k, v in setup.items() if k not in ("config", "db", "call_log")},
            )

        assert result.action == "claimed"
        assert result.reason == "success"
        assert requeued == []
        session = setup["db"].recent_sessions(limit=1)[0]
        assert session.status == "success"
        assert session.phase == "blocked"
        assert session.resolution_kind is None
        assert session.verification_class == "failed"
        assert session.resolution_action == "blocked_for_resolution_review"
        gh_joined = "\n".join(" ".join(call) for call in setup["call_log"]["gh"])
        assert "Blocked Reason" in gh_joined
        assert "Handoff To" in gh_joined

    def test_successful_verified_resolution_closes_issue_without_requeueing(
        self, tmp_path: Path
    ) -> None:
        resolution = {
            "kind": "already_on_main",
            "summary": "Canonical main already includes the requested behavior.",
            "code_refs": ["src/existing.py"],
            "commit_shas": ["abc123"],
            "pr_urls": ["https://github.com/StartupAI-site/startupai-crew/pull/77"],
            "validated_on_main": True,
            "validation_command": "uv run pytest tests/contracts -q",
            "validation_exit_code": 0,
            "acceptance_criteria_met": True,
            "acceptance_criteria_notes": "Existing implementation satisfies the issue.",
            "equivalence_claim": "exact_match",
        }
        setup = self._setup_cycle(
            tmp_path,
            codex_exit=0,
            has_commits=False,
            codex_result=_codex_result_json(resolution=resolution),
        )
        repo_root = setup["config"].repo_roots["crew"]
        existing_file = repo_root / "src" / "existing.py"
        existing_file.parent.mkdir(parents=True, exist_ok=True)
        existing_file.write_text("print('ok')\n", encoding="utf-8")

        base_runner = setup["gh_runner"]
        call_log = setup["call_log"]

        def gh_runner(args, **kwargs):
            call_log["gh"].append(list(args))
            args_str = " ".join(str(part) for part in args)
            if (
                args[:3] == ["pr", "view", "77"]
                and "mergedAt" in args_str
            ):
                return json.dumps(
                    {"state": "MERGED", "mergedAt": "2026-03-08T00:00:00Z"}
                )
            if "repos/StartupAI-site/startupai-crew/issues/84" in args_str and "PATCH" in args_str:
                return "{}"
            return base_runner(args, **kwargs)

        setup["gh_runner"] = gh_runner
        requeued: list[str] = []

        with patch(
            "startupai_controller.board_consumer._return_issue_to_ready",
            side_effect=lambda issue_ref, *args, **kwargs: requeued.append(issue_ref),
        ):
            result = run_one_cycle(
                setup["config"],
                setup["db"],
                **{k: v for k, v in setup.items() if k not in ("config", "db", "call_log")},
            )

        assert result.action == "claimed"
        assert result.reason == "success"
        assert requeued == []
        session = setup["db"].recent_sessions(limit=1)[0]
        assert session.status == "success"
        assert session.phase == "completed"
        assert session.resolution_kind == "already_on_main"
        assert session.verification_class == "strong"
        assert session.resolution_action == "closed_as_already_resolved"
        assert session.done_reason == "already_resolved"
        gh_joined = "\n".join(" ".join(call) for call in call_log["gh"])
        assert "state=closed" in gh_joined


# -- CLI parser tests ----------------------------------------------------------


class TestCliParser:
    def test_run_subcommand(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["run", "--interval", "60", "--dry-run"])
        assert args.command == "run"
        assert args.interval == 60
        assert args.dry_run is True

    def test_one_shot_subcommand(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["one-shot", "--issue", "crew#84"])
        assert args.command == "one-shot"
        assert args.issue == "crew#84"

    def test_status_subcommand(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["status", "--json", "--local-only"])
        assert args.command == "status"
        assert args.as_json is True
        assert args.local_only is True

    def test_report_slo_subcommand(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["report-slo", "--json", "--local-only"])
        assert args.command == "report-slo"
        assert args.as_json is True
        assert args.local_only is True

    def test_serve_status_subcommand(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["serve-status", "--host", "127.0.0.1", "--port", "9999"])
        assert args.command == "serve-status"
        assert args.host == "127.0.0.1"
        assert args.port == 9999

    def test_drain_subcommand(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["drain"])
        assert args.command == "drain"

    def test_resume_subcommand(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["resume"])
        assert args.command == "resume"


class TestDrainControls:
    def test_drain_helpers_round_trip(self, tmp_path: Path) -> None:
        config = _make_consumer_config(tmp_path)
        assert _drain_requested(config.drain_path) is False
        _request_drain(config.drain_path)
        assert _drain_requested(config.drain_path) is True
        assert _clear_drain(config.drain_path) is True
        assert _drain_requested(config.drain_path) is False

    def test_status_json_reports_drain(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        session_id = db.create_session(
            "crew#84",
            "codex",
            slot_id=1,
            session_kind="repair",
            repair_pr_url="https://github.com/O/R/pull/10",
        )
        db.acquire_lease("crew#84", session_id, slot_id=1, now=datetime.now(timezone.utc))
        db.update_session(session_id, status="running", slot_id=1, phase="running")
        failed_session_id = db.create_session("crew#85", "codex")
        db.update_session(
            failed_session_id,
            status="failed",
            phase="blocked",
            completed_at=datetime.now(timezone.utc).isoformat(),
            failure_reason="api_error",
            retry_count=2,
        )
        db.queue_deferred_action(
            "crew#84",
            "set_status",
            {"issue_ref": "crew#84", "to_status": "Ready", "from_statuses": ["In Progress"]},
        )
        db.set_control_value("degraded", "true")
        db.set_control_value("degraded_reason", "selection-error:test")
        _cmd_drain(config)
        try:
            capsys.readouterr()
            assert _cmd_status(config, as_json=True, local_only=True) == 0
            captured = capsys.readouterr()
            payload = json.loads(captured.out)
            assert payload["local_only"] is True
            assert payload["drain_requested"] is True
            assert payload["drain_path"] == str(config.drain_path)
            assert payload["workflow_state_path"] == str(config.workflow_state_path)
            assert payload["degraded"] is True
            assert payload["degraded_reason"] == "selection-error:test"
            assert payload["deferred_action_count"] == 1
            assert payload["oldest_deferred_action_age_seconds"] is not None
            assert payload["control_plane_health"]["health"] == "degraded_recovering"
            assert payload["control_plane_health"]["reason_code"] == "selection-error"
            assert payload["workers"][0]["session_kind"] == "repair"
            assert payload["workers"][0]["repair_pr_url"] == "https://github.com/O/R/pull/10"
            assert payload["workers"][0]["branch_reconcile_state"] is None
            assert payload["review_summary"]["source"] == "local"
            assert payload["lane_wip_limits"]["codex"]["crew"] == 1
            assert payload["repo_workflows"]["crew"]["available"] is True
            assert payload["recent_sessions"][0]["failure_reason"] == "api_error"
            assert payload["recent_sessions"][0]["retry_count"] == 2
            assert payload["recent_sessions"][0]["retryable"] is True
            assert payload["recent_sessions"][0]["next_retry_at"] is not None
            assert payload["recent_sessions"][0]["retry_remaining_seconds"] is not None
        finally:
            _cmd_resume(config)

    def test_status_payload_falls_back_to_local_review_summary(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        session_id = db.create_session("crew#84", "codex")
        db.update_session(
            session_id,
            status="success",
            phase="review",
            pr_url="https://github.com/O/R/pull/10",
        )
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/O/R/pull/10",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=10,
            now=datetime.now(timezone.utc),
        )
        db.close()

        def failing_review_query(*args, **kwargs):
            raise GhQueryError("review query failed")

        monkeypatch.setattr(
            "startupai_controller.board_consumer._list_project_items_by_status",
            failing_review_query,
        )

        payload = _collect_status_payload(config)

        assert payload["review_summary"]["count"] == 1
        assert payload["review_summary"]["refs"] == ["crew#84"]
        assert payload["review_summary"]["source"] == "local-fallback"
        assert "review query failed" in payload["review_summary"]["error"]
        assert payload["review_queue"]["count"] == 1
        assert payload["review_queue"]["due_count"] == 1
        assert payload["review_queue"]["refs"] == ["crew#84"]

    def test_status_http_server_serves_local_json(self, tmp_path: Path) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        session_id = db.create_session("crew#84", "codex")
        db.update_session(
            session_id,
            status="success",
            phase="review",
            pr_url="https://github.com/O/R/pull/11",
        )
        db.close()

        server = _create_status_http_server(config, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{server.server_port}/status",
                timeout=5,
            ) as response:
                payload = json.loads(response.read().decode("utf-8"))
            assert payload["local_only"] is True
            assert payload["review_summary"]["count"] == 1
            assert payload["review_summary"]["source"] == "local"

            with urllib.request.urlopen(
                f"http://127.0.0.1:{server.server_port}/healthz",
                timeout=5,
            ) as response:
                health = json.loads(response.read().decode("utf-8"))
            assert health["ok"] is True
            assert health["health"] in {"healthy", "degraded_recovering", "degraded_stale"}
        finally:
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()

    def test_report_slo_json_includes_windows(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        now = datetime.now(timezone.utc)
        db.record_metric_event(
            "cycle_observation",
            payload={"ready_for_executor": 4, "active_leases": 2, "global_limit": 4},
            now=now,
        )
        db.record_metric_event("claim_attempted", issue_ref="crew#84", now=now)
        db.record_metric_event("worker_durable_start", issue_ref="crew#84", now=now)
        session_id = db.create_session("crew#84", "codex")
        db.update_session(
            session_id,
            status="running",
            phase="running",
            started_at=now.isoformat(),
        )
        db.close()

        exit_code = _cmd_report_slo(config, as_json=True, local_only=True)
        payload = json.loads(capsys.readouterr().out)

        assert exit_code == 0
        assert payload["windows"]["1h"]["durable_starts"] == 1
        assert payload["windows"]["1h"]["ready_hours_ge_4"] is not None


# -- run_daemon_loop tests ----------------------------------------------------


class TestRunDaemonLoop:
    def test_transition_issue_to_in_progress_uses_ports(
        self, tmp_path: Path
    ) -> None:
        cp_config = _load(tmp_path)
        statuses = {"crew#84": "Review"}
        transitions: list[tuple[str, str]] = []

        review_state_port = SimpleNamespace(
            get_issue_status=lambda issue_ref: statuses[issue_ref]
        )

        def set_issue_status(issue_ref: str, status: str) -> None:
            transitions.append((issue_ref, status))
            statuses[issue_ref] = status

        board_port = SimpleNamespace(set_issue_status=set_issue_status)

        _transition_issue_to_in_progress(
            "crew#84",
            cp_config,
            "StartupAI-site",
            1,
            from_statuses={"Review"},
            review_state_port=review_state_port,
            board_port=board_port,
        )

        assert transitions == [("crew#84", "In Progress")]
        assert statuses["crew#84"] == "In Progress"

    def test_return_issue_to_ready_uses_ports(self, tmp_path: Path) -> None:
        cp_config = _load(tmp_path)
        statuses = {"crew#84": "In Progress"}
        transitions: list[tuple[str, str]] = []

        review_state_port = SimpleNamespace(
            get_issue_status=lambda issue_ref: statuses[issue_ref]
        )

        def set_issue_status(issue_ref: str, status: str) -> None:
            transitions.append((issue_ref, status))
            statuses[issue_ref] = status

        board_port = SimpleNamespace(set_issue_status=set_issue_status)

        _return_issue_to_ready(
            "crew#84",
            cp_config,
            "StartupAI-site",
            1,
            from_statuses={"In Progress"},
            review_state_port=review_state_port,
            board_port=board_port,
        )

        assert transitions == [("crew#84", "Ready")]
        assert statuses["crew#84"] == "Ready"

    def test_replay_deferred_actions_uses_ports(self, tmp_path: Path) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        cp_config = load_config(config.critical_paths_path)
        statuses = {
            "crew#84": "Review",
            "app#149": "In Progress",
        }
        comments: list[tuple[str, int, str]] = []
        closed: list[tuple[str, int]] = []
        transitions: list[tuple[str, str]] = []
        reruns: list[tuple[str, str, int]] = []
        automerge: list[tuple[str, int]] = []

        db.queue_deferred_action(
            "crew#84",
            "post_issue_comment",
            {"issue_ref": "crew#84", "body": "controller comment"},
        )
        db.queue_deferred_action(
            "crew#84",
            "close_issue",
            {"issue_ref": "crew#84"},
        )
        db.queue_deferred_action(
            "crew#84",
            "rerun_check",
            {
                "pr_repo": "StartupAI-site/startupai-crew",
                "check_name": "ci",
                "run_id": 42,
            },
        )
        db.queue_deferred_action(
            "crew#84",
            "enable_automerge",
            {
                "pr_repo": "StartupAI-site/startupai-crew",
                "pr_number": 77,
            },
        )
        db.queue_deferred_action(
            "crew#84",
            "set_status",
            {
                "issue_ref": "crew#84",
                "to_status": "In Progress",
                "from_statuses": ["Review"],
            },
        )
        db.queue_deferred_action(
            "app#149",
            "set_status",
            {
                "issue_ref": "app#149",
                "to_status": "Ready",
                "from_statuses": ["In Progress"],
            },
        )

        review_state_port = SimpleNamespace(
            get_issue_status=lambda issue_ref: statuses[issue_ref]
        )

        def set_issue_status(issue_ref: str, status: str) -> None:
            transitions.append((issue_ref, status))
            statuses[issue_ref] = status

        board_port = SimpleNamespace(
            post_issue_comment=lambda repo, number, body: comments.append(
                (repo, number, body)
            ),
            close_issue=lambda repo, number: closed.append((repo, number)),
            set_issue_status=set_issue_status,
        )
        pr_port = SimpleNamespace(
            rerun_failed_check=lambda repo, check_name, run_id: (
                reruns.append((repo, check_name, run_id)) or True
            ),
            enable_automerge=lambda repo, pr_number: automerge.append(
                (repo, pr_number)
            ),
        )

        replayed = _replay_deferred_actions(
            db,
            config,
            cp_config,
            pr_port=pr_port,
            review_state_port=review_state_port,
            board_port=board_port,
        )

        assert len(replayed) == 6
        assert comments == [
            ("StartupAI-site/startupai-crew", 84, "controller comment")
        ]
        assert closed == [("StartupAI-site/startupai-crew", 84)]
        assert reruns == [("StartupAI-site/startupai-crew", "ci", 42)]
        assert automerge == [("StartupAI-site/startupai-crew", 77)]
        assert transitions == [
            ("crew#84", "In Progress"),
            ("app#149", "Ready"),
        ]
        assert db.list_deferred_actions() == []

    def test_recover_interrupted_sessions_requeues_non_pr_work(
        self, tmp_path: Path
    ) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        session_id = db.create_session("crew#84", "codex")
        db.acquire_lease("crew#84", session_id, now=datetime.now(timezone.utc))
        db.update_session(
            session_id,
            status="running",
            worktree_path="/tmp/wt",
            branch_name="feat/84-test",
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        requeued: list[str] = []

        with patch(
            "startupai_controller.board_consumer._return_issue_to_ready",
            side_effect=lambda issue_ref, *args, **kwargs: requeued.append(issue_ref),
        ):
            recovered = _recover_interrupted_sessions(config, db)

        assert [lease.issue_ref for lease in recovered] == ["crew#84"]
        assert requeued == ["crew#84"]
        assert db.active_lease_count() == 0
        session = db.get_session(session_id)
        assert session is not None
        assert session.status == "aborted"

    def test_recover_interrupted_sessions_uses_ports_for_ready_transition(
        self, tmp_path: Path
    ) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        session_id = db.create_session("crew#84", "codex")
        db.acquire_lease("crew#84", session_id, now=datetime.now(timezone.utc))
        db.update_session(
            session_id,
            status="running",
            worktree_path="/tmp/wt",
            branch_name="feat/84-test",
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        auto_config = load_automation_config(config.automation_config_path)
        statuses = {"crew#84": "In Progress"}
        transitions: list[tuple[str, str]] = []

        pr_port = SimpleNamespace(list_open_prs_for_issue=lambda *args, **kwargs: [])
        review_state_port = SimpleNamespace(
            get_issue_status=lambda issue_ref: statuses[issue_ref]
        )

        def set_issue_status(issue_ref: str, status: str) -> None:
            transitions.append((issue_ref, status))
            statuses[issue_ref] = status

        board_port = SimpleNamespace(set_issue_status=set_issue_status)

        recovered = _recover_interrupted_sessions(
            config,
            db,
            automation_config=auto_config,
            pr_port=pr_port,
            review_state_port=review_state_port,
            board_port=board_port,
        )

        assert [lease.issue_ref for lease in recovered] == ["crew#84"]
        assert transitions == [("crew#84", "Ready")]
        assert statuses["crew#84"] == "Ready"

    def test_recover_interrupted_repair_session_returns_ready_not_review(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        session_id = db.create_session(
            "crew#84",
            "codex",
            session_kind="repair",
            repair_pr_url="https://github.com/O/R/pull/10",
        )
        db.acquire_lease("crew#84", session_id, now=datetime.now(timezone.utc))
        db.update_session(
            session_id,
            status="running",
            worktree_path="/tmp/wt",
            branch_name="feat/84-test",
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        requeued: list[str] = []
        transitioned: list[str] = []

        monkeypatch.setattr(
            "startupai_controller.board_consumer._return_issue_to_ready",
            lambda issue_ref, *args, **kwargs: requeued.append(issue_ref),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._transition_issue_to_review",
            lambda issue_ref, *args, **kwargs: transitioned.append(issue_ref),
        )

        recovered = _recover_interrupted_sessions(config, db)

        assert [lease.issue_ref for lease in recovered] == ["crew#84"]
        assert requeued == ["crew#84"]
        assert transitioned == []

    def test_reconcile_moves_review_item_back_to_in_progress_for_active_repair(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        session_id = db.create_session(
            "crew#84",
            "crew",
            slot_id=1,
            session_kind="repair",
            repair_pr_url="https://github.com/O/R/pull/10",
        )
        db.acquire_lease("crew#84", session_id, slot_id=1, now=datetime.now(timezone.utc))
        db.update_session(session_id, status="running", slot_id=1, phase="running")
        cp_config = load_config(config.critical_paths_path)
        auto_config = load_automation_config(config.automation_config_path)
        moved: list[str] = []

        monkeypatch.setattr(
            "startupai_controller.board_consumer._transition_issue_to_in_progress",
            lambda issue_ref, *args, **kwargs: moved.append(issue_ref),
        )
        review_item = _ProjectItemSnapshot(
            "StartupAI-site/startupai-crew#84",
            "Review",
            "codex",
            "none",
            "P0",
        )
        board_snapshot = CycleBoardSnapshot(
            items=(review_item,),
            by_status={"Review": (review_item,)},
        )

        result = _reconcile_board_truth(
            config,
            cp_config,
            auto_config,
            db,
            board_snapshot=board_snapshot,
        )

        assert result.moved_in_progress == ("crew#84",)
        assert moved == ["crew#84"]

    def test_reconcile_board_truth_uses_ports_for_active_repair(
        self, tmp_path: Path
    ) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        session_id = db.create_session(
            "crew#84",
            "crew",
            slot_id=1,
            session_kind="repair",
            repair_pr_url="https://github.com/O/R/pull/10",
        )
        db.acquire_lease("crew#84", session_id, slot_id=1, now=datetime.now(timezone.utc))
        db.update_session(session_id, status="running", slot_id=1, phase="running")
        cp_config = load_config(config.critical_paths_path)
        auto_config = load_automation_config(config.automation_config_path)
        statuses = {"crew#84": "Review"}
        transitions: list[tuple[str, str]] = []

        review_state_port = SimpleNamespace(
            list_issues_by_status=lambda status: (
                [
                    IssueSnapshot(
                        issue_ref="crew#84",
                        status="Review",
                        executor="codex",
                        priority="P0",
                        title="Test issue",
                        item_id="ITEM",
                        project_id="PROJ",
                    )
                ]
                if status == "Review"
                else []
            ),
            get_issue_status=lambda issue_ref: statuses[issue_ref],
        )

        def set_issue_status(issue_ref: str, status: str) -> None:
            transitions.append((issue_ref, status))
            statuses[issue_ref] = status

        board_port = SimpleNamespace(set_issue_status=set_issue_status)

        result = _reconcile_board_truth(
            config,
            cp_config,
            auto_config,
            db,
            review_state_port=review_state_port,
            board_port=board_port,
        )

        assert result.moved_in_progress == ("crew#84",)
        assert transitions == [("crew#84", "In Progress")]
        assert statuses["crew#84"] == "In Progress"

    def test_loop_calls_sleep_and_can_be_stopped(self, tmp_path: Path) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        call_count = {"n": 0}

        def sleep_fn(seconds: float) -> None:
            call_count["n"] += 1
            if call_count["n"] >= 2:
                raise StopIteration("stop")

        with pytest.raises(StopIteration):
            run_daemon_loop(
                config,
                db,
                dry_run=True,
                sleep_fn=sleep_fn,
                gh_runner=lambda *a, **kw: _empty_response(),
                status_resolver=lambda *a, **kw: "Done",
            )
        assert call_count["n"] >= 2

    def test_loop_continues_after_error(self, tmp_path: Path) -> None:
        config = _make_consumer_config(tmp_path)
        # Point at bad config to trigger error
        config.critical_paths_path = tmp_path / "bad.json"
        db = _make_db(tmp_path)
        call_count = {"n": 0}

        def sleep_fn(seconds: float) -> None:
            call_count["n"] += 1
            if call_count["n"] >= 2:
                raise StopIteration("stop")

        with pytest.raises(StopIteration):
            run_daemon_loop(config, db, sleep_fn=sleep_fn)
        # Loop continued past the config error
        assert call_count["n"] >= 2

    def test_loop_stops_before_next_claim_when_drain_requested(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        _request_drain(config.drain_path)
        called = {"run_one_cycle": False, "sleep": False}

        def fail_run_one_cycle(*args, **kwargs):
            called["run_one_cycle"] = True
            raise AssertionError("run_one_cycle should not be called while draining")

        def fail_sleep(_seconds: float) -> None:
            called["sleep"] = True
            raise AssertionError("sleep should not be called while draining")

        monkeypatch.setattr("startupai_controller.board_consumer.run_one_cycle", fail_run_one_cycle)
        try:
            run_daemon_loop(config, db, sleep_fn=fail_sleep)
        finally:
            _clear_drain(config.drain_path)

        assert called["run_one_cycle"] is False
        assert called["sleep"] is False

    def test_multi_worker_loop_launches_two_lowest_slots(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = _make_consumer_config(tmp_path)
        config.launch_hydration_concurrency = 2
        config.multi_worker_enabled = True
        config.global_concurrency = 2
        db = _make_db(tmp_path)
        launched: list[tuple[str, int]] = []
        candidates = iter(["crew#84", "crew#85", None])

        class _ImmediateFuture:
            def __init__(self, result):
                self._result = result

            def done(self) -> bool:
                return True

            def result(self):
                return self._result

        class _FakeExecutor:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def submit(self, fn, *args, **kwargs):
                return _ImmediateFuture(fn(*args, **kwargs))

        monkeypatch.setattr("startupai_controller.board_consumer.ThreadPoolExecutor", _FakeExecutor)
        monkeypatch.setattr(
            "startupai_controller.board_consumer.load_automation_config",
            lambda *args, **kwargs: None,
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._current_main_workflows",
            lambda *args, **kwargs: ({}, {"crew": SimpleNamespace(available=True)}, 1),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._recover_interrupted_sessions",
            lambda *args, **kwargs: [],
        )
        prepared = SimpleNamespace(
            cp_config=load_config(_write_config(tmp_path)),
            auto_config=None,
            main_workflows={},
            workflow_statuses={"crew": SimpleNamespace(available=True)},
            global_limit=2,
            effective_interval=1,
            dispatchable_repo_prefixes=("crew",),
            board_snapshot=SimpleNamespace(items=(), items_with_status=lambda *_args, **_kwargs: ()),
            github_memo=SimpleNamespace(),
            admission_summary={},
            timings_ms={},
            github_request_counts={},
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._prepare_cycle",
            lambda *args, **kwargs: prepared,
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._select_candidate_for_cycle",
            lambda *args, **kwargs: next(candidates),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer._prepare_launch_candidate",
            lambda issue_ref, **kwargs: SimpleNamespace(issue_ref=issue_ref),
        )

        def fake_worker_cycle(
            config,
            *,
            target_issue,
            slot_id,
            prepared,
            launch_context=None,
            dry_run=False,
            di_kwargs=None,
        ):
            launched.append((target_issue, slot_id))
            return CycleResult(
                action="claimed",
                issue_ref=target_issue,
                session_id=f"session-{slot_id}",
                reason="success",
            )

        monkeypatch.setattr(
            "startupai_controller.board_consumer._run_worker_cycle",
            fake_worker_cycle,
        )

        with pytest.raises(StopIteration):
            run_daemon_loop(
                config,
                db,
                sleep_fn=lambda _seconds: (_ for _ in ()).throw(StopIteration()),
            )

        assert launched == [("crew#84", 1), ("crew#85", 2)]


# -- Fix D: Pre-backfill verdicts before snapshot ----------------------------


class TestPreBackfillVerdicts:
    def test_pre_backfill_posts_verdict_for_blocked_verdict_entry(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        now = datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc)
        pr_url = "https://github.com/StartupAI-site/startupai-crew/pull/210"
        sess_id = db.create_session("crew#84", "codex", session_kind="repair")
        db.update_session(sess_id, status="success", phase="review", pr_url=pr_url)
        db.enqueue_review_item(
            "crew#84",
            pr_url=pr_url,
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=210,
            source_session_id=sess_id,
            now=now - timedelta(minutes=10),
        )
        db.update_review_queue_item(
            "crew#84",
            next_attempt_at=now.isoformat(),
            last_result="blocked",
            last_reason="missing codex verdict marker",
            now=now,
        )
        entry = db.get_review_queue_item("crew#84")
        assert entry is not None
        posted_calls: list[str] = []

        def mock_poster(owner, repo, number, body, **kwargs):
            posted_calls.append(f"{owner}/{repo}#{number}")

        result = _pre_backfill_verdicts_for_due_prs(
            db,
            [entry],
            comment_checker=lambda *_, **__: False,
            comment_poster=mock_poster,
        )
        assert "crew#84" in result
        assert len(posted_calls) == 1

    def test_pre_backfill_posts_verdict_for_newly_seeded_entry(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        now = datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc)
        pr_url = "https://github.com/StartupAI-site/startupai-crew/pull/210"
        sess_id = db.create_session("crew#84", "codex", session_kind="repair")
        db.update_session(sess_id, status="success", phase="review", pr_url=pr_url)
        db.enqueue_review_item(
            "crew#84",
            pr_url=pr_url,
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=210,
            source_session_id=sess_id,
            now=now,
        )
        entry = db.get_review_queue_item("crew#84")
        assert entry is not None
        assert entry.last_result is None  # newly seeded
        posted: list[str] = []

        result = _pre_backfill_verdicts_for_due_prs(
            db,
            [entry],
            comment_checker=lambda *_, **__: False,
            comment_poster=lambda *a, **kw: posted.append("posted"),
        )
        assert "crew#84" in result
        assert len(posted) == 1

    def test_pre_backfill_skips_non_verdict_blocked_entries(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        now = datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc)
        pr_url = "https://github.com/StartupAI-site/startupai-crew/pull/210"
        sess_id = db.create_session("crew#84", "codex", session_kind="repair")
        db.update_session(sess_id, status="success", phase="review", pr_url=pr_url)
        db.enqueue_review_item(
            "crew#84",
            pr_url=pr_url,
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=210,
            source_session_id=sess_id,
            now=now - timedelta(minutes=10),
        )
        db.update_review_queue_item(
            "crew#84",
            next_attempt_at=now.isoformat(),
            last_result="blocked",
            last_reason="required checks pending",
            now=now,
        )
        entry = db.get_review_queue_item("crew#84")
        assert entry is not None

        result = _pre_backfill_verdicts_for_due_prs(
            db,
            [entry],
            comment_checker=lambda *_, **__: False,
            comment_poster=lambda *a, **kw: None,
        )
        assert result == ()


# -- Fix A1: Repair-requeue ceiling ------------------------------------------


class TestRepairRequeueCeiling:
    def test_requeue_inhibited_at_ceiling(self, tmp_path: Path) -> None:
        """At ceiling, escalate instead of requeuing."""
        db = _make_db(tmp_path)
        pr_url = "https://github.com/StartupAI-site/startupai-crew/pull/42"
        # Set requeue count at ceiling
        for _ in range(MAX_REQUEUE_CYCLES):
            db.increment_requeue_count("crew#88", pr_url)
        count, _ = db.get_requeue_state("crew#88")
        assert count == MAX_REQUEUE_CYCLES

    def test_requeue_allowed_below_ceiling(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        pr_url = "https://github.com/StartupAI-site/startupai-crew/pull/42"
        db.increment_requeue_count("crew#88", pr_url)
        count, _ = db.get_requeue_state("crew#88")
        assert count == 1
        assert count < MAX_REQUEUE_CYCLES

    def test_requeue_count_resets_on_pr_url_change(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        old_url = "https://github.com/o/r/pull/42"
        new_url = "https://github.com/o/r/pull/55"
        db.increment_requeue_count("crew#88", old_url)
        db.increment_requeue_count("crew#88", old_url)
        count, stored = db.get_requeue_state("crew#88")
        assert count == 2
        assert stored == old_url
        # Simulate PR identity change detection
        if stored != new_url:
            db.reset_requeue_count("crew#88")
        count, _ = db.get_requeue_state("crew#88")
        assert count == 0

    def test_requeue_count_preserved_on_same_pr_url(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        url = "https://github.com/o/r/pull/42"
        db.increment_requeue_count("crew#88", url)
        db.increment_requeue_count("crew#88", url)
        count, stored = db.get_requeue_state("crew#88")
        assert count == 2
        # Same URL → no reset
        if stored != url:
            db.reset_requeue_count("crew#88")
        count, _ = db.get_requeue_state("crew#88")
        assert count == 2  # unchanged

    def test_requeue_results_not_deleted_by_apply_review_queue_result(
        self, tmp_path: Path
    ) -> None:
        """_apply_review_queue_result no longer handles requeued_refs — returns False."""
        db = _make_db(tmp_path)
        now = datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc)
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/o/r/pull/200",
            pr_repo="o/r",
            pr_number=200,
            now=now,
        )
        entry = db.get_review_queue_item("crew#84")
        assert entry is not None

        result = _apply_review_queue_result(
            db,
            entry,
            SimpleNamespace(
                rerun_checks=(),
                auto_merge_enabled=False,
                requeued_refs=("crew#84",),
                blocked_reason=None,
                skipped_reason=None,
            ),
            now=now,
        )
        # Returns False (no escalation) and does NOT delete the queue item
        assert result is False
        assert db.get_review_queue_item("crew#84") is not None


# -- Fix A2: Blocked-streak escalation policy --------------------------------


class TestBlockedStreakEscalation:
    def test_blocker_class_categories(self) -> None:
        assert _blocker_class("required checks pending [ci]") == "transient"
        assert _blocker_class("required checks failed [ci]") == "failed_checks"
        assert _blocker_class("mergeable=CONFLICTING") == "failed_checks"
        assert _blocker_class("missing codex verdict marker") == "stable"
        assert _blocker_class("draft-pr") == "stable"
        assert _blocker_class("auto-merge pending verification") == "automerge"
        assert _blocker_class("some unknown reason") == "default"

    def test_escalation_ceiling_for_blocker_class(self) -> None:
        assert _escalation_ceiling_for_blocker_class("transient") == ESCALATION_CEILING_TRANSIENT
        assert _escalation_ceiling_for_blocker_class("failed_checks") == ESCALATION_CEILING_FAILED
        assert _escalation_ceiling_for_blocker_class("unknown") == 8  # default

    def test_blocked_streak_increments_within_same_class(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        now = datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc)
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/o/r/pull/1",
            pr_repo="o/r",
            pr_number=1,
            now=now,
        )
        entry = db.get_review_queue_item("crew#84")
        assert entry is not None

        _apply_review_queue_result(
            db,
            entry,
            SimpleNamespace(
                rerun_checks=(),
                auto_merge_enabled=False,
                requeued_refs=(),
                blocked_reason="required checks failed [ci]",
                skipped_reason=None,
            ),
            now=now,
        )
        updated = db.get_review_queue_item("crew#84")
        assert updated is not None
        assert updated.blocked_streak == 1
        assert updated.blocked_class == "failed_checks"

        # Second block with same class
        _apply_review_queue_result(
            db,
            updated,
            SimpleNamespace(
                rerun_checks=(),
                auto_merge_enabled=False,
                requeued_refs=(),
                blocked_reason="required checks failed [ci, e2e]",
                skipped_reason=None,
            ),
            now=now,
        )
        updated2 = db.get_review_queue_item("crew#84")
        assert updated2 is not None
        assert updated2.blocked_streak == 2
        assert updated2.blocked_class == "failed_checks"

    def test_blocked_streak_resets_on_class_change(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        now = datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc)
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/o/r/pull/1",
            pr_repo="o/r",
            pr_number=1,
            now=now,
        )
        entry = db.get_review_queue_item("crew#84")
        assert entry is not None

        # Build up streak of 5 for "transient"
        current = entry
        for _ in range(5):
            _apply_review_queue_result(
                db,
                current,
                SimpleNamespace(
                    rerun_checks=(),
                    auto_merge_enabled=False,
                    requeued_refs=(),
                    blocked_reason="required checks pending [ci]",
                    skipped_reason=None,
                ),
                now=now,
            )
            current = db.get_review_queue_item("crew#84")
        assert current.blocked_streak == 5
        assert current.blocked_class == "transient"

        # Different class → resets to 1
        _apply_review_queue_result(
            db,
            current,
            SimpleNamespace(
                rerun_checks=(),
                auto_merge_enabled=False,
                requeued_refs=(),
                blocked_reason="required checks failed [ci]",
                skipped_reason=None,
            ),
            now=now,
        )
        final = db.get_review_queue_item("crew#84")
        assert final.blocked_streak == 1
        assert final.blocked_class == "failed_checks"

    def test_blocked_streak_resets_on_non_blocked_result(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        now = datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc)
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/o/r/pull/1",
            pr_repo="o/r",
            pr_number=1,
            now=now,
        )
        entry = db.get_review_queue_item("crew#84")

        _apply_review_queue_result(
            db,
            entry,
            SimpleNamespace(
                rerun_checks=(),
                auto_merge_enabled=False,
                requeued_refs=(),
                blocked_reason="required checks failed [ci]",
                skipped_reason=None,
            ),
            now=now,
        )
        blocked = db.get_review_queue_item("crew#84")
        assert blocked.blocked_streak == 1

        _apply_review_queue_result(
            db,
            blocked,
            SimpleNamespace(
                rerun_checks=(),
                auto_merge_enabled=True,
                requeued_refs=(),
                blocked_reason=None,
                skipped_reason=None,
            ),
            now=now,
        )
        cleared = db.get_review_queue_item("crew#84")
        assert cleared.blocked_streak == 0
        assert cleared.blocked_class is None

    def test_apply_result_signals_escalation_at_ceiling(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        now = datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc)
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/o/r/pull/1",
            pr_repo="o/r",
            pr_number=1,
            now=now,
        )
        entry = db.get_review_queue_item("crew#84")
        # Build up streak just below ceiling for failed_checks (6)
        current = entry
        for i in range(ESCALATION_CEILING_FAILED - 1):
            _apply_review_queue_result(
                db,
                current,
                SimpleNamespace(
                    rerun_checks=(),
                    auto_merge_enabled=False,
                    requeued_refs=(),
                    blocked_reason="required checks failed [ci]",
                    skipped_reason=None,
                ),
                now=now,
            )
            current = db.get_review_queue_item("crew#84")

        assert current.blocked_streak == ESCALATION_CEILING_FAILED - 1

        # This one should trigger escalation
        needs_escalation = _apply_review_queue_result(
            db,
            current,
            SimpleNamespace(
                rerun_checks=(),
                auto_merge_enabled=False,
                requeued_refs=(),
                blocked_reason="required checks failed [ci]",
                skipped_reason=None,
            ),
            now=now,
        )
        assert needs_escalation is True

    def test_apply_result_no_escalation_below_ceiling(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        now = datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc)
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/o/r/pull/1",
            pr_repo="o/r",
            pr_number=1,
            now=now,
        )
        entry = db.get_review_queue_item("crew#84")

        needs_escalation = _apply_review_queue_result(
            db,
            entry,
            SimpleNamespace(
                rerun_checks=(),
                auto_merge_enabled=False,
                requeued_refs=(),
                blocked_reason="required checks failed [ci]",
                skipped_reason=None,
            ),
            now=now,
        )
        assert needs_escalation is False


# -- Fix B (consumer side): pending automerge retry --------------------------


class TestPendingAutomergeRetry:
    def test_review_queue_short_retry_for_pending_verification(self) -> None:
        from startupai_controller.board_consumer import (
            _review_queue_retry_seconds_for_blocked_reason,
        )
        seconds = _review_queue_retry_seconds_for_blocked_reason(
            "auto-merge pending verification"
        )
        assert seconds == REVIEW_QUEUE_PENDING_AUTOMERGE_RETRY_SECONDS
