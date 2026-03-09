"""Unit tests for the atomic promote-ready script."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
from unittest.mock import MagicMock

import pytest

import startupai_controller.promote_ready as promoter
from startupai_controller.github_http import GitHubTransportError
from startupai_controller.promote_ready import BoardInfo, promote_to_ready
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig, load_config


def _valid_payload() -> dict:
    return {
        "issue_prefixes": {
            "app": "StartupAI-site/app.startupai-site",
            "crew": "StartupAI-site/startupai-crew",
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


def _make_info(status: str) -> BoardInfo:
    return BoardInfo(status=status, item_id="ITEM_123", project_id="PROJ_456")


# ── Successful promotions ─────────────────────────────────────────


def test_promote_from_backlog_succeeds(tmp_path: Path) -> None:
    config = _load(tmp_path)
    mutator = MagicMock()

    code, output = promote_to_ready(
        issue_ref="crew#88",
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        board_info_resolver=lambda *_: _make_info("Backlog"),
        status_resolver=lambda *_: "Done",
        board_mutator=mutator,
    )

    assert code == 0
    assert "Backlog -> Ready (promoted)" in output
    mutator.assert_called_once_with("PROJ_456", "ITEM_123")


def test_promote_from_blocked_succeeds(tmp_path: Path) -> None:
    config = _load(tmp_path)
    mutator = MagicMock()

    code, output = promote_to_ready(
        issue_ref="crew#88",
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        board_info_resolver=lambda *_: _make_info("Blocked"),
        status_resolver=lambda *_: "Done",
        board_mutator=mutator,
    )

    assert code == 0
    assert "Blocked -> Ready (promoted)" in output
    mutator.assert_called_once()


# ── Status rejections ─────────────────────────────────────────────


def test_reject_already_ready(tmp_path: Path) -> None:
    config = _load(tmp_path)
    code, output = promote_to_ready(
        issue_ref="crew#88",
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        board_info_resolver=lambda *_: _make_info("Ready"),
        status_resolver=lambda *_: "Done",
    )
    assert code == 2
    assert "must be Backlog or Blocked" in output


def test_reject_in_progress(tmp_path: Path) -> None:
    config = _load(tmp_path)
    code, output = promote_to_ready(
        issue_ref="crew#88",
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        board_info_resolver=lambda *_: _make_info("In Progress"),
        status_resolver=lambda *_: "Done",
    )
    assert code == 2
    assert "must be Backlog or Blocked" in output


def test_reject_review(tmp_path: Path) -> None:
    config = _load(tmp_path)
    code, output = promote_to_ready(
        issue_ref="crew#88",
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        board_info_resolver=lambda *_: _make_info("Review"),
        status_resolver=lambda *_: "Done",
    )
    assert code == 2
    assert "must be Backlog or Blocked" in output


def test_reject_done(tmp_path: Path) -> None:
    config = _load(tmp_path)
    code, output = promote_to_ready(
        issue_ref="crew#88",
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        board_info_resolver=lambda *_: _make_info("Done"),
        status_resolver=lambda *_: "Done",
    )
    assert code == 2
    assert "must be Backlog or Blocked" in output


def test_reject_not_on_board(tmp_path: Path) -> None:
    config = _load(tmp_path)
    code, output = promote_to_ready(
        issue_ref="crew#88",
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        board_info_resolver=lambda *_: _make_info("NOT_ON_BOARD"),
        status_resolver=lambda *_: "Done",
    )
    assert code == 2
    assert "must be Backlog or Blocked" in output


# ── Validator-level rejections ────────────────────────────────────


def test_blocked_by_predecessor(tmp_path: Path) -> None:
    config = _load(tmp_path)
    mutator = MagicMock()

    code, output = promote_to_ready(
        issue_ref="crew#88",
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        board_info_resolver=lambda *_: _make_info("Backlog"),
        status_resolver=lambda *_: "In Progress",
        board_mutator=mutator,
    )

    assert code == 2
    assert "BLOCKED" in output
    mutator.assert_not_called()


def test_blocked_not_in_graph(tmp_path: Path) -> None:
    config = _load(tmp_path)
    mutator = MagicMock()

    code, output = promote_to_ready(
        issue_ref="app#999",
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        board_info_resolver=lambda *_: _make_info("Backlog"),
        status_resolver=lambda *_: "Done",
        board_mutator=mutator,
    )

    assert code == 2
    assert "--require-in-graph" in output
    mutator.assert_not_called()


# ── Dry-run behavior ─────────────────────────────────────────────


def test_dry_run_no_mutation(tmp_path: Path) -> None:
    config = _load(tmp_path)
    mutator = MagicMock()

    code, output = promote_to_ready(
        issue_ref="crew#88",
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        dry_run=True,
        board_info_resolver=lambda *_: _make_info("Backlog"),
        status_resolver=lambda *_: "Done",
        board_mutator=mutator,
    )

    assert code == 0
    assert "would promote" in output
    mutator.assert_not_called()


def test_dry_run_reports_failure(tmp_path: Path) -> None:
    config = _load(tmp_path)

    code, output = promote_to_ready(
        issue_ref="crew#88",
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        dry_run=True,
        board_info_resolver=lambda *_: _make_info("In Progress"),
        status_resolver=lambda *_: "Done",
    )

    assert code == 2
    assert "must be Backlog or Blocked" in output


def test_rejects_when_controller_owns_admission(tmp_path: Path) -> None:
    config = _load(tmp_path)
    code, output = promote_to_ready(
        issue_ref="crew#88",
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        controller_owned_resolver=lambda issue_ref: issue_ref == "crew#88",
    )

    assert code == 2
    assert "controller_owned_admission" in output


# ── CLI integration ───────────────────────────────────────────────


def test_main_passes_dry_run_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = _write_config(tmp_path)

    monkeypatch.setattr(
        promoter,
        "_query_issue_board_info",
        lambda *_: _make_info("Backlog"),
    )
    # Predecessor resolver for the validator
    monkeypatch.setattr(
        promoter, "evaluate_ready_promotion",
        lambda issue_ref, config, project_owner, project_number,
        status_resolver=None, require_in_graph=False: (0, ""),
    )

    code = promoter.main(
        ["--issue", "crew#88", "--file", str(config_path), "--dry-run"]
    )
    assert code == 0
    captured = capsys.readouterr().out
    assert "would promote" in captured


def test_main_issue_guard() -> None:
    with pytest.raises(SystemExit):
        promoter.main([])


def test_query_issue_board_info_reads_transport_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _load(tmp_path)
    payload = json.dumps(
        {
            "data": {
                "repository": {
                    "issue": {
                        "projectItems": {
                            "nodes": [
                                {
                                    "id": "ITEM_123",
                                    "project": {
                                        "id": "PROJ_456",
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
    monkeypatch.setattr(promoter, "run_github_command", lambda *args, **kwargs: payload)

    info = promoter._query_issue_board_info("crew#88", config, "StartupAI-site", 1)

    assert info == BoardInfo(status="Ready", item_id="ITEM_123", project_id="PROJ_456")


def test_query_issue_board_info_surfaces_transport_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _load(tmp_path)
    monkeypatch.setattr(
        promoter,
        "run_github_command",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            GitHubTransportError(
                operation_type="query",
                failure_kind="network",
                command_excerpt="api graphql",
                detail="timed out after 8.0s",
            )
        ),
    )

    with pytest.raises(promoter.GhQueryError, match="timed out after"):
        promoter._query_issue_board_info("crew#88", config, "StartupAI-site", 1)
