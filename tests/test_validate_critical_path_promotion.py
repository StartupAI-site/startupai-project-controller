"""Unit tests for critical-path promotion validator script."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess

import pytest

import startupai_controller.validate_critical_path_promotion as validator
from startupai_controller.github_http import GitHubTransportError


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
                ],
            }
        },
    }


def _write_config(tmp_path: Path, payload: dict | None = None) -> Path:
    config_path = tmp_path / "critical-paths.json"
    data = payload if payload is not None else _valid_payload()
    config_path.write_text(json.dumps(data), encoding="utf-8")
    return config_path


def test_load_config_valid(tmp_path: Path) -> None:
    config = validator.load_config(_write_config(tmp_path))
    assert "planning-surface" in config.critical_paths
    assert config.critical_paths["planning-surface"].first_value_at == "app#149"


def test_load_config_rejects_invalid_first_value_type(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["critical_paths"]["planning-surface"]["first_value_at"] = None
    with pytest.raises(validator.ConfigError, match="first_value_at must be a string"):
        validator.load_config(_write_config(tmp_path, payload))


def test_load_config_rejects_invalid_issue_ref(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["critical_paths"]["planning-surface"]["edges"][0][0] = "bad#149"
    with pytest.raises(validator.ConfigError, match="Invalid issue ref"):
        validator.load_config(_write_config(tmp_path, payload))


def test_parse_issue_ref_accepts_site_prefix() -> None:
    parsed = validator.parse_issue_ref("site#55")
    assert parsed.prefix == "site"
    assert parsed.number == 55


def test_load_config_rejects_cycle(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["critical_paths"]["planning-surface"]["edges"] = [
        ["app#149", "crew#84"],
        ["crew#84", "app#149"],
    ]
    with pytest.raises(validator.ConfigError, match="contains a cycle"):
        validator.load_config(_write_config(tmp_path, payload))


def test_direct_predecessors_are_direct_only(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["critical_paths"]["planning-surface"]["edges"] = [
        ["app#149", "crew#84"],
        ["crew#84", "crew#85"],
        ["crew#85", "crew#88"],
    ]
    config = validator.load_config(_write_config(tmp_path, payload))
    assert validator.direct_predecessors(config, "crew#88") == {"crew#85"}


def test_evaluate_ready_promotion_warns_for_non_critical_issue(tmp_path: Path) -> None:
    config = validator.load_config(_write_config(tmp_path))
    code, output = validator.evaluate_ready_promotion(
        issue_ref="app#999",
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        status_resolver=lambda *_args: "Done",
    )
    assert code == 0
    assert output == "WARN: issue app#999 is not present in any critical path."


def test_evaluate_ready_promotion_blocks_on_unmet_direct_predecessor(
    tmp_path: Path,
) -> None:
    config = validator.load_config(_write_config(tmp_path))
    code, output = validator.evaluate_ready_promotion(
        issue_ref="crew#88",
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        status_resolver=lambda *_args: "In Progress",
    )
    assert code == 2
    assert output.splitlines() == [
        "BLOCKED: crew#88 cannot move to Ready.",
        "Unmet predecessors:",
        "- crew#87 (Status=In Progress)",
    ]


def test_evaluate_ready_promotion_passes_when_predecessors_done(tmp_path: Path) -> None:
    config = validator.load_config(_write_config(tmp_path))
    code, output = validator.evaluate_ready_promotion(
        issue_ref="crew#88",
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        status_resolver=lambda *_args: "Done",
    )
    assert code == 0
    assert output == ""


def test_main_returns_3_for_config_error_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    assert (
        validator.main(
            [
                "--issue",
                "crew#88",
                "--file",
                str(missing),
            ]
        )
        == 3
    )


def test_main_returns_4_for_gh_query_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _write_config(tmp_path)

    def _raise_gh(*_args, **_kwargs) -> str:
        raise validator.GhQueryError("simulated gh failure")

    monkeypatch.setattr(validator, "_query_board_status", _raise_gh)
    assert (
        validator.main(
            [
                "--issue",
                "crew#88",
                "--file",
                str(config_path),
            ]
        )
        == 4
    )


def test_main_returns_2_for_blocked_predecessor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = _write_config(tmp_path)

    monkeypatch.setattr(
        validator, "_query_board_status", lambda *_args, **_kwargs: "Blocked"
    )
    code = validator.main(
        [
            "--issue",
            "crew#88",
            "--file",
            str(config_path),
        ]
    )
    assert code == 2
    assert "BLOCKED: crew#88 cannot move to Ready." in capsys.readouterr().out


def test_main_returns_0_for_warning_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = _write_config(tmp_path)

    monkeypatch.setattr(
        validator, "_query_board_status", lambda *_args, **_kwargs: "Done"
    )
    code = validator.main(
        [
            "--issue",
            "crew#999",
            "--file",
            str(config_path),
        ]
    )
    assert code == 0
    assert (
        "WARN: issue crew#999 is not present in any critical path."
        in capsys.readouterr().out
    )


def test_evaluate_ready_promotion_blocks_when_require_in_graph(tmp_path: Path) -> None:
    config = validator.load_config(_write_config(tmp_path))
    code, output = validator.evaluate_ready_promotion(
        issue_ref="app#999",
        config=config,
        project_owner="StartupAI-site",
        project_number=1,
        status_resolver=lambda *_args: "Done",
        require_in_graph=True,
    )
    assert code == 2
    assert "BLOCKED" in output
    assert "--require-in-graph" in output


def test_main_returns_2_for_require_in_graph_missing_issue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _write_config(tmp_path)
    monkeypatch.setattr(
        validator, "_query_board_status", lambda *_args, **_kwargs: "Done"
    )
    code = validator.main(
        ["--issue", "crew#999", "--file", str(config_path), "--require-in-graph"]
    )
    assert code == 2


def test_query_board_status_raises_on_graphql_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = validator.load_config(_write_config(tmp_path))
    monkeypatch.setattr(
        validator,
        "run_github_command",
        lambda *args, **kwargs: json.dumps({"errors": [{"message": "rate limited"}]}),
    )
    with pytest.raises(validator.GhQueryError, match="rate limited"):
        validator._query_board_status("crew#88", config, "StartupAI-site", 1)


def test_query_board_status_raises_on_invalid_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = validator.load_config(_write_config(tmp_path))
    monkeypatch.setattr(
        validator, "run_github_command", lambda *args, **kwargs: "not-json"
    )
    with pytest.raises(validator.GhQueryError, match="invalid JSON response"):
        validator._query_board_status("crew#88", config, "StartupAI-site", 1)


def test_query_board_status_returns_not_on_board(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = validator.load_config(_write_config(tmp_path))
    monkeypatch.setattr(
        validator,
        "run_github_command",
        lambda *args, **kwargs: json.dumps(
            {"data": {"repository": {"issue": {"projectItems": {"nodes": []}}}}}
        ),
    )
    assert (
        validator._query_board_status("crew#88", config, "StartupAI-site", 1)
        == "NOT_ON_BOARD"
    )


def test_query_board_status_reads_transport_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = validator.load_config(_write_config(tmp_path))
    payload = json.dumps(
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
                                    "statusField": {"name": "Done"},
                                }
                            ]
                        }
                    }
                }
            }
        }
    )
    monkeypatch.setattr(
        validator, "run_github_command", lambda *args, **kwargs: payload
    )

    status = validator._query_board_status("crew#88", config, "StartupAI-site", 1)

    assert status == "Done"


def test_query_board_status_surfaces_transport_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = validator.load_config(_write_config(tmp_path))
    monkeypatch.setattr(
        validator,
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

    with pytest.raises(validator.GhQueryError, match="timed out after"):
        validator._query_board_status("crew#88", config, "StartupAI-site", 1)
