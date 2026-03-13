"""Unit tests for pull-request adapter helper functions."""

from __future__ import annotations

import json
from pathlib import Path

from startupai_controller.adapters.pull_requests import _repo_to_prefix
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
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


def test_repo_to_prefix(tmp_path: Path) -> None:
    """Reverse lookup works for all configured repos."""
    config = _load(tmp_path)

    assert _repo_to_prefix("StartupAI-site/startupai-crew", config) == "crew"
    assert _repo_to_prefix("StartupAI-site/app.startupai-site", config) == "app"
    assert _repo_to_prefix("StartupAI-site/startupai.site", config) == "site"
    assert _repo_to_prefix("StartupAI-site/unknown", config) is None
