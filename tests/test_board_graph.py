"""Unit tests for board_graph module.

All tests use dependency injection (DI) -- NO real GitHub API calls.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from startupai_controller.board_graph import (
    _issue_sort_key,
    _resolve_issue_coordinates,
    _ready_snapshot_rank,
    _count_wip_by_executor,
    _count_wip_by_executor_lane,
    admission_watermarks,
    classify_parallelism_snapshot,
    find_unmet_ready_dependencies,
    has_structured_acceptance_criteria,
)
from startupai_controller.domain.models import IssueSnapshot
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    ConfigError,
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


def _make_snapshot(
    issue_ref: str = "crew#84",
    status: str = "Ready",
    executor: str = "claude",
    priority: str = "P0",
) -> IssueSnapshot:
    """Build a typed issue snapshot for graph-level tests."""
    return IssueSnapshot(
        issue_ref=issue_ref,
        status=status,
        executor=executor,
        priority=priority,
        title="Example",
        item_id="ITEM",
        project_id="PROJECT",
    )


# -- _issue_sort_key tests ---------------------------------------------------


def test_issue_sort_key_ordering() -> None:
    """Prefix grouping and numeric ordering."""
    assert _issue_sort_key("crew#84") < _issue_sort_key("crew#88")
    assert _issue_sort_key("app#149") < _issue_sort_key("crew#84")


# -- _resolve_issue_coordinates tests -----------------------------------------


def test_resolve_issue_coordinates_happy(tmp_path: Path) -> None:
    """Known prefix resolves to owner/repo/number."""
    config = _load(tmp_path)
    owner, repo, number = _resolve_issue_coordinates("crew#88", config)
    assert owner == "StartupAI-site"
    assert repo == "startupai-crew"
    assert number == 88


def test_resolve_issue_coordinates_unknown_prefix(tmp_path: Path) -> None:
    """Unknown prefix raises ConfigError."""
    config = _load(tmp_path)
    with pytest.raises(ConfigError):
        _resolve_issue_coordinates("unknown#1", config)


# -- _ready_snapshot_rank tests -----------------------------------------------


def test_ready_snapshot_rank_critical_path_first(tmp_path: Path) -> None:
    """Critical-path item ranks before non-critical."""
    config = _load(tmp_path)
    critical = _make_snapshot(issue_ref="crew#84")
    non_critical = _make_snapshot(issue_ref="crew#999")
    assert _ready_snapshot_rank(critical, config) < _ready_snapshot_rank(
        non_critical, config
    )


def test_ready_snapshot_rank_priority_tiebreak(tmp_path: Path) -> None:
    """Same critical status, P0 ranks before P2."""
    config = _load(tmp_path)
    p0 = _make_snapshot(issue_ref="crew#84", priority="P0")
    p2 = _make_snapshot(issue_ref="crew#85", priority="P2")
    assert _ready_snapshot_rank(p0, config) < _ready_snapshot_rank(p2, config)


def test_acceptance_criteria_requires_structured_section() -> None:
    """Admission only accepts bodies with a supported heading and list items."""
    assert has_structured_acceptance_criteria("""
## Acceptance Criteria
- user can see the result
- tests cover the controller
""")
    assert has_structured_acceptance_criteria("""
## Definition of Done
1. Ready buffer replenishes automatically
""")
    assert not has_structured_acceptance_criteria(
        "Acceptance criteria should probably be added later."
    )


def test_admission_watermarks_follow_global_concurrency() -> None:
    """Admission floor/cap default to 2x/3x global concurrency."""
    assert admission_watermarks(2) == (4, 6)


# -- _count_wip_by_executor tests ---------------------------------------------


def test_count_wip_by_executor(tmp_path: Path) -> None:
    """Injected board snapshot produces correct per-executor counts."""
    items = [
        _make_snapshot(issue_ref="crew#84", executor="claude", status="In Progress"),
        _make_snapshot(issue_ref="crew#85", executor="claude", status="In Progress"),
        _make_snapshot(issue_ref="crew#86", executor="codex", status="In Progress"),
    ]
    counts = _count_wip_by_executor(items)
    assert counts["claude"] == 2
    assert counts["codex"] == 1


def test_count_wip_by_executor_empty() -> None:
    """Empty board returns empty dict."""
    counts = _count_wip_by_executor([])
    assert counts == {}


# -- _count_wip_by_executor_lane tests ----------------------------------------


def test_count_wip_by_executor_lane(tmp_path: Path) -> None:
    """Injected board snapshot produces correct per-lane counts."""
    config = _load(tmp_path)
    items = [
        _make_snapshot(issue_ref="crew#84", executor="claude", status="In Progress"),
        _make_snapshot(issue_ref="app#149", executor="claude", status="In Progress"),
        _make_snapshot(issue_ref="crew#85", executor="codex", status="In Progress"),
    ]
    counts = _count_wip_by_executor_lane(config, items)
    assert counts[("claude", "crew")] == 1
    assert counts[("claude", "app")] == 1
    assert counts[("codex", "crew")] == 1


# -- classify_parallelism_snapshot tests ---------------------------------------


def test_classify_parallelism_snapshot_outputs_expected_keys(
    tmp_path: Path,
) -> None:
    """Result contains parallel/waiting/blocked/non_graph keys."""
    config = _load(tmp_path)
    result = classify_parallelism_snapshot(
        config=config,
        ready_items=[],
        blocked_items=[],
        status_resolver=lambda *_: "Done",
        project_owner="StartupAI-site",
        project_number=1,
    )
    assert "parallel" in result
    assert "waiting_on_dependency" in result
    assert "blocked_policy" in result
    assert "non_graph" in result


# -- find_unmet_ready_dependencies tests ---------------------------------------


def test_find_unmet_ready_dependencies_returns_pairs(tmp_path: Path) -> None:
    """Ready item with unmet predecessor returns (ref, reason) pair."""
    config = _load(tmp_path)
    # crew#85 depends on crew#84 — if crew#84 is not Done, crew#85 is unmet
    item = _make_snapshot(issue_ref="crew#85", executor="claude", status="Ready")
    result = find_unmet_ready_dependencies(
        config=config,
        ready_items=[item],
        all_prefixes=True,
        status_resolver=lambda *_: "Ready",  # predecessor not Done
        project_owner="StartupAI-site",
        project_number=1,
    )
    refs = [ref for ref, _ in result]
    assert "crew#85" in refs
    # Reason should reference the predecessor
    reasons = {ref: reason for ref, reason in result}
    assert "dependency-unmet:" in reasons["crew#85"]


def test_find_unmet_ready_dependencies_skips_non_graph(
    tmp_path: Path,
) -> None:
    """Non-graph Ready item is not in results."""
    config = _load(tmp_path)
    # crew#999 is not in any critical path
    item = _make_snapshot(issue_ref="crew#999", executor="claude", status="Ready")
    result = find_unmet_ready_dependencies(
        config=config,
        ready_items=[item],
        all_prefixes=True,
        status_resolver=lambda *_: "Ready",
        project_owner="StartupAI-site",
        project_number=1,
    )
    refs = [ref for ref, _ in result]
    assert "crew#999" not in refs


def test_find_unmet_ready_dependencies_filters_this_repo_prefix(
    tmp_path: Path,
) -> None:
    """this_repo_prefix='crew' excludes app items."""
    config = _load(tmp_path)
    app_item = _make_snapshot(issue_ref="app#153", executor="claude", status="Ready")
    result = find_unmet_ready_dependencies(
        config=config,
        ready_items=[app_item],
        this_repo_prefix="crew",
        status_resolver=lambda *_: "Ready",
        project_owner="StartupAI-site",
        project_number=1,
    )
    refs = [ref for ref, _ in result]
    assert "app#153" not in refs


def test_find_unmet_ready_dependencies_all_prefixes_includes_other_repo(
    tmp_path: Path,
) -> None:
    """all_prefixes=True includes cross-repo items."""
    config = _load(tmp_path)
    app_item = _make_snapshot(issue_ref="app#153", executor="claude", status="Ready")
    result = find_unmet_ready_dependencies(
        config=config,
        ready_items=[app_item],
        all_prefixes=True,
        status_resolver=lambda *_: "Ready",  # predecessor not Done
        project_owner="StartupAI-site",
        project_number=1,
    )
    refs = [ref for ref, _ in result]
    assert "app#153" in refs


def test_find_unmet_ready_dependencies_status_resolver_drives_result(
    tmp_path: Path,
) -> None:
    """Custom status_resolver returning 'Done' means no unmet deps."""
    config = _load(tmp_path)
    # crew#85 depends on crew#84 — if status_resolver says "Done", deps are met
    item = _make_snapshot(issue_ref="crew#85", executor="claude", status="Ready")
    result = find_unmet_ready_dependencies(
        config=config,
        ready_items=[item],
        all_prefixes=True,
        status_resolver=lambda *_: "Done",  # all predecessors Done
        project_owner="StartupAI-site",
        project_number=1,
    )
    refs = [ref for ref, _ in result]
    assert "crew#85" not in refs
