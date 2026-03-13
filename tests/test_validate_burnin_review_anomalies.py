from __future__ import annotations

import json
from pathlib import Path

from tools.validate_burnin_review_anomalies import validate_review_anomalies


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_validate_review_anomalies_uses_artifacts_first(tmp_path: Path) -> None:
    run_dir = tmp_path / "20260313T185906Z"
    log_path = run_dir / "artifacts" / "logs" / "consumer-run.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(
        "\n".join(
            [
                "2026-03-13 15:40:20,359 board-consumer INFO Review queue: "
                "ReviewQueueDrainSummary(... blocked=('StartupAI-site/app.startupai-site#219:missing-copilot-review',), ...)",
                "2026-03-13 15:40:21,368 board-consumer INFO Worker result [slot=4 issue=app#126]: "
                "CycleResult(action='claimed', issue_ref='app#126', session_id='3aa17aad9ca1', reason='timeout', "
                "pr_url='https://github.com/StartupAI-site/app.startupai-site/pull/219')",
                "2026-03-13 15:44:42,285 board-consumer INFO Review queue: "
                "ReviewQueueDrainSummary(... blocked=('StartupAI-site/app.startupai-site#219:StartupAI-site/app.startupai-site#219: missing codex verdict marker (codex-review: pass|fail from trusted actor)',), ...)",
            ]
        ),
        encoding="utf-8",
    )
    _write_json(
        run_dir / "artifacts" / "timeline" / "002-status-local.json",
        {
            "recent_sessions": [
                {
                    "issue_ref": "app#126",
                    "status": "running",
                    "phase": "running",
                    "pr_url": "https://github.com/StartupAI-site/app.startupai-site/pull/219",
                }
            ]
        },
    )

    report = validate_review_anomalies(run_dir)

    assert (
        report["overall_conclusion"]
        == "artifact evidence supports the recorded review anomalies as live conditions; no controller policy fix is justified from artifacts alone"
    )
    assert report["validations"][0]["anomaly"] == "missing-copilot-review"
    assert report["validations"][0]["evidence_lines"]
    assert report["validations"][1]["anomaly"] == "missing-codex-verdict-marker"
    assert report["validations"][1]["status_evidence"][0]["issue_ref"] == "app#126"


def test_validate_review_anomalies_reports_inconclusive_without_artifacts(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "20260313T185906Z"
    report = validate_review_anomalies(run_dir)

    assert report["overall_conclusion"] == (
        "artifact review remains inconclusive; do not change review policy yet"
    )
