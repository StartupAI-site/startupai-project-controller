"""Validate burn-in review anomalies from recorded artifacts before changing policy."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

DEFAULT_RUN_DIR = (
    Path.home() / ".local" / "share" / "startupai" / "test-runs" / "20260313T185906Z"
)


@dataclass(frozen=True)
class StatusEvidence:
    """One status snapshot showing the issue/session state."""

    snapshot: str
    issue_ref: str
    status: str | None
    phase: str | None
    pr_url: str | None


@dataclass(frozen=True)
class AnomalyValidation:
    """Artifact-first validation result for one burn-in review anomaly."""

    anomaly: str
    issue_ref: str
    pr_url: str | None
    evidence_lines: list[str]
    status_evidence: list[StatusEvidence]
    conclusion: str


def _parse_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _collect_status_evidence(run_dir: Path, issue_ref: str) -> list[StatusEvidence]:
    artifacts_dir = run_dir / "artifacts"
    records: list[StatusEvidence] = []
    for pattern in (
        "baseline/status*.json",
        "timeline/*status*.json",
        "final/status*.json",
    ):
        for path in sorted(artifacts_dir.glob(pattern)):
            payload = _parse_json(path)
            if payload is None:
                continue
            for session in payload.get("recent_sessions", []):
                if not isinstance(session, dict):
                    continue
                if session.get("issue_ref") != issue_ref:
                    continue
                records.append(
                    StatusEvidence(
                        snapshot=path.name,
                        issue_ref=issue_ref,
                        status=_string_or_none(session.get("status")),
                        phase=_string_or_none(session.get("phase")),
                        pr_url=_string_or_none(session.get("pr_url")),
                    )
                )
    return records


def _string_or_none(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _log_lines(run_dir: Path) -> list[str]:
    log_path = run_dir / "artifacts" / "logs" / "consumer-run.log"
    if not log_path.exists():
        return []
    return log_path.read_text(encoding="utf-8").splitlines()


def _matching_log_lines(run_dir: Path, *patterns: str) -> list[str]:
    lowered_patterns = tuple(pattern.lower() for pattern in patterns)
    matches: list[str] = []
    for line in _log_lines(run_dir):
        lowered = line.lower()
        if any(pattern in lowered for pattern in lowered_patterns):
            matches.append(line)
    return matches


def _issue_status_conclusion(
    *,
    anomaly: str,
    issue_ref: str,
    pr_url: str | None,
    evidence_lines: list[str],
    status_evidence: list[StatusEvidence],
) -> str:
    if not evidence_lines:
        return (
            f"inconclusive: no burn-in artifact evidence recorded for {anomaly} on "
            f"{issue_ref}"
        )
    if not status_evidence:
        return (
            f"inconclusive: log evidence exists for {anomaly} on {issue_ref}, but no "
            "matching status snapshots were found"
        )
    if pr_url is None:
        return (
            f"inconclusive: artifact evidence for {anomaly} on {issue_ref} never "
            "showed an associated PR URL"
        )
    return (
        f"artifact evidence supports a real live condition for {anomaly} on "
        f"{issue_ref} ({pr_url}); no contradictory artifact evidence shows controller "
        "misclassification"
    )


def validate_review_anomalies(run_dir: Path) -> dict[str, Any]:
    """Return an artifact-first validation report for the known burn-in anomalies."""
    issue_ref = "app#126"
    pr_url = "https://github.com/StartupAI-site/app.startupai-site/pull/219"

    missing_copilot_lines = _matching_log_lines(
        run_dir,
        "app#126",
        "app.startupai-site#219:missing-copilot-review",
    )
    missing_verdict_lines = _matching_log_lines(
        run_dir,
        "app#126",
        "missing codex verdict marker",
    )
    status_evidence = _collect_status_evidence(run_dir, issue_ref)

    validations = [
        AnomalyValidation(
            anomaly="missing-copilot-review",
            issue_ref=issue_ref,
            pr_url=pr_url,
            evidence_lines=missing_copilot_lines,
            status_evidence=status_evidence,
            conclusion=_issue_status_conclusion(
                anomaly="missing-copilot-review",
                issue_ref=issue_ref,
                pr_url=pr_url,
                evidence_lines=missing_copilot_lines,
                status_evidence=status_evidence,
            ),
        ),
        AnomalyValidation(
            anomaly="missing-codex-verdict-marker",
            issue_ref=issue_ref,
            pr_url=pr_url,
            evidence_lines=missing_verdict_lines,
            status_evidence=status_evidence,
            conclusion=_issue_status_conclusion(
                anomaly="missing codex verdict marker",
                issue_ref=issue_ref,
                pr_url=pr_url,
                evidence_lines=missing_verdict_lines,
                status_evidence=status_evidence,
            ),
        ),
    ]

    artifact_supports_live_conditions = all(
        validation.evidence_lines and validation.status_evidence
        for validation in validations
    )
    overall_conclusion = (
        "artifact evidence supports the recorded review anomalies as live conditions; "
        "no controller policy fix is justified from artifacts alone"
        if artifact_supports_live_conditions
        else "artifact review remains inconclusive; do not change review policy yet"
    )

    return {
        "run_dir": str(run_dir),
        "overall_conclusion": overall_conclusion,
        "validations": [
            {
                **asdict(validation),
                "status_evidence": [
                    asdict(item) for item in validation.status_evidence
                ],
            }
            for validation in validations
        ],
    }


def _build_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Burn-In Review Anomaly Validation",
        "",
        f"- Run directory: `{report['run_dir']}`",
        f"- Overall conclusion: {report['overall_conclusion']}",
        "",
    ]
    for validation in report["validations"]:
        lines.extend(
            [
                f"## {validation['anomaly']}",
                "",
                f"- Issue: `{validation['issue_ref']}`",
                f"- PR: `{validation['pr_url']}`",
                f"- Conclusion: {validation['conclusion']}",
                "- Log evidence:",
            ]
        )
        for line in validation["evidence_lines"] or ["None recorded."]:
            lines.append(f"  - {line}")
        lines.append("- Status evidence:")
        for item in validation["status_evidence"] or ["None recorded."]:
            if isinstance(item, str):
                lines.append(f"  - {item}")
                continue
            lines.append(
                "  - "
                f"{item['snapshot']}: status={item['status']} phase={item['phase']} "
                f"pr={item['pr_url']}"
            )
        lines.append("")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate known burn-in review anomalies from recorded artifacts.",
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=DEFAULT_RUN_DIR,
        help="Burn-in artifact run directory to inspect.",
    )
    parser.add_argument(
        "--write-report",
        action="store_true",
        help="Write JSON and Markdown validation reports into the run directory.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = validate_review_anomalies(args.run_dir)
    if args.write_report:
        (args.run_dir / "review-anomaly-validation.json").write_text(
            json.dumps(report, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        (args.run_dir / "review-anomaly-validation.md").write_text(
            _build_markdown(report),
            encoding="utf-8",
        )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
