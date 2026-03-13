"""Supervised one-hour live burn-in harness for the board consumer."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

UTC = timezone.utc
TRANSPORT_LOG_PATTERN = re.compile(
    r"(rate limit|github outage|graphql|rest api|gh command|auth|network|timeout|suppressed|degraded)",
    re.IGNORECASE,
)
SUMMARY_CONCLUSIONS = (
    "current transport acceptable for live use",
    "current transport acceptable but needs deeper instrumentation",
    "current transport needs a narrow GitHub MCP comparison spike before broader use",
    "test inconclusive due to insufficient board activity",
)


class HarnessError(RuntimeError):
    """Raised when the supervised burn-in cannot be executed safely."""


class ManagedProcess(Protocol):
    """Minimal process protocol needed by the harness."""

    pid: int

    def poll(self) -> int | None: ...

    def wait(self, timeout: float | None = None) -> int: ...

    def send_signal(self, sig: int) -> None: ...

    def terminate(self) -> None: ...

    def close(self) -> None: ...


class Clock(Protocol):
    """Time abstraction for deterministic tests."""

    def now_utc(self) -> datetime: ...

    def monotonic(self) -> float: ...

    def sleep(self, seconds: float) -> None: ...


@dataclass
class CommandResult:
    argv: list[str]
    returncode: int
    stdout: str
    stderr: str
    started_at: str
    completed_at: str
    timed_out: bool = False
    timeout_seconds: float | None = None

    @property
    def ok(self) -> bool:
        return self.returncode == 0


@dataclass
class SnapshotRecord:
    name: str
    path: str
    command: list[str]
    returncode: int
    captured_at: str
    reason: str | None
    payload: dict[str, Any] | None


@dataclass
class RunSummary:
    conclusion: str
    meaningful_board_activity: bool
    shutdown_mode: str
    drain_latency_seconds: float | None
    unexpected_exit: bool
    forced_shutdown: bool
    snapshot_failures: list[str]
    degraded_periods: list[dict[str, Any]]
    claim_suppression_periods: list[dict[str, Any]]
    consumer_transport_log_highlights: list[str]
    control_plane_proxy_counts: list[dict[str, Any]]
    control_plane_health_transitions: list[dict[str, Any]]
    worked: list[str]
    issues: list[str]
    improvements: list[str]


@dataclass
class LimitedLiveTestConfig:
    state_root: Path
    artifact_root: Path
    db_path: Path | None
    duration_seconds: int
    local_snapshot_seconds: int
    full_snapshot_seconds: int
    shutdown_poll_seconds: int
    drain_timeout_seconds: int
    post_quiesce_exit_seconds: int
    command_timeout_seconds: int
    consumer_interval_seconds: int | None
    confirm_single_consumer: bool
    confirmation_note: str


class RealClock:
    def now_utc(self) -> datetime:
        return datetime.now(tz=UTC)

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class PopenManagedProcess:
    """Managed process wrapper that also closes the harness log file."""

    def __init__(self, process: subprocess.Popen[str], log_handle: Any) -> None:
        self._process = process
        self._log_handle = log_handle
        self.pid = process.pid

    def poll(self) -> int | None:
        return self._process.poll()

    def wait(self, timeout: float | None = None) -> int:
        return self._process.wait(timeout=timeout)

    def send_signal(self, sig: int) -> None:
        self._process.send_signal(sig)

    def terminate(self) -> None:
        self._process.terminate()

    def close(self) -> None:
        self._log_handle.close()


class SubprocessBackend:
    """Real subprocess-backed command runner."""

    def __init__(self, clock: Clock) -> None:
        self._clock = clock

    def run(
        self,
        argv: list[str],
        *,
        timeout_seconds: float | None = None,
    ) -> CommandResult:
        started_at = self._clock.now_utc().isoformat()
        try:
            completed = subprocess.run(
                argv,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            completed_at = self._clock.now_utc().isoformat()
            stdout = exc.stdout if isinstance(exc.stdout, str) else ""
            stderr = exc.stderr if isinstance(exc.stderr, str) else ""
            timeout_note = (
                f"command timed out after {timeout_seconds} seconds"
                if timeout_seconds is not None
                else "command timed out"
            )
            stderr = f"{stderr}\n{timeout_note}".strip()
            return CommandResult(
                argv=list(argv),
                returncode=124,
                stdout=stdout,
                stderr=stderr,
                started_at=started_at,
                completed_at=completed_at,
                timed_out=True,
                timeout_seconds=timeout_seconds,
            )
        completed_at = self._clock.now_utc().isoformat()
        return CommandResult(
            argv=list(argv),
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            started_at=started_at,
            completed_at=completed_at,
            timeout_seconds=timeout_seconds,
        )

    def start_consumer(self, argv: list[str], log_path: Path) -> ManagedProcess:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_handle = log_path.open("a", encoding="utf-8")
        process = subprocess.Popen(
            argv,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        return PopenManagedProcess(process, log_handle)

    def local_consumer_processes(
        self,
        *,
        timeout_seconds: float | None = None,
    ) -> list[str]:
        result = self.run(
            ["ps", "-eo", "pid=,command="],
            timeout_seconds=timeout_seconds,
        )
        if result.returncode != 0:
            return []
        matches: list[str] = []
        for line in result.stdout.splitlines():
            command = line.strip()
            if (
                "startupai_controller.board_consumer run" in command
                or "board-consumer run" in command
            ):
                matches.append(command)
        return matches

    def systemd_consumer_active(
        self,
        *,
        timeout_seconds: float | None = None,
    ) -> bool:
        result = self.run(
            ["systemctl", "--user", "is-active", "startupai-consumer"],
            timeout_seconds=timeout_seconds,
        )
        return result.returncode == 0 and result.stdout.strip() in {
            "active",
            "activating",
        }


class LimitedLiveTestHarness:
    """Coordinates a supervised live run plus artifact collection."""

    def __init__(
        self,
        config: LimitedLiveTestConfig,
        backend: SubprocessBackend,
        clock: Clock,
    ) -> None:
        self.config = config
        self.backend = backend
        self.clock = clock
        self.run_dir = config.artifact_root / _utc_stamp(clock.now_utc())
        self.baseline_dir = self.run_dir / "artifacts" / "baseline"
        self.timeline_dir = self.run_dir / "artifacts" / "timeline"
        self.final_dir = self.run_dir / "artifacts" / "final"
        self.logs_dir = self.run_dir / "artifacts" / "logs"
        self.snapshot_failures: list[str] = []
        self.status_records: list[SnapshotRecord] = []
        self.tick_records: list[SnapshotRecord] = []
        self.slo_records: list[SnapshotRecord] = []
        self.issues: list[str] = []
        self.improvements: list[str] = []
        self.worked: list[str] = []
        self._timeline_counter = 0
        self._seen_degraded = False
        self._seen_claim_suppressed = False
        self._meaningful_board_activity = False
        self._unexpected_exit = False
        self._forced_shutdown = False
        self._shutdown_mode = "natural"
        self._drain_latency_seconds: float | None = None

    def run(self) -> RunSummary:
        self._prepare_artifacts()
        self._write_run_meta()
        self._perform_preflight()
        self._capture_baseline_snapshots()
        child = self._start_consumer()
        try:
            self._monitor_run(child)
            self._drain_and_stop(child)
        finally:
            child.close()
        self._capture_final_snapshots()
        summary = self._build_summary()
        self._write_summary(summary)
        return summary

    def _prepare_artifacts(self) -> None:
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.baseline_dir.mkdir(parents=True, exist_ok=True)
        self.timeline_dir.mkdir(parents=True, exist_ok=True)
        self.final_dir.mkdir(parents=True, exist_ok=True)
        self.config.state_root.mkdir(parents=True, exist_ok=True)
        self._backup_state_root()

    def _backup_state_root(self) -> None:
        backup_root = self.run_dir / "artifacts" / "backups" / "state-root"
        if not self.config.state_root.exists():
            return
        shutil.copytree(
            self.config.state_root,
            backup_root,
            ignore=shutil.ignore_patterns("test-runs"),
            dirs_exist_ok=True,
        )

    def _write_run_meta(self) -> None:
        meta = {
            "git_commit": self._git_commit(),
            "hostname": socket.gethostname(),
            "started_at": self.clock.now_utc().isoformat(),
            "state_root": str(self.config.state_root),
            "artifact_root": str(self.config.artifact_root),
            "run_dir": str(self.run_dir),
            "db_path": str(self.config.db_path) if self.config.db_path else None,
            "selected_mode": "live-normal-ready-queue",
            "consumer_command": self._consumer_run_command(),
            "single_consumer_confirmation": {
                "confirmed": self.config.confirm_single_consumer,
                "note": self.config.confirmation_note,
                "procedural_only": True,
            },
        }
        _write_json(self.run_dir / "run_meta.json", meta)

    def _perform_preflight(self) -> None:
        if not self.config.confirm_single_consumer:
            raise HarnessError(
                "Cross-machine single-consumer confirmation is required."
            )
        if self.backend.systemd_consumer_active(
            timeout_seconds=float(self.config.command_timeout_seconds)
        ):
            raise HarnessError("Local startupai-consumer systemd unit is active.")
        if self.backend.local_consumer_processes(
            timeout_seconds=float(self.config.command_timeout_seconds)
        ):
            raise HarnessError("Another local board_consumer run process is active.")
        self._require_ok(
            self._run_aux_command(["gh", "auth", "status"]),
            "gh auth status",
        )
        self._require_ok(
            self._run_aux_command(self._consumer_status_command(local_only=False)),
            "board_consumer status --json",
        )
        self._require_one_shot_ok(
            self._run_aux_command(self._consumer_one_shot_command()),
            "board_consumer one-shot --dry-run --json",
        )

    def _capture_baseline_snapshots(self) -> None:
        for name, command in self._baseline_snapshot_commands():
            self._capture_snapshot(name, command, self.baseline_dir)

    def _capture_final_snapshots(self) -> None:
        for name, command in self._baseline_snapshot_commands():
            self._capture_snapshot(name, command, self.final_dir)
        self._capture_snapshot(
            "reconcile-dry-run",
            self._consumer_reconcile_command(),
            self.final_dir,
        )

    def _start_consumer(self) -> ManagedProcess:
        return self.backend.start_consumer(
            self._consumer_run_command(),
            self.logs_dir / "consumer-run.log",
        )

    def _monitor_run(self, child: ManagedProcess) -> None:
        start = self.clock.monotonic()
        end = start + self.config.duration_seconds
        next_local = start + self.config.local_snapshot_seconds
        next_full = start + self.config.full_snapshot_seconds

        while True:
            now = self.clock.monotonic()
            returncode = child.poll()
            if returncode is not None:
                if now < end:
                    self._unexpected_exit = True
                    self.issues.append(
                        f"Consumer exited unexpectedly with code {returncode}."
                    )
                    self._capture_event_bundle("unexpected-exit")
                return
            if now >= end:
                return
            if now >= next_local:
                snapshot = self._capture_snapshot(
                    self._timeline_name("status-local"),
                    self._consumer_status_command(local_only=True),
                    self.timeline_dir,
                    reason="cadence-local",
                )
                self._check_status_transitions(snapshot)
                next_local += self.config.local_snapshot_seconds
                continue
            if now >= next_full:
                self._capture_timeline_bundle("cadence-full")
                next_full += self.config.full_snapshot_seconds
                continue
            sleep_for = min(1.0, end - now, next_local - now, next_full - now)
            if sleep_for > 0:
                self.clock.sleep(sleep_for)

    def _drain_and_stop(self, child: ManagedProcess) -> None:
        if child.poll() is not None:
            self._shutdown_mode = "unexpected-exit"
            return
        drain_requested_at = self.clock.monotonic()
        drain_result = self._run_aux_command(self._consumer_drain_command())
        if not drain_result.ok:
            self.snapshot_failures.append("drain command failed")
            self.issues.append("Drain request failed; forcing shutdown escalation.")
        while (
            self.clock.monotonic() - drain_requested_at
            < self.config.drain_timeout_seconds
        ):
            snapshot = self._capture_snapshot(
                self._timeline_name("status-local"),
                self._consumer_status_command(local_only=True),
                self.timeline_dir,
                reason="shutdown-poll",
            )
            payload = snapshot.payload or {}
            if child.poll() is not None:
                self._drain_latency_seconds = (
                    self.clock.monotonic() - drain_requested_at
                )
                self._shutdown_mode = "natural"
                self.worked.append("consumer drained and exited naturally")
                return
            if payload.get("active_leases") == 0 and not payload.get("workers"):
                wait_deadline = (
                    self.clock.monotonic() + self.config.post_quiesce_exit_seconds
                )
                while self.clock.monotonic() < wait_deadline:
                    if child.poll() is not None:
                        self._drain_latency_seconds = (
                            self.clock.monotonic() - drain_requested_at
                        )
                        self._shutdown_mode = "natural"
                        self.worked.append("consumer drained and exited naturally")
                        return
                    self.clock.sleep(1.0)
            self.clock.sleep(float(self.config.shutdown_poll_seconds))
        self._forced_shutdown = True
        self._shutdown_mode = "forced"
        child.send_signal(signal.SIGINT)
        sigint_deadline = self.clock.monotonic() + 60
        while self.clock.monotonic() < sigint_deadline:
            if child.poll() is not None:
                self._drain_latency_seconds = (
                    self.clock.monotonic() - drain_requested_at
                )
                return
            self.clock.sleep(1.0)
        child.send_signal(signal.SIGTERM)
        child.wait(timeout=60)
        self._drain_latency_seconds = self.clock.monotonic() - drain_requested_at

    def _capture_timeline_bundle(self, reason: str) -> None:
        self._capture_snapshot(
            self._timeline_name("status"),
            self._consumer_status_command(local_only=False),
            self.timeline_dir,
            reason=reason,
        )
        self._capture_snapshot(
            self._timeline_name("report-slo"),
            self._consumer_report_slo_command(local_only=False),
            self.timeline_dir,
            reason=reason,
        )
        self._capture_snapshot(
            self._timeline_name("tick-dry-run"),
            self._control_plane_tick_command(),
            self.timeline_dir,
            reason=reason,
        )

    def _capture_event_bundle(self, reason: str) -> None:
        snapshot = self._capture_snapshot(
            self._timeline_name("status-local"),
            self._consumer_status_command(local_only=True),
            self.timeline_dir,
            reason=reason,
        )
        self._check_status_transitions(snapshot, allow_follow_up=False)
        self._capture_timeline_bundle(reason)

    def _check_status_transitions(
        self,
        snapshot: SnapshotRecord,
        *,
        allow_follow_up: bool = True,
    ) -> None:
        payload = snapshot.payload or {}
        degraded = bool(payload.get("degraded"))
        claim_suppressed = bool(payload.get("claim_suppressed_until"))
        self._meaningful_board_activity = (
            self._meaningful_board_activity or self._payload_has_activity(payload)
        )
        if not allow_follow_up:
            self._seen_degraded = self._seen_degraded or degraded
            self._seen_claim_suppressed = (
                self._seen_claim_suppressed or claim_suppressed
            )
            return
        reasons: list[str] = []
        if degraded and not self._seen_degraded:
            self._seen_degraded = True
            reasons.append("degraded-transition")
        if claim_suppressed and not self._seen_claim_suppressed:
            self._seen_claim_suppressed = True
            reasons.append("claim-suppressed-transition")
        if reasons:
            self._capture_event_bundle("+".join(reasons))

    def _payload_has_activity(self, payload: dict[str, Any]) -> bool:
        workers = payload.get("workers") or []
        recent_sessions = payload.get("recent_sessions") or []
        return bool(
            payload.get("active_leases")
            or workers
            or recent_sessions
            or payload.get("last_successful_github_mutation_at")
            or payload.get("claim_suppressed_until")
            or payload.get("degraded")
        )

    def _capture_snapshot(
        self,
        name: str,
        command: list[str],
        directory: Path,
        *,
        reason: str | None = None,
    ) -> SnapshotRecord:
        directory.mkdir(parents=True, exist_ok=True)
        result = self._run_aux_command(command)
        base = directory / name
        payload = _parse_json(result.stdout)
        if payload is not None:
            _write_json(base.with_suffix(".json"), payload)
        else:
            base.with_suffix(".stdout.txt").write_text(result.stdout, encoding="utf-8")
        if result.stderr:
            base.with_suffix(".stderr.txt").write_text(result.stderr, encoding="utf-8")
        _write_json(
            base.with_suffix(".meta.json"),
            {
                "command": result.argv,
                "returncode": result.returncode,
                "started_at": result.started_at,
                "completed_at": result.completed_at,
                "reason": reason,
                "timed_out": result.timed_out,
                "timeout_seconds": result.timeout_seconds,
            },
        )
        record = SnapshotRecord(
            name=name,
            path=str(base),
            command=list(result.argv),
            returncode=result.returncode,
            captured_at=result.completed_at,
            reason=reason,
            payload=payload,
        )
        if "status" in name:
            self.status_records.append(record)
        elif "tick-dry-run" in name:
            self.tick_records.append(record)
        elif "report-slo" in name:
            self.slo_records.append(record)
        if result.returncode != 0 and not self._is_nonfatal_one_shot_idle(
            result,
            payload,
        ):
            message = f"{name} failed with exit code {result.returncode}"
            if result.timed_out:
                message = f"{name} timed out after {result.timeout_seconds} seconds"
            self.snapshot_failures.append(message)
            self.issues.append(message)
        return record

    def _build_summary(self) -> RunSummary:
        acceptable_forced_shutdown = self._acceptable_forced_shutdown()
        degraded_periods = _derive_periods(
            self.status_records, "degraded", "degraded_reason"
        )
        claim_periods = _derive_periods(
            self.status_records,
            "claim_suppressed_until",
            "claim_suppressed_reason",
        )
        log_highlights = self._transport_log_highlights()
        control_plane_counts = []
        for record in self.tick_records:
            payload = record.payload or {}
            counts = payload.get("github_request_counts") or {}
            control_plane_counts.append(
                {
                    "captured_at": record.captured_at,
                    "graphql": counts.get("graphql"),
                    "rest": counts.get("rest"),
                    "reason": record.reason,
                }
            )
        control_plane_health_transitions = _derive_health_transitions(
            self.status_records
        )
        conclusion = self._determine_conclusion(
            degraded_periods,
            claim_periods,
            log_highlights,
            acceptable_forced_shutdown=acceptable_forced_shutdown,
        )
        if not self.snapshot_failures:
            self.worked.append(
                "baseline, timeline, and final snapshot collection completed"
            )
        if not self._unexpected_exit:
            self.worked.append("consumer remained under supervised harness control")
        if self._forced_shutdown:
            if acceptable_forced_shutdown:
                self.worked.append(
                    "forced shutdown occurred only after waiting on in-flight external execution"
                )
            else:
                self.issues.append(
                    "Drain timeout exceeded; sent SIGINT/SIGTERM escalation."
                )
        improvements = list(self.improvements)
        if conclusion == SUMMARY_CONCLUSIONS[1]:
            improvements.append(
                "add per-request latency, retry, and fallback transport metrics"
            )
        elif conclusion == SUMMARY_CONCLUSIONS[2]:
            improvements.append("run a narrow read-only GitHub MCP comparison spike")
        elif conclusion == SUMMARY_CONCLUSIONS[3]:
            improvements.append("repeat the burn-in during a busier Ready-queue window")
        return RunSummary(
            conclusion=conclusion,
            meaningful_board_activity=self._meaningful_board_activity,
            shutdown_mode=self._shutdown_mode,
            drain_latency_seconds=self._drain_latency_seconds,
            unexpected_exit=self._unexpected_exit,
            forced_shutdown=self._forced_shutdown,
            snapshot_failures=list(self.snapshot_failures),
            degraded_periods=degraded_periods,
            claim_suppression_periods=claim_periods,
            consumer_transport_log_highlights=log_highlights,
            control_plane_proxy_counts=control_plane_counts,
            control_plane_health_transitions=control_plane_health_transitions,
            worked=_dedupe(self.worked),
            issues=_dedupe(self.issues),
            improvements=_dedupe(improvements),
        )

    def _determine_conclusion(
        self,
        degraded_periods: list[dict[str, Any]],
        claim_periods: list[dict[str, Any]],
        log_highlights: list[str],
        *,
        acceptable_forced_shutdown: bool,
    ) -> str:
        if not self._meaningful_board_activity:
            return SUMMARY_CONCLUSIONS[3]
        severe_transport_issue = any(
            "rate" in (period.get("reason") or "").lower()
            or "github" in (period.get("reason") or "").lower()
            or "auth" in (period.get("reason") or "").lower()
            or "network" in (period.get("reason") or "").lower()
            for period in degraded_periods
        ) or any(
            re.search(
                r"(rate limit|github outage|auth|network|timeout)", line, re.IGNORECASE
            )
            for line in log_highlights
        )
        if severe_transport_issue or claim_periods:
            return SUMMARY_CONCLUSIONS[2]
        if (
            degraded_periods
            or self.snapshot_failures
            or self._unexpected_exit
            or (self._forced_shutdown and not acceptable_forced_shutdown)
        ):
            return SUMMARY_CONCLUSIONS[1]
        return SUMMARY_CONCLUSIONS[0]

    def _acceptable_forced_shutdown(self) -> bool:
        """Return True when forced shutdown waited only on in-flight execution."""
        if not self._forced_shutdown:
            return False
        payload = self._latest_local_status_payload()
        if payload is None:
            return False
        workers = payload.get("workers") or []
        if not workers:
            return False
        return all(
            worker.get("status") == "running"
            and worker.get("external_execution_started") is True
            and worker.get("drain_wait_class") == "finishing_inflight_execution"
            for worker in workers
        )

    def _latest_local_status_payload(self) -> dict[str, Any] | None:
        """Return the latest local-only status payload captured by the harness."""
        for record in reversed(self.status_records):
            payload = record.payload
            if payload and payload.get("local_only") is True:
                return payload
        return None

    def _write_summary(self, summary: RunSummary) -> None:
        summary_payload = asdict(summary)
        summary_payload["report_notes"] = {
            "github_request_counts_source": (
                "Periodic board_control_plane tick --json --dry-run snapshots; "
                "proxy visibility, not a full measurement of every live consumer-loop request."
            ),
            "single_consumer_guarantee": (
                "Cross-machine exclusivity during this test was based on procedural operator "
                "confirmation. The repo does not provide a distributed consumer lock."
            ),
        }
        _write_json(self.run_dir / "summary.json", summary_payload)
        lines = [
            "# Limited Live Test Summary",
            "",
            f"- Meaningful board activity observed: `{summary.meaningful_board_activity}`",
            f"- Shutdown mode: `{summary.shutdown_mode}`",
            f"- Drain latency seconds: `{summary.drain_latency_seconds}`",
            f"- Unexpected exit: `{summary.unexpected_exit}`",
            f"- Forced shutdown: `{summary.forced_shutdown}`",
            "",
            "## Worked",
        ]
        for item in summary.worked or ["None recorded."]:
            lines.append(f"- {item}")
        lines.extend(["", "## Issues"])
        for item in summary.issues or ["None recorded."]:
            lines.append(f"- {item}")
        lines.extend(["", "## Improvements"])
        for item in summary.improvements or ["None recorded."]:
            lines.append(f"- {item}")
        lines.extend(
            [
                "",
                "## Evidence Notes",
                "- `github_request_counts` come from periodic `board_control_plane tick --json --dry-run` snapshots and are a proxy, not a full live-loop measurement.",
                "- Cross-machine single-consumer safety was recorded procedurally by the operator; the repo does not provide a distributed exclusivity lock.",
                "",
                f"Conclusion: {summary.conclusion}",
                "",
            ]
        )
        (self.run_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")

    def _transport_log_highlights(self) -> list[str]:
        log_path = self.logs_dir / "consumer-run.log"
        if not log_path.exists():
            return []
        highlights: list[str] = []
        for line in log_path.read_text(encoding="utf-8").splitlines():
            if TRANSPORT_LOG_PATTERN.search(line):
                highlights.append(line.strip())
        return highlights[:20]

    def _timeline_name(self, stem: str) -> str:
        self._timeline_counter += 1
        return f"{self._timeline_counter:03d}-{_utc_stamp(self.clock.now_utc())}-{stem}"

    def _consumer_run_command(self) -> list[str]:
        command = [
            "uv",
            "run",
            "python",
            "-m",
            "startupai_controller.board_consumer",
            "run",
            "--verbose",
        ]
        if self.config.db_path:
            command.extend(["--db-path", str(self.config.db_path)])
        if self.config.consumer_interval_seconds is not None:
            command.extend(["--interval", str(self.config.consumer_interval_seconds)])
        return command

    def _consumer_status_command(self, *, local_only: bool) -> list[str]:
        command = [
            "uv",
            "run",
            "python",
            "-m",
            "startupai_controller.board_consumer",
            "status",
            "--json",
        ]
        if self.config.db_path:
            command.extend(["--db-path", str(self.config.db_path)])
        if local_only:
            command.append("--local-only")
        return command

    def _consumer_report_slo_command(self, *, local_only: bool) -> list[str]:
        command = [
            "uv",
            "run",
            "python",
            "-m",
            "startupai_controller.board_consumer",
            "report-slo",
            "--json",
        ]
        if self.config.db_path:
            command.extend(["--db-path", str(self.config.db_path)])
        if local_only:
            command.append("--local-only")
        return command

    def _consumer_one_shot_command(self) -> list[str]:
        command = [
            "uv",
            "run",
            "python",
            "-m",
            "startupai_controller.board_consumer",
            "one-shot",
            "--dry-run",
            "--json",
        ]
        if self.config.db_path:
            command.extend(["--db-path", str(self.config.db_path)])
        return command

    def _control_plane_tick_command(self) -> list[str]:
        command = [
            "uv",
            "run",
            "python",
            "-m",
            "startupai_controller.board_control_plane",
        ]
        if self.config.db_path:
            command.extend(["--db-path", str(self.config.db_path)])
        command.extend(["tick", "--json", "--dry-run"])
        return command

    def _consumer_drain_command(self) -> list[str]:
        command = [
            "uv",
            "run",
            "python",
            "-m",
            "startupai_controller.board_consumer",
            "drain",
        ]
        if self.config.db_path:
            command.extend(["--db-path", str(self.config.db_path)])
        return command

    def _consumer_reconcile_command(self) -> list[str]:
        command = [
            "uv",
            "run",
            "python",
            "-m",
            "startupai_controller.board_consumer",
            "reconcile",
            "--dry-run",
        ]
        if self.config.db_path:
            command.extend(["--db-path", str(self.config.db_path)])
        return command

    def _baseline_snapshot_commands(self) -> list[tuple[str, list[str]]]:
        return [
            ("status", self._consumer_status_command(local_only=False)),
            ("status-local", self._consumer_status_command(local_only=True)),
            ("report-slo", self._consumer_report_slo_command(local_only=False)),
            ("report-slo-local", self._consumer_report_slo_command(local_only=True)),
            ("tick-dry-run", self._control_plane_tick_command()),
            ("one-shot-dry-run", self._consumer_one_shot_command()),
        ]

    def _require_ok(self, result: CommandResult, label: str) -> None:
        if result.ok:
            return
        if result.timed_out:
            raise HarnessError(
                f"{label} timed out after {result.timeout_seconds} seconds."
            )
        raise HarnessError(
            f"{label} failed with exit code {result.returncode}: {result.stderr or result.stdout}"
        )

    def _require_one_shot_ok(self, result: CommandResult, label: str) -> None:
        payload = _parse_json(result.stdout)
        if result.ok or self._is_nonfatal_one_shot_idle(result, payload):
            return
        if result.timed_out:
            raise HarnessError(
                f"{label} timed out after {result.timeout_seconds} seconds."
            )
        raise HarnessError(
            f"{label} failed with exit code {result.returncode}: {result.stderr or result.stdout}"
        )

    def _git_commit(self) -> str | None:
        result = self._run_aux_command(["git", "rev-parse", "HEAD"])
        if result.ok:
            return result.stdout.strip()
        return None

    def _run_aux_command(self, argv: list[str]) -> CommandResult:
        return self.backend.run(
            argv,
            timeout_seconds=float(self.config.command_timeout_seconds),
        )

    def _is_nonfatal_one_shot_idle(
        self,
        result: CommandResult,
        payload: dict[str, Any] | None,
    ) -> bool:
        if "one-shot" not in result.argv:
            return False
        if result.returncode != 2:
            return False
        if payload is None:
            return False
        return payload.get("exit_class") == "idle"


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _parse_json(text: str) -> dict[str, Any] | None:
    stripped = text.strip()
    if not stripped:
        return None
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else {"value": parsed}


def _utc_stamp(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def _derive_periods(
    records: list[SnapshotRecord],
    value_key: str,
    reason_key: str,
) -> list[dict[str, Any]]:
    periods: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    ordered = sorted(
        (record for record in records if "status-local" in record.name),
        key=lambda record: record.captured_at,
    )
    for record in ordered:
        payload = record.payload or {}
        raw_value = payload.get(value_key)
        active = bool(raw_value)
        reason = payload.get(reason_key)
        if active and current is None:
            current = {
                "started_at": record.captured_at,
                "ended_at": None,
                "reason": reason,
            }
        elif not active and current is not None:
            current["ended_at"] = record.captured_at
            periods.append(current)
            current = None
    if current is not None:
        periods.append(current)
    return periods


def _derive_health_transitions(records: list[SnapshotRecord]) -> list[dict[str, Any]]:
    transitions: list[dict[str, Any]] = []
    last_health: str | None = None
    ordered = sorted(records, key=lambda record: record.captured_at)
    for record in ordered:
        payload = record.payload or {}
        health_payload = payload.get("control_plane_health")
        if not isinstance(health_payload, dict):
            continue
        health = health_payload.get("health")
        if health != last_health:
            transitions.append(
                {
                    "captured_at": record.captured_at,
                    "health": health,
                    "reason_code": health_payload.get("reason_code"),
                }
            )
            last_health = health
    return transitions


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a supervised one-hour controller burn-in and collect transport evidence."
    )
    default_state_root = Path.home() / ".local" / "share" / "startupai"
    parser.add_argument(
        "--state-root",
        type=Path,
        default=default_state_root,
        help="Controller state root to back up and observe.",
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=default_state_root / "test-runs",
        help="Directory where test-run artifacts are written.",
    )
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=3600,
        help="Live run duration before drain is requested.",
    )
    parser.add_argument(
        "--local-snapshot-seconds",
        type=int,
        default=300,
        help="Cadence for local-only status snapshots.",
    )
    parser.add_argument(
        "--full-snapshot-seconds",
        type=int,
        default=900,
        help="Cadence for full status, SLO, and tick snapshots.",
    )
    parser.add_argument(
        "--shutdown-poll-seconds",
        type=int,
        default=30,
        help="Polling cadence while waiting for drain shutdown.",
    )
    parser.add_argument(
        "--drain-timeout-seconds",
        type=int,
        default=900,
        help="Maximum graceful drain window before signal escalation.",
    )
    parser.add_argument(
        "--post-quiesce-exit-seconds",
        type=int,
        default=60,
        help="Extra wait after local quiescence before escalation.",
    )
    parser.add_argument(
        "--command-timeout-seconds",
        type=int,
        default=300,
        help="Timeout for auxiliary snapshot and preflight commands.",
    )
    parser.add_argument(
        "--consumer-interval-seconds",
        type=int,
        default=None,
        help="Optional override for the consumer poll interval during the run.",
    )
    parser.add_argument(
        "--confirm-single-consumer",
        action="store_true",
        required=True,
        help="Required procedural confirmation that no other machine is running a consumer.",
    )
    parser.add_argument(
        "--confirmation-note",
        default="",
        help="Free-form operator note about the cross-machine single-consumer confirmation.",
    )
    return parser


def config_from_args(args: argparse.Namespace) -> LimitedLiveTestConfig:
    return LimitedLiveTestConfig(
        state_root=args.state_root,
        artifact_root=args.artifact_root,
        db_path=args.db_path,
        duration_seconds=args.duration_seconds,
        local_snapshot_seconds=args.local_snapshot_seconds,
        full_snapshot_seconds=args.full_snapshot_seconds,
        shutdown_poll_seconds=args.shutdown_poll_seconds,
        drain_timeout_seconds=args.drain_timeout_seconds,
        post_quiesce_exit_seconds=args.post_quiesce_exit_seconds,
        command_timeout_seconds=args.command_timeout_seconds,
        consumer_interval_seconds=args.consumer_interval_seconds,
        confirm_single_consumer=args.confirm_single_consumer,
        confirmation_note=args.confirmation_note,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = config_from_args(args)
    clock = RealClock()
    harness = LimitedLiveTestHarness(
        config=config,
        backend=SubprocessBackend(clock),
        clock=clock,
    )
    try:
        summary = harness.run()
    except HarnessError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "run_dir": str(harness.run_dir),
                "conclusion": summary.conclusion,
                "meaningful_board_activity": summary.meaningful_board_activity,
                "shutdown_mode": summary.shutdown_mode,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
