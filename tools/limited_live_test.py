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
ISSUE_REF_PATTERN = re.compile(r"\b(?:app|crew|site)#\d+\b")
BRANCH_NUMBER_PATTERN = re.compile(r"feat/(?P<number>\d+)-")
WORKTREE_REUSE_BLOCKED_PATTERN = re.compile(
    r"CycleResult\(action='error', issue_ref='(?P<issue_ref>[^']+)', "
    r"session_id=(?:'[^']+'|None), reason='(?P<reason>worktree_in_use:[^']+)'"
)
CLAIMED_RESULT_PATTERN = re.compile(
    r"CycleResult\(action='claimed', issue_ref='(?P<issue_ref>[^']+)', "
    r"session_id='[^']*', reason='[^']*', pr_url='(?P<pr_url>[^']+)'"
)
REMOVED_REFS_PATTERN = re.compile(r"removed=\((?P<refs>[^)]*)\)")
SEEDED_REFS_PATTERN = re.compile(r"seeded=\((?P<refs>[^)]*)\)")
REQUEUED_REFS_PATTERN = re.compile(r"requeued=\((?P<refs>[^)]*)\)")
FULLY_QUALIFIED_ISSUE_REF_PATTERN = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+#\d+")
LOG_TIMESTAMP_PATTERN = re.compile(
    r"^(?P<stamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d{3}"
)
SUMMARY_CONCLUSIONS = (
    "current transport acceptable for live use",
    "current transport acceptable but needs deeper instrumentation",
    "current transport needs a narrow GitHub MCP comparison spike before broader use",
    "test inconclusive due to insufficient board activity",
)
FULL_SNAPSHOT_GUARD_SECONDS = 20.0
LOCAL_SNAPSHOT_GUARD_SECONDS = 5.0
RECLAIM_COMPLETION_GRACE_SECONDS = 10.0


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
    deadline_preempted: bool = False

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
    scheduled_drain_at: str | None
    actual_drain_requested_at: str | None
    drain_request_slip_seconds: float | None
    unexpected_exit: bool
    forced_shutdown: bool
    forced_shutdown_blockers: list[dict[str, Any]]
    snapshot_failures: list[str]
    degraded_periods: list[dict[str, Any]]
    claim_suppression_periods: list[dict[str, Any]]
    consumer_transport_log_highlights: list[str]
    workflow_issues: list[dict[str, Any]]
    control_plane_proxy_counts: list[dict[str, Any]]
    control_plane_health_transitions: list[dict[str, Any]]
    worked: list[str]
    issues: list[str]
    improvements: list[str]


@dataclass
class WorkflowIssue:
    kind: str
    issue_ref: str
    pr_url: str | None
    count: int
    sample: str


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
        self._scheduled_drain_monotonic: float | None = None
        self._run_meta: dict[str, Any] = {}
        self._scheduled_drain_at: str | None = None
        self._actual_drain_requested_at: str | None = None
        self._drain_request_slip_seconds: float | None = None
        self._forced_shutdown_blockers: list[dict[str, Any]] = []

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
        self._update_run_meta()
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
        self._run_meta = {
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
            "scheduled_drain_at": self._scheduled_drain_at,
            "actual_drain_requested_at": self._actual_drain_requested_at,
            "drain_request_slip_seconds": self._drain_request_slip_seconds,
        }
        _write_json(self.run_dir / "run_meta.json", self._run_meta)

    def _update_run_meta(self) -> None:
        self._run_meta["scheduled_drain_at"] = self._scheduled_drain_at
        self._run_meta["actual_drain_requested_at"] = self._actual_drain_requested_at
        self._run_meta["drain_request_slip_seconds"] = self._drain_request_slip_seconds
        _write_json(self.run_dir / "run_meta.json", self._run_meta)

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
        local_status = self._run_aux_command(
            self._consumer_status_command(local_only=True)
        )
        self._require_ok(local_status, "board_consumer status --json --local-only")
        local_payload = _parse_json(local_status.stdout) or {}
        if local_payload.get("active_leases") or local_payload.get("workers"):
            self._require_ok(
                self._run_aux_command(self._consumer_recover_interrupted_command()),
                "board_consumer recover-interrupted --json",
            )
        self._require_one_shot_ok(
            self._run_aux_command(self._consumer_one_shot_command()),
            "board_consumer one-shot --dry-run --json",
        )

    def _shutdown_state_path(self) -> Path:
        return self.config.state_root / "shutdown-state.json"

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
        scheduled_drain_at = (
            self.clock.now_utc().timestamp() + self.config.duration_seconds
        )
        self._scheduled_drain_monotonic = end
        self._scheduled_drain_at = datetime.fromtimestamp(
            scheduled_drain_at,
            tz=UTC,
        ).isoformat()
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
                if end - now <= LOCAL_SNAPSHOT_GUARD_SECONDS:
                    next_local = end + self.config.local_snapshot_seconds
                    continue
                snapshot = self._capture_snapshot(
                    self._timeline_name("status-local"),
                    self._consumer_status_command(local_only=True),
                    self.timeline_dir,
                    reason="cadence-local",
                    deadline_monotonic=end,
                )
                self._check_status_transitions(snapshot, deadline_monotonic=end)
                next_local += self.config.local_snapshot_seconds
                continue
            if now >= next_full:
                if end - now <= FULL_SNAPSHOT_GUARD_SECONDS:
                    next_full = end + self.config.full_snapshot_seconds
                    continue
                self._capture_timeline_bundle(
                    "cadence-full",
                    deadline_monotonic=end,
                )
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
        self._actual_drain_requested_at = self.clock.now_utc().isoformat()
        if self._scheduled_drain_monotonic is not None:
            self._drain_request_slip_seconds = max(
                0.0,
                drain_requested_at - self._scheduled_drain_monotonic,
            )
        drain_result = self._run_aux_command(self._consumer_drain_command())
        if not drain_result.ok:
            self.snapshot_failures.append("drain command failed")
            self.issues.append("Drain request failed; forcing shutdown escalation.")
        latest_payload: dict[str, Any] = {}
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
            latest_payload = payload
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
        self._forced_shutdown_blockers = self._extract_forced_shutdown_blockers(
            latest_payload
        )
        self._write_forced_shutdown_state()
        self._forced_shutdown = True
        self._shutdown_mode = "forced"
        child.send_signal(signal.SIGINT)
        sigint_deadline = self.clock.monotonic() + 60
        while self.clock.monotonic() < sigint_deadline:
            if child.poll() is not None:
                self._drain_latency_seconds = (
                    self.clock.monotonic() - drain_requested_at
                )
                self._recover_after_forced_shutdown()
                return
            self.clock.sleep(1.0)
        child.send_signal(signal.SIGTERM)
        child.wait(timeout=60)
        self._drain_latency_seconds = self.clock.monotonic() - drain_requested_at
        self._recover_after_forced_shutdown()

    def _recover_after_forced_shutdown(self) -> None:
        result = self._run_aux_command(self._consumer_recover_interrupted_command())
        if not result.ok:
            message = "recover-interrupted failed after forced shutdown"
            self.snapshot_failures.append(message)
            self.issues.append(message)
            return
        payload = _parse_json(result.stdout) or {}
        recovered_leases = int(payload.get("recovered_leases") or 0)
        if recovered_leases:
            self.worked.append(
                f"recovered {recovered_leases} interrupted lease(s) after forced shutdown"
            )

    def _extract_forced_shutdown_blockers(
        self,
        payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        blockers = payload.get("drain_blockers")
        if not isinstance(blockers, list) or not blockers:
            workers = payload.get("workers") or []
            blockers = [
                {
                    "issue_ref": worker.get("issue_ref"),
                    "session_id": worker.get("id"),
                    "phase": worker.get("phase"),
                    "external_execution_started": worker.get(
                        "external_execution_started"
                    ),
                    "active_seconds": worker.get("active_seconds"),
                    "shutdown_class": worker.get("shutdown_class")
                    or worker.get("drain_wait_class"),
                }
                for worker in workers
                if isinstance(worker, dict)
            ]
        rendered: list[dict[str, Any]] = []
        for blocker in blockers:
            if not isinstance(blocker, dict):
                continue
            external_execution_started = bool(
                blocker.get("external_execution_started") is True
            )
            shutdown_reason = (
                "drain_timeout_during_external_execution"
                if external_execution_started
                else "drain_timeout_after_graceful_shutdown_escalation"
            )
            rendered.append(
                {
                    "issue_ref": blocker.get("issue_ref"),
                    "session_id": blocker.get("session_id"),
                    "phase": blocker.get("phase"),
                    "external_execution_started": external_execution_started,
                    "active_seconds": blocker.get("active_seconds"),
                    "shutdown_class": blocker.get("shutdown_class"),
                    "shutdown_reason": shutdown_reason,
                    "failure_reason_override": (
                        "drain_timeout_during_external_execution"
                        if external_execution_started
                        else None
                    ),
                }
            )
        return rendered

    def _write_forced_shutdown_state(self) -> None:
        if not self._forced_shutdown_blockers:
            return
        _write_json(
            self._shutdown_state_path(),
            {
                "cause": "harness-forced-drain-timeout",
                "written_at": self.clock.now_utc().isoformat(),
                "blockers": self._forced_shutdown_blockers,
            },
        )

    def _capture_timeline_bundle(
        self,
        reason: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> None:
        if self._deadline_reached(deadline_monotonic):
            return
        self._capture_snapshot(
            self._timeline_name("status"),
            self._consumer_status_command(local_only=False),
            self.timeline_dir,
            reason=reason,
            deadline_monotonic=deadline_monotonic,
        )
        if self._deadline_reached(deadline_monotonic):
            return
        self._capture_snapshot(
            self._timeline_name("report-slo"),
            self._consumer_report_slo_command(local_only=False),
            self.timeline_dir,
            reason=reason,
            deadline_monotonic=deadline_monotonic,
        )
        if self._deadline_reached(deadline_monotonic):
            return
        self._capture_snapshot(
            self._timeline_name("tick-dry-run"),
            self._control_plane_tick_command(),
            self.timeline_dir,
            reason=reason,
            deadline_monotonic=deadline_monotonic,
        )

    def _capture_event_bundle(
        self,
        reason: str,
        *,
        deadline_monotonic: float | None = None,
    ) -> None:
        if self._deadline_reached(deadline_monotonic):
            return
        snapshot = self._capture_snapshot(
            self._timeline_name("status-local"),
            self._consumer_status_command(local_only=True),
            self.timeline_dir,
            reason=reason,
            deadline_monotonic=deadline_monotonic,
        )
        self._check_status_transitions(
            snapshot,
            allow_follow_up=False,
            deadline_monotonic=deadline_monotonic,
        )
        self._capture_timeline_bundle(reason, deadline_monotonic=deadline_monotonic)

    def _check_status_transitions(
        self,
        snapshot: SnapshotRecord,
        *,
        allow_follow_up: bool = True,
        deadline_monotonic: float | None = None,
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
            self._capture_event_bundle(
                "+".join(reasons),
                deadline_monotonic=deadline_monotonic,
            )

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
        deadline_monotonic: float | None = None,
    ) -> SnapshotRecord:
        directory.mkdir(parents=True, exist_ok=True)
        result = self._run_aux_command(command, deadline_monotonic=deadline_monotonic)
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
                "deadline_preempted": result.deadline_preempted,
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
        if payload is not None and "status" in name:
            self._meaningful_board_activity = (
                self._meaningful_board_activity or self._payload_has_activity(payload)
            )
        if "status" in name and not result.deadline_preempted:
            self.status_records.append(record)
        elif "tick-dry-run" in name and not result.deadline_preempted:
            self.tick_records.append(record)
        elif "report-slo" in name and not result.deadline_preempted:
            self.slo_records.append(record)
        if (
            result.returncode != 0
            and not result.deadline_preempted
            and not self._is_nonfatal_one_shot_idle(result, payload)
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
        workflow_issues = sorted(
            self._workflow_log_issues() + self._runtime_workflow_issues(),
            key=lambda issue: (
                issue.kind,
                issue.issue_ref,
                issue.pr_url or "",
                issue.sample,
            ),
        )
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
        public_forced_shutdown_blockers = [
            {
                key: value
                for key, value in blocker.items()
                if key
                in {
                    "issue_ref",
                    "session_id",
                    "phase",
                    "external_execution_started",
                    "active_seconds",
                    "shutdown_class",
                    "shutdown_reason",
                }
            }
            for blocker in self._forced_shutdown_blockers
        ]
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
        for issue in workflow_issues:
            qualifier = (
                f" {issue.issue_ref} pr={issue.pr_url}"
                if issue.pr_url
                else f" {issue.issue_ref}"
            )
            self.issues.append(
                f"Workflow issue [{issue.kind}{qualifier} x{issue.count}]: "
                f"{issue.sample}"
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
            scheduled_drain_at=self._scheduled_drain_at,
            actual_drain_requested_at=self._actual_drain_requested_at,
            drain_request_slip_seconds=self._drain_request_slip_seconds,
            unexpected_exit=self._unexpected_exit,
            forced_shutdown=self._forced_shutdown,
            forced_shutdown_blockers=public_forced_shutdown_blockers,
            snapshot_failures=list(self.snapshot_failures),
            degraded_periods=degraded_periods,
            claim_suppression_periods=claim_periods,
            consumer_transport_log_highlights=log_highlights,
            workflow_issues=[asdict(issue) for issue in workflow_issues],
            control_plane_proxy_counts=control_plane_counts,
            control_plane_health_transitions=control_plane_health_transitions,
            worked=_dedupe(self.worked),
            issues=_dedupe(self.issues),
            improvements=_dedupe(improvements),
        )

    def _runtime_workflow_issues(self) -> list[WorkflowIssue]:
        issues: list[WorkflowIssue] = []
        if (
            self._drain_request_slip_seconds is not None
            and self._drain_request_slip_seconds > 5
        ):
            issues.append(
                WorkflowIssue(
                    kind="drain_request_late",
                    issue_ref="global",
                    pr_url=None,
                    count=1,
                    sample=(
                        "Drain request slipped by "
                        f"{self._drain_request_slip_seconds:.3f}s "
                        f"(scheduled={self._scheduled_drain_at}, actual={self._actual_drain_requested_at})"
                    ),
                )
            )
        if self._has_stale_local_state_after_shutdown():
            issues.append(
                WorkflowIssue(
                    kind="stale_local_state_after_shutdown",
                    issue_ref="global",
                    pr_url=None,
                    count=1,
                    sample="Consumer stopped, but final local-only status still showed active leases/workers.",
                )
            )
        return issues

    def _has_stale_local_state_after_shutdown(self) -> bool:
        payload = self._latest_local_status_payload()
        if payload is None:
            return False
        if self.backend.systemd_consumer_active(
            timeout_seconds=float(self.config.command_timeout_seconds)
        ):
            return False
        if self.backend.local_consumer_processes(
            timeout_seconds=float(self.config.command_timeout_seconds)
        ):
            return False
        return bool(payload.get("active_leases") or payload.get("workers"))

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
        if not self._forced_shutdown_blockers:
            return False
        return all(
            blocker.get("external_execution_started") is True
            and blocker.get("shutdown_class") == "finishing_inflight_execution"
            for blocker in self._forced_shutdown_blockers
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
            f"- Scheduled drain at: `{summary.scheduled_drain_at}`",
            f"- Actual drain requested at: `{summary.actual_drain_requested_at}`",
            f"- Drain request slip seconds: `{summary.drain_request_slip_seconds}`",
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
        lines.extend(["", "## Forced Shutdown Blockers"])
        if summary.forced_shutdown_blockers:
            for blocker in summary.forced_shutdown_blockers:
                lines.append(
                    "- "
                    f"{blocker.get('issue_ref')} session={blocker.get('session_id')} "
                    f"class={blocker.get('shutdown_class')} "
                    f"reason={blocker.get('shutdown_reason')} "
                    f"active_seconds={blocker.get('active_seconds')}"
                )
        else:
            lines.append("- None recorded.")
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

    def _workflow_log_issues(self) -> list[WorkflowIssue]:
        log_path = self.logs_dir / "consumer-run.log"
        lines = (
            log_path.read_text(encoding="utf-8").splitlines()
            if log_path.exists()
            else []
        )
        grouped: dict[tuple[str, str, str | None], WorkflowIssue] = {}
        claim_streaks: dict[tuple[str, str], int] = {}
        reclaim_samples: dict[tuple[str, str], str] = {}
        removed_review_refs: dict[str, datetime | None] = {}
        for index, line in enumerate(lines):
            stripped = line.strip()
            worktree_match = WORKTREE_REUSE_BLOCKED_PATTERN.search(stripped)
            if worktree_match is not None:
                issue_ref = worktree_match.group("issue_ref")
                _record_workflow_issue(
                    grouped,
                    kind="worktree_reuse_blocked",
                    issue_ref=issue_ref,
                    pr_url=None,
                    sample=stripped,
                )
                continue
            removed_refs = set(_extract_removed_refs(stripped))
            seeded_refs = set(_extract_seeded_refs(stripped))
            requeued_refs = _extract_requeued_refs(stripped)
            log_ts = _parse_log_timestamp(stripped)
            for ref in removed_refs:
                if ref not in requeued_refs:
                    removed_review_refs[ref] = log_ts
            for issue_ref in seeded_refs:
                removed_review_refs.pop(issue_ref, None)
            if requeued_refs:
                for issue_ref in requeued_refs:
                    removed_review_refs.pop(issue_ref, None)
                    for claim_key in tuple(claim_streaks):
                        if claim_key[0] == issue_ref:
                            claim_streaks.pop(claim_key, None)
                continue
            claimed_match = CLAIMED_RESULT_PATTERN.search(stripped)
            if claimed_match is not None:
                issue_ref = claimed_match.group("issue_ref")
                pr_url = claimed_match.group("pr_url")
                removed_at = removed_review_refs.pop(issue_ref, None)
                if removed_at is not None:
                    claim_ts = _parse_log_timestamp(stripped)
                    if (
                        removed_at is not None
                        and claim_ts is not None
                        and (claim_ts - removed_at).total_seconds()
                        <= RECLAIM_COMPLETION_GRACE_SECONDS
                    ):
                        pass
                    else:
                        _record_workflow_issue(
                            grouped,
                            kind="review_reclaimed_after_removal",
                            issue_ref=issue_ref,
                            pr_url=pr_url,
                            sample=stripped,
                        )
                claim_key = (issue_ref, pr_url)
                claim_streaks[claim_key] = claim_streaks.get(claim_key, 0) + 1
                if claim_streaks[claim_key] >= 2:
                    _record_workflow_issue(
                        grouped,
                        kind="review_reclaim_loop",
                        issue_ref=issue_ref,
                        pr_url=pr_url,
                        sample=reclaim_samples.setdefault(claim_key, stripped),
                    )
                continue
            kind = _workflow_issue_kind(stripped)
            if kind is not None:
                issue_ref = _infer_workflow_issue_ref(lines, index)
                _record_workflow_issue(
                    grouped,
                    kind=kind,
                    issue_ref=issue_ref,
                    pr_url=None,
                    sample=stripped,
                )
        for record in self.status_records:
            payload = record.payload or {}
            review_summary = payload.get("review_summary")
            if not isinstance(review_summary, dict):
                continue
            if review_summary.get("source") != "local-fallback":
                continue
            error = str(review_summary.get("error") or "")
            if "Invalid issue ref" not in error:
                continue
            sample = error
            qualified_ref = FULLY_QUALIFIED_ISSUE_REF_PATTERN.search(error)
            if qualified_ref is not None:
                sample = f"{error} [{qualified_ref.group(0)}]"
            _record_workflow_issue(
                grouped,
                kind="review_summary_parse_error",
                issue_ref="global",
                pr_url=None,
                sample=sample,
            )
        return sorted(
            grouped.values(),
            key=lambda issue: (
                issue.kind,
                issue.issue_ref,
                issue.pr_url or "",
                issue.sample,
            ),
        )

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

    def _consumer_recover_interrupted_command(self) -> list[str]:
        command = [
            "uv",
            "run",
            "python",
            "-m",
            "startupai_controller.board_consumer",
            "recover-interrupted",
            "--json",
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

    def _run_aux_command(
        self,
        argv: list[str],
        *,
        deadline_monotonic: float | None = None,
    ) -> CommandResult:
        configured_timeout = float(self.config.command_timeout_seconds)
        effective_timeout = configured_timeout
        deadline_clipped = False
        if deadline_monotonic is not None:
            remaining = max(0.0, deadline_monotonic - self.clock.monotonic())
            if remaining <= 0.0:
                timestamp = self.clock.now_utc().isoformat()
                return CommandResult(
                    argv=list(argv),
                    returncode=124,
                    stdout="",
                    stderr="command preempted at drain deadline",
                    started_at=timestamp,
                    completed_at=timestamp,
                    timed_out=True,
                    timeout_seconds=0.0,
                    deadline_preempted=True,
                )
            effective_timeout = min(configured_timeout, remaining)
            deadline_clipped = effective_timeout < configured_timeout
        result = self.backend.run(
            argv,
            timeout_seconds=effective_timeout,
        )
        if result.timed_out and deadline_clipped:
            return CommandResult(
                argv=result.argv,
                returncode=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
                started_at=result.started_at,
                completed_at=result.completed_at,
                timed_out=result.timed_out,
                timeout_seconds=result.timeout_seconds,
                deadline_preempted=True,
            )
        return result

    def _deadline_reached(self, deadline_monotonic: float | None) -> bool:
        return (
            deadline_monotonic is not None
            and self.clock.monotonic() >= deadline_monotonic
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
        return payload.get("exit_class") == "idle" and payload.get("action") == "idle"


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


def _workflow_issue_kind(line: str) -> str | None:
    if "PR creation skipped:" in line:
        return "pr_creation_skipped"
    if "PR creation failed:" in line:
        return "pr_creation_failed"
    if " ERROR " in line and not TRANSPORT_LOG_PATTERN.search(line):
        return "workflow_error"
    return None


def _extract_requeued_refs(line: str) -> tuple[str, ...]:
    match = REQUEUED_REFS_PATTERN.search(line)
    if match is None:
        return ()
    raw_refs = match.group("refs").strip()
    if not raw_refs:
        return ()
    return tuple(ISSUE_REF_PATTERN.findall(raw_refs))


def _extract_seeded_refs(line: str) -> tuple[str, ...]:
    match = SEEDED_REFS_PATTERN.search(line)
    if match is None:
        return ()
    raw_refs = match.group("refs").strip()
    if not raw_refs:
        return ()
    return tuple(ISSUE_REF_PATTERN.findall(raw_refs))


def _extract_removed_refs(line: str) -> tuple[str, ...]:
    match = REMOVED_REFS_PATTERN.search(line)
    if match is None:
        return ()
    raw_refs = match.group("refs").strip()
    if not raw_refs:
        return ()
    return tuple(ISSUE_REF_PATTERN.findall(raw_refs))


def _parse_log_timestamp(line: str) -> datetime | None:
    match = LOG_TIMESTAMP_PATTERN.search(line)
    if match is None:
        return None
    return datetime.strptime(match.group("stamp"), "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=UTC
    )


def _record_workflow_issue(
    grouped: dict[tuple[str, str, str | None], WorkflowIssue],
    *,
    kind: str,
    issue_ref: str,
    pr_url: str | None,
    sample: str,
) -> None:
    key = (kind, issue_ref, pr_url)
    existing = grouped.get(key)
    if existing is None:
        grouped[key] = WorkflowIssue(
            kind=kind,
            issue_ref=issue_ref,
            pr_url=pr_url,
            count=1,
            sample=sample,
        )
        return
    existing.count += 1


def _infer_workflow_issue_ref(lines: list[str], index: int) -> str:
    same_line_match = ISSUE_REF_PATTERN.search(lines[index])
    if same_line_match is not None:
        return same_line_match.group(0)

    branch_match = BRANCH_NUMBER_PATTERN.search(lines[index])
    branch_number = branch_match.group("number") if branch_match is not None else None
    window_start = max(0, index - 3)
    window_end = min(len(lines), index + 4)
    window_refs: list[str] = []
    for line in lines[window_start:window_end]:
        for issue_ref in ISSUE_REF_PATTERN.findall(line):
            if branch_number is None or issue_ref.endswith(f"#{branch_number}"):
                window_refs.append(issue_ref)
    if window_refs:
        return window_refs[0]
    if branch_number is not None:
        return f"unknown#{branch_number}"
    return "unknown"


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
