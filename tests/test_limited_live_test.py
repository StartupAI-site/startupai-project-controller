from __future__ import annotations

import json
import signal
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tools.limited_live_test import (
    CommandResult,
    HarnessError,
    LimitedLiveTestConfig,
    LimitedLiveTestHarness,
)

UTC = timezone.utc


class FakeClock:
    def __init__(self) -> None:
        self._monotonic = 0.0
        self._now = datetime(2026, 3, 13, 12, 0, tzinfo=UTC)

    def now_utc(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._monotonic

    def sleep(self, seconds: float) -> None:
        self._monotonic += seconds
        self._now += timedelta(seconds=seconds)


@dataclass
class FakeProcess:
    clock: FakeClock
    drain_exit_delay: float | None
    sigint_stops: bool = False
    sigterm_stops: bool = True

    def __post_init__(self) -> None:
        self.pid = 4242
        self.exit_at: float | None = None
        self.returncode: int | None = None
        self.signals: list[int] = []

    def poll(self) -> int | None:
        if (
            self.returncode is None
            and self.exit_at is not None
            and self.clock.monotonic() >= self.exit_at
        ):
            self.returncode = 0
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        if self.returncode is None:
            if self.exit_at is not None:
                self.clock.sleep(max(0.0, self.exit_at - self.clock.monotonic()))
                self.returncode = 0
            else:
                self.returncode = 0
        return self.returncode

    def send_signal(self, sig: int) -> None:
        self.signals.append(sig)
        if sig == signal.SIGINT and self.sigint_stops:
            self.returncode = 130
        if sig == signal.SIGTERM and self.sigterm_stops:
            self.returncode = 143

    def terminate(self) -> None:
        self.returncode = 143

    def close(self) -> None:
        return None


class FakeBackend:
    def __init__(
        self,
        *,
        clock: FakeClock,
        process: FakeProcess,
        status_local: list[dict],
        status_full: list[dict],
        report_slo: list[dict],
        tick_payloads: list[dict],
        log_text: str = "",
        systemd_active: bool = False,
        local_processes: list[str] | None = None,
        failures: dict[tuple[str, bool | None], int] | None = None,
        timeouts: dict[tuple[str, bool | None], bool] | None = None,
    ) -> None:
        self.clock = clock
        self.process = process
        self.status_local = list(status_local)
        self.status_full = list(status_full)
        self.report_slo = list(report_slo)
        self.tick_payloads = list(tick_payloads)
        self.log_text = log_text
        self._systemd_active = systemd_active
        self._local_processes = local_processes or []
        self.failures = failures or {}
        self.timeouts = timeouts or {}
        self.commands: list[list[str]] = []
        self.timeout_requests: list[tuple[list[str], float | None]] = []

    def run(
        self,
        argv: list[str],
        *,
        timeout_seconds: float | None = None,
    ) -> CommandResult:
        self.commands.append(list(argv))
        self.timeout_requests.append((list(argv), timeout_seconds))
        key = self._command_key(argv)
        timed_out = self.timeouts.get(key, False)
        returncode = self.failures.get(key, 0)
        stdout = ""
        stderr = ""
        if argv[:3] == ["git", "rev-parse", "HEAD"]:
            stdout = "abc123\n"
        elif argv[:3] == ["gh", "auth", "status"]:
            stdout = "logged in\n"
        elif self._is_consumer_command(argv) and "status" in argv:
            payloads = self.status_local if "--local-only" in argv else self.status_full
            stdout = json.dumps(self._next_payload(payloads))
        elif self._is_consumer_command(argv) and "report-slo" in argv:
            stdout = json.dumps(self._next_payload(self.report_slo))
        elif self._is_control_plane_command(argv) and "tick" in argv:
            stdout = json.dumps(self._next_payload(self.tick_payloads))
        elif self._is_consumer_command(argv) and "one-shot" in argv:
            payload = {
                "action": "claimed" if returncode == 0 else "idle",
                "reason": "" if returncode == 0 else "lease-cap",
                "issue_ref": None,
                "session_id": None,
                "pr_url": None,
                "exit_class": "success" if returncode == 0 else "idle",
            }
            if returncode == 4:
                payload["action"] = "error"
                payload["reason"] = "error"
                payload["exit_class"] = "error"
            stdout = json.dumps(payload)
        elif self._is_consumer_command(argv) and "drain" in argv:
            if (
                self.process.drain_exit_delay is not None
                and self.process.exit_at is None
            ):
                self.process.exit_at = (
                    self.clock.monotonic() + self.process.drain_exit_delay
                )
            stdout = '{"drain_requested": true}\n'
        elif self._is_consumer_command(argv) and "reconcile" in argv:
            stdout = "reconcile dry run\n"
        elif argv[:2] == ["ps", "-eo"]:
            stdout = "\n".join(self._local_processes)
        elif argv[:3] == ["systemctl", "--user", "is-active"]:
            stdout = "inactive\n"
        else:
            stdout = "{}\n"
        if timed_out:
            returncode = 124
            stderr = (
                f"command timed out after {timeout_seconds} seconds"
                if timeout_seconds is not None
                else "command timed out"
            )
        if returncode != 0:
            if not stderr:
                stderr = f"simulated failure for {key}\n"
        started_at = self.clock.now_utc().isoformat()
        completed_at = self.clock.now_utc().isoformat()
        return CommandResult(
            argv=list(argv),
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
            started_at=started_at,
            completed_at=completed_at,
            timed_out=timed_out,
            timeout_seconds=timeout_seconds,
        )

    def start_consumer(self, argv: list[str], log_path: Path) -> FakeProcess:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(self.log_text, encoding="utf-8")
        return self.process

    def local_consumer_processes(
        self,
        *,
        timeout_seconds: float | None = None,
    ) -> list[str]:
        self.timeout_requests.append((["ps", "-eo", "pid=,command="], timeout_seconds))
        return list(self._local_processes)

    def systemd_consumer_active(
        self,
        *,
        timeout_seconds: float | None = None,
    ) -> bool:
        self.timeout_requests.append(
            (
                ["systemctl", "--user", "is-active", "startupai-consumer"],
                timeout_seconds,
            )
        )
        return self._systemd_active

    def _next_payload(self, items: list[dict]) -> dict:
        if not items:
            return {}
        if len(items) == 1:
            return items[0]
        return items.pop(0)

    def _command_key(self, argv: list[str]) -> tuple[str, bool | None]:
        if "status" in argv:
            return ("status", "--local-only" in argv)
        if "report-slo" in argv:
            return ("report-slo", "--local-only" in argv)
        if "tick" in argv:
            return ("tick", None)
        return ("other", None)

    def _is_consumer_command(self, argv: list[str]) -> bool:
        return any(
            part.endswith("startupai_controller.board_consumer") for part in argv
        )

    def _is_control_plane_command(self, argv: list[str]) -> bool:
        return any(
            part.endswith("startupai_controller.board_control_plane") for part in argv
        )


def _status_payload(
    *,
    active_leases: int = 0,
    workers: list[dict] | None = None,
    recent_sessions: list[dict] | None = None,
    degraded: bool = False,
    degraded_reason: str | None = None,
    claim_suppressed_until: str | None = None,
    claim_suppressed_reason: str | None = None,
    last_successful_github_mutation_at: str | None = None,
) -> dict:
    return {
        "active_leases": active_leases,
        "workers": workers or [],
        "recent_sessions": recent_sessions or [],
        "degraded": degraded,
        "degraded_reason": degraded_reason,
        "claim_suppressed_until": claim_suppressed_until,
        "claim_suppressed_reason": claim_suppressed_reason,
        "last_rate_limit_at": None,
        "last_successful_board_sync_at": "2026-03-13T12:00:00+00:00",
        "last_successful_github_mutation_at": last_successful_github_mutation_at,
        "control_plane_health": {"health": "healthy", "reason_code": None},
    }


def _report_payload() -> dict:
    return {"windows": {"1h": {"durable_starts": 1}}}


def _tick_payload() -> dict:
    return {
        "health": "healthy",
        "github_request_counts": {"graphql": 4, "rest": 2},
    }


def _config(tmp_path: Path) -> LimitedLiveTestConfig:
    state_root = tmp_path / "state"
    return LimitedLiveTestConfig(
        state_root=state_root,
        artifact_root=state_root / "test-runs",
        db_path=None,
        duration_seconds=10,
        local_snapshot_seconds=5,
        full_snapshot_seconds=10,
        shutdown_poll_seconds=2,
        drain_timeout_seconds=6,
        post_quiesce_exit_seconds=2,
        command_timeout_seconds=60,
        consumer_interval_seconds=None,
        confirm_single_consumer=True,
        confirmation_note="verified no other host is running the consumer",
    )


def test_limited_live_test_happy_path_creates_artifacts(tmp_path: Path) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[
            _status_payload(),
            _status_payload(
                active_leases=1,
                workers=[{"issue_ref": "crew#84"}],
                recent_sessions=[{"issue_ref": "crew#84"}],
                last_successful_github_mutation_at="2026-03-13T12:05:00+00:00",
            ),
            _status_payload(),
        ],
        status_full=[_status_payload(), _status_payload()],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
        log_text="GitHub check completed cleanly\n",
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert summary.meaningful_board_activity is True
    assert summary.shutdown_mode == "natural"
    assert summary.conclusion == "current transport acceptable for live use"


def test_one_shot_idle_json_is_nonfatal_for_harness(tmp_path: Path) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload()],
        status_full=[_status_payload()],
        report_slo=[_report_payload()],
        tick_payloads=[_tick_payload()],
        failures={("other", None): 2},
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    result = backend.run(harness._consumer_one_shot_command())
    payload = json.loads(result.stdout)

    harness._require_one_shot_ok(result, "board_consumer one-shot --dry-run --json")
    assert harness._is_nonfatal_one_shot_idle(result, payload) is True


def test_one_shot_idle_requires_idle_action_for_nonfatal_harness_classification(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload()],
        status_full=[_status_payload()],
        report_slo=[_report_payload()],
        tick_payloads=[_tick_payload()],
        failures={("other", None): 2},
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    result = backend.run(harness._consumer_one_shot_command())
    payload = json.loads(result.stdout)
    payload["action"] = "error"

    assert harness._is_nonfatal_one_shot_idle(result, payload) is False
    with pytest.raises(HarnessError, match="failed with exit code 2"):
        harness._require_one_shot_ok(
            CommandResult(
                argv=result.argv,
                returncode=result.returncode,
                stdout=json.dumps(payload),
                stderr=result.stderr,
                started_at=result.started_at,
                completed_at=result.completed_at,
                timed_out=result.timed_out,
                timeout_seconds=result.timeout_seconds,
            ),
            "board_consumer one-shot --dry-run --json",
        )


def test_limited_live_test_fails_preflight_when_systemd_active(tmp_path: Path) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload()],
        status_full=[_status_payload()],
        report_slo=[_report_payload()],
        tick_payloads=[_tick_payload()],
        systemd_active=True,
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    with pytest.raises(HarnessError, match="systemd"):
        harness.run()


def test_limited_live_test_marks_quiet_run_inconclusive(tmp_path: Path) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    quiet_status = _status_payload()
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[quiet_status, quiet_status, quiet_status],
        status_full=[quiet_status, quiet_status],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert summary.meaningful_board_activity is False
    assert summary.conclusion == "test inconclusive due to insufficient board activity"


def test_limited_live_test_escalates_when_drain_does_not_finish(tmp_path: Path) -> None:
    clock = FakeClock()
    process = FakeProcess(
        clock=clock,
        drain_exit_delay=None,
        sigint_stops=False,
        sigterm_stops=True,
    )
    active_status = _status_payload(
        active_leases=1,
        workers=[{"issue_ref": "crew#84"}],
        recent_sessions=[{"issue_ref": "crew#84"}],
        last_successful_github_mutation_at="2026-03-13T12:05:00+00:00",
    )
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[active_status, active_status, active_status, active_status],
        status_full=[active_status, active_status],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert summary.shutdown_mode == "forced"
    assert summary.forced_shutdown is True
    assert process.signals == [signal.SIGINT, signal.SIGTERM]
    assert "Drain timeout exceeded; sent SIGINT/SIGTERM escalation." in summary.issues


def test_limited_live_test_treats_forced_shutdown_as_acceptable_when_only_waiting_on_inflight_execution(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(
        clock=clock,
        drain_exit_delay=None,
        sigint_stops=False,
        sigterm_stops=True,
    )
    inflight_worker = {
        "issue_ref": "crew#84",
        "status": "running",
        "external_execution_started": True,
        "drain_wait_class": "finishing_inflight_execution",
    }
    active_status = _status_payload(
        active_leases=1,
        workers=[inflight_worker],
        recent_sessions=[inflight_worker],
        last_successful_github_mutation_at="2026-03-13T12:05:00+00:00",
    )
    active_status["local_only"] = True
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[active_status, active_status, active_status, active_status],
        status_full=[
            _status_payload(active_leases=1),
            _status_payload(active_leases=1),
        ],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert summary.shutdown_mode == "forced"
    assert summary.forced_shutdown is True
    assert (
        "Drain timeout exceeded; sent SIGINT/SIGTERM escalation." not in summary.issues
    )
    assert (
        "forced shutdown occurred only after waiting on in-flight external execution"
        in summary.worked
    )
    assert summary.conclusion == "current transport acceptable for live use"


def test_limited_live_test_records_snapshot_failure_and_continues(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    active_status = _status_payload(
        active_leases=1,
        workers=[{"issue_ref": "crew#84"}],
        recent_sessions=[{"issue_ref": "crew#84"}],
        last_successful_github_mutation_at="2026-03-13T12:05:00+00:00",
    )
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[active_status, active_status, _status_payload()],
        status_full=[active_status, active_status],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
        failures={("tick", None): 1},
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert summary.snapshot_failures
    assert any("tick" in item for item in summary.snapshot_failures)
    assert (harness.run_dir / "summary.json").exists()


def test_limited_live_test_preflight_timeout_is_fatal(tmp_path: Path) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload()],
        status_full=[_status_payload()],
        report_slo=[_report_payload()],
        tick_payloads=[_tick_payload()],
        timeouts={("status", False): True},
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    with pytest.raises(HarnessError, match="timed out"):
        harness.run()


def test_limited_live_test_records_snapshot_timeout_and_continues(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    active_status = _status_payload(
        active_leases=1,
        workers=[{"issue_ref": "crew#84"}],
        recent_sessions=[{"issue_ref": "crew#84"}],
        last_successful_github_mutation_at="2026-03-13T12:05:00+00:00",
    )
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[active_status, active_status, _status_payload()],
        status_full=[active_status, active_status],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
        timeouts={("tick", None): True},
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert any("timed out after" in item for item in summary.snapshot_failures)
    assert (harness.run_dir / "summary.json").exists()


def test_limited_live_test_applies_timeout_to_preflight_process_checks(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload(), _status_payload(), _status_payload()],
        status_full=[_status_payload(), _status_payload()],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    harness.run()

    assert (
        ["systemctl", "--user", "is-active", "startupai-consumer"],
        60.0,
    ) in backend.timeout_requests
    assert (["ps", "-eo", "pid=,command="], 60.0) in backend.timeout_requests
