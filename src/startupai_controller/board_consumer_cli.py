"""CLI surface for the board consumer application service."""

from __future__ import annotations

import argparse
import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from startupai_controller.board_consumer import ConsumerConfig


def _core():
    from startupai_controller import board_consumer as core

    return core


def _cmd_status(
    config: ConsumerConfig,
    *,
    as_json: bool = False,
    local_only: bool = False,
) -> int:
    """Show current consumer status."""
    payload = _core()._collect_status_payload(config, local_only=local_only)
    if as_json:
        print(json.dumps(payload, indent=2))
        return 0

    print(f"degraded={payload['degraded']}")
    print(f"reason={payload['degraded_reason']}")
    print(f"active_leases={payload['active_leases']}")
    print(f"global_concurrency={payload['global_concurrency']}")
    if payload["claim_suppressed_until"]:
        print(
            "claim_suppressed="
            f"{payload['claim_suppressed_scope']} until {payload['claim_suppressed_until']}"
        )
    if payload["review_summary"]["count"]:
        print("review:")
        for item in payload["review_summary"]["items"]:
            stale = f" stale={item['stale']}" if item["stale"] else ""
            print(
                f"  {item['issue_ref']}: pr={item['pr_url']} state={item['last_result']}"
                f"{stale}"
            )
    if payload["sessions"]:
        print("sessions:")
        for session in payload["sessions"]:
            slot = f" slot={session['slot_index']}" if session["slot_index"] else ""
            phase = f" phase={session['phase']}" if session["phase"] else ""
            kind = (
                f" kind={session['session_kind']}"
                if session["session_kind"]
                else ""
            )
            reconcile = (
                f" reconcile={session['branch_reconcile_state']}"
                if session["branch_reconcile_state"]
                else ""
            )
            pr = f" pr={session['pr_url']}" if session["pr_url"] else ""
            failure = (
                f" failure={session['failure_reason']}"
                if session["failure_reason"]
                else ""
            )
            retry = (
                f" retry={session['retry_count']}"
                if session["retry_count"]
                else ""
            )
            next_retry = (
                f" next_retry={session['next_retry_at']}"
                if session["next_retry_at"]
                else ""
            )
            resolution = (
                f" resolution={session['resolution_kind']}/{session['verification_class']}"
                if session["resolution_kind"]
                else ""
            )
            resolution_action = (
                f" action={session['resolution_action']}"
                if session["resolution_action"]
                else ""
            )
            done_reason = (
                f" done={session['done_reason']}"
                if session["done_reason"]
                else ""
            )
            print(
                f"  {session['id']}  {session['issue_ref']:>10}  "
                f"{session['status']:<8}  {session['executor']}{slot}{phase}"
                f"{kind}{reconcile}"
                f"{failure}{retry}{next_retry}{resolution}{resolution_action}{done_reason}{pr}"
            )

    return 0


def _cmd_report_slo(
    config: ConsumerConfig,
    *,
    as_json: bool = False,
    local_only: bool = False,
) -> int:
    """Report rolling reliability and throughput metrics."""
    data = _core()._collect_status_payload(config, local_only=local_only)
    report = {
        "baseline_status": data["throughput_metrics"]["baseline_status"],
        "claim_suppressed_until": data["claim_suppressed_until"],
        "claim_suppressed_reason": data["claim_suppressed_reason"],
        "claim_suppressed_scope": data["claim_suppressed_scope"],
        "degraded": data["degraded"],
        "degraded_reason": data["degraded_reason"],
        "windows": data["throughput_metrics"]["windows"],
        "reliability_metrics": data["reliability_metrics"],
        "context_cache_metrics": data["context_cache_metrics"],
        "worktree_reuse_metrics": data["worktree_reuse_metrics"],
    }
    if as_json:
        print(json.dumps(report, indent=2))
        return 0

    print(f"Baseline status: {report['baseline_status']}")
    if report["claim_suppressed_until"]:
        print(
            "Claim suppression: "
            f"{report['claim_suppressed_scope']} until {report['claim_suppressed_until']}"
        )
        if report["claim_suppressed_reason"]:
            print(f"Claim suppression reason: {report['claim_suppressed_reason']}")
    print(
        "Degraded: "
        f"{'yes' if report['degraded'] else 'no'}"
        + (
            f" ({report['degraded_reason']})"
            if report["degraded"] and report["degraded_reason"]
            else ""
        )
    )
    for window_name in ("1h", "24h"):
        window = report["windows"][window_name]
        print(f"{window_name}:")
        print(
            "  durable_starts="
            f"{window['durable_starts']} startup_failures={window['startup_failures']} "
            f"reliability={window['durable_start_reliability']}"
        )
        print(
            "  occupied_slots_per_hour="
            f"{window['occupied_slots_per_hour']:.2f} "
            f"occupied_slots_per_ready_hour_ge_1={window['occupied_slots_per_ready_hour_ge_1']}"
        )
        print(
            "  ready_hours_ge_1="
            f"{window['ready_hours_ge_1']:.2f} "
            f"ready_hours_ge_4={window['ready_hours_ge_4']:.2f}"
        )
    return 0


def _create_status_http_server(
    config: ConsumerConfig,
    *,
    host: str | None = None,
    port: int | None = None,
) -> ThreadingHTTPServer:
    """Create a local HTTP server that exposes consumer status."""
    core = _core()
    host = host or core.DEFAULT_STATUS_HOST
    port = port or core.DEFAULT_STATUS_PORT

    class StatusHandler(BaseHTTPRequestHandler):
        def _write_json(self, payload: dict[str, Any], *, status_code: int = 200) -> None:
            body = json.dumps(payload, indent=2).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            if self.path in {"/", "/status"}:
                self._write_json(core._collect_status_payload(config, local_only=True))
                return
            if self.path == "/healthz":
                payload = core._collect_status_payload(config, local_only=True)
                self._write_json(
                    {
                        "ok": True,
                        "health": payload["control_plane_health"]["health"],
                        "degraded": payload["degraded"],
                        "degraded_reason": payload["degraded_reason"],
                    }
                )
                return
            self._write_json({"error": "not_found", "path": self.path}, status_code=404)

        def log_message(self, format: str, *args: Any) -> None:
            core.logger.debug("status-http %s - %s", self.address_string(), format % args)

    return ThreadingHTTPServer((host, port), StatusHandler)


def _cmd_serve_status(
    config: ConsumerConfig,
    *,
    host: str | None = None,
    port: int | None = None,
) -> int:
    """Serve local-only consumer status over localhost HTTP."""
    core = _core()
    host = host or core.DEFAULT_STATUS_HOST
    port = port or core.DEFAULT_STATUS_PORT
    server = _create_status_http_server(config, host=host, port=port)
    core.logger.info("Serving local consumer status on http://%s:%s", host, server.server_port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        core.logger.info("Status server stopped by user")
    finally:
        server.server_close()
    return 0


def _cmd_reconcile(config: ConsumerConfig, *, dry_run: bool = False) -> int:
    """Reconcile board truth against local consumer state."""
    core = _core()
    db = core.open_consumer_db(config.db_path)
    try:
        cp_config = core.load_config(config.critical_paths_path)
        auto_config = core.load_automation_config(config.automation_config_path)
        core._apply_automation_runtime(config, auto_config)
        session_store = core.build_session_store(db)
        result = core._reconcile_board_truth(
            config,
            cp_config,
            auto_config,
            db,
            session_store=session_store,
            dry_run=dry_run,
        )
    finally:
        db.close()

    data = {
        "dry_run": dry_run,
        "moved_ready": list(result.moved_ready),
        "moved_in_progress": list(result.moved_in_progress),
        "moved_review": list(result.moved_review),
        "moved_blocked": list(result.moved_blocked),
    }
    print(json.dumps(data, indent=2))
    return 0


def _cmd_drain(config: ConsumerConfig) -> int:
    """Request a graceful drain at the next cycle boundary."""
    _core()._request_drain(config.drain_path)
    print(
        json.dumps(
            {
                "drain_requested": True,
                "drain_path": str(config.drain_path),
            },
            indent=2,
        )
    )
    return 0


def _cmd_resume(config: ConsumerConfig) -> int:
    """Clear a pending graceful drain request."""
    cleared = _core()._clear_drain(config.drain_path)
    print(
        json.dumps(
            {
                "drain_requested": False,
                "drain_path": str(config.drain_path),
                "cleared": cleared,
            },
            indent=2,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the board-consumer CLI parser."""
    core = _core()
    parser = argparse.ArgumentParser(
        description="Board consumer daemon — poll, claim, execute.",
    )
    parser.add_argument(
        "--file",
        default=core.DEFAULT_CONFIG_PATH,
        help="Path to critical-paths.json",
    )
    parser.add_argument(
        "--automation-config",
        default=core.DEFAULT_AUTOMATION_CONFIG_PATH,
        help="Path to board-automation-config.json",
    )
    parser.add_argument("--project-owner", default="StartupAI-site")
    parser.add_argument("--project-number", type=int, default=1)

    sub = parser.add_subparsers(dest="command")

    run_p = sub.add_parser("run", help="Run daemon loop")
    run_p.add_argument("--interval", type=int, default=180, help="Poll interval seconds")
    run_p.add_argument("--db-path", type=Path, default=core.DEFAULT_DB_PATH)
    run_p.add_argument("--dry-run", action="store_true")
    run_p.add_argument("--verbose", action="store_true")

    one_p = sub.add_parser("one-shot", help="Run one cycle")
    one_p.add_argument("--issue", default=None, help="Target issue ref (e.g. crew#84)")
    one_p.add_argument("--db-path", type=Path, default=core.DEFAULT_DB_PATH)
    one_p.add_argument("--dry-run", action="store_true")
    one_p.add_argument("--verbose", action="store_true")

    stat_p = sub.add_parser("status", help="Show consumer state")
    stat_p.add_argument("--db-path", type=Path, default=core.DEFAULT_DB_PATH)
    stat_p.add_argument("--json", action="store_true", dest="as_json")
    stat_p.add_argument(
        "--local-only",
        action="store_true",
        help="Skip GitHub queries and report local consumer state only",
    )

    report_p = sub.add_parser("report-slo", help="Show rolling reliability and throughput metrics")
    report_p.add_argument("--db-path", type=Path, default=core.DEFAULT_DB_PATH)
    report_p.add_argument("--json", action="store_true", dest="as_json")
    report_p.add_argument(
        "--local-only",
        action="store_true",
        help="Skip GitHub queries and report local consumer state only",
    )

    serve_p = sub.add_parser("serve-status", help="Serve local-only consumer status over localhost HTTP")
    serve_p.add_argument("--db-path", type=Path, default=core.DEFAULT_DB_PATH)
    serve_p.add_argument("--host", default=core.DEFAULT_STATUS_HOST)
    serve_p.add_argument("--port", type=int, default=core.DEFAULT_STATUS_PORT)

    rec_p = sub.add_parser(
        "reconcile",
        help="Reconcile board In Progress truth against local consumer state",
    )
    rec_p.add_argument("--db-path", type=Path, default=core.DEFAULT_DB_PATH)
    rec_p.add_argument("--dry-run", action="store_true")

    drain_p = sub.add_parser("drain", help="Request a graceful drain before the next issue claim")
    drain_p.add_argument("--db-path", type=Path, default=core.DEFAULT_DB_PATH)

    resume_p = sub.add_parser("resume", help="Clear a pending graceful drain request")
    resume_p.add_argument("--db-path", type=Path, default=core.DEFAULT_DB_PATH)
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for the board consumer."""
    core = _core()
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 3

    log_level = logging.DEBUG if getattr(args, "verbose", False) else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    db_path = getattr(args, "db_path", core.DEFAULT_DB_PATH)
    config = core.ConsumerConfig(
        critical_paths_path=Path(args.file),
        automation_config_path=Path(args.automation_config),
        project_owner=args.project_owner,
        project_number=args.project_number,
        db_path=db_path,
        poll_interval_seconds=getattr(args, "interval", 180),
    )
    try:
        auto_config = core.load_automation_config(config.automation_config_path)
    except core.ConfigError:
        auto_config = None
    core._apply_automation_runtime(config, auto_config)

    if args.command == "status":
        return _cmd_status(
            config,
            as_json=getattr(args, "as_json", False),
            local_only=getattr(args, "local_only", False),
        )
    if args.command == "report-slo":
        return _cmd_report_slo(
            config,
            as_json=getattr(args, "as_json", False),
            local_only=getattr(args, "local_only", False),
        )
    if args.command == "serve-status":
        return _cmd_serve_status(
            config,
            host=getattr(args, "host", core.DEFAULT_STATUS_HOST),
            port=getattr(args, "port", core.DEFAULT_STATUS_PORT),
        )
    if args.command == "reconcile":
        return _cmd_reconcile(config, dry_run=getattr(args, "dry_run", False))
    if args.command == "drain":
        return _cmd_drain(config)
    if args.command == "resume":
        return _cmd_resume(config)

    db = core.open_consumer_db(config.db_path)

    if args.command == "one-shot":
        result = core.run_one_cycle(
            config,
            db,
            dry_run=args.dry_run,
            target_issue=getattr(args, "issue", None),
        )
        core.logger.info("Result: %s", result)
        db.close()
        if result.action == "idle":
            return 2
        if result.action == "error":
            return 4
        return 0

    if args.command == "run":
        try:
            core.run_daemon_loop(config, db, dry_run=args.dry_run)
        except KeyboardInterrupt:
            core.logger.info("Daemon stopped by user")
        finally:
            db.close()
        return 0

    return 3
