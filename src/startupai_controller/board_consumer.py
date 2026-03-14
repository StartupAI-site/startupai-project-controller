#!/usr/bin/env python3
"""Thin shell entrypoint for the board consumer daemon."""

from __future__ import annotations

import logging
import sys

from startupai_controller.board_consumer_cli import (
    _cmd_drain,
    _cmd_reconcile,
    _cmd_report_slo,
    _cmd_resume,
    _cmd_serve_status,
    _cmd_status,
    _create_status_http_server,
    build_parser,
    main,
)
from startupai_controller.board_automation_config import load_automation_config
from startupai_controller.consumer_config import (
    ConsumerConfig,
    DEFAULT_AUTOMATION_CONFIG_PATH,
    DEFAULT_CONFIG_PATH,
    DEFAULT_DB_PATH,
    DEFAULT_DRAIN_PATH,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SCHEMA_PATH,
    DEFAULT_WORKFLOW_STATE_PATH,
)
from startupai_controller.consumer_drain_control import (
    clear_drain as _clear_drain,
    request_drain as _request_drain,
)
from startupai_controller.consumer_operational_wiring import (
    recover_interrupted_sessions as _recover_interrupted_sessions,
    reconcile_board_truth as _reconcile_board_truth,
)
from startupai_controller import consumer_runtime_wiring as _runtime_wiring
from startupai_controller.control_plane_runtime import _apply_automation_runtime
from startupai_controller.domain.models import CycleResult, ReviewQueueDrainSummary
from startupai_controller.application.consumer.preflight import ReconciliationResult
from startupai_controller.runtime.wiring import build_session_store, open_consumer_db
from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    load_config,
)

logger = logging.getLogger("board-consumer")
DEFAULT_STATUS_HOST = "127.0.0.1"
DEFAULT_STATUS_PORT = 8765

_collect_status_payload = _runtime_wiring.collect_status_payload
run_one_cycle = _runtime_wiring.run_one_cycle_live


def run_daemon_loop(
    config: ConsumerConfig,
    db,
    *,
    dry_run: bool = False,
    sleep_fn=None,
    **di_kwargs,
) -> None:
    """Preserve the public daemon-loop shell surface."""
    _runtime_wiring.run_daemon_loop(
        config,
        db,
        dry_run=dry_run,
        sleep_fn=sleep_fn,
        di_kwargs=di_kwargs,
    )


if __name__ == "__main__":
    sys.exit(main())
