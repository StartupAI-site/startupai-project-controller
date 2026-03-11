#!/usr/bin/env python3
"""Thin shell entrypoint for the board consumer daemon."""

from __future__ import annotations

import sys

from startupai_controller.board_consumer_compat import (
    ConfigError,
    ConsumerConfig,
    CycleResult,
    DEFAULT_AUTOMATION_CONFIG_PATH,
    DEFAULT_CONFIG_PATH,
    DEFAULT_DB_PATH,
    DEFAULT_DRAIN_PATH,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SCHEMA_PATH,
    DEFAULT_STATUS_HOST,
    DEFAULT_STATUS_PORT,
    DEFAULT_WORKFLOW_STATE_PATH,
    ReconciliationResult,
    ReviewQueueDrainSummary,
    _clear_drain,
    _collect_status_payload,
    _reconcile_board_truth,
    _request_drain,
    load_automation_config,
    load_config,
    logger,
    run_daemon_loop,
    run_one_cycle,
)
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
from startupai_controller.control_plane_runtime import _apply_automation_runtime
from startupai_controller.runtime.wiring import build_session_store, open_consumer_db


if __name__ == "__main__":
    sys.exit(main())
