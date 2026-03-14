"""Shared consumer runtime configuration and default controller paths."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from startupai_controller.consumer_workflow import (
    DEFAULT_WORKFLOW_FILENAME,
    default_repo_roots,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_CONFIG_PATH = str(_REPO_ROOT / "config" / "critical-paths.json")
DEFAULT_AUTOMATION_CONFIG_PATH = str(
    _REPO_ROOT / "config" / "board-automation-config.json"
)
DEFAULT_DB_PATH = Path.home() / ".local" / "share" / "startupai" / "consumer.db"
DEFAULT_OUTPUT_DIR = Path.home() / ".local" / "share" / "startupai" / "outputs"
DEFAULT_DRAIN_PATH = Path.home() / ".local" / "share" / "startupai" / "consumer.drain"
DEFAULT_WORKFLOW_STATE_PATH = (
    Path.home() / ".local" / "share" / "startupai" / "workflow-state.json"
)
DEFAULT_SHUTDOWN_STATE_PATH = (
    Path.home() / ".local" / "share" / "startupai" / "shutdown-state.json"
)
DEFAULT_SCHEMA_PATH = _REPO_ROOT / "config" / "codex_session_result.schema.json"


@dataclass
class ConsumerConfig:
    """Runtime configuration for the board consumer daemon."""

    critical_paths_path: Path
    automation_config_path: Path
    project_owner: str = "StartupAI-site"
    project_number: int = 1
    db_path: Path = field(default_factory=lambda: DEFAULT_DB_PATH)
    schema_path: Path = field(default_factory=lambda: DEFAULT_SCHEMA_PATH)
    output_dir: Path = field(default_factory=lambda: DEFAULT_OUTPUT_DIR)
    drain_path: Path = field(default_factory=lambda: DEFAULT_DRAIN_PATH)
    workflow_state_path: Path = field(
        default_factory=lambda: DEFAULT_WORKFLOW_STATE_PATH
    )
    shutdown_state_path: Path = field(
        default_factory=lambda: DEFAULT_SHUTDOWN_STATE_PATH
    )
    poll_interval_seconds: int = 180
    codex_timeout_seconds: int = 1800
    heartbeat_expiry_seconds: int = 3600
    max_retries: int = 3
    retry_backoff_base_seconds: int = 30
    retry_backoff_seconds: int = 300
    repo_prefixes: tuple[str, ...] = ("crew",)
    executor: str = "codex"
    global_concurrency: int = 1
    deferred_replay_enabled: bool = True
    multi_worker_enabled: bool = False
    validation_cmd: str = "uv run pytest tests/ -v --tb=short"
    workflow_filename: str = DEFAULT_WORKFLOW_FILENAME
    repo_roots: dict[str, Path] = field(default_factory=default_repo_roots)
    issue_context_cache_enabled: bool = True
    issue_context_cache_ttl_seconds: int = 900
    launch_hydration_concurrency: int = 1
    rate_limit_pause_enabled: bool = True
    rate_limit_cooldown_seconds: int = 300
    worktree_reuse_enabled: bool = True
    slo_metrics_enabled: bool = True
