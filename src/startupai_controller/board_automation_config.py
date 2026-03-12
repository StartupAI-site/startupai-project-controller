"""Board automation policy/config models and loading helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path

from startupai_controller.domain.scheduling_policy import (
    VALID_DISPATCH_TARGETS,
    VALID_EXECUTION_AUTHORITY_MODES,
)
from startupai_controller.validate_critical_path_promotion import ConfigError

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_CONFIG_PATH = str(_REPO_ROOT / "config" / "critical-paths.json")
DEFAULT_AUTOMATION_CONFIG_PATH = str(
    _REPO_ROOT / "config" / "board-automation-config.json"
)
DEFAULT_PROJECT_OWNER = "StartupAI-site"
DEFAULT_PROJECT_NUMBER = 1
DEFAULT_MISSING_EXECUTOR_BLOCK_CAP = 5
DEFAULT_REBALANCE_CYCLE_MINUTES = 30
VALID_NON_LOCAL_PR_POLICIES = {"block", "requeue", "close"}


@dataclass(frozen=True)
class AdmissionConfig:
    """Runtime policy for autonomous Backlog -> Ready admission."""

    enabled: bool = False
    source_statuses: tuple[str, ...] = ("Backlog",)
    ready_floor_multiplier: int = 2
    ready_cap_multiplier: int = 3
    assignment_owner: str = "codex:local-consumer"
    max_batch_size: int = 6
    acceptance_headings: tuple[str, ...] = (
        "Acceptance Criteria",
        "Definition of Done",
    )


@dataclass(frozen=True)
class BoardAutomationConfig:
    """Runtime policy config for board dispatch/rebalance workflows."""

    wip_limits: dict[str, dict[str, int]]
    freshness_hours: int
    stale_confirmation_cycles: int
    trusted_codex_actors: set[str]
    trusted_local_authors: set[str] = field(default_factory=set)
    dispatch_target: str = "executor"
    canary_thresholds: dict[str, float] = field(default_factory=dict)
    execution_authority_mode: str = "board"
    execution_authority_repos: tuple[str, ...] = ("crew",)
    execution_authority_executors: tuple[str, ...] = ("codex",)
    global_concurrency: int = 1
    deprecated_workflow_mutations: dict[str, bool] = field(default_factory=dict)
    required_checks_by_repo: dict[str, tuple[str, ...]] = field(default_factory=dict)
    non_local_pr_policy: str = "requeue"
    deferred_replay_enabled: bool = True
    multi_worker_enabled: bool = False
    admission: AdmissionConfig = field(default_factory=AdmissionConfig)
    issue_context_cache_enabled: bool = True
    issue_context_cache_ttl_seconds: int = 900
    launch_hydration_concurrency: int = 1
    rate_limit_pause_enabled: bool = True
    rate_limit_cooldown_seconds: int = 300
    worktree_reuse_enabled: bool = True
    slo_metrics_enabled: bool = True


def load_automation_config(path: Path) -> BoardAutomationConfig:
    """Load board automation policy config from JSON."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ConfigError(f"Cannot read automation config '{path}': {error}") from error
    except json.JSONDecodeError as error:
        raise ConfigError(
            f"Invalid JSON in automation config '{path}': {error}"
        ) from error

    version = payload.get("version")
    if version not in {1, 2}:
        raise ConfigError(f"Unsupported automation config version '{version}'.")

    wip_limits_raw = payload.get("wip_limits")
    if not isinstance(wip_limits_raw, dict):
        raise ConfigError("automation config wip_limits must be an object")
    wip_limits: dict[str, dict[str, int]] = {}
    for executor, lanes in wip_limits_raw.items():
        if not isinstance(lanes, dict):
            raise ConfigError(
                f"automation config wip_limits.{executor} must be an object"
            )
        executor_key = str(executor).strip().lower()
        if not executor_key:
            continue
        normalized_lanes: dict[str, int] = {}
        for lane, value in lanes.items():
            lane_key = str(lane).strip().lower()
            if not lane_key:
                continue
            try:
                limit = int(value)
            except (TypeError, ValueError) as error:
                raise ConfigError(
                    f"automation config wip_limits.{executor_key}.{lane_key} "
                    "must be an integer"
                ) from error
            if limit < 1:
                raise ConfigError(
                    f"automation config wip_limits.{executor_key}.{lane_key} "
                    "must be >= 1"
                )
            normalized_lanes[lane_key] = limit
        wip_limits[executor_key] = normalized_lanes

    try:
        freshness_hours = int(payload.get("freshness_hours", 24))
    except (TypeError, ValueError) as error:
        raise ConfigError(
            "automation config freshness_hours must be an integer"
        ) from error
    if freshness_hours < 1:
        raise ConfigError("automation config freshness_hours must be >= 1")

    try:
        stale_confirmation_cycles = int(payload.get("stale_confirmation_cycles", 2))
    except (TypeError, ValueError) as error:
        raise ConfigError(
            "automation config stale_confirmation_cycles must be an integer"
        ) from error
    if stale_confirmation_cycles < 1:
        raise ConfigError("automation config stale_confirmation_cycles must be >= 1")

    trusted_raw = payload.get("trusted_codex_actors", [])
    if not isinstance(trusted_raw, list):
        raise ConfigError("automation config trusted_codex_actors must be a list")
    trusted_codex_actors = {
        str(actor).strip().lower() for actor in trusted_raw if str(actor).strip()
    }
    trusted_local_authors_raw = payload.get("trusted_local_authors", trusted_raw)
    if not isinstance(trusted_local_authors_raw, list):
        raise ConfigError("automation config trusted_local_authors must be a list")
    trusted_local_authors = {
        str(actor).strip().lower()
        for actor in trusted_local_authors_raw
        if str(actor).strip()
    }

    dispatch_raw = payload.get("dispatch") or {}
    if not isinstance(dispatch_raw, dict):
        raise ConfigError("automation config dispatch must be an object")
    dispatch_target = str(dispatch_raw.get("target", "executor")).strip().lower()
    if dispatch_target not in VALID_DISPATCH_TARGETS:
        raise ConfigError(
            "automation config dispatch.target must be one of: "
            f"{sorted(VALID_DISPATCH_TARGETS)}"
        )

    canary_raw = payload.get("canary_thresholds") or {}
    if not isinstance(canary_raw, dict):
        raise ConfigError("automation config canary_thresholds must be an object")
    canary_thresholds: dict[str, float] = {}
    for key, value in canary_raw.items():
        try:
            canary_thresholds[str(key)] = float(value)
        except (TypeError, ValueError):
            continue

    authority_raw = payload.get("execution_authority") or {}
    if authority_raw and not isinstance(authority_raw, dict):
        raise ConfigError("automation config execution_authority must be an object")
    authority_mode = (
        str(authority_raw.get("mode", "board" if version == 1 else "single_machine"))
        .strip()
        .lower()
    )
    if authority_mode not in VALID_EXECUTION_AUTHORITY_MODES:
        raise ConfigError(
            "automation config execution_authority.mode must be one of: "
            f"{sorted(VALID_EXECUTION_AUTHORITY_MODES)}"
        )
    authority_repos_raw = authority_raw.get("repos", ["crew"])
    if not isinstance(authority_repos_raw, list) or not authority_repos_raw:
        raise ConfigError(
            "automation config execution_authority.repos must be a non-empty list"
        )
    authority_repos = tuple(
        sorted(
            {
                str(repo).strip().lower()
                for repo in authority_repos_raw
                if str(repo).strip()
            }
        )
    )
    if not authority_repos:
        raise ConfigError(
            "automation config execution_authority.repos must contain values"
        )
    authority_executors_raw = authority_raw.get("executors", ["codex"])
    if not isinstance(authority_executors_raw, list) or not authority_executors_raw:
        raise ConfigError(
            "automation config execution_authority.executors must be a non-empty list"
        )
    authority_executors = tuple(
        sorted(
            {
                str(executor).strip().lower()
                for executor in authority_executors_raw
                if str(executor).strip()
            }
        )
    )
    if not authority_executors:
        raise ConfigError(
            "automation config execution_authority.executors must contain values"
        )
    try:
        global_concurrency = int(
            authority_raw.get("global_concurrency", 1 if version == 1 else 2)
        )
    except (TypeError, ValueError) as error:
        raise ConfigError(
            "automation config execution_authority.global_concurrency must be an integer"
        ) from error
    if global_concurrency < 1:
        raise ConfigError(
            "automation config execution_authority.global_concurrency must be >= 1"
        )
    try:
        issue_context_cache_ttl_seconds = int(
            authority_raw.get("issue_context_cache_ttl_seconds", 900)
        )
        launch_hydration_concurrency = int(
            authority_raw.get("launch_hydration_concurrency", 1)
        )
        rate_limit_cooldown_seconds = int(
            authority_raw.get("rate_limit_cooldown_seconds", 300)
        )
    except (TypeError, ValueError) as error:
        raise ConfigError(
            "automation config execution_authority cache/hydration/cooldown "
            "controls must be integers"
        ) from error
    if issue_context_cache_ttl_seconds < 1:
        raise ConfigError(
            "automation config execution_authority.issue_context_cache_ttl_seconds "
            "must be >= 1"
        )
    if launch_hydration_concurrency < 1:
        raise ConfigError(
            "automation config execution_authority.launch_hydration_concurrency "
            "must be >= 1"
        )
    if rate_limit_cooldown_seconds < 1:
        raise ConfigError(
            "automation config execution_authority.rate_limit_cooldown_seconds "
            "must be >= 1"
        )
    issue_context_cache_enabled = bool(
        authority_raw.get("issue_context_cache_enabled", True)
    )
    rate_limit_pause_enabled = bool(authority_raw.get("rate_limit_pause_enabled", True))
    worktree_reuse_enabled = bool(authority_raw.get("worktree_reuse_enabled", True))
    slo_metrics_enabled = bool(authority_raw.get("slo_metrics_enabled", True))

    deprecated_raw = payload.get("deprecated_workflow_mutations") or {}
    if deprecated_raw and not isinstance(deprecated_raw, dict):
        raise ConfigError(
            "automation config deprecated_workflow_mutations must be an object"
        )
    deprecated_workflow_mutations = {
        str(name).strip(): bool(enabled)
        for name, enabled in deprecated_raw.items()
        if str(name).strip()
    }

    checks_raw = payload.get("required_checks_by_repo") or {}
    if checks_raw and not isinstance(checks_raw, dict):
        raise ConfigError("automation config required_checks_by_repo must be an object")
    required_checks_by_repo: dict[str, tuple[str, ...]] = {}
    for repo_name, checks in checks_raw.items():
        if not isinstance(checks, list):
            raise ConfigError(
                "automation config required_checks_by_repo entries must be lists"
            )
        normalized_checks = tuple(
            check for check in [str(item).strip() for item in checks] if check
        )
        required_checks_by_repo[str(repo_name).strip().lower()] = normalized_checks

    non_local_pr_policy = (
        str(payload.get("non_local_pr_policy", "block" if version == 2 else "requeue"))
        .strip()
        .lower()
    )
    if non_local_pr_policy not in VALID_NON_LOCAL_PR_POLICIES:
        raise ConfigError(
            "automation config non_local_pr_policy must be one of: "
            f"{sorted(VALID_NON_LOCAL_PR_POLICIES)}"
        )

    feature_flags_raw = payload.get("feature_flags") or {}
    if feature_flags_raw and not isinstance(feature_flags_raw, dict):
        raise ConfigError("automation config feature_flags must be an object")
    deferred_replay_enabled = bool(feature_flags_raw.get("deferred_replay", True))
    multi_worker_enabled = bool(feature_flags_raw.get("multi_worker", False))

    admission_raw = payload.get("admission") or {}
    if admission_raw and not isinstance(admission_raw, dict):
        raise ConfigError("automation config admission must be an object")
    try:
        floor_multiplier = int(admission_raw.get("ready_floor_multiplier", 2))
        cap_multiplier = int(admission_raw.get("ready_cap_multiplier", 3))
        max_batch_size = int(admission_raw.get("max_batch_size", 6))
    except (TypeError, ValueError) as error:
        raise ConfigError(
            "automation config admission multipliers/batch size must be integers"
        ) from error
    if floor_multiplier < 1 or cap_multiplier < 1 or max_batch_size < 1:
        raise ConfigError(
            "automation config admission multipliers and batch size must be >= 1"
        )
    source_statuses_raw = admission_raw.get("source_statuses", ["Backlog"])
    if not isinstance(source_statuses_raw, list) or not source_statuses_raw:
        raise ConfigError(
            "automation config admission.source_statuses must be a non-empty list"
        )
    source_statuses = tuple(
        str(status).strip() for status in source_statuses_raw if str(status).strip()
    )
    if not source_statuses:
        raise ConfigError(
            "automation config admission.source_statuses must contain values"
        )
    acceptance_headings_raw = admission_raw.get(
        "acceptance_headings",
        ["Acceptance Criteria", "Definition of Done"],
    )
    if not isinstance(acceptance_headings_raw, list) or not acceptance_headings_raw:
        raise ConfigError(
            "automation config admission.acceptance_headings must be a non-empty list"
        )
    acceptance_headings = tuple(
        str(heading).strip()
        for heading in acceptance_headings_raw
        if str(heading).strip()
    )
    if not acceptance_headings:
        raise ConfigError(
            "automation config admission.acceptance_headings must contain values"
        )
    assignment_owner = str(
        admission_raw.get("assignment_owner", "codex:local-consumer")
    ).strip()
    if not assignment_owner:
        raise ConfigError(
            "automation config admission.assignment_owner must be non-empty"
        )
    admission_enabled = bool(admission_raw.get("enabled", False))
    admission = AdmissionConfig(
        enabled=admission_enabled,
        source_statuses=source_statuses,
        ready_floor_multiplier=floor_multiplier,
        ready_cap_multiplier=cap_multiplier,
        assignment_owner=assignment_owner,
        max_batch_size=max_batch_size,
        acceptance_headings=acceptance_headings,
    )

    if authority_mode == "single_machine" and multi_worker_enabled:
        protected_repos = tuple(
            repo for repo in authority_repos if repo in {"app", "crew", "site"}
        )
        if "codex" in authority_executors:
            codex_limits = wip_limits.get("codex")
            if not isinstance(codex_limits, dict):
                raise ConfigError(
                    "automation config wip_limits.codex is required in "
                    "single-machine multi-worker mode"
                )
            for repo in protected_repos:
                lane_limit = codex_limits.get(repo)
                if lane_limit is None:
                    raise ConfigError(
                        f"automation config wip_limits.codex.{repo} is required "
                        "in single-machine multi-worker mode"
                    )
                if lane_limit < global_concurrency:
                    raise ConfigError(
                        f"automation config wip_limits.codex.{repo} "
                        f"wip limit {lane_limit} is below global concurrency "
                        f"{global_concurrency}"
                    )

    return BoardAutomationConfig(
        wip_limits=wip_limits,
        freshness_hours=freshness_hours,
        stale_confirmation_cycles=stale_confirmation_cycles,
        trusted_codex_actors=trusted_codex_actors,
        trusted_local_authors=trusted_local_authors,
        dispatch_target=dispatch_target,
        canary_thresholds=canary_thresholds,
        execution_authority_mode=authority_mode,
        execution_authority_repos=authority_repos,
        execution_authority_executors=authority_executors,
        global_concurrency=global_concurrency,
        deprecated_workflow_mutations=deprecated_workflow_mutations,
        required_checks_by_repo=required_checks_by_repo,
        non_local_pr_policy=non_local_pr_policy,
        deferred_replay_enabled=deferred_replay_enabled,
        multi_worker_enabled=multi_worker_enabled,
        admission=admission,
        issue_context_cache_enabled=issue_context_cache_enabled,
        issue_context_cache_ttl_seconds=issue_context_cache_ttl_seconds,
        launch_hydration_concurrency=launch_hydration_concurrency,
        rate_limit_pause_enabled=rate_limit_pause_enabled,
        rate_limit_cooldown_seconds=rate_limit_cooldown_seconds,
        worktree_reuse_enabled=worktree_reuse_enabled,
        slo_metrics_enabled=slo_metrics_enabled,
    )
