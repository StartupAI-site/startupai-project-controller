#!/usr/bin/env python3
"""Repo-owned consumer workflow contracts and runtime snapshots."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any

import yaml


DEFAULT_WORKFLOW_FILENAME = "WORKFLOW.md"
WORKFLOW_CONFIG_KEY = "startupai_consumer"
ALLOWED_WORKFLOW_KEYS = {
    "poll_interval_seconds",
    "codex_timeout_seconds",
    "max_retries",
    "retry_backoff_base_seconds",
    "retry_backoff_seconds",
    "validation_cmd",
    "workspace_hooks",
}
ALLOWED_WORKFLOW_HOOKS = {"after_create", "before_run"}
WORKFLOW_PLACEHOLDER_RE = re.compile(r"{{\s*([a-z0-9_]+)\s*}}", re.IGNORECASE)


class WorkflowConfigError(ValueError):
    """Raised when a repo workflow contract is missing or invalid."""


@dataclass(frozen=True)
class WorkflowRuntimeConfig:
    """Typed repo-local runtime overrides."""

    poll_interval_seconds: int | None = None
    codex_timeout_seconds: int | None = None
    max_retries: int | None = None
    retry_backoff_base_seconds: int | None = None
    retry_backoff_seconds: int | None = None
    validation_cmd: str | None = None
    workspace_hooks: dict[str, tuple[str, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class WorkflowDefinition:
    """Parsed repo-owned workflow contract."""

    repo_prefix: str
    source_path: Path
    source_kind: str
    loaded_at: str
    workflow_hash: str
    prompt_template: str
    runtime: WorkflowRuntimeConfig


@dataclass(frozen=True)
class WorkflowRepoStatus:
    """Operator-visible workflow availability for one repo."""

    repo_prefix: str
    source_path: str
    source_kind: str
    workflow_hash: str | None
    loaded_at: str | None
    available: bool
    disabled_reason: str | None = None


@dataclass(frozen=True)
class WorkflowStateSnapshot:
    """Persisted main-checkout workflow state for status surfaces."""

    generated_at: str
    effective_poll_interval_seconds: int
    repos: dict[str, WorkflowRepoStatus]


def default_repo_roots() -> dict[str, Path]:
    """Return canonical main-checkout roots for protected repos."""
    home = Path.home() / "projects"
    return {
        "crew": home / "startupai-crew",
        "app": home / "app.startupai.site",
        "site": home / "startupai.site",
    }


def workflow_path_for_root(root: Path, filename: str = DEFAULT_WORKFLOW_FILENAME) -> Path:
    """Resolve a workflow path from a repo root or worktree root."""
    return root / filename


def load_workflow_definition(
    path: Path,
    *,
    repo_prefix: str,
    source_kind: str,
) -> WorkflowDefinition:
    """Load and validate one repo workflow definition."""
    if not path.exists():
        raise WorkflowConfigError(f"missing:{path}")

    try:
        text = path.read_text(encoding="utf-8")
    except OSError as error:
        raise WorkflowConfigError(f"unreadable:{path}:{error}") from error

    match = re.match(r"\A---\n(.*?)\n---\n?(.*)\Z", text, re.DOTALL)
    if match is None:
        raise WorkflowConfigError(
            f"invalid-frontmatter:{path}:expected-leading-yaml-front-matter"
        )

    try:
        frontmatter = yaml.safe_load(match.group(1)) or {}
    except yaml.YAMLError as error:
        raise WorkflowConfigError(f"invalid-yaml:{path}:{error}") from error

    if not isinstance(frontmatter, dict):
        raise WorkflowConfigError(f"invalid-frontmatter:{path}:expected-object")

    unknown_top_level = sorted(key for key in frontmatter if key != WORKFLOW_CONFIG_KEY)
    if unknown_top_level:
        raise WorkflowConfigError(
            f"unknown-top-level-keys:{path}:{','.join(unknown_top_level)}"
        )

    raw_config = frontmatter.get(WORKFLOW_CONFIG_KEY, {})
    if raw_config is None:
        raw_config = {}
    if not isinstance(raw_config, dict):
        raise WorkflowConfigError(
            f"invalid-{WORKFLOW_CONFIG_KEY}:{path}:expected-object"
        )

    unknown_keys = sorted(key for key in raw_config if key not in ALLOWED_WORKFLOW_KEYS)
    if unknown_keys:
        raise WorkflowConfigError(
            f"unknown-keys:{path}:{','.join(unknown_keys)}"
        )

    def _parse_optional_int(key: str) -> int | None:
        value = raw_config.get(key)
        if value is None:
            return None
        try:
            parsed = int(value)
        except (TypeError, ValueError) as error:
            raise WorkflowConfigError(
                f"invalid-{key}:{path}:expected-positive-integer"
            ) from error
        if parsed < 1:
            raise WorkflowConfigError(
                f"invalid-{key}:{path}:expected-positive-integer"
            )
        return parsed

    validation_cmd = raw_config.get("validation_cmd")
    if validation_cmd is not None:
        validation_cmd = str(validation_cmd).strip()
        if not validation_cmd:
            raise WorkflowConfigError(
                f"invalid-validation_cmd:{path}:expected-non-empty-string"
            )

    workspace_hooks_raw = raw_config.get("workspace_hooks", {}) or {}
    if not isinstance(workspace_hooks_raw, dict):
        raise WorkflowConfigError(
            f"invalid-workspace_hooks:{path}:expected-object"
        )
    unknown_hooks = sorted(
        hook_name for hook_name in workspace_hooks_raw if hook_name not in ALLOWED_WORKFLOW_HOOKS
    )
    if unknown_hooks:
        raise WorkflowConfigError(
            f"unknown-workspace-hooks:{path}:{','.join(unknown_hooks)}"
        )
    workspace_hooks: dict[str, tuple[str, ...]] = {}
    for hook_name, commands in workspace_hooks_raw.items():
        if not isinstance(commands, list):
            raise WorkflowConfigError(
                f"invalid-workspace-hooks:{path}:{hook_name}:expected-list"
            )
        normalized = tuple(str(command).strip() for command in commands if str(command).strip())
        workspace_hooks[str(hook_name)] = normalized

    prompt_template = match.group(2).strip()
    if not prompt_template:
        raise WorkflowConfigError(f"missing-prompt-template:{path}")

    loaded_at = datetime.now(timezone.utc).isoformat()
    workflow_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
    runtime = WorkflowRuntimeConfig(
        poll_interval_seconds=_parse_optional_int("poll_interval_seconds"),
        codex_timeout_seconds=_parse_optional_int("codex_timeout_seconds"),
        max_retries=_parse_optional_int("max_retries"),
        retry_backoff_base_seconds=_parse_optional_int("retry_backoff_base_seconds"),
        retry_backoff_seconds=_parse_optional_int("retry_backoff_seconds"),
        validation_cmd=validation_cmd,
        workspace_hooks=workspace_hooks,
    )
    return WorkflowDefinition(
        repo_prefix=repo_prefix,
        source_path=path,
        source_kind=source_kind,
        loaded_at=loaded_at,
        workflow_hash=workflow_hash,
        prompt_template=prompt_template,
        runtime=runtime,
    )


def load_repo_workflows(
    repo_prefixes: tuple[str, ...],
    repo_roots: dict[str, Path],
    *,
    filename: str = DEFAULT_WORKFLOW_FILENAME,
) -> tuple[dict[str, WorkflowDefinition], dict[str, WorkflowRepoStatus]]:
    """Load all main-checkout workflows for the requested repo prefixes."""
    workflows: dict[str, WorkflowDefinition] = {}
    statuses: dict[str, WorkflowRepoStatus] = {}
    for repo_prefix in repo_prefixes:
        root = repo_roots.get(repo_prefix)
        if root is None:
            statuses[repo_prefix] = WorkflowRepoStatus(
                repo_prefix=repo_prefix,
                source_path="",
                source_kind="main",
                workflow_hash=None,
                loaded_at=None,
                available=False,
                disabled_reason=f"missing-repo-root:{repo_prefix}",
            )
            continue
        workflow_path = workflow_path_for_root(root, filename)
        try:
            workflow = load_workflow_definition(
                workflow_path,
                repo_prefix=repo_prefix,
                source_kind="main",
            )
        except WorkflowConfigError as error:
            statuses[repo_prefix] = WorkflowRepoStatus(
                repo_prefix=repo_prefix,
                source_path=str(workflow_path),
                source_kind="main",
                workflow_hash=None,
                loaded_at=None,
                available=False,
                disabled_reason=str(error),
            )
            continue

        workflows[repo_prefix] = workflow
        statuses[repo_prefix] = WorkflowRepoStatus(
            repo_prefix=repo_prefix,
            source_path=str(workflow.source_path),
            source_kind=workflow.source_kind,
            workflow_hash=workflow.workflow_hash,
            loaded_at=workflow.loaded_at,
            available=True,
        )
    return workflows, statuses


def effective_poll_interval(
    workflows: dict[str, WorkflowDefinition],
    *,
    default_seconds: int,
) -> int:
    """Return the effective daemon poll interval across enabled repos."""
    candidates = [
        workflow.runtime.poll_interval_seconds
        for workflow in workflows.values()
        if workflow.runtime.poll_interval_seconds is not None
    ]
    if not candidates:
        return default_seconds
    return min(candidates)


def render_workflow_prompt(
    workflow: WorkflowDefinition,
    context: dict[str, str],
) -> str:
    """Render a repo prompt template with strict placeholder validation."""

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in context:
            raise WorkflowConfigError(
                f"unknown-placeholder:{workflow.source_path}:{key}"
            )
        return context[key]

    return WORKFLOW_PLACEHOLDER_RE.sub(replace, workflow.prompt_template).strip()


def load_worktree_workflow(
    repo_prefix: str,
    worktree_root: Path,
    *,
    filename: str = DEFAULT_WORKFLOW_FILENAME,
) -> WorkflowDefinition:
    """Load a workflow snapshot from the claimed worktree."""
    return load_workflow_definition(
        workflow_path_for_root(worktree_root, filename),
        repo_prefix=repo_prefix,
        source_kind="worktree",
    )


def snapshot_from_statuses(
    statuses: dict[str, WorkflowRepoStatus],
    *,
    effective_poll_interval_seconds: int,
) -> WorkflowStateSnapshot:
    """Create a persisted status snapshot from repo workflow statuses."""
    return WorkflowStateSnapshot(
        generated_at=datetime.now(timezone.utc).isoformat(),
        effective_poll_interval_seconds=effective_poll_interval_seconds,
        repos=dict(sorted(statuses.items())),
    )


def write_workflow_snapshot(path: Path, snapshot: WorkflowStateSnapshot) -> None:
    """Persist main-checkout workflow state for status surfaces."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": snapshot.generated_at,
        "effective_poll_interval_seconds": snapshot.effective_poll_interval_seconds,
        "repos": {
            repo_prefix: {
                "repo_prefix": status.repo_prefix,
                "source_path": status.source_path,
                "source_kind": status.source_kind,
                "workflow_hash": status.workflow_hash,
                "loaded_at": status.loaded_at,
                "available": status.available,
                "disabled_reason": status.disabled_reason,
            }
            for repo_prefix, status in snapshot.repos.items()
        },
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def read_workflow_snapshot(path: Path) -> WorkflowStateSnapshot | None:
    """Read the last persisted workflow snapshot if available."""
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    repos = {
        repo_prefix: WorkflowRepoStatus(
            repo_prefix=repo_prefix,
            source_path=str(data.get("source_path", "")),
            source_kind=str(data.get("source_kind", "main")),
            workflow_hash=data.get("workflow_hash"),
            loaded_at=data.get("loaded_at"),
            available=bool(data.get("available")),
            disabled_reason=data.get("disabled_reason"),
        )
        for repo_prefix, data in (payload.get("repos") or {}).items()
        if isinstance(data, dict)
    }
    try:
        effective_poll = int(payload.get("effective_poll_interval_seconds", 0))
    except (TypeError, ValueError):
        effective_poll = 0
    return WorkflowStateSnapshot(
        generated_at=str(payload.get("generated_at", "")),
        effective_poll_interval_seconds=effective_poll,
        repos=repos,
    )


def workflow_status_payload(status: WorkflowRepoStatus) -> dict[str, Any]:
    """Convert a workflow repo status into JSON-ready data."""
    return {
        "available": status.available,
        "source_path": status.source_path,
        "source_kind": status.source_kind,
        "workflow_hash": status.workflow_hash,
        "loaded_at": status.loaded_at,
        "disabled_reason": status.disabled_reason,
    }
