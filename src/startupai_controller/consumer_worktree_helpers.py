"""Worktree helper cluster extracted from board_consumer."""

from __future__ import annotations

from pathlib import Path
import re
import subprocess
from collections.abc import Callable
from typing import cast

from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import WorktreePrepareError
from startupai_controller.domain.models import RepairBranchReconcileOutcome
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.ports.worktrees import WorktreePort
from startupai_controller.runtime.wiring import ConsumerDB
from startupai_controller.validate_critical_path_promotion import IssueRef

SubprocessRunnerFn = Callable[..., subprocess.CompletedProcess[str]]


def list_repo_worktrees(
    repo_root: Path | str,
    *,
    build_worktree_port: Callable[..., WorktreePort],
    worktree_port: WorktreePort | None = None,
    subprocess_runner: SubprocessRunnerFn | None = None,
) -> list[tuple[str, str]]:
    """Return (worktree_path, branch_name) pairs for a repo root."""
    port = worktree_port or build_worktree_port(subprocess_runner=subprocess_runner)
    return [
        (entry.path, entry.branch_name) for entry in port.list_worktrees(str(repo_root))
    ]


def worktree_is_clean(
    worktree_path: str,
    *,
    build_worktree_port: Callable[..., WorktreePort],
    worktree_port: WorktreePort | None = None,
    subprocess_runner: SubprocessRunnerFn | None = None,
) -> bool:
    """Return True when a worktree has no local changes."""
    port = worktree_port or build_worktree_port(subprocess_runner=subprocess_runner)
    return port.is_clean(worktree_path)


def worktree_ownership_is_safe(
    store: SessionStorePort,
    issue_ref: str,
    worktree_path: str,
) -> bool:
    """Return True when a clean worktree is safe to adopt for an issue."""
    for worker in store.active_workers():
        if worker.worktree_path == worktree_path and worker.issue_ref != issue_ref:
            return False
    latest = store.latest_session_for_worktree(worktree_path)
    if latest is None:
        return True
    return latest.issue_ref == issue_ref


def create_worktree(
    issue_ref: str,
    title: str,
    config: ConsumerConfig,
    *,
    build_worktree_port: Callable[..., WorktreePort],
    branch_name_override: str | None = None,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: SubprocessRunnerFn | None = None,
) -> tuple[str, str]:
    """Create a worktree for the issue."""
    port = worktree_port or build_worktree_port(subprocess_runner=subprocess_runner)
    entry = port.create_issue_worktree(
        issue_ref,
        title,
        branch_name_override=branch_name_override,
    )
    return entry.path, entry.branch_name


def fast_forward_existing_worktree(
    worktree_path: str,
    branch: str,
    *,
    build_worktree_port: Callable[..., WorktreePort],
    worktree_port: WorktreePort | None = None,
    subprocess_runner: SubprocessRunnerFn | None = None,
) -> None:
    """Fast-forward a clean reused worktree to the remote branch head when possible."""
    port = worktree_port or build_worktree_port(subprocess_runner=subprocess_runner)
    port.fast_forward_existing(worktree_path, branch)


def reconcile_repair_branch(
    worktree_path: str,
    branch: str,
    *,
    build_worktree_port: Callable[..., WorktreePort],
    worktree_port: WorktreePort | None = None,
    subprocess_runner: SubprocessRunnerFn | None = None,
) -> RepairBranchReconcileOutcome:
    """Reconcile a repair branch against its remote and origin/main."""
    port = worktree_port or build_worktree_port(subprocess_runner=subprocess_runner)
    return port.reconcile_repair_branch(worktree_path, branch)


def prepare_worktree(
    issue_ref: str,
    title: str,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    *,
    parse_issue_ref: Callable[[str], IssueRef],
    build_session_store: Callable[[ConsumerDB], SessionStorePort],
    build_worktree_port: Callable[..., WorktreePort],
    list_repo_worktrees: Callable[..., list[tuple[str, str]]],
    worktree_is_clean: Callable[..., bool],
    worktree_ownership_is_safe: Callable[[SessionStorePort, str, str], bool],
    create_worktree: Callable[..., tuple[str, str]],
    record_metric: Callable[..., None],
    error_cls: type[WorktreePrepareError],
    log_warning: Callable[[str, Exception], None],
    branch_name_override: str | None = None,
    session_store: SessionStorePort | None = None,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: SubprocessRunnerFn | None = None,
) -> tuple[str, str]:
    """Create or safely adopt a worktree for an issue."""
    parsed = parse_issue_ref(issue_ref)
    store = session_store or build_session_store(cast(ConsumerDB, db))
    port = worktree_port or build_worktree_port(subprocess_runner=subprocess_runner)
    if config.worktree_reuse_enabled:
        repo_root = config.repo_roots.get(parsed.prefix)
        if repo_root is None:
            raise error_cls(
                "unknown_repo_prefix",
                f"unknown repo prefix for worktree prep: {parsed.prefix}",
            )
        slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:40]
        target_branch = branch_name_override or f"feat/{parsed.number}-{slug}"
        try:
            worktree_records = list_repo_worktrees(
                repo_root,
                worktree_port=port,
            )
        except RuntimeError as err:
            log_warning(issue_ref, err)
            worktree_records = []
        for worktree_path, branch_name in worktree_records:
            if branch_name != target_branch:
                continue
            if not worktree_is_clean(worktree_path, worktree_port=port):
                raise error_cls(
                    "worktree_in_use",
                    f"existing worktree is dirty for {target_branch}: {worktree_path}",
                )
            if not worktree_ownership_is_safe(store, issue_ref, worktree_path):
                raise error_cls(
                    "worktree_in_use",
                    f"existing worktree ownership is ambiguous for {target_branch}: {worktree_path}",
                )
            port.fast_forward_existing(worktree_path, target_branch)
            record_metric(
                db,
                config,
                "worktree_reused",
                issue_ref=issue_ref,
                payload={"worktree_path": worktree_path, "branch_name": target_branch},
            )
            return worktree_path, target_branch

    return create_worktree(
        issue_ref,
        title,
        config,
        branch_name_override=branch_name_override,
        worktree_port=port,
        subprocess_runner=subprocess_runner,
    )
