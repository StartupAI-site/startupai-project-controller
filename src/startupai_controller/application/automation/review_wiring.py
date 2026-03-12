"""Review-family port wiring — assemble ports and delegate to use-case modules.

This module contains the port-resolution and closure-building logic that was
previously inline in board_automation.py for the review family of subcommands:
sync-review-state, codex-review-gate, automerge-review, review-rescue,
review-rescue-all.

Each public function here has the same signature as the corresponding
board_automation.py wrapper, but receives port-factory callables as injected
parameters instead of importing concrete factories directly.
"""

from __future__ import annotations

from typing import Callable

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.domain.models import (
    ReviewRescueResult,
    ReviewRescueSweep,
    ReviewSnapshot,
)
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig

from startupai_controller.application.automation.codex_gate import (
    codex_review_gate as _app_codex_review_gate,
)
from startupai_controller.application.automation.review_rescue import (
    automerge_review as _app_automerge_review,
    review_rescue as _app_review_rescue,
    review_rescue_all as _app_review_rescue_all,
)
from startupai_controller.application.automation.review_sync import (
    sync_review_state as _app_sync_review_state,
)
from startupai_controller.automation_compat_ports import (
    wrap_status_transition_board_port,
    wrap_status_transition_review_state_port,
)


# ---------------------------------------------------------------------------
# Shared review-family helpers
# ---------------------------------------------------------------------------


def has_copilot_review_signal(
    pr_repo: str,
    pr_number: int,
    *,
    pr_port=None,
    config: CriticalPathConfig | None = None,
    project_owner: str = "",
    project_number: int = 0,
    gh_runner: Callable[..., str] | None = None,
    default_pr_port_fn: Callable[..., object] | None = None,
) -> bool:
    """Return True when Copilot has submitted approved/commented review."""
    if pr_port is None and default_pr_port_fn is not None:
        pr_port = default_pr_port_fn(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    if pr_port is None:
        return False
    return pr_port.has_copilot_review_signal(pr_repo, pr_number)


def review_scope_refs(
    pr_repo: str,
    pr_number: int,
    *,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
) -> list[str]:
    """Return linked issue refs currently in Review for a PR."""
    return [
        issue_ref
        for issue_ref in pr_port.linked_issue_refs(pr_repo, pr_number)
        if review_state_port.get_issue_status(issue_ref) == "Review"
    ]


def configured_review_checks(
    pr_repo: str,
    automation_config: BoardAutomationConfig,
) -> tuple[str, ...]:
    """Return repo-specific review checks that should be reconciled."""
    from startupai_controller.domain.rescue_policy import (
        configured_review_checks as _domain_configured_review_checks,
    )
    return _domain_configured_review_checks(
        pr_repo, automation_config.required_checks_by_repo
    )


def build_review_snapshot(
    pr_repo: str,
    pr_number: int,
    automation_config: BoardAutomationConfig,
    *,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
) -> ReviewSnapshot:
    """Project PR review state into one explicit snapshot."""
    _review_refs = tuple(
        review_scope_refs(
            pr_repo,
            pr_number,
            pr_port=pr_port,
            review_state_port=review_state_port,
        )
    )
    return pr_port.review_snapshots(
        {(pr_repo, pr_number): _review_refs},
        trusted_codex_actors=frozenset(automation_config.trusted_codex_actors),
    )[(pr_repo, pr_number)]


# ---------------------------------------------------------------------------
# Subcommand wiring
# ---------------------------------------------------------------------------


def sync_review_state(
    event_kind: str,
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    automation_config: BoardAutomationConfig | None = None,
    pr_state: str | None = None,
    review_state: str | None = None,
    checks_state: str | None = None,
    failed_checks: list[str] | None = None,
    dry_run: bool = False,
    github_bundle=None,
    pr_port=None,
    review_state_port=None,
    board_port=None,
    board_info_resolver: Callable[..., object] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
    # Injected port factories
    default_pr_port_fn: Callable[..., object] | None = None,
    default_review_state_port_fn: Callable[..., object] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
    legacy_board_status_mutator_fn: Callable[..., Callable] | None = None,
) -> tuple[int, str]:
    """Assemble ports and delegate to the sync_review_state use case."""
    pr_port, review_state_port, board_port = _resolve_sync_review_state_ports(
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        github_bundle=github_bundle,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        default_pr_port_fn=default_pr_port_fn,
        default_review_state_port_fn=default_review_state_port_fn,
        default_board_mutation_port_fn=default_board_mutation_port_fn,
        legacy_board_status_mutator_fn=legacy_board_status_mutator_fn,
    )

    return _app_sync_review_state(
        event_kind=event_kind,
        issue_ref=issue_ref,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        automation_config=automation_config,
        pr_state=pr_state,
        review_state=review_state,
        checks_state=checks_state,
        failed_checks=failed_checks,
        dry_run=dry_run,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
    )


def _resolve_sync_review_state_ports(
    *,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    github_bundle,
    pr_port,
    review_state_port,
    board_port,
    board_info_resolver: Callable[..., object] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    default_pr_port_fn: Callable[..., object] | None,
    default_review_state_port_fn: Callable[..., object] | None,
    default_board_mutation_port_fn: Callable[..., object] | None,
    legacy_board_status_mutator_fn: Callable[..., Callable] | None,
):
    """Acquire and adapt the ports needed by sync-review-state."""
    if github_bundle is not None:
        pr_port = pr_port or github_bundle.pull_requests
        review_state_port = review_state_port or github_bundle.review_state
        board_port = board_port or github_bundle.board_mutations
    if pr_port is None and default_pr_port_fn is not None:
        pr_port = default_pr_port_fn(
            project_owner, project_number, config, gh_runner=gh_runner
        )
    if review_state_port is None and default_review_state_port_fn is not None:
        review_state_port = default_review_state_port_fn(
            project_owner, project_number, config, gh_runner=gh_runner
        )
    if board_port is None and default_board_mutation_port_fn is not None:
        board_port = default_board_mutation_port_fn(
            project_owner, project_number, config, gh_runner=gh_runner
        )
    if (
        board_mutator is None
        and board_info_resolver is not None
        and legacy_board_status_mutator_fn is not None
    ):
        board_mutator = legacy_board_status_mutator_fn(
            project_owner, project_number, config, gh_runner=gh_runner
        )
    board_port = wrap_status_transition_board_port(
        board_port,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
    )
    review_state_port = wrap_status_transition_review_state_port(
        review_state_port,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        board_info_resolver=board_info_resolver,
    )
    return pr_port, review_state_port, board_port


def codex_review_gate(
    pr_repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    apply_fail_routing: bool = True,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    apply_codex_fail_routing_fn: Callable[..., None] | None = None,
) -> tuple[int, str]:
    """Assemble review data and delegate to the codex_review_gate use case."""
    snapshot = build_review_snapshot(
        pr_repo,
        pr_number,
        automation_config,
        pr_port=pr_port,
        review_state_port=review_state_port,
    )

    def _fail_router(**kwargs) -> None:
        if apply_codex_fail_routing_fn is not None:
            apply_codex_fail_routing_fn(
                issue_ref=kwargs["issue_ref"],
                route=kwargs["route"],
                checklist=kwargs["checklist"],
                config=config,
                project_owner=project_owner,
                project_number=project_number,
                dry_run=kwargs["dry_run"],
            )

    return _app_codex_review_gate(
        pr_repo=pr_repo,
        pr_number=pr_number,
        linked_refs=list(pr_port.linked_issue_refs(pr_repo, pr_number)),
        review_refs=list(snapshot.review_refs),
        verdict=snapshot.codex_verdict,
        dry_run=dry_run,
        apply_fail_routing=apply_fail_routing,
        fail_router=_fail_router,
    )


def review_rescue(
    pr_repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    snapshot: ReviewSnapshot | None = None,
    gh_runner: Callable[..., str] | None = None,
    pr_port=None,
    review_state_port=None,
    board_port=None,
    # Injected port factories and helpers
    default_pr_port_fn: Callable[..., object] | None = None,
    default_review_state_port_fn: Callable[..., object] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
    query_closing_issues_fn: Callable[..., list] | None = None,
    query_issue_board_info_fn: Callable[..., object] | None = None,
    automerge_review_fn: Callable[..., tuple[int, str]] | None = None,
) -> ReviewRescueResult:
    """Assemble ports and delegate to the review_rescue use case."""
    if pr_port is None and default_pr_port_fn is not None:
        pr_port = default_pr_port_fn(
            project_owner, project_number, config, gh_runner
        )
    if review_state_port is None and default_review_state_port_fn is not None:
        review_state_port = default_review_state_port_fn(
            project_owner, project_number, config=config, gh_runner=gh_runner
        )
    if board_port is None and default_board_mutation_port_fn is not None:
        board_port = default_board_mutation_port_fn(
            project_owner, project_number, config, gh_runner
        )
    snapshot = snapshot or build_review_snapshot(
        pr_repo=pr_repo,
        pr_number=pr_number,
        automation_config=automation_config,
        pr_port=pr_port,
        review_state_port=review_state_port,
    )

    def _automerge_runner(**kwargs) -> tuple[int, str]:
        if automerge_review_fn is not None:
            return automerge_review_fn(
                pr_repo=kwargs["pr_repo"],
                pr_number=kwargs["pr_number"],
                config=config,
                automation_config=automation_config,
                project_owner=project_owner,
                project_number=project_number,
                dry_run=kwargs["dry_run"],
                snapshot=kwargs["snapshot"],
                gh_runner=gh_runner,
                pr_port=kwargs["pr_port"],
                review_state_port=kwargs["review_state_port"],
            )
        return 4, "automerge_review_fn not provided"

    return _app_review_rescue(
        pr_repo=pr_repo,
        pr_number=pr_number,
        snapshot=snapshot,
        automation_config=automation_config,
        dry_run=dry_run,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        automerge_runner=_automerge_runner,
    )


def review_rescue_all(
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
    pr_port=None,
    review_state_port=None,
    board_port=None,
    # Injected port factories and helpers
    default_pr_port_fn: Callable[..., object] | None = None,
    default_review_state_port_fn: Callable[..., object] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
    review_rescue_fn: Callable[..., ReviewRescueResult] | None = None,
) -> ReviewRescueSweep:
    """Assemble ports and delegate to the review_rescue_all use case."""
    if pr_port is None and default_pr_port_fn is not None:
        pr_port = default_pr_port_fn(
            project_owner, project_number, config, gh_runner
        )
    if review_state_port is None and default_review_state_port_fn is not None:
        review_state_port = default_review_state_port_fn(
            project_owner, project_number, config, gh_runner
        )
    if board_port is None and default_board_mutation_port_fn is not None:
        board_port = default_board_mutation_port_fn(
            project_owner, project_number, config, gh_runner
        )

    def _review_rescue_runner(**kwargs) -> ReviewRescueResult:
        if review_rescue_fn is not None:
            return review_rescue_fn(
                pr_repo=kwargs["pr_repo"],
                pr_number=kwargs["pr_number"],
                config=config,
                automation_config=automation_config,
                project_owner=project_owner,
                project_number=project_number,
                dry_run=kwargs["dry_run"],
                gh_runner=gh_runner,
                pr_port=kwargs["pr_port"],
                review_state_port=kwargs["review_state_port"],
                board_port=kwargs["board_port"],
            )
        return ReviewRescueResult(
            pr_repo=kwargs["pr_repo"],
            pr_number=kwargs["pr_number"],
            blocked_reason="review_rescue_fn not provided",
        )

    return _app_review_rescue_all(
        config=config,
        automation_config=automation_config,
        dry_run=dry_run,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        review_rescue_runner=_review_rescue_runner,
    )


def automerge_review(
    pr_repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    update_branch: bool = True,
    delete_branch: bool = True,
    snapshot: ReviewSnapshot | None = None,
    gh_runner: Callable[..., str] | None = None,
    pr_port=None,
    review_state_port=None,
    # Injected port factories and helpers
    default_pr_port_fn: Callable[..., object] | None = None,
    default_review_state_port_fn: Callable[..., object] | None = None,
    codex_review_gate_fn: Callable[..., tuple[int, str]] | None = None,
) -> tuple[int, str]:
    """Assemble ports and delegate to the automerge_review use case."""
    if pr_port is None and default_pr_port_fn is not None:
        pr_port = default_pr_port_fn(
            project_owner, project_number, config, gh_runner
        )
    if review_state_port is None and default_review_state_port_fn is not None:
        review_state_port = default_review_state_port_fn(
            project_owner, project_number, config, gh_runner
        )

    def _codex_gate_evaluator(**kwargs) -> tuple[int, str]:
        if codex_review_gate_fn is not None:
            return codex_review_gate_fn(
                pr_repo=kwargs["pr_repo"],
                pr_number=kwargs["pr_number"],
                config=config,
                automation_config=automation_config,
                project_owner=project_owner,
                project_number=project_number,
                dry_run=kwargs["dry_run"],
                apply_fail_routing=True,
                pr_port=pr_port,
                review_state_port=review_state_port,
            )
        return 4, "codex_review_gate_fn not provided"

    return _app_automerge_review(
        pr_repo=pr_repo,
        pr_number=pr_number,
        automation_config=automation_config,
        dry_run=dry_run,
        update_branch=update_branch,
        delete_branch=delete_branch,
        snapshot=snapshot,
        pr_port=pr_port,
        review_state_port=review_state_port,
        codex_gate_evaluator=_codex_gate_evaluator,
    )
