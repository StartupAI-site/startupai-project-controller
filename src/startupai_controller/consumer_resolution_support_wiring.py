"""Resolution-verification support wiring extracted from consumer_support_wiring."""

from __future__ import annotations

from typing import Any, Callable, cast

from startupai_controller import consumer_resolution_helpers as _resolution_helpers
from startupai_controller.domain.models import ResolutionEvaluation
from startupai_controller.domain.repair_policy import parse_pr_url as _parse_pr_url
from startupai_controller.domain.resolution_policy import (
    NON_AUTO_CLOSE_RESOLUTION_KINDS,
    normalize_resolution_payload,
    resolution_allows_autoclose,
    resolution_has_meaningful_signal,
)
from startupai_controller.runtime.wiring import _run_gh
from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    parse_issue_ref,
)


def verify_resolution_payload(
    issue_ref: str,
    resolution: dict[str, Any] | None,
    *,
    config: Any,
    workflows: dict[str, Any],
    pr_port: Any | None = None,
    subprocess_runner: Callable[..., Any] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> Any:
    """Verify a structured resolution payload against canonical main."""
    return _resolution_helpers.verify_resolution_payload(
        issue_ref,
        resolution,
        config=config,
        workflows=workflows,
        pr_port=pr_port,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
        build_resolution_evaluation=ResolutionEvaluation,
        normalize_resolution_payload=cast(
            Callable[[dict[str, Any] | None], dict[str, Any] | None],
            normalize_resolution_payload,
        ),
        resolution_has_meaningful_signal=cast(
            Callable[[dict[str, Any]], bool],
            resolution_has_meaningful_signal,
        ),
        resolution_allows_autoclose=cast(
            Callable[[dict[str, Any]], bool],
            resolution_allows_autoclose,
        ),
        non_auto_close_resolution_kinds=NON_AUTO_CLOSE_RESOLUTION_KINDS,
        repo_root_for_issue_ref_fn=lambda cfg, ref: _resolution_helpers.repo_root_for_issue_ref(
            cfg,
            ref,
            parse_issue_ref=parse_issue_ref,
            config_error_type=ConfigError,
        ),
        resolution_validation_command_fn=lambda issue_ref, normalized, **kwargs: _resolution_helpers.resolution_validation_command(
            issue_ref,
            normalized,
            parse_issue_ref=parse_issue_ref,
            **kwargs,
        ),
        resolution_evidence_payload_fn=lambda repo_root, normalized, validation_command, **kwargs: _resolution_helpers.resolution_evidence_payload(
            repo_root,
            normalized,
            validation_command,
            verify_code_refs_on_main_fn=_resolution_helpers.verify_code_refs_on_main,
            commit_reachable_from_origin_main_fn=_resolution_helpers.commit_reachable_from_origin_main,
            pr_is_merged_fn=lambda pr_url, **pr_kwargs: _resolution_helpers.pr_is_merged(
                pr_url,
                parse_pr_url=_parse_pr_url,
                run_gh=_run_gh,
                **pr_kwargs,
            ),
            **kwargs,
        ),
        resolution_is_strong_fn=lambda normalized, evidence: _resolution_helpers.resolution_is_strong(
            normalized,
            evidence,
            resolution_allows_autoclose=cast(
                Callable[[dict[str, Any]], bool],
                resolution_allows_autoclose,
            ),
        ),
        resolution_blocked_reason_fn=lambda normalized, evidence: _resolution_helpers.resolution_blocked_reason(
            normalized,
            evidence,
            resolution_allows_autoclose=cast(
                Callable[[dict[str, Any]], bool],
                resolution_allows_autoclose,
            ),
        ),
    )


def resolution_evidence_payload(
    repo_root: Any,
    normalized: dict[str, Any],
    validation_command: str,
    *,
    pr_port: Any | None = None,
    subprocess_runner: Callable[..., Any] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> dict[str, Any]:
    """Collect deterministic evidence for resolution verification."""
    return _resolution_helpers.resolution_evidence_payload(
        repo_root,
        normalized,
        validation_command,
        pr_port=pr_port,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
        verify_code_refs_on_main_fn=_resolution_helpers.verify_code_refs_on_main,
        commit_reachable_from_origin_main_fn=_resolution_helpers.commit_reachable_from_origin_main,
        pr_is_merged_fn=_resolution_helpers.pr_is_merged,
    )
