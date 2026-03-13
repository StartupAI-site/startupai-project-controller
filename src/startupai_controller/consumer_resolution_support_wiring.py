"""Resolution-verification support wiring extracted from consumer_support_wiring."""

from __future__ import annotations

from pathlib import Path
import subprocess
from typing import Callable

from startupai_controller import consumer_resolution_helpers as _resolution_helpers
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_workflow import WorkflowDefinition
from startupai_controller.domain.models import ResolutionEvaluation
from startupai_controller.domain.repair_policy import parse_pr_url as _parse_pr_url
from startupai_controller.domain.resolution_policy import (
    NON_AUTO_CLOSE_RESOLUTION_KINDS,
    ResolutionPayload,
    normalize_resolution_payload,
    resolution_allows_autoclose,
    resolution_has_meaningful_signal,
)
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.runtime.wiring import _run_gh
from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    parse_issue_ref,
)

ResolutionEvidencePayload = dict[str, object]
ResolutionPayloadDict = dict[str, object]
SubprocessRunnerFn = Callable[..., subprocess.CompletedProcess[str]]


def _resolution_payload_dict(
    resolution: ResolutionPayload | None,
) -> ResolutionPayloadDict | None:
    if resolution is None:
        return None
    return dict(resolution)


def _normalize_resolution_mapping(
    raw: ResolutionPayloadDict | None,
) -> ResolutionPayloadDict | None:
    normalized = normalize_resolution_payload(raw)
    if normalized is None:
        return None
    return dict(normalized)


def _require_resolution_payload(raw: ResolutionPayloadDict) -> ResolutionPayload:
    normalized = normalize_resolution_payload(raw)
    assert normalized is not None
    return normalized


def _resolution_has_meaningful_signal(resolution: ResolutionPayloadDict) -> bool:
    return resolution_has_meaningful_signal(_require_resolution_payload(resolution))


def _resolution_allows_autoclose(resolution: ResolutionPayloadDict) -> bool:
    return resolution_allows_autoclose(_require_resolution_payload(resolution))


def _repo_root_for_issue_ref(config: ConsumerConfig, issue_ref: str) -> Path:
    return _resolution_helpers.repo_root_for_issue_ref(
        config,
        issue_ref,
        parse_issue_ref=parse_issue_ref,
        config_error_type=ConfigError,
    )


def _resolution_validation_command(
    issue_ref: str,
    normalized: ResolutionPayloadDict,
    *,
    config: ConsumerConfig,
    workflows: dict[str, WorkflowDefinition],
) -> str:
    return _resolution_helpers.resolution_validation_command(
        issue_ref,
        dict(_require_resolution_payload(normalized)),
        config=config,
        workflows=workflows,
        parse_issue_ref=parse_issue_ref,
    )


def _resolution_evidence_payload(
    repo_root: Path,
    normalized: ResolutionPayloadDict,
    validation_command: str,
    *,
    pr_port: PullRequestPort | None = None,
    subprocess_runner: SubprocessRunnerFn | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> ResolutionEvidencePayload:
    payload: ResolutionEvidencePayload = (
        _resolution_helpers.resolution_evidence_payload(
            repo_root,
            dict(_require_resolution_payload(normalized)),
            validation_command,
            pr_port=pr_port,
            subprocess_runner=subprocess_runner,
            gh_runner=gh_runner,
            verify_code_refs_on_main_fn=_resolution_helpers.verify_code_refs_on_main,
            commit_reachable_from_origin_main_fn=_resolution_helpers.commit_reachable_from_origin_main,
            pr_is_merged_fn=lambda pr_url, **pr_kwargs: _resolution_helpers.pr_is_merged(
                pr_url,
                parse_pr_url=_parse_pr_url,
                run_gh=_run_gh,
                **pr_kwargs,
            ),
        )
    )
    return payload


def _resolution_is_strong(
    normalized: ResolutionPayloadDict,
    evidence: ResolutionPayloadDict,
) -> bool:
    return _resolution_helpers.resolution_is_strong(
        dict(_require_resolution_payload(normalized)),
        dict(evidence),
        resolution_allows_autoclose=_resolution_allows_autoclose,
    )


def _resolution_blocked_reason(
    normalized: ResolutionPayloadDict,
    evidence: ResolutionPayloadDict,
) -> str:
    return _resolution_helpers.resolution_blocked_reason(
        dict(_require_resolution_payload(normalized)),
        dict(evidence),
        resolution_allows_autoclose=_resolution_allows_autoclose,
    )


def verify_resolution_payload(
    issue_ref: str,
    resolution: ResolutionPayload | None,
    *,
    config: ConsumerConfig,
    workflows: dict[str, WorkflowDefinition],
    pr_port: PullRequestPort | None = None,
    subprocess_runner: SubprocessRunnerFn | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> ResolutionEvaluation:
    """Verify a structured resolution payload against canonical main."""
    evaluation: ResolutionEvaluation = _resolution_helpers.verify_resolution_payload(
        issue_ref,
        _resolution_payload_dict(resolution),
        config=config,
        workflows=workflows,
        pr_port=pr_port,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
        build_resolution_evaluation=ResolutionEvaluation,
        normalize_resolution_payload=_normalize_resolution_mapping,
        resolution_has_meaningful_signal=_resolution_has_meaningful_signal,
        resolution_allows_autoclose=_resolution_allows_autoclose,
        non_auto_close_resolution_kinds=NON_AUTO_CLOSE_RESOLUTION_KINDS,
        repo_root_for_issue_ref_fn=_repo_root_for_issue_ref,
        resolution_validation_command_fn=_resolution_validation_command,
        resolution_evidence_payload_fn=_resolution_evidence_payload,
        resolution_is_strong_fn=_resolution_is_strong,
        resolution_blocked_reason_fn=_resolution_blocked_reason,
    )
    return evaluation


def resolution_evidence_payload(
    repo_root: Path,
    normalized: ResolutionPayload,
    validation_command: str,
    *,
    pr_port: PullRequestPort | None = None,
    subprocess_runner: SubprocessRunnerFn | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> ResolutionEvidencePayload:
    """Collect deterministic evidence for resolution verification."""
    return _resolution_evidence_payload(
        repo_root,
        dict(normalized),
        validation_command,
        pr_port=pr_port,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
    )
