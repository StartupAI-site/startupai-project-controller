"""Resolution verification and application helpers."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
from typing import Any, Callable

import startupai_controller.consumer_automation_bridge as _automation_bridge
import startupai_controller.consumer_codex_comment_wiring as _codex_comment_wiring
from startupai_controller.board_graph import _resolve_issue_coordinates
from startupai_controller.control_plane_runtime import (
    _mark_degraded,
    _record_successful_github_mutation,
)
from startupai_controller.domain.models import LinkedIssue
from startupai_controller.domain.resolution_policy import build_resolution_comment
from startupai_controller.runtime.wiring import gh_reason_code
from startupai_controller.validate_critical_path_promotion import GhQueryError


def repo_root_for_issue_ref(
    config: Any,
    issue_ref: str,
    *,
    parse_issue_ref: Callable[[str], Any],
    config_error_type: type[Exception],
) -> Path:
    """Return the canonical main-checkout root for an issue ref."""
    repo_prefix = parse_issue_ref(issue_ref).prefix
    root = config.repo_roots.get(repo_prefix)
    if root is None:
        raise config_error_type(f"Missing repo root for {issue_ref}")
    return root


def verify_code_refs_on_main(
    repo_root: Path,
    code_refs: list[str],
) -> tuple[bool, list[str]]:
    """Verify that every referenced path exists on canonical main."""
    if not code_refs:
        return False, []
    missing: list[str] = []
    resolved_root = repo_root.resolve()
    for ref in code_refs:
        candidate = (repo_root / ref).resolve()
        try:
            candidate.relative_to(resolved_root)
        except ValueError:
            missing.append(ref)
            continue
        if not candidate.exists():
            missing.append(ref)
    return len(missing) == 0, missing


def run_validation_on_main(
    repo_root: Path,
    command: str,
    *,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> tuple[bool, int | None, str]:
    """Run the repo validation command against canonical main."""
    if not command.strip():
        return False, None, "missing-validation-command"
    runner = subprocess_runner or (lambda args, **kwargs: subprocess.run(args, **kwargs))
    result = runner(
        ["bash", "-lc", command],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
    )
    detail = (result.stderr or result.stdout or "").strip()
    return result.returncode == 0, result.returncode, detail


def commit_reachable_from_origin_main(
    repo_root: Path,
    commit_sha: str,
    *,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> bool:
    """Return True when a commit is reachable from origin/main."""
    runner = subprocess_runner or (lambda args, **kwargs: subprocess.run(args, **kwargs))
    result = runner(
        [
            "git",
            "-C",
            str(repo_root),
            "merge-base",
            "--is-ancestor",
            commit_sha,
            "origin/main",
        ],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def pr_is_merged(
    pr_url: str,
    *,
    pr_port: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
    parse_pr_url: Callable[[str], tuple[str, str, int] | None],
    run_gh: Callable[..., str],
) -> bool:
    """Return True when a PR URL points at a merged pull request."""
    parsed = parse_pr_url(pr_url)
    if parsed is None:
        return False
    owner, repo, pr_number = parsed
    if pr_port is not None:
        try:
            return pr_port.is_pull_request_merged(f"{owner}/{repo}", pr_number)
        except Exception:
            return False
    output = run_gh(
        [
            "pr",
            "view",
            str(pr_number),
            "--repo",
            f"{owner}/{repo}",
            "--json",
            "state,mergedAt",
        ],
        gh_runner=gh_runner,
    )
    try:
        payload = json.loads(output)
    except json.JSONDecodeError:
        return False
    merged_at = payload.get("mergedAt")
    state = str(payload.get("state") or "").strip().upper()
    return bool(merged_at) or state == "MERGED"


def resolution_validation_command(
    issue_ref: str,
    normalized: dict[str, Any],
    *,
    config: Any,
    workflows: dict[str, Any],
    parse_issue_ref: Callable[[str], Any],
) -> str:
    """Resolve the validation command for a resolution verification run."""
    workflow = workflows.get(parse_issue_ref(issue_ref).prefix)
    return (
        str(normalized["validation_command"]).strip()
        if normalized["validation_command"]
        else (
            workflow.runtime.validation_cmd
            if workflow is not None and workflow.runtime.validation_cmd is not None
            else config.validation_cmd
        )
    )


def resolution_evidence_payload(
    repo_root: Path,
    normalized: dict[str, Any],
    validation_command: str,
    *,
    pr_port: Any | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    gh_runner: Callable[..., str] | None = None,
    verify_code_refs_on_main_fn: Callable[[Path, list[str]], tuple[bool, list[str]]],
    commit_reachable_from_origin_main_fn: Callable[..., bool],
    pr_is_merged_fn: Callable[..., bool],
) -> dict[str, Any]:
    """Collect deterministic evidence for resolution verification."""
    code_refs = list(normalized["code_refs"])
    commit_shas = list(normalized["commit_shas"])
    pr_urls = list(normalized["pr_urls"])
    code_refs_ok, missing_code_refs = verify_code_refs_on_main_fn(repo_root, code_refs)
    reachable_commits = [
        sha
        for sha in commit_shas
        if commit_reachable_from_origin_main_fn(
            repo_root,
            sha,
            subprocess_runner=subprocess_runner,
        )
    ]
    merged_pr_urls = [
        pr_url
        for pr_url in pr_urls
        if pr_is_merged_fn(pr_url, pr_port=pr_port, gh_runner=gh_runner)
    ]
    validation_ok, validation_exit_code, validation_detail = run_validation_on_main(
        repo_root,
        validation_command,
        subprocess_runner=subprocess_runner,
    )
    return {
        "code_refs": code_refs,
        "missing_code_refs": missing_code_refs,
        "code_refs_ok": code_refs_ok,
        "commit_shas": commit_shas,
        "reachable_commit_shas": reachable_commits,
        "pr_urls": pr_urls,
        "merged_pr_urls": merged_pr_urls,
        "validated_on_main_claim": bool(normalized["validated_on_main"]),
        "validation_command": validation_command,
        "validation_ok": validation_ok,
        "validation_exit_code": validation_exit_code,
        "validation_detail": validation_detail[:500] if validation_detail else "",
        "acceptance_criteria_met": bool(normalized["acceptance_criteria_met"]),
        "acceptance_criteria_notes": normalized["acceptance_criteria_notes"],
        "equivalence_claim": normalized["equivalence_claim"],
    }


def resolution_is_strong(
    normalized: dict[str, Any],
    evidence: dict[str, Any],
    *,
    resolution_allows_autoclose: Callable[[dict[str, Any]], bool],
) -> bool:
    """Return True when resolution evidence is strong enough to auto-close."""
    has_reference_evidence = bool(
        evidence["code_refs"] or evidence["commit_shas"] or evidence["pr_urls"]
    )
    return all(
        [
            resolution_allows_autoclose(normalized),
            has_reference_evidence,
            bool(evidence["code_refs_ok"]),
            bool(evidence["reachable_commit_shas"] or evidence["merged_pr_urls"]),
            bool(normalized["validated_on_main"]),
            bool(evidence["validation_ok"]),
            bool(normalized["acceptance_criteria_met"]),
        ]
    )


def resolution_blocked_reason(
    normalized: dict[str, Any],
    evidence: dict[str, Any],
    *,
    resolution_allows_autoclose: Callable[[dict[str, Any]], bool],
) -> str:
    """Return the deterministic blocked reason for a non-strong resolution."""
    has_reference_evidence = bool(
        evidence["code_refs"] or evidence["commit_shas"] or evidence["pr_urls"]
    )
    if not resolution_allows_autoclose(normalized):
        return "resolution-review-required:unsupported-resolution-kind"
    if not has_reference_evidence:
        return "resolution-review-required:missing-evidence"
    if not evidence["code_refs_ok"]:
        return "resolution-review-required:missing-code-refs"
    if not (evidence["reachable_commit_shas"] or evidence["merged_pr_urls"]):
        return "resolution-review-required:unverified-main-evidence"
    if not normalized["validated_on_main"] or not evidence["validation_ok"]:
        return "resolution-review-required:validation-failed"
    if not normalized["acceptance_criteria_met"]:
        return "resolution-review-required:acceptance-not-met"
    return "resolution-review-required:ambiguous"


def verify_resolution_payload(
    issue_ref: str,
    resolution: dict[str, Any] | None,
    *,
    config: Any,
    workflows: dict[str, Any],
    pr_port: Any | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    gh_runner: Callable[..., str] | None = None,
    build_resolution_evaluation: Callable[..., Any],
    normalize_resolution_payload: Callable[[dict[str, Any] | None], dict[str, Any] | None],
    resolution_has_meaningful_signal: Callable[[dict[str, Any]], bool],
    resolution_allows_autoclose: Callable[[dict[str, Any]], bool],
    non_auto_close_resolution_kinds: set[str] | tuple[str, ...],
    repo_root_for_issue_ref_fn: Callable[..., Path],
    resolution_validation_command_fn: Callable[..., str],
    resolution_evidence_payload_fn: Callable[..., dict[str, Any]],
    resolution_is_strong_fn: Callable[..., bool],
    resolution_blocked_reason_fn: Callable[..., str],
) -> Any:
    """Verify a structured resolution payload against canonical main."""
    normalized = normalize_resolution_payload(resolution)
    if normalized is None:
        return build_resolution_evaluation(
            resolution_kind=None,
            verification_class="failed",
            final_action="blocked_for_resolution_review",
            summary="Successful no-op session returned no structured resolution evidence.",
            evidence={},
            blocked_reason="resolution-review-required:no-structured-resolution",
        )

    kind = str(normalized["kind"])
    summary = str(normalized["summary"] or "").strip()
    repo_root = repo_root_for_issue_ref_fn(config, issue_ref)
    validation_command = resolution_validation_command_fn(
        issue_ref,
        normalized,
        config=config,
        workflows=workflows,
    )
    evidence = resolution_evidence_payload_fn(
        repo_root,
        normalized,
        validation_command,
        pr_port=pr_port,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
    )
    if resolution_is_strong_fn(normalized, evidence):
        return build_resolution_evaluation(
            resolution_kind=kind,
            verification_class="strong",
            final_action="closed_as_already_resolved",
            summary=summary or "Verified existing implementation already satisfies the issue.",
            evidence=evidence,
        )

    if kind in non_auto_close_resolution_kinds:
        return build_resolution_evaluation(
            resolution_kind=kind,
            verification_class="weak",
            final_action="blocked_for_resolution_review",
            summary=summary or f"Resolution `{kind}` requires review.",
            evidence=evidence,
            blocked_reason=f"resolution-review-required:{kind}",
        )

    verification_class = (
        "ambiguous"
        if resolution_has_meaningful_signal(normalized)
        else "failed"
    )
    blocked_reason = resolution_blocked_reason_fn(normalized, evidence)
    return build_resolution_evaluation(
        resolution_kind=kind,
        verification_class=verification_class,
        final_action="blocked_for_resolution_review",
        summary=summary or "Resolution evidence was not strong enough to auto-close.",
        evidence=evidence,
        blocked_reason=blocked_reason,
    )


def queue_issue_comment(
    db: Any,
    issue_ref: str,
    body: str,
) -> None:
    """Queue an issue comment for replay after GitHub recovery."""
    db.queue_deferred_action(
        issue_ref,
        "post_issue_comment",
        {"issue_ref": issue_ref, "body": body},
    )


def queue_issue_close(
    db: Any,
    issue_ref: str,
) -> None:
    """Queue an issue close mutation for replay."""
    db.queue_deferred_action(
        issue_ref,
        "close_issue",
        {"issue_ref": issue_ref},
    )


def queue_status_transition(
    db: Any,
    issue_ref: str,
    *,
    to_status: str,
    from_statuses: set[str],
    blocked_reason: str | None = None,
) -> None:
    """Queue a board status mutation for replay after GitHub recovery."""
    payload: dict[str, Any] = {
        "issue_ref": issue_ref,
        "to_status": to_status,
        "from_statuses": sorted(from_statuses),
    }
    if blocked_reason is not None:
        payload["blocked_reason"] = blocked_reason
    db.queue_deferred_action(issue_ref, "set_status", payload)


def set_issue_handoff_target(
    issue_ref: str,
    target: str,
    config: Any,
    project_owner: str,
    project_number: int,
    *,
    board_port: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
    build_github_port_bundle: Callable[..., Any],
) -> None:
    """Set the board Handoff To field for an issue."""
    port = board_port
    if port is None:
        port = build_github_port_bundle(
            project_owner,
            project_number,
            config=config,
            gh_runner=gh_runner,
        ).board_mutations
    port.set_issue_field(issue_ref, "Handoff To", target)


def apply_resolution_action(
    issue_ref: str,
    evaluation: Any,
    *,
    session_id: str | None,
    db: Any,
    config: Any,
    critical_path_config: Any,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
    resolve_issue_coordinates: Callable[[str, Any], tuple[str, str, int]],
    build_resolution_comment: Callable[..., str],
    mark_issues_done: Callable[..., None],
    record_successful_github_mutation: Callable[[Any], None],
    mark_degraded: Callable[[Any, str], None],
    gh_reason_code: Callable[[Exception], str],
    queue_status_transition: Callable[..., None],
    runtime_comment_poster: Callable[..., None],
    runtime_issue_closer: Callable[..., None],
    set_blocked_with_reason: Callable[..., None],
    set_issue_handoff_target_fn: Callable[..., None],
    linked_issue_type: type[Any],
    gh_query_error_type: type[Exception],
) -> str:
    """Apply the verified resolution decision to the board and issue."""
    owner, repo, number = resolve_issue_coordinates(issue_ref, critical_path_config)
    comment_body = build_resolution_comment(
        issue_ref=issue_ref,
        session_id=session_id,
        resolution_kind=evaluation.resolution_kind or "unknown",
        summary=evaluation.summary,
        verification_class=evaluation.verification_class,
        final_action=evaluation.final_action,
        evidence=evaluation.evidence,
    )

    if evaluation.final_action == "closed_as_already_resolved":
        try:
            mark_issues_done(
                [linked_issue_type(owner=owner, repo=repo, number=number, ref=issue_ref)],
                critical_path_config,
                config.project_owner,
                config.project_number,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                gh_runner=gh_runner,
            )
            record_successful_github_mutation(db)
        except (gh_query_error_type, Exception) as err:
            mark_degraded(db, f"resolution-done:{gh_reason_code(err)}:{err}")
            queue_status_transition(
                db,
                issue_ref,
                to_status="Done",
                from_statuses={"Backlog", "In Progress", "Ready"},
            )
        try:
            poster = comment_poster or runtime_comment_poster
            poster(owner, repo, number, comment_body, gh_runner=gh_runner)
            record_successful_github_mutation(db)
        except (gh_query_error_type, Exception) as err:
            mark_degraded(db, f"resolution-comment:{gh_reason_code(err)}:{err}")
            queue_issue_comment(db, issue_ref, comment_body)
        try:
            runtime_issue_closer(owner, repo, number, gh_runner=gh_runner)
            record_successful_github_mutation(db)
        except (gh_query_error_type, Exception) as err:
            mark_degraded(db, f"resolution-close:{gh_reason_code(err)}:{err}")
            queue_issue_close(db, issue_ref)
        return "already_resolved"

    blocked_reason = evaluation.blocked_reason or "resolution-review-required"
    try:
        set_blocked_with_reason(
            issue_ref,
            blocked_reason,
            critical_path_config,
            config.project_owner,
            config.project_number,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        set_issue_handoff_target_fn(
            issue_ref,
            "claude",
            critical_path_config,
            config.project_owner,
            config.project_number,
            gh_runner=gh_runner,
        )
        record_successful_github_mutation(db)
    except (gh_query_error_type, Exception) as err:
        mark_degraded(db, f"resolution-blocked:{gh_reason_code(err)}:{err}")
        queue_status_transition(
            db,
            issue_ref,
            to_status="Blocked",
            from_statuses={"Backlog", "In Progress", "Ready"},
            blocked_reason=blocked_reason,
        )
    try:
        poster = comment_poster or runtime_comment_poster
        poster(owner, repo, number, comment_body, gh_runner=gh_runner)
        record_successful_github_mutation(db)
    except (gh_query_error_type, Exception) as err:
        mark_degraded(db, f"resolution-comment:{gh_reason_code(err)}:{err}")
        queue_issue_comment(db, issue_ref, comment_body)
    return "resolution_review"


def apply_resolution_action_from_shell(
    issue_ref: str,
    evaluation: Any,
    *,
    session_id: str | None,
    db: Any,
    config: Any,
    critical_path_config: Any,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Apply a verified resolution decision using live shell seams."""
    return apply_resolution_action(
        issue_ref,
        evaluation,
        session_id=session_id,
        db=db,
        config=config,
        critical_path_config=critical_path_config,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        resolve_issue_coordinates=_resolve_issue_coordinates,
        build_resolution_comment=build_resolution_comment,
        mark_issues_done=_automation_bridge.mark_issues_done,
        record_successful_github_mutation=_record_successful_github_mutation,
        mark_degraded=_mark_degraded,
        gh_reason_code=gh_reason_code,
        queue_status_transition=queue_status_transition,
        runtime_comment_poster=_codex_comment_wiring.runtime_comment_poster,
        runtime_issue_closer=_codex_comment_wiring.runtime_issue_closer,
        set_blocked_with_reason=_automation_bridge.set_blocked_with_reason,
        set_issue_handoff_target_fn=set_issue_handoff_target,
        linked_issue_type=LinkedIssue,
        gh_query_error_type=GhQueryError,
    )
