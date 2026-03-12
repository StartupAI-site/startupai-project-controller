"""Admission pipeline helpers — evaluation, filtering, and mutation logic."""

from __future__ import annotations

import re
from dataclasses import dataclass

from startupai_controller.board_graph import (
    _admission_candidate_rank,
    _resolve_issue_coordinates,
)
from startupai_controller.domain.models import (
    AdmissionCandidate,
    AdmissionDecision,
    AdmissionSkip,
    LinkedIssue,
    OpenPullRequest,
    ProjectItemSnapshot as _ProjectItemSnapshot,
)
from startupai_controller.domain.repair_policy import (
    MARKER_PREFIX,
    parse_consumer_provenance as _parse_consumer_provenance,
)
from startupai_controller.domain.resolution_policy import parse_resolution_comment
from startupai_controller.domain.scheduling_policy import (
    has_structured_acceptance_criteria,
    priority_rank as _priority_rank,
    snapshot_to_issue_ref as _snapshot_to_issue_ref,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.issue_context import IssueContextPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    GhQueryError,
    evaluate_ready_promotion,
    in_any_critical_path,
    parse_issue_ref,
)


# ---------------------------------------------------------------------------
# Deps injection
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AdmissionPipelineDeps:
    """Typed infrastructure capabilities for the admission pipeline."""

    review_state_port: ReviewStatePort
    pr_port: PullRequestPort
    board_port: BoardMutationPort
    issue_context_port: IssueContextPort


# ---------------------------------------------------------------------------
# Pure helpers (no board deps)
# ---------------------------------------------------------------------------


def _deterministic_issue_branch_pattern(issue_ref: str) -> re.Pattern[str]:
    """Return the canonical issue branch naming pattern."""
    parsed = parse_issue_ref(issue_ref)
    return re.compile(rf"^feat/{parsed.number}-[a-z0-9-]+$")


def _extract_closing_issue_numbers(body: str) -> set[int]:
    """Return issue numbers referenced by common auto-close keywords."""
    matches = re.findall(
        r"\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s+#(\d+)\b",
        body,
        flags=re.IGNORECASE,
    )
    results: set[int] = set()
    for match in matches:
        try:
            results.add(int(match))
        except ValueError:
            continue
    return results


def _classify_admission_pr_state(
    issue_ref: str,
    issue_number: int,
    candidates: list[OpenPullRequest],
    automation_config,
) -> str:
    """Classify PR state for backlog admission without extra per-issue queries."""
    linked_candidates = [
        pr for pr in candidates if issue_number in _extract_closing_issue_numbers(pr.body)
    ]
    if not linked_candidates:
        return "none"

    branch_pattern = _deterministic_issue_branch_pattern(issue_ref)
    local_count = 0
    ambiguous = False
    for candidate in linked_candidates:
        provenance = _parse_consumer_provenance(candidate.body)
        is_local = (
            provenance is not None
            and provenance.get("issue_ref") == issue_ref
            and provenance.get("executor") == "codex"
            and candidate.author in automation_config.trusted_local_authors
            and bool(branch_pattern.match(candidate.head_ref_name))
        )
        if is_local:
            local_count += 1
        else:
            ambiguous = True

    if local_count == 1 and not ambiguous:
        return "existing_local_pr"
    if local_count > 1 or (local_count and ambiguous):
        return "ambiguous_pr"
    return "non_local_pr"


def admission_summary_payload(
    decision: AdmissionDecision,
    *,
    enabled: bool,
) -> dict[str, object]:
    """Convert an AdmissionDecision into a JSON-friendly payload."""
    return {
        "enabled": enabled,
        "ready_count": decision.ready_count,
        "ready_floor": decision.ready_floor,
        "ready_cap": decision.ready_cap,
        "needed": decision.needed,
        "scanned_backlog": decision.scanned_backlog,
        "eligible_count": decision.eligible_count,
        "admitted": list(decision.admitted),
        "resolved": list(decision.resolved),
        "blocked": list(decision.blocked),
        "skip_reason_counts": decision.skip_reason_counts,
        "top_candidates": [
            {
                "issue_ref": candidate.issue_ref,
                "priority": candidate.priority,
                "title": candidate.title,
                "graph_member": candidate.is_graph_member,
            }
            for candidate in decision.eligible[:10]
        ],
        "top_skipped": [
            {
                "issue_ref": skip.issue_ref,
                "reason": skip.reason_code,
            }
            for skip in decision.skipped[:10]
        ],
        "partial_failure": decision.partial_failure,
        "error": decision.error,
        "deep_evaluation_performed": decision.deep_evaluation_performed,
        "deep_evaluation_truncated": decision.deep_evaluation_truncated,
        "controller_owned_admission_rejections": decision.skip_reason_counts.get(
            "controller_owned_admission", 0
        ),
    }


# ---------------------------------------------------------------------------
# Helpers that need board deps
# ---------------------------------------------------------------------------


def _query_open_prs_by_prefix(
    config,
    repo_prefixes: tuple[str, ...],
    *,
    deps: AdmissionPipelineDeps,
    github_memo=None,
) -> dict[str, list[OpenPullRequest]]:
    """Return open PRs keyed by governed repo prefix."""
    result: dict[str, list[OpenPullRequest]] = {}
    for repo_prefix in repo_prefixes:
        repo_slug = config.issue_prefixes.get(repo_prefix)
        if not repo_slug:
            result[repo_prefix] = []
            continue
        if github_memo is not None:
            cached = github_memo.open_pull_requests.get(repo_slug)
            if cached is None:
                cached = deps.pr_port.list_open_prs(repo_slug)
                github_memo.open_pull_requests[repo_slug] = list(cached)
            result[repo_prefix] = list(cached)
        else:
            result[repo_prefix] = deps.pr_port.list_open_prs(repo_slug)
    return result


def _latest_resolution_signal(
    issue_ref: str,
    owner: str,
    repo: str,
    number: int,
    *,
    deps: AdmissionPipelineDeps,
    github_memo=None,
) -> dict[str, object] | None:
    """Return the latest machine-readable resolution signal, if any."""
    if github_memo is not None:
        key = (owner, repo, number)
        cached = github_memo.issue_comment_bodies.get(key)
        if cached is None:
            cached = deps.review_state_port.list_issue_comment_bodies(
                f"{owner}/{repo}",
                number,
            )
            github_memo.issue_comment_bodies[key] = list(cached)
        comment_bodies = list(cached)
    else:
        comment_bodies = list(
            deps.review_state_port.list_issue_comment_bodies(
                f"{owner}/{repo}",
                number,
            )
        )

    for body in reversed(comment_bodies):
        parsed = parse_resolution_comment(body)
        if parsed is None or parsed.issue_ref != issue_ref:
            continue
        payload = parsed.payload
        resolution_kind = str(payload.get("resolution_kind") or "").strip()
        verification_class = str(payload.get("verification_class") or "").strip()
        final_action = str(payload.get("final_action") or "").strip()
        summary = str(payload.get("summary") or "").strip()
        evidence = payload.get("evidence")
        if not isinstance(evidence, dict):
            evidence = {}
        return {
            "resolution_kind": resolution_kind,
            "verification_class": verification_class,
            "final_action": final_action,
            "summary": summary,
            "evidence": evidence,
        }
    return None


def _apply_prior_resolution_signal(
    issue_ref: str,
    signal: dict[str, object],
    config,
    project_owner: str,
    project_number: int,
    *,
    deps: AdmissionPipelineDeps,
    dry_run: bool = False,
) -> str:
    """Apply a prior machine-verified resolution signal before admission."""
    owner, repo, number = _resolve_issue_coordinates(issue_ref, config)
    verification_class = str(signal.get("verification_class") or "")
    final_action = str(signal.get("final_action") or "")
    resolution_kind = str(signal.get("resolution_kind") or "")
    blocked_reason = f"resolution-review-required:{resolution_kind or 'prior-signal'}"

    if verification_class == "strong" and final_action == "closed_as_already_resolved":
        if not dry_run:
            deps.board_port.set_issue_status(issue_ref, "Done")
            deps.board_port.close_issue(f"{owner}/{repo}", number)
        return "resolved"

    if not dry_run:
        deps.board_port.set_issue_status(issue_ref, "Blocked")
        deps.board_port.set_issue_field(issue_ref, "Blocked Reason", blocked_reason)
        deps.board_port.set_issue_field(issue_ref, "Handoff To", "claude")
    return "blocked"


def _admit_backlog_item(
    candidate: AdmissionCandidate,
    *,
    deps: AdmissionPipelineDeps,
    executor: str,
    assignment_owner: str,
) -> None:
    """Mutate one backlog card into a controller-owned Ready card."""
    deps.board_port.set_project_single_select(
        candidate.project_id,
        candidate.item_id,
        "Status",
        "Ready",
    )
    deps.board_port.set_project_single_select(
        candidate.project_id,
        candidate.item_id,
        "Executor",
        executor,
    )
    deps.board_port.set_project_text_field(
        candidate.project_id,
        candidate.item_id,
        "Owner",
        assignment_owner,
    )
    deps.board_port.set_project_single_select(
        candidate.project_id,
        candidate.item_id,
        "Handoff To",
        "none",
    )
    deps.board_port.set_project_text_field(
        candidate.project_id,
        candidate.item_id,
        "Blocked Reason",
        "",
    )


# ---------------------------------------------------------------------------
# Strategy functions (injected into admit_backlog orchestrator)
# ---------------------------------------------------------------------------


def load_admission_source_items(
    automation_config,
    *,
    deps: AdmissionPipelineDeps,
    board_snapshot=None,
) -> list[_ProjectItemSnapshot]:
    """Load backlog/ready items needed for one admission pass."""
    statuses = set(automation_config.admission.source_statuses)
    statuses.add("Ready")
    if board_snapshot is None:
        return [
            item
            for item in deps.review_state_port.build_board_snapshot().items
            if item.status in statuses
        ]
    return [item for item in board_snapshot.items if item.status in statuses]


def partition_admission_source_items(
    items: list[_ProjectItemSnapshot],
    *,
    config,
    automation_config,
    target_executor: str,
) -> tuple[int, list[_ProjectItemSnapshot]]:
    """Count governed ready items and collect governed backlog items."""
    ready_count = 0
    backlog_items: list[_ProjectItemSnapshot] = []
    for item in items:
        issue_ref = _snapshot_to_issue_ref(item.issue_ref, config.issue_prefixes)
        if issue_ref is None:
            continue
        repo_prefix = parse_issue_ref(issue_ref).prefix
        if repo_prefix not in automation_config.execution_authority_repos:
            continue
        if item.status == "Ready" and item.executor.strip().lower() == target_executor:
            ready_count += 1
        elif item.status in automation_config.admission.source_statuses:
            backlog_items.append(item)
    return ready_count, backlog_items


def build_provisional_admission_candidates(
    backlog_items: list[_ProjectItemSnapshot],
    *,
    config,
    automation_config,
    dispatchable_repo_prefixes: tuple[str, ...],
    active_lease_issue_refs: tuple[str, ...],
) -> tuple[list[_ProjectItemSnapshot], list[AdmissionSkip]]:
    """Apply cheap exact admission filters to backlog items."""
    dispatchable = set(dispatchable_repo_prefixes)
    active_leases = set(active_lease_issue_refs)
    provisional_candidates: list[_ProjectItemSnapshot] = []
    skipped: list[AdmissionSkip] = []

    for item in backlog_items:
        issue_ref = _snapshot_to_issue_ref(item.issue_ref, config.issue_prefixes)
        if issue_ref is None:
            skipped.append(AdmissionSkip(item.issue_ref, "unknown_repo_prefix"))
            continue
        repo_prefix = parse_issue_ref(issue_ref).prefix
        if repo_prefix not in automation_config.execution_authority_repos:
            skipped.append(AdmissionSkip(issue_ref, "not_governed"))
            continue
        if item.status != "Backlog":
            skipped.append(AdmissionSkip(issue_ref, "status_not_backlog"))
            continue
        if repo_prefix not in dispatchable:
            skipped.append(AdmissionSkip(issue_ref, "repo_dispatch_disabled"))
            continue
        if issue_ref in active_leases:
            skipped.append(AdmissionSkip(issue_ref, "already_active_locally"))
            continue
        if not item.priority.strip():
            skipped.append(AdmissionSkip(issue_ref, "missing_priority"))
            continue
        if _priority_rank(item.priority)[0] == 99:
            skipped.append(AdmissionSkip(issue_ref, "invalid_priority"))
            continue
        if not item.sprint.strip():
            skipped.append(AdmissionSkip(issue_ref, "missing_sprint"))
            continue
        if not item.agent.strip():
            skipped.append(AdmissionSkip(issue_ref, "missing_agent"))
            continue
        provisional_candidates.append(item)

    provisional_candidates.sort(
        key=lambda item: _admission_candidate_rank(
            _snapshot_to_issue_ref(item.issue_ref, config.issue_prefixes) or item.issue_ref,
            priority=item.priority,
            is_graph_member=in_any_critical_path(
                config,
                _snapshot_to_issue_ref(item.issue_ref, config.issue_prefixes)
                or item.issue_ref,
            ),
        )
    )
    return provisional_candidates, skipped


def evaluate_admission_candidates(
    provisional_candidates: list[_ProjectItemSnapshot],
    *,
    deps: AdmissionPipelineDeps,
    config,
    automation_config,
    project_owner: str,
    project_number: int,
    needed: int,
    dry_run: bool,
    memo,
    skipped: list[AdmissionSkip],
) -> tuple[
    list[AdmissionCandidate],
    list[str],
    list[str],
    bool,
    str | None,
    bool,
]:
    """Run the expensive admission checks needed to choose candidates."""
    eligible: list[AdmissionCandidate] = []
    resolved: list[str] = []
    blocked: list[str] = []
    partial_failure = False
    error: str | None = None
    deep_evaluation_truncated = False

    for item in provisional_candidates:
        if len(eligible) >= needed:
            deep_evaluation_truncated = True
            break
        issue_ref = _snapshot_to_issue_ref(item.issue_ref, config.issue_prefixes)
        assert issue_ref is not None
        repo_prefix = parse_issue_ref(issue_ref).prefix
        owner, repo, number = _resolve_issue_coordinates(issue_ref, config)
        if item.body:
            body = item.body
        else:
            key = (owner, repo, number)
            body = memo.issue_bodies.get(key)
            if body is None:
                body = deps.issue_context_port.get_issue_context(owner, repo, number).body
                memo.issue_bodies[key] = body
        if not has_structured_acceptance_criteria(
            body,
            automation_config.admission.acceptance_headings,
        ):
            skipped.append(AdmissionSkip(issue_ref, "missing_acceptance_criteria"))
            continue

        pr_state = _classify_admission_pr_state(
            issue_ref,
            item.issue_number,
            _query_open_prs_by_prefix(
                config,
                (repo_prefix,),
                deps=deps,
                github_memo=memo,
            ).get(repo_prefix, []),
            automation_config,
        )
        if pr_state != "none":
            skipped.append(AdmissionSkip(issue_ref, pr_state))
            continue
        is_graph_member = in_any_critical_path(config, issue_ref)
        if is_graph_member:
            is_ready = memo.dependency_ready.get(issue_ref)
            if is_ready is None:
                val_code, _ = evaluate_ready_promotion(
                    issue_ref=issue_ref,
                    config=config,
                    project_owner=project_owner,
                    project_number=project_number,
                    require_in_graph=True,
                )
                is_ready = val_code == 0
                memo.dependency_ready[issue_ref] = is_ready
            if not is_ready:
                skipped.append(AdmissionSkip(issue_ref, "dependency_unmet"))
                continue
        prior_resolution = _latest_resolution_signal(
            issue_ref,
            owner,
            repo,
            number,
            deps=deps,
            github_memo=memo,
        )
        if prior_resolution is not None:
            try:
                signal_action = _apply_prior_resolution_signal(
                    issue_ref,
                    prior_resolution,
                    config,
                    project_owner,
                    project_number,
                    deps=deps,
                    dry_run=dry_run,
                )
            except GhQueryError as exc:
                partial_failure = True
                error = str(exc)
                break
            if signal_action == "resolved":
                resolved.append(issue_ref)
            else:
                blocked.append(issue_ref)
            skipped.append(AdmissionSkip(issue_ref, "prior_resolution_signal"))
            continue
        eligible.append(
            AdmissionCandidate(
                issue_ref=issue_ref,
                repo_prefix=repo_prefix,
                item_id=item.item_id,
                project_id=item.project_id,
                priority=item.priority,
                title=item.title,
                is_graph_member=is_graph_member,
            )
        )

    return (
        eligible,
        resolved,
        blocked,
        partial_failure,
        error,
        deep_evaluation_truncated,
    )


def apply_admitted_backlog_candidates(
    selected: list[AdmissionCandidate],
    *,
    deps: AdmissionPipelineDeps,
    executor: str,
    assignment_owner: str,
    dry_run: bool,
) -> tuple[list[str], bool, str | None]:
    """Apply backlog-to-ready mutations for the selected candidates."""
    admitted: list[str] = []
    partial_failure = False
    error: str | None = None
    if dry_run:
        return admitted, partial_failure, error
    for candidate in selected:
        try:
            _admit_backlog_item(
                candidate,
                deps=deps,
                executor=executor,
                assignment_owner=assignment_owner,
            )
        except GhQueryError as exc:
            partial_failure = True
            error = str(exc)
            break
        admitted.append(candidate.issue_ref)
    return admitted, partial_failure, error
