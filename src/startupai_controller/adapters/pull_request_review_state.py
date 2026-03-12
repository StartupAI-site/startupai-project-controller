"""Shared PR review-state builders for GitHub capability adapters."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Callable

from startupai_controller.adapters.github_types import (
    CodexReviewVerdict,
    PullRequestStateProbe,
    PullRequestViewPayload,
)
from startupai_controller.domain.models import (
    CheckObservation,
    PrGateStatus,
    ReviewSnapshot,
)


def _extract_run_id(details_url: str) -> int | None:
    """Extract a GitHub Actions run ID from a details URL when present."""
    match = re.search(r"/actions/runs/(\d+)", details_url)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def pull_request_state_probe_from_payload(
    payload: PullRequestViewPayload,
) -> PullRequestStateProbe:
    """Build a lightweight review-state probe from an expanded PR payload."""
    latest_comment_at = ""
    if payload.comments:
        latest_comment_at = max(
            str(comment.get("createdAt") or "")
            for comment in payload.comments
            if isinstance(comment, dict)
        )
    latest_review_at = ""
    if payload.reviews:
        latest_review_at = max(
            str(review.get("submittedAt") or "")
            for review in payload.reviews
            if isinstance(review, dict)
        )
    return PullRequestStateProbe(
        pr_repo=payload.pr_repo,
        pr_number=payload.pr_number,
        state=payload.state,
        is_draft=payload.is_draft,
        merge_state_status=payload.merge_state_status,
        mergeable=payload.mergeable,
        base_ref_name=payload.base_ref_name,
        auto_merge_enabled=payload.auto_merge_enabled,
        head_ref_oid="",
        updated_at="",
        latest_comment_at=latest_comment_at,
        latest_review_at=latest_review_at,
        status_check_rollup=payload.status_check_rollup,
    )


def review_state_digest_from_probe(probe: PullRequestStateProbe) -> str:
    """Return a stable digest for the lightweight state of a review PR."""
    latest_checks: dict[str, tuple[str, str]] = {}
    for check in probe.status_check_rollup:
        typename = check.get("__typename", "")
        if typename == "CheckRun":
            name = str(check.get("name") or "")
            timestamp = str(check.get("completedAt") or check.get("startedAt") or "")
            status = str(check.get("status") or "").lower()
            conclusion = str(check.get("conclusion") or "").lower()
            result = (
                "pending"
                if status != "completed"
                else (
                    "pass"
                    if conclusion in {"success", "neutral", "skipped"}
                    else (
                        "cancelled"
                        if conclusion in {"cancelled", "startup_failure", "stale"}
                        else "fail"
                    )
                )
            )
        elif typename == "StatusContext":
            name = str(check.get("context") or "")
            timestamp = str(check.get("startedAt") or "")
            state = str(check.get("state") or "").lower()
            result = (
                "pass"
                if state == "success"
                else ("fail" if state in {"error", "failure"} else "pending")
            )
        else:
            continue
        if not name:
            continue
        previous = latest_checks.get(name)
        if previous is None or timestamp >= previous[0]:
            latest_checks[name] = (timestamp, result)

    payload = {
        "state": probe.state.strip().upper(),
        "is_draft": bool(probe.is_draft),
        "merge_state_status": probe.merge_state_status,
        "mergeable": probe.mergeable,
        "base_ref_name": probe.base_ref_name,
        "auto_merge_enabled": bool(probe.auto_merge_enabled),
        "head_ref_oid": probe.head_ref_oid,
        "updated_at": probe.updated_at,
        "latest_comment_at": probe.latest_comment_at,
        "latest_review_at": probe.latest_review_at,
        "checks": sorted(
            (name, result) for name, (_ts, result) in latest_checks.items()
        ),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def review_state_digest_from_payload(payload: PullRequestViewPayload) -> str:
    """Return a stable review-state digest from an expanded PR payload."""
    return review_state_digest_from_probe(
        pull_request_state_probe_from_payload(payload)
    )


def parse_codex_verdict_from_text(
    text: str,
) -> tuple[str | None, str | None, list[str]]:
    """Extract codex verdict markers from free text."""
    decision_match = re.search(r"\bcodex-review\s*:\s*(pass|fail)\b", text, re.I)
    route_match = re.search(
        r"\bcodex-route\s*:\s*(none|codex|executor|claude|human)\b",
        text,
        re.I,
    )
    checklist = re.findall(r"^\s*-\s*\[\s\]\s+(.+)$", text, flags=re.M)
    decision = decision_match.group(1).lower() if decision_match else None
    route = route_match.group(1).lower() if route_match else None
    return decision, route, checklist


def latest_codex_verdict_from_payload(
    payload: PullRequestViewPayload,
    *,
    trusted_actors: set[str] | frozenset[str] | None = None,
) -> CodexReviewVerdict | None:
    """Return the latest codex verdict marker from one expanded PR payload."""
    candidates: list[CodexReviewVerdict] = []

    for comment in payload.comments:
        body = comment.get("body") or ""
        decision, route, checklist = parse_codex_verdict_from_text(body)
        if decision is None:
            continue
        actor = (
            (
                (comment.get("author") or {}).get("login")
                or (comment.get("user") or {}).get("login")
                or ""
            )
            .strip()
            .lower()
        )
        if trusted_actors and actor not in trusted_actors:
            continue
        ts = comment.get("createdAt", "")
        chosen_route = "none" if decision == "pass" else (route or "executor")
        candidates.append(
            CodexReviewVerdict(
                decision=decision,
                route=chosen_route,
                source="comment",
                timestamp=ts,
                actor=actor,
                checklist=checklist,
            )
        )

    for review in payload.reviews:
        body = review.get("body") or ""
        decision, route, checklist = parse_codex_verdict_from_text(body)
        if decision is None:
            continue
        actor = (
            (
                (review.get("author") or {}).get("login")
                or (review.get("user") or {}).get("login")
                or ""
            )
            .strip()
            .lower()
        )
        if trusted_actors and actor not in trusted_actors:
            continue
        ts = review.get("submittedAt", "")
        chosen_route = "none" if decision == "pass" else (route or "executor")
        candidates.append(
            CodexReviewVerdict(
                decision=decision,
                route=chosen_route,
                source="review",
                timestamp=ts,
                actor=actor,
                checklist=checklist,
            )
        )

    if not candidates:
        return None
    candidates.sort(key=lambda item: item.timestamp)
    return candidates[-1]


def has_copilot_review_signal_from_payload(payload: PullRequestViewPayload) -> bool:
    """Return True when Copilot has submitted an approved/commented review."""
    accepted_states = {"APPROVED", "COMMENTED"}
    for review in payload.reviews:
        state = str(review.get("state", "")).upper()
        actor = ((review.get("author") or {}).get("login") or "").lower()
        if "copilot" in actor and state in accepted_states:
            return True
    return False


def build_pr_gate_status_from_payload(
    payload: PullRequestViewPayload,
    *,
    required: set[str],
) -> PrGateStatus:
    """Build gate readiness from one expanded PR payload and required checks."""
    latest: dict[str, tuple[str, CheckObservation]] = {}
    for check in payload.status_check_rollup:
        typename = check.get("__typename", "")
        if typename == "CheckRun":
            name = str(check.get("name") or "")
            timestamp = str(check.get("completedAt") or check.get("startedAt") or "")
            status = str(check.get("status") or "").lower()
            conclusion = str(check.get("conclusion") or "").lower()
            details_url = str(check.get("detailsUrl") or "")
            workflow_name = str(check.get("workflowName") or "")
            if not name:
                continue
            result = (
                "pending"
                if status != "completed"
                else (
                    "pass"
                    if conclusion in {"success", "neutral", "skipped"}
                    else (
                        "cancelled"
                        if conclusion in {"cancelled", "startup_failure", "stale"}
                        else "fail"
                    )
                )
            )
            observation = CheckObservation(
                name=name,
                result=result,
                status=status,
                conclusion=conclusion,
                details_url=details_url,
                workflow_name=workflow_name,
                run_id=_extract_run_id(details_url),
            )
            previous = latest.get(name)
            if previous is None or timestamp >= previous[0]:
                latest[name] = (timestamp, observation)
        elif typename == "StatusContext":
            name = str(check.get("context") or "")
            timestamp = str(check.get("startedAt") or "")
            state = str(check.get("state") or "").lower()
            details_url = str(check.get("targetUrl") or "")
            if not name:
                continue
            if state == "success":
                result = "pass"
            elif state in {"error", "failure"}:
                result = "fail"
            else:
                result = "pending"
            observation = CheckObservation(
                name=name,
                result=result,
                status=state,
                conclusion=state,
                details_url=details_url,
                workflow_name="",
                run_id=_extract_run_id(details_url),
            )
            previous = latest.get(name)
            if previous is None or timestamp >= previous[0]:
                latest[name] = (timestamp, observation)

    passed: set[str] = set()
    failed: set[str] = set()
    pending: set[str] = set()
    cancelled: set[str] = set()
    for context in required:
        if context not in latest:
            pending.add(context)
            continue
        _timestamp, observation = latest[context]
        if observation.result == "pass":
            passed.add(context)
        elif observation.result == "fail":
            failed.add(context)
        elif observation.result == "cancelled":
            cancelled.add(context)
            pending.add(context)
        else:
            pending.add(context)

    return PrGateStatus(
        required=required,
        passed=passed,
        failed=failed,
        pending=pending,
        cancelled=cancelled,
        merge_state_status=payload.merge_state_status,
        mergeable=payload.mergeable,
        is_draft=payload.is_draft,
        state=payload.state.strip().upper(),
        auto_merge_enabled=payload.auto_merge_enabled,
        checks={name: observation for name, (_ts, observation) in latest.items()},
    )


def codex_gate_from_payload(
    pr_repo: str,
    pr_number: int,
    *,
    review_refs: tuple[str, ...],
    verdict: CodexReviewVerdict | None,
) -> tuple[int, str]:
    """Build the codex gate message for one review PR payload."""
    if not review_refs:
        return 0, (
            f"{pr_repo}#{pr_number}: codex gate not applicable "
            "(linked issues not in Review)"
        )
    if verdict is None:
        return 2, (
            f"{pr_repo}#{pr_number}: missing codex verdict marker "
            "(codex-review: pass|fail from trusted actor)"
        )
    if verdict.decision == "pass":
        return 0, (
            f"{pr_repo}#{pr_number}: codex-review=pass "
            f"(source={verdict.source}, actor={verdict.actor})"
        )
    return 2, (
        f"{pr_repo}#{pr_number}: codex-review=fail "
        f"(route={verdict.route}, source={verdict.source}, actor={verdict.actor})"
    )


def build_review_snapshot_from_payload(
    *,
    pr_repo: str,
    pr_number: int,
    review_refs: tuple[str, ...],
    pr_payload: PullRequestViewPayload,
    trusted_codex_actors: frozenset[str],
    required_checks: set[str],
    has_copilot_review_signal_from_payload_fn: Callable[
        [PullRequestViewPayload], bool
    ] = has_copilot_review_signal_from_payload,
    latest_codex_verdict_from_payload_fn: Callable[
        ..., CodexReviewVerdict | None
    ] = latest_codex_verdict_from_payload,
    build_pr_gate_status_from_payload_fn: Callable[
        ..., PrGateStatus
    ] = build_pr_gate_status_from_payload,
    codex_gate_from_payload_fn: Callable[
        ..., tuple[int, str]
    ] = codex_gate_from_payload,
) -> ReviewSnapshot:
    """Build the typed review snapshot for one PR payload."""
    copilot_review_present = has_copilot_review_signal_from_payload_fn(pr_payload)
    verdict = latest_codex_verdict_from_payload_fn(
        pr_payload,
        trusted_actors=trusted_codex_actors,
    )
    codex_gate_code, codex_gate_message = codex_gate_from_payload_fn(
        pr_repo,
        pr_number,
        review_refs=review_refs,
        verdict=verdict,
    )
    gate_status = build_pr_gate_status_from_payload_fn(
        pr_payload,
        required=required_checks,
    )

    rescue_checks = tuple(sorted(gate_status.required))
    rescue_passed: set[str] = set()
    rescue_pending: set[str] = set()
    rescue_failed: set[str] = set()
    rescue_cancelled: set[str] = set()
    rescue_missing: set[str] = set()
    for name in rescue_checks:
        observation = gate_status.checks.get(name)
        if observation is None:
            rescue_missing.add(name)
            continue
        if observation.result == "pass":
            rescue_passed.add(name)
        elif observation.result == "cancelled":
            rescue_cancelled.add(name)
        elif observation.result == "fail":
            rescue_failed.add(name)
        else:
            rescue_pending.add(name)

    return ReviewSnapshot(
        pr_repo=pr_repo,
        pr_number=pr_number,
        review_refs=review_refs,
        pr_author=pr_payload.author,
        pr_body=pr_payload.body,
        pr_comment_bodies=tuple(
            str(comment.get("body") or "") for comment in pr_payload.comments
        ),
        copilot_review_present=copilot_review_present,
        codex_verdict=verdict,
        codex_gate_code=codex_gate_code,
        codex_gate_message=codex_gate_message,
        gate_status=gate_status,
        rescue_checks=rescue_checks,
        rescue_passed=rescue_passed,
        rescue_pending=rescue_pending,
        rescue_failed=rescue_failed,
        rescue_cancelled=rescue_cancelled,
        rescue_missing=rescue_missing,
    )
