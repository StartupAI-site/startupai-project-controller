"""Codex review gate use case."""

from __future__ import annotations

from typing import Callable, Sequence


def codex_review_gate(
    pr_repo: str,
    pr_number: int,
    *,
    linked_refs: Sequence[str],
    review_refs: Sequence[str],
    verdict,
    dry_run: bool = False,
    apply_fail_routing: bool = True,
    fail_router: Callable[..., None] | None = None,
) -> tuple[int, str]:
    """Evaluate the codex gate after transport data has been loaded."""
    if not linked_refs:
        return 0, (
            f"{pr_repo}#{pr_number}: codex gate not applicable " "(no linked issues)"
        )

    if not review_refs:
        return 0, (
            f"{pr_repo}#{pr_number}: codex gate not applicable "
            f"(linked issues not in Review: {list(linked_refs)})"
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

    if apply_fail_routing and fail_router is not None:
        for issue_ref in review_refs:
            fail_router(
                issue_ref=issue_ref,
                route=verdict.route,
                checklist=verdict.checklist,
                dry_run=dry_run,
            )

    return 2, (
        f"{pr_repo}#{pr_number}: codex-review=fail "
        f"(route={verdict.route}, source={verdict.source}, actor={verdict.actor})"
    )
