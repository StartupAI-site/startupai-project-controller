"""Ready-item selection helpers."""

from __future__ import annotations

from typing import Any, Callable


def select_best_candidate(
    config: Any,
    project_owner: str,
    project_number: int,
    *,
    executor: str = "codex",
    this_repo_prefix: str | None = None,
    repo_prefixes: tuple[str, ...] = ("crew",),
    status_resolver: Callable[..., str] | None = None,
    ready_items: tuple[Any, ...] | None = None,
    github_memo: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
    issue_filter: Callable[[str], bool] | None = None,
    build_github_port_bundle: Callable[..., Any],
    parse_issue_ref: Callable[[str], Any],
    config_error_type: type[Exception],
    snapshot_to_issue_ref: Callable[[str, Any], str | None],
    in_any_critical_path: Callable[[Any, str], bool],
    evaluate_ready_promotion: Callable[..., tuple[int, str | None]],
    ready_snapshot_rank: Callable[[Any, Any], Any],
) -> str | None:
    """Select the highest-ranked ready issue for the executor."""
    if this_repo_prefix is not None:
        repo_prefixes = (this_repo_prefix,)
    ready_items = ready_items or tuple(
        build_github_port_bundle(
            project_owner,
            project_number,
            config=config,
            github_memo=github_memo,
            gh_runner=gh_runner,
        ).review_state.list_issues_by_status("Ready")
    )
    eligible: list[Any] = []
    for snapshot in ready_items:
        if snapshot.executor.strip().lower() != executor:
            continue
        ref = snapshot.issue_ref
        try:
            parsed_ref = parse_issue_ref(ref)
        except config_error_type:
            ref = snapshot_to_issue_ref(snapshot.issue_ref, config.issue_prefixes)
            if ref is None:
                continue
            parsed_ref = parse_issue_ref(ref)
        if parsed_ref.prefix not in repo_prefixes:
            continue
        if issue_filter is not None and not issue_filter(ref):
            continue
        if in_any_critical_path(config, ref):
            is_ready = None
            if github_memo is not None:
                is_ready = github_memo.dependency_ready.get(ref)
            if is_ready is None:
                val_code, _ = evaluate_ready_promotion(
                    issue_ref=ref,
                    config=config,
                    project_owner=project_owner,
                    project_number=project_number,
                    status_resolver=status_resolver,
                    require_in_graph=True,
                )
                is_ready = val_code == 0
                if github_memo is not None:
                    github_memo.dependency_ready[ref] = is_ready
            if not is_ready:
                continue
        eligible.append(snapshot)

    if not eligible:
        return None

    eligible.sort(key=lambda snapshot: ready_snapshot_rank(snapshot, config))
    return snapshot_to_issue_ref(eligible[0].issue_ref, config.issue_prefixes)


def list_project_items_by_status(
    status: str,
    project_owner: str,
    project_number: int,
    *,
    config: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
    build_github_port_bundle: Callable[..., Any],
) -> list[Any]:
    """Read board items for one status through the review-state port."""
    return list(
        build_github_port_bundle(
            project_owner,
            project_number,
            config=config,
            gh_runner=gh_runner,
        ).review_state.build_board_snapshot().items_with_status(status)
    )
