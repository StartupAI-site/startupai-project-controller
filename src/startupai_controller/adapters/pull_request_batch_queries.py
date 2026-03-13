"""Batched GraphQL pull-request hydration helpers for the PR adapter."""

from __future__ import annotations

from collections.abc import Callable, Sequence

from startupai_controller.adapters.github_types import (
    GitHubActorPayload,
    GitHubCommentNode,
    GitHubReviewNode,
    GitHubStatusCheckRollupNode,
    PullRequestStateProbe,
    PullRequestViewPayload,
)

_PULL_REQUEST_VIEW_FIELDS = """
      number
      url
      state
      isDraft
      mergeStateStatus
      mergeable
      baseRefName
      headRefName
      mergedAt
      autoMergeRequest { enabledAt }
      body
      author { login }
      reviews(last: 100) {
        nodes {
          body
          submittedAt
          state
          author { login }
        }
      }
      comments(last: 100) {
        nodes {
          body
          createdAt
          author { login }
        }
      }
      commits(last: 1) {
        nodes {
          commit {
            statusCheckRollup {
              contexts(first: 100) {
                nodes {
                  __typename
                  ... on CheckRun {
                    name
                    status
                    conclusion
                    detailsUrl
                    completedAt
                    startedAt
                  }
                  ... on StatusContext {
                    context
                    state
                    targetUrl
                    createdAt
                  }
                }
              }
            }
          }
        }
      }
    """

_PULL_REQUEST_STATE_PROBE_FIELDS = """
      number
      state
      isDraft
      mergeStateStatus
      mergeable
      baseRefName
      headRefOid
      updatedAt
      autoMergeRequest { enabledAt }
      reviews(last: 1) {
        nodes { submittedAt }
      }
      comments(last: 1) {
        nodes { createdAt }
      }
      commits(last: 1) {
        nodes {
          commit {
            statusCheckRollup {
              contexts(first: 100) {
                nodes {
                  __typename
                  ... on CheckRun {
                    name
                    status
                    conclusion
                    detailsUrl
                    completedAt
                    startedAt
                  }
                  ... on StatusContext {
                    context
                    state
                    targetUrl
                    createdAt
                  }
                }
              }
            }
          }
        }
      }
    """


def _graphql_query_for_numbers(numbers: Sequence[int], fields: str) -> str:
    query_parts = "\n".join(
        f"pr_{number}: pullRequest(number: {number}) {{ {fields} }}"
        for number in numbers
    )
    return (
        "query($owner: String!, $repo: String!) {\n"
        "  repository(owner: $owner, name: $repo) {\n"
        f"{query_parts}\n"
        "  }\n"
        "}"
    )


def _author_payload(raw: object) -> GitHubActorPayload:
    node = raw if isinstance(raw, dict) else {}
    return GitHubActorPayload(login=str(node.get("login") or ""))


def _connection_nodes(node: dict[str, object], key: str) -> list[dict[str, object]]:
    payload = node.get(key)
    if not isinstance(payload, dict):
        return []
    nodes = payload.get("nodes")
    if not isinstance(nodes, list):
        return []
    return [item for item in nodes if isinstance(item, dict)]


def _comment_nodes(node: dict[str, object]) -> tuple[GitHubCommentNode, ...]:
    return tuple(
        GitHubCommentNode(
            body=str(item.get("body") or ""),
            createdAt=str(item.get("createdAt") or ""),
            author=_author_payload(item.get("author")),
            user=_author_payload(item.get("user")),
        )
        for item in _connection_nodes(node, "comments")
    )


def _review_nodes(node: dict[str, object]) -> tuple[GitHubReviewNode, ...]:
    return tuple(
        GitHubReviewNode(
            body=str(item.get("body") or ""),
            submittedAt=str(item.get("submittedAt") or ""),
            state=str(item.get("state") or ""),
            author=_author_payload(item.get("author")),
            user=_author_payload(item.get("user")),
        )
        for item in _connection_nodes(node, "reviews")
    )


def _status_rollup_nodes(
    node: dict[str, object],
    *,
    normalize_graphql_rollup_node: Callable[
        [dict[str, object]],
        GitHubStatusCheckRollupNode | None,
    ],
) -> tuple[GitHubStatusCheckRollupNode, ...]:
    commit_nodes = _connection_nodes(node, "commits")
    if not commit_nodes:
        return ()
    latest_commit = commit_nodes[-1]
    commit_payload = latest_commit.get("commit")
    if not isinstance(commit_payload, dict):
        return ()
    rollup_payload = commit_payload.get("statusCheckRollup")
    if not isinstance(rollup_payload, dict):
        return ()
    contexts_payload = rollup_payload.get("contexts")
    if not isinstance(contexts_payload, dict):
        return ()
    nodes = contexts_payload.get("nodes")
    if not isinstance(nodes, list):
        return ()

    normalized_nodes: list[GitHubStatusCheckRollupNode] = []
    for item in nodes:
        if not isinstance(item, dict):
            continue
        normalized = normalize_graphql_rollup_node(item)
        if normalized is not None:
            normalized_nodes.append(normalized)
    return tuple(normalized_nodes)


def _repository_payload(
    graphql: Callable[..., dict[str, object]],
    *,
    owner: str,
    repo: str,
    numbers: tuple[int, ...],
    fields: str,
) -> dict[str, object]:
    payload = graphql(
        _graphql_query_for_numbers(numbers, fields),
        fields=[
            "-f",
            f"owner={owner}",
            "-f",
            f"repo={repo}",
        ],
    )
    data = payload.get("data")
    if not isinstance(data, dict):
        return {}
    repository = data.get("repository")
    return repository if isinstance(repository, dict) else {}


def query_pull_request_view_payloads(
    *,
    graphql: Callable[..., dict[str, object]],
    pr_repo: str,
    pr_numbers: Sequence[int],
    normalize_graphql_rollup_node: Callable[
        [dict[str, object]],
        GitHubStatusCheckRollupNode | None,
    ],
) -> dict[int, PullRequestViewPayload]:
    """Return expanded PR payloads for a bounded set of PR numbers."""
    if "/" not in pr_repo:
        raise ValueError(f"pr_repo must be owner/repo, got '{pr_repo}'.")
    owner, repo = pr_repo.split("/", maxsplit=1)
    numbers = tuple(sorted({int(number) for number in pr_numbers}))
    if not numbers:
        return {}

    repository = _repository_payload(
        graphql,
        owner=owner,
        repo=repo,
        numbers=numbers,
        fields=_PULL_REQUEST_VIEW_FIELDS,
    )
    results: dict[int, PullRequestViewPayload] = {}
    for number in numbers:
        node = repository.get(f"pr_{number}")
        if not isinstance(node, dict):
            continue
        results[number] = PullRequestViewPayload(
            pr_repo=pr_repo,
            pr_number=number,
            url=str(node.get("url") or ""),
            head_ref_name=str(node.get("headRefName") or ""),
            author=str((_author_payload(node.get("author")).get("login") or ""))
            .strip()
            .lower(),
            body=str(node.get("body") or ""),
            state=str(node.get("state") or ""),
            is_draft=bool(node.get("isDraft", False)),
            merge_state_status=str(node.get("mergeStateStatus") or ""),
            mergeable=str(node.get("mergeable") or ""),
            base_ref_name=str(node.get("baseRefName") or "main"),
            merged_at=str(node.get("mergedAt") or ""),
            auto_merge_enabled=node.get("autoMergeRequest") is not None,
            comments=_comment_nodes(node),
            reviews=_review_nodes(node),
            status_check_rollup=_status_rollup_nodes(
                node,
                normalize_graphql_rollup_node=normalize_graphql_rollup_node,
            ),
        )
    return results


def query_pull_request_state_probes(
    *,
    graphql: Callable[..., dict[str, object]],
    pr_repo: str,
    pr_numbers: Sequence[int],
    normalize_graphql_rollup_node: Callable[
        [dict[str, object]],
        GitHubStatusCheckRollupNode | None,
    ],
    latest_node_timestamp: Callable[
        [Sequence[GitHubCommentNode | GitHubReviewNode], str], str
    ],
) -> dict[int, PullRequestStateProbe]:
    """Return lightweight PR probes for digest-based review scheduling."""
    if "/" not in pr_repo:
        raise ValueError(f"pr_repo must be owner/repo, got '{pr_repo}'.")
    owner, repo = pr_repo.split("/", maxsplit=1)
    numbers = tuple(sorted({int(number) for number in pr_numbers}))
    if not numbers:
        return {}

    repository = _repository_payload(
        graphql,
        owner=owner,
        repo=repo,
        numbers=numbers,
        fields=_PULL_REQUEST_STATE_PROBE_FIELDS,
    )
    results: dict[int, PullRequestStateProbe] = {}
    for number in numbers:
        node = repository.get(f"pr_{number}")
        if not isinstance(node, dict):
            continue
        comments = _comment_nodes(node)
        reviews = _review_nodes(node)
        results[number] = PullRequestStateProbe(
            pr_repo=pr_repo,
            pr_number=number,
            state=str(node.get("state") or ""),
            is_draft=bool(node.get("isDraft", False)),
            merge_state_status=str(node.get("mergeStateStatus") or ""),
            mergeable=str(node.get("mergeable") or ""),
            base_ref_name=str(node.get("baseRefName") or "main"),
            auto_merge_enabled=node.get("autoMergeRequest") is not None,
            head_ref_oid=str(node.get("headRefOid") or ""),
            updated_at=str(node.get("updatedAt") or ""),
            latest_comment_at=latest_node_timestamp(comments, "createdAt"),
            latest_review_at=latest_node_timestamp(reviews, "submittedAt"),
            status_check_rollup=_status_rollup_nodes(
                node,
                normalize_graphql_rollup_node=normalize_graphql_rollup_node,
            ),
        )
    return results
