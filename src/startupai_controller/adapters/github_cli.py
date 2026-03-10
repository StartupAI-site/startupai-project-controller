"""GitHub CLI adapter implementing review, PR, and board ports."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import json
import time

from startupai_controller.domain.models import (
    IssueContext,
    IssueFields,
    IssueSnapshot,
    OpenPullRequest,
    PrGateStatus,
    ReviewSnapshot,
)
from startupai_controller.domain.verdict_policy import (
    verdict_comment_body,
    verdict_marker_text,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
    parse_issue_ref,
)
from startupai_controller.board_io import (  # noqa: F401
    COPILOT_CODING_AGENT_LOGINS,
    CodexReviewVerdict,
    CycleBoardSnapshot,
    CycleGitHubMemo,
    LinkedIssue,
    PullRequestViewPayload,
    _ProjectItemSnapshot,
    _comment_activity_timestamp,
    _comment_exists,
    _is_automation_login,
    _is_copilot_coding_agent_actor,
    _is_pr_open,
    _issue_ref_to_repo_parts,
    _list_project_items,
    _list_project_items_by_status,
    _marker_for,
    _parse_codex_verdict_from_text,
    _parse_github_timestamp,
    _parse_pr_url,
    _post_comment,
    _query_failed_check_runs,
    _query_issue_assignees,
    _query_issue_comments,
    _query_issue_updated_at,
    _query_latest_marker_timestamp,
    _query_latest_matching_comment_timestamp,
    _query_latest_non_automation_comment_timestamp,
    _query_latest_wip_activity_timestamp,
    _query_open_pr_updated_at,
    _query_pr_head_sha,
    _query_project_item_field,
    _query_single_select_field_option,
    _repo_to_prefix,
    _run_gh,
    _set_issue_assignees,
    _set_text_field,
    _set_single_select_field,
    _set_status_if_changed,
    _snapshot_to_issue_ref,
    build_cycle_board_snapshot,
    build_pr_gate_status_from_payload,
    clear_cycle_board_snapshot_cache,
    close_issue,
    close_pull_request,
    gh_reason_code,
    has_copilot_review_signal_from_payload,
    latest_codex_verdict_from_payload,
    list_issue_comment_bodies,
    memoized_query_issue_body,
    memoized_query_pull_request_state_probes,
    memoized_query_pull_request_view_payloads,
    memoized_query_required_status_checks,
    query_latest_codex_verdict,
    review_state_digest_from_payload,
    review_state_digest_from_probe,
)


@dataclass(frozen=True)
class _BoardItemInfo:
    """Minimal board identity/status needed for adapter-owned board mutations."""

    status: str
    item_id: str
    project_id: str


@dataclass(frozen=True)
class _PullRequestListItem:
    """Minimal PR list payload used to build OpenPullRequest objects."""

    number: int
    url: str
    head_ref_name: str
    is_draft: bool
    body: str
    author: str


class GitHubCliAdapter:
    """Adapter wrapping gh CLI interactions behind port protocols."""

    _SINGLE_SELECT_FIELDS = frozenset(
        {
            "Status",
            "Priority",
            "Sprint",
            "Agent",
            "Executor",
            "Handoff To",
            "CI",
        }
    )

    def __init__(
        self,
        *,
        project_owner: str,
        project_number: int,
        config: CriticalPathConfig | None = None,
        github_memo: CycleGitHubMemo | None = None,
        gh_runner: Callable[..., str] | None = None,
    ) -> None:
        self._project_owner = project_owner
        self._project_number = project_number
        self._config = config
        self._github_memo = github_memo or CycleGitHubMemo()
        self._gh_runner = gh_runner

    def _require_config(self) -> CriticalPathConfig:
        if self._config is None:
            raise ValueError(
                "GitHubCliAdapter requires config for board-state operations"
            )
        return self._config

    def _graphql(self, query: str, *, fields: list[str]) -> dict:
        """Run a GraphQL request and return parsed JSON payload."""
        output = _run_gh(
            [
                "api",
                "graphql",
                "-f",
                f"query={query}",
                *fields,
            ],
            gh_runner=self._gh_runner,
        )
        try:
            payload = json.loads(output)
        except json.JSONDecodeError as error:
            raise GhQueryError("Invalid GraphQL JSON response.") from error
        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            messages = [
                err.get("message", "unknown GraphQL error")
                for err in errors
                if isinstance(err, dict)
            ]
            joined = "; ".join(messages) if messages else "unknown GraphQL error"
            raise GhQueryError(joined)
        return payload

    def _gh_json(
        self,
        args: list[str],
        *,
        operation_type: str = "query",
        error_message: str,
    ) -> object:
        """Run gh and parse the JSON payload with adapter-owned error shaping."""
        output = _run_gh(
            args,
            gh_runner=self._gh_runner,
            operation_type=operation_type,
        )
        try:
            return json.loads(output) if output else None
        except json.JSONDecodeError as error:
            raise GhQueryError(error_message) from error

    def _issue_ref_from_repo_slug(self, repo_slug: str, issue_number: int) -> str:
        """Map a repo slug + issue number to canonical issue_ref when config is known."""
        if self._config is not None:
            for prefix, slug in self._config.issue_prefixes.items():
                if slug == repo_slug:
                    return f"{prefix}#{issue_number}"
        return f"{repo_slug}#{issue_number}"

    def _query_board_info(self, issue_ref: str) -> _BoardItemInfo:
        """Query the issue's project board item, returning status + IDs for mutation."""
        config = self._require_config()
        parsed = parse_issue_ref(issue_ref)
        repo_slug = config.issue_prefixes.get(parsed.prefix)
        if not repo_slug:
            raise ValueError(
                f"Missing repo mapping for prefix '{parsed.prefix}' in issue_prefixes."
            )
        owner, repo = repo_slug.split("/", maxsplit=1)
        query = """
query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    issue(number: $number) {
      projectItems(first: 20) {
        nodes {
          id
          project {
            id
            owner {
              ... on Organization { login }
              ... on User { login }
            }
            number
          }
          statusField: fieldValueByName(name: "Status") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
        }
      }
    }
  }
}
"""
        payload = self._graphql(
            query,
            fields=[
                "-f",
                f"owner={owner}",
                "-f",
                f"repo={repo}",
                "-F",
                f"number={parsed.number}",
            ],
        )
        nodes = (
            payload.get("data", {})
            .get("repository", {})
            .get("issue", {})
            .get("projectItems", {})
            .get("nodes", [])
        )
        for node in nodes:
            project = node.get("project") or {}
            owner_data = project.get("owner") or {}
            owner_login = owner_data.get("login")
            number = project.get("number")
            if owner_login == self._project_owner and number == self._project_number:
                return _BoardItemInfo(
                    status=(node.get("statusField") or {}).get("name") or "UNKNOWN",
                    item_id=str(node.get("id") or ""),
                    project_id=str(project.get("id") or ""),
                )
        return _BoardItemInfo(status="NOT_ON_BOARD", item_id="", project_id="")

    def _pull_request_list_items(self, payload: object) -> list[_PullRequestListItem]:
        """Normalize gh `pr list` output into typed list items."""
        results: list[_PullRequestListItem] = []
        for item in payload or []:
            if not isinstance(item, dict):
                continue
            number = item.get("number")
            if not isinstance(number, int):
                continue
            results.append(
                _PullRequestListItem(
                    number=number,
                    url=str(item.get("url") or ""),
                    head_ref_name=str(item.get("headRefName") or ""),
                    is_draft=bool(item.get("isDraft", False)),
                    body=str(item.get("body") or ""),
                    author=str(((item.get("author") or {}).get("login") or "")).strip().lower(),
                )
            )
        return results

    def _to_open_pull_request(self, item: _PullRequestListItem) -> OpenPullRequest:
        """Convert a typed list payload into the domain PR type."""
        return OpenPullRequest(
            number=item.number,
            url=item.url,
            head_ref_name=item.head_ref_name,
            is_draft=item.is_draft,
            body=item.body,
            author=item.author,
        )

    def _query_project_field_value(self, issue_ref: str, field_name: str) -> str:
        """Read a project field value for a single issue."""
        config = self._require_config()
        parsed = parse_issue_ref(issue_ref)
        repo_slug = config.issue_prefixes.get(parsed.prefix)
        if not repo_slug:
            raise ValueError(
                f"Missing repo mapping for prefix '{parsed.prefix}' in issue_prefixes."
            )
        owner, repo = repo_slug.split("/", maxsplit=1)
        query = """
query($owner: String!, $repo: String!, $number: Int!, $fieldName: String!) {
  repository(owner: $owner, name: $repo) {
    issue(number: $number) {
      projectItems(first: 20) {
        nodes {
          project {
            owner {
              ... on Organization { login }
              ... on User { login }
            }
            number
          }
          fieldByName: fieldValueByName(name: $fieldName) {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
            ... on ProjectV2ItemFieldTextValue { text }
          }
        }
      }
    }
  }
}
"""
        payload = self._graphql(
            query,
            fields=[
                "-f",
                f"owner={owner}",
                "-f",
                f"repo={repo}",
                "-F",
                f"number={parsed.number}",
                "-f",
                f"fieldName={field_name}",
            ],
        )
        nodes = (
            payload.get("data", {})
            .get("repository", {})
            .get("issue", {})
            .get("projectItems", {})
            .get("nodes", [])
        )
        for node in nodes:
            project = node.get("project") or {}
            owner_data = project.get("owner") or {}
            owner_login = owner_data.get("login")
            proj_number = project.get("number")
            if owner_login == self._project_owner and proj_number == self._project_number:
                field_data = node.get("fieldByName") or {}
                return field_data.get("name") or field_data.get("text") or ""
        return ""

    def _query_field_id(self, project_id: str, field_name: str) -> str:
        """Resolve a project field id by name."""
        query = """
query($projectId: ID!, $fieldName: String!) {
  node(id: $projectId) {
    ... on ProjectV2 {
      field(name: $fieldName) {
        ... on ProjectV2Field { id }
        ... on ProjectV2SingleSelectField { id }
      }
    }
  }
}
"""
        payload = self._graphql(
            query,
            fields=[
                "-f",
                f"projectId={project_id}",
                "-f",
                f"fieldName={field_name}",
            ],
        )
        field = payload.get("data", {}).get("node", {}).get("field") or {}
        field_id = str(field.get("id") or "")
        if not field_id:
            raise GhQueryError(f"Field '{field_name}' not found on project.")
        return field_id

    def _query_single_select_field_option(
        self,
        project_id: str,
        field_name: str,
        option_name: str,
    ) -> tuple[str, str]:
        """Resolve a single-select field id and option id by name."""
        query = """
query($projectId: ID!, $fieldName: String!) {
  node(id: $projectId) {
    ... on ProjectV2 {
      field(name: $fieldName) {
        ... on ProjectV2SingleSelectField {
          id
          options { id name }
        }
      }
    }
  }
}
"""
        payload = self._graphql(
            query,
            fields=[
                "-f",
                f"projectId={project_id}",
                "-f",
                f"fieldName={field_name}",
            ],
        )
        field = payload.get("data", {}).get("node", {}).get("field") or {}
        field_id = str(field.get("id") or "")
        if not field_id:
            raise GhQueryError(f"Field '{field_name}' not found on project.")
        for option in field.get("options") or []:
            if option.get("name") == option_name:
                return field_id, str(option.get("id") or "")
        raise GhQueryError(
            f"Option '{option_name}' not found in field '{field_name}'."
        )

    def _set_project_single_select(
        self,
        *,
        project_id: str,
        item_id: str,
        field_id: str,
        option_id: str,
    ) -> None:
        """Update a project single-select field option."""
        mutation = """
mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $optionId: String!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $projectId,
    itemId: $itemId,
    fieldId: $fieldId,
    value: { singleSelectOptionId: $optionId }
  }) {
    projectV2Item { id }
  }
}
"""
        self._graphql(
            mutation,
            fields=[
                "-f",
                f"projectId={project_id}",
                "-f",
                f"itemId={item_id}",
                "-f",
                f"fieldId={field_id}",
                "-f",
                f"optionId={option_id}",
            ],
        )

    def _set_project_text_field(
        self,
        *,
        project_id: str,
        item_id: str,
        field_id: str,
        value: str,
    ) -> None:
        """Update a project text field value."""
        mutation = """
mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $textValue: String!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $projectId,
    itemId: $itemId,
    fieldId: $fieldId,
    value: { text: $textValue }
  }) {
    projectV2Item { id }
  }
}
"""
        self._graphql(
            mutation,
            fields=[
                "-f",
                f"projectId={project_id}",
                "-f",
                f"itemId={item_id}",
                "-f",
                f"fieldId={field_id}",
                "-f",
                f"textValue={value}",
            ],
        )

    def _query_pull_request_view_payload(
        self,
        pr_repo: str,
        pr_number: int,
    ) -> PullRequestViewPayload:
        """Return one expanded PR payload directly from gh."""
        payload = self._gh_json(
            [
                "pr",
                "view",
                str(pr_number),
                "--repo",
                pr_repo,
                "--json",
                (
                    "author,body,comments,reviews,state,isDraft,mergeStateStatus,"
                    "mergeable,baseRefName,autoMergeRequest,statusCheckRollup"
                ),
            ],
            error_message=f"Failed querying PR {pr_repo}#{pr_number}: invalid JSON.",
        )
        if not isinstance(payload, dict):
            raise GhQueryError(
                f"Failed querying PR {pr_repo}#{pr_number}: pull request not found."
            )
        comments = tuple(
            item for item in (payload.get("comments", []) or []) if isinstance(item, dict)
        )
        reviews = tuple(
            item for item in (payload.get("reviews", []) or []) if isinstance(item, dict)
        )
        status_check_rollup = tuple(
            item
            for item in (payload.get("statusCheckRollup", []) or [])
            if isinstance(item, dict)
        )
        return PullRequestViewPayload(
            pr_repo=pr_repo,
            pr_number=pr_number,
            author=str(((payload.get("author") or {}).get("login") or "")).strip().lower(),
            body=str(payload.get("body") or ""),
            state=str(payload.get("state") or ""),
            is_draft=bool(payload.get("isDraft", False)),
            merge_state_status=str(payload.get("mergeStateStatus") or ""),
            mergeable=str(payload.get("mergeable") or ""),
            base_ref_name=str(payload.get("baseRefName") or "main"),
            auto_merge_enabled=payload.get("autoMergeRequest") is not None,
            comments=comments,
            reviews=reviews,
            status_check_rollup=status_check_rollup,
        )

    def _query_closing_issue_refs(self, pr_repo: str, pr_number: int) -> tuple[str, ...]:
        """Return linked issue refs for one PR using the configured repo-prefix map."""
        config = self._require_config()
        pr_owner, pr_repo_name = pr_repo.split("/", maxsplit=1)
        query = """
query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      closingIssuesReferences(first: 50) {
        nodes {
          number
          repository { nameWithOwner }
        }
      }
    }
  }
}
"""
        payload = self._graphql(
            query,
            fields=[
                "-f",
                f"owner={pr_owner}",
                "-f",
                f"repo={pr_repo_name}",
                "-F",
                f"number={pr_number}",
            ],
        )
        nodes = (
            payload.get("data", {})
            .get("repository", {})
            .get("pullRequest", {})
            .get("closingIssuesReferences", {})
            .get("nodes", [])
        )
        refs: list[str] = []
        for node in nodes:
            if not isinstance(node, dict):
                continue
            issue_number = node.get("number")
            repo_with_owner = (node.get("repository") or {}).get("nameWithOwner", "")
            if not issue_number or not repo_with_owner:
                continue
            prefix = _repo_to_prefix(repo_with_owner, config)
            if prefix is None:
                continue
            refs.append(f"{prefix}#{issue_number}")
        return tuple(refs)

    def _query_required_status_checks(
        self,
        pr_repo: str,
        base_ref_name: str = "main",
    ) -> set[str]:
        """Query required status checks directly from branch protection."""
        key = (pr_repo, base_ref_name)
        cached = self._github_memo.required_status_checks.get(key)
        if cached is not None:
            return set(cached)
        owner, repo = pr_repo.split("/", maxsplit=1)
        payload = self._gh_json(
            [
                "api",
                f"repos/{owner}/{repo}/branches/{base_ref_name}/protection/required_status_checks",
            ],
            error_message=(
                f"Failed parsing branch protection for {pr_repo}:{base_ref_name}."
            ),
        )
        if not isinstance(payload, dict):
            raise GhQueryError(
                f"Failed querying branch protection for {pr_repo}:{base_ref_name}."
            )
        required: set[str] = set()
        for context in payload.get("contexts", []) or []:
            if isinstance(context, str) and context:
                required.add(context)
        for check in payload.get("checks", []) or []:
            if isinstance(check, dict):
                name = check.get("context")
                if isinstance(name, str) and name:
                    required.add(name)
        self._github_memo.required_status_checks[key] = set(required)
        return set(required)


    # -- PullRequestPort methods --

    def list_open_prs(self, repo: str) -> list[OpenPullRequest]:
        payload = self._gh_json(
            [
                "pr",
                "list",
                "--repo",
                repo,
                "--state",
                "open",
                "--limit",
                "100",
                "--json",
                "number,url,headRefName,isDraft,body,author",
            ],
            error_message=f"Failed querying open PRs for {repo}: invalid JSON.",
        )
        return [
            self._to_open_pull_request(item)
            for item in self._pull_request_list_items(payload)
        ]

    def get_pull_request(self, repo: str, number: int) -> OpenPullRequest | None:
        try:
            payload = self._query_pull_request_view_payload(repo, number)
        except Exception:
            return None
        return OpenPullRequest(
            number=number,
            url=payload.url,
            head_ref_name=payload.head_ref_name,
            is_draft=payload.is_draft,
            body=payload.body,
            author=payload.author,
        )

    def linked_issue_refs(self, pr_repo: str, pr_number: int) -> tuple[str, ...]:
        return self._query_closing_issue_refs(pr_repo, pr_number)

    def has_copilot_review_signal(self, pr_repo: str, pr_number: int) -> bool:
        payload = self._query_pull_request_view_payload(pr_repo, pr_number)
        return has_copilot_review_signal_from_payload(payload)

    def get_gate_status(self, pr_repo: str, pr_number: int) -> PrGateStatus:
        payload = self._query_pull_request_view_payload(pr_repo, pr_number)
        required = self._query_required_status_checks(
            pr_repo,
            payload.base_ref_name or "main",
        )
        return build_pr_gate_status_from_payload(payload, required=required)

    def required_status_checks(
        self, pr_repo: str, base_ref_name: str = "main"
    ) -> set[str]:
        return self._query_required_status_checks(pr_repo, base_ref_name)

    def list_open_prs_for_issue(
        self, repo: str, issue_number: int
    ) -> list[OpenPullRequest]:
        payload = self._gh_json(
            [
                "pr",
                "list",
                "--repo",
                repo,
                "--state",
                "open",
                "--search",
                f"Closes #{issue_number}",
                "--json",
                "number,url,headRefName,isDraft,body,author",
            ],
            error_message=f"Failed querying open PRs for {repo}: invalid JSON.",
        )
        return [
            self._to_open_pull_request(item)
            for item in self._pull_request_list_items(payload)
        ]

    def enable_automerge(
        self, pr_repo: str, pr_number: int, *, delete_branch: bool = False
    ) -> str:
        args = ["pr", "merge", str(pr_number), "--repo", pr_repo, "--auto", "--squash"]
        if delete_branch:
            args.append("--delete-branch")
        _run_gh(
            args,
            gh_runner=self._gh_runner,
            operation_type="automerge",
        )
        for _attempt in range(3):
            time.sleep(1.0)
            try:
                payload = self._gh_json(
                    [
                        "pr",
                        "view",
                        str(pr_number),
                        "--repo",
                        pr_repo,
                        "--json",
                        "autoMergeRequest",
                    ],
                    error_message=(
                        f"Failed querying automerge state for {pr_repo}#{pr_number}: invalid JSON."
                    ),
                )
                if isinstance(payload, dict) and payload.get("autoMergeRequest") is not None:
                    return "confirmed"
            except GhQueryError:
                continue
        return "pending"

    def rerun_failed_check(
        self, pr_repo: str, check_name: str, run_id: int
    ) -> bool:
        del check_name  # run id is the actual rerun handle
        try:
            _run_gh(
                ["run", "rerun", str(run_id), "--repo", pr_repo],
                gh_runner=self._gh_runner,
                operation_type="check_rerun",
            )
            return True
        except Exception:
            return False

    def update_branch(self, pr_repo: str, pr_number: int) -> None:
        _run_gh(
            ["pr", "update-branch", str(pr_number), "--repo", pr_repo],
            gh_runner=self._gh_runner,
            operation_type="mutation",
        )

    def review_state_digests(
        self, pr_refs: list[tuple[str, int]]
    ) -> dict[tuple[str, int], str]:
        digests: dict[tuple[str, int], str] = {}
        numbers_by_repo: dict[str, list[int]] = {}
        for pr_repo, pr_number in pr_refs:
            numbers_by_repo.setdefault(pr_repo, []).append(pr_number)

        for pr_repo, pr_numbers in sorted(numbers_by_repo.items()):
            probes = memoized_query_pull_request_state_probes(
                self._github_memo,
                pr_repo,
                tuple(sorted(set(pr_numbers))),
                gh_runner=self._gh_runner,
            )
            for pr_number, probe in probes.items():
                digests[(pr_repo, pr_number)] = review_state_digest_from_probe(probe)
        return digests

    def review_snapshots(
        self,
        review_refs_by_pr: dict[tuple[str, int], tuple[str, ...]],
        *,
        trusted_codex_actors: frozenset[str],
    ) -> dict[tuple[str, int], ReviewSnapshot]:
        snapshots: dict[tuple[str, int], ReviewSnapshot] = {}
        numbers_by_repo: dict[str, list[int]] = {}
        for pr_repo, pr_number in review_refs_by_pr:
            numbers_by_repo.setdefault(pr_repo, []).append(pr_number)

        for pr_repo, pr_numbers in sorted(numbers_by_repo.items()):
            payloads = memoized_query_pull_request_view_payloads(
                self._github_memo,
                pr_repo,
                tuple(sorted(set(pr_numbers))),
                gh_runner=self._gh_runner,
            )
            for pr_number in sorted(set(pr_numbers)):
                payload = payloads.get(pr_number)
                if payload is None:
                    from startupai_controller.validate_critical_path_promotion import (
                        GhQueryError,
                    )

                    raise GhQueryError(
                        f"Failed querying PR {pr_repo}#{pr_number}: pull request not found."
                    )
                required_checks = memoized_query_required_status_checks(
                    self._github_memo,
                    pr_repo,
                    payload.base_ref_name or "main",
                    gh_runner=self._gh_runner,
                )
                snapshots[(pr_repo, pr_number)] = _build_review_snapshot_from_payload(
                    pr_repo=pr_repo,
                    pr_number=pr_number,
                    review_refs=review_refs_by_pr[(pr_repo, pr_number)],
                    pr_payload=payload,
                    trusted_codex_actors=trusted_codex_actors,
                    required_checks=required_checks,
                )
        return snapshots

    def post_codex_verdict_if_missing(self, pr_url: str, session_id: str) -> bool:
        parsed = _parse_pr_url(pr_url)
        if parsed is None:
            from startupai_controller.validate_critical_path_promotion import GhQueryError

            raise GhQueryError(f"Invalid PR URL for codex verdict: {pr_url}")
        owner, repo, pr_number = parsed
        marker = verdict_marker_text(session_id)
        if _comment_exists(owner, repo, pr_number, marker, gh_runner=self._gh_runner):
            return False
        body = verdict_comment_body(session_id)
        _post_comment(owner, repo, pr_number, body, gh_runner=self._gh_runner)
        return True

    # -- IssueContextPort methods --

    def get_issue_context(self, owner: str, repo: str, number: int) -> IssueContext:
        output = _run_gh(
            [
                "api",
                f"repos/{owner}/{repo}/issues/{number}",
                "--jq",
                '{title: .title, body: .body, labels: [.labels[].name], updated_at: .updated_at}',
            ],
            gh_runner=self._gh_runner,
        )
        payload = json.loads(output)
        labels = payload.get("labels")
        if not isinstance(labels, list):
            labels = []
        return IssueContext(
            title=str(payload.get("title") or ""),
            body=str(payload.get("body") or ""),
            labels=tuple(str(label) for label in labels if str(label)),
            updated_at=str(payload.get("updated_at") or ""),
        )

    # -- BoardMutationPort methods --

    def set_issue_status(self, issue_ref: str, status: str) -> None:
        info = self._query_board_info(issue_ref)
        field_id, option_id = self._query_single_select_field_option(
            info.project_id,
            "Status",
            status,
        )
        self._set_project_single_select(
            project_id=info.project_id,
            item_id=info.item_id,
            field_id=field_id,
            option_id=option_id,
        )

    def set_issue_field(self, issue_ref: str, field_name: str, value: str) -> None:
        info = self._query_board_info(issue_ref)
        if field_name in self._SINGLE_SELECT_FIELDS:
            field_id, option_id = self._query_single_select_field_option(
                info.project_id,
                field_name,
                value,
            )
            self._set_project_single_select(
                project_id=info.project_id,
                item_id=info.item_id,
                field_id=field_id,
                option_id=option_id,
            )
        else:
            field_id = self._query_field_id(info.project_id, field_name)
            self._set_project_text_field(
                project_id=info.project_id,
                item_id=info.item_id,
                field_id=field_id,
                value=value,
            )

    def post_issue_comment(self, repo: str, issue_number: int, body: str) -> None:
        owner, repo_name = repo.split("/", maxsplit=1) if "/" in repo else ("", repo)
        _run_gh(
            [
                "api",
                f"repos/{owner}/{repo_name}/issues/{issue_number}/comments",
                "-f",
                f"body={body}",
            ],
            gh_runner=self._gh_runner,
            operation_type="mutation",
        )

    def close_issue(self, repo: str, issue_number: int) -> None:
        owner, repo_name = repo.split("/", maxsplit=1)
        _run_gh(
            [
                "api",
                f"repos/{owner}/{repo_name}/issues/{issue_number}",
                "-X",
                "PATCH",
                "-f",
                "state=closed",
            ],
            gh_runner=self._gh_runner,
            operation_type="mutation",
        )

    # -- ReviewStatePort methods --

    def get_issue_status(self, issue_ref: str) -> str | None:
        info = self._query_board_info(issue_ref)
        return info.status if info else None

    def list_issues_by_status(self, status: str) -> list[IssueSnapshot]:
        query = """
query($owner: String!, $number: Int!, $cursor: String) {
  organization(login: $owner) {
    projectV2(number: $number) {
      id
      items(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          fieldValueByName(name: "Status") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          executorField: fieldValueByName(name: "Executor") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          priorityField: fieldValueByName(name: "Priority") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          content {
            ... on Issue {
              number
              title
              repository { nameWithOwner }
            }
          }
        }
      }
    }
  }
}
"""
        results: list[IssueSnapshot] = []
        cursor = ""
        has_next = True
        while has_next:
            fields = [
                "-f",
                f"owner={self._project_owner}",
                "-F",
                f"number={self._project_number}",
                "-f",
                f"cursor={cursor}",
            ]
            payload = self._graphql(query, fields=fields)
            project_data = payload.get("data", {}).get("organization", {}).get("projectV2", {})
            project_id = str(project_data.get("id") or "")
            items_data = project_data.get("items", {})
            page_info = items_data.get("pageInfo", {})
            has_next = bool(page_info.get("hasNextPage", False))
            cursor = str(page_info.get("endCursor") or "")

            for node in items_data.get("nodes", []):
                node_status = ((node.get("fieldValueByName") or {}).get("name") or "")
                if node_status != status:
                    continue
                content = node.get("content") or {}
                issue_number = content.get("number")
                repo_with_owner = (content.get("repository") or {}).get("nameWithOwner", "")
                if not issue_number or not repo_with_owner:
                    continue
                results.append(
                    IssueSnapshot(
                        issue_ref=self._issue_ref_from_repo_slug(repo_with_owner, int(issue_number)),
                        status=node_status or status,
                        executor=str((node.get("executorField") or {}).get("name") or ""),
                        priority=str((node.get("priorityField") or {}).get("name") or ""),
                        title=str(content.get("title") or ""),
                        item_id=str(node.get("id") or ""),
                        project_id=project_id,
                    )
                )
        return results

    def get_issue_fields(self, issue_ref: str) -> IssueFields:
        def field(name: str) -> str:
            return self._query_project_field_value(issue_ref, name) or ""

        return IssueFields(
            issue_ref=issue_ref,
            status=field("Status"),
            priority=field("Priority"),
            sprint=field("Sprint"),
            executor=field("Executor"),
            owner=field("Owner"),
            handoff_to=field("Handoff To"),
            blocked_reason=field("Blocked Reason"),
        )


def query_open_pull_requests(
    pr_repo: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[OpenPullRequest]:
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
    adapter = GitHubCliAdapter(project_owner="", project_number=0, gh_runner=gh_runner)
    return adapter.list_open_prs(pr_repo)


def query_pull_request_view_payload(
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> PullRequestViewPayload:
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
    adapter = GitHubCliAdapter(project_owner="", project_number=0, gh_runner=gh_runner)
    return adapter._query_pull_request_view_payload(pr_repo, pr_number)


def query_required_status_checks(
    pr_repo: str,
    base_ref_name: str = "main",
    *,
    gh_runner: Callable[..., str] | None = None,
) -> set[str]:
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
    adapter = GitHubCliAdapter(project_owner="", project_number=0, gh_runner=gh_runner)
    return adapter._query_required_status_checks(pr_repo, base_ref_name)


def query_closing_issues(
    pr_owner: str,
    pr_repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[LinkedIssue]:
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
    adapter = GitHubCliAdapter(
        project_owner="",
        project_number=0,
        config=config,
        gh_runner=gh_runner,
    )
    refs = adapter._query_closing_issue_refs(f"{pr_owner}/{pr_repo}", pr_number)
    issues: list[LinkedIssue] = []
    for ref in refs:
        parsed = parse_issue_ref(ref)
        repo_slug = config.issue_prefixes.get(parsed.prefix)
        if not repo_slug:
            continue
        owner, repo = repo_slug.split("/", maxsplit=1)
        issues.append(
            LinkedIssue(
                owner=owner,
                repo=repo,
                number=parsed.number,
                ref=ref,
            )
        )
    return issues


def rerun_actions_run(
    pr_repo: str,
    run_id: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
    _run_gh(
        ["run", "rerun", str(run_id), "--repo", pr_repo],
        gh_runner=gh_runner,
        operation_type="check_rerun",
    )


def update_pull_request_branch(
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
    _run_gh(
        ["pr", "update-branch", str(pr_number), "--repo", pr_repo],
        gh_runner=gh_runner,
        operation_type="mutation",
    )


def enable_pull_request_automerge(
    pr_repo: str,
    pr_number: int,
    *,
    delete_branch: bool = False,
    confirm_retries: int = 3,
    confirm_delay_seconds: float = 1.0,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Compatibility wrapper implemented on the adapter-owned mechanism."""
    args = ["pr", "merge", str(pr_number), "--repo", pr_repo, "--auto", "--squash"]
    if delete_branch:
        args.append("--delete-branch")
    _run_gh(
        args,
        gh_runner=gh_runner,
        operation_type="automerge",
    )
    adapter = GitHubCliAdapter(project_owner="", project_number=0, gh_runner=gh_runner)
    for _attempt in range(confirm_retries):
        time.sleep(confirm_delay_seconds)
        try:
            payload = adapter._gh_json(
                [
                    "pr",
                    "view",
                    str(pr_number),
                    "--repo",
                    pr_repo,
                    "--json",
                    "autoMergeRequest",
                ],
                error_message=(
                    f"Failed querying automerge state for {pr_repo}#{pr_number}: invalid JSON."
                ),
            )
            if isinstance(payload, dict) and payload.get("autoMergeRequest") is not None:
                return "confirmed"
        except GhQueryError:
            continue
    return "pending"


def _codex_gate_from_payload(
    pr_repo: str,
    pr_number: int,
    *,
    review_refs: tuple[str, ...],
    verdict: CodexReviewVerdict | None,
) -> tuple[int, str]:
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


def _build_review_snapshot_from_payload(
    *,
    pr_repo: str,
    pr_number: int,
    review_refs: tuple[str, ...],
    pr_payload: PullRequestViewPayload,
    trusted_codex_actors: frozenset[str],
    required_checks: set[str],
) -> ReviewSnapshot:
    copilot_review_present = has_copilot_review_signal_from_payload(pr_payload)
    verdict = latest_codex_verdict_from_payload(
        pr_payload,
        trusted_actors=trusted_codex_actors,
    )
    codex_gate_code, codex_gate_message = _codex_gate_from_payload(
        pr_repo,
        pr_number,
        review_refs=review_refs,
        verdict=verdict,
    )
    gate_status = build_pr_gate_status_from_payload(
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
