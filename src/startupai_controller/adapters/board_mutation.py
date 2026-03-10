"""GitHub board mutation adapter.

Owns project board identity, field lookup, and board mutation mechanics for the
BoardMutationPort capability.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from startupai_controller.adapters.github_base import GitHubAdapterBase
from startupai_controller.validate_critical_path_promotion import GhQueryError, parse_issue_ref


@dataclass(frozen=True)
class _BoardItemInfo:
    """Minimal board identity/status needed for adapter-owned board mutations."""

    status: str
    item_id: str
    project_id: str


class GitHubBoardMutationAdapter(GitHubAdapterBase):
    """Capability adapter implementing project board mutation mechanics."""

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

    def set_project_single_select(
        self,
        project_id: str,
        item_id: str,
        field_name: str,
        option_name: str,
    ) -> None:
        """Set a project single-select field by raw project/item ids."""
        field_id, option_id = self._query_single_select_field_option(
            project_id,
            field_name,
            option_name,
        )
        self._set_project_single_select(
            project_id=project_id,
            item_id=item_id,
            field_id=field_id,
            option_id=option_id,
        )

    def set_project_text_field(
        self,
        project_id: str,
        item_id: str,
        field_name: str,
        value: str,
    ) -> None:
        """Set a project text field by raw project/item ids."""
        field_id = self._query_field_id(project_id, field_name)
        self._set_project_text_field(
            project_id=project_id,
            item_id=item_id,
            field_id=field_id,
            value=value,
        )

    def set_issue_status(self, issue_ref: str, status: str) -> None:
        """Set the board status for an issue."""
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
        """Set a text or single-select field on an issue."""
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
        """Post a comment on an issue."""
        owner, repo_name = repo.split("/", maxsplit=1) if "/" in repo else ("", repo)
        from startupai_controller.adapters.github_transport import _run_gh

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
        """Close an issue."""
        owner, repo_name = repo.split("/", maxsplit=1)
        from startupai_controller.adapters.github_transport import _run_gh

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

    def set_issue_assignees(
        self,
        repo: str,
        issue_number: int,
        assignees: Sequence[str],
    ) -> None:
        """Set issue assignees explicitly."""
        owner, repo_name = repo.split("/", maxsplit=1)
        args = [
            "api",
            f"repos/{owner}/{repo_name}/issues/{issue_number}",
            "-X",
            "PATCH",
        ]
        for login in assignees:
            args.extend(["-f", f"assignees[]={login}"])
        from startupai_controller.adapters.github_transport import _run_gh

        _run_gh(args, gh_runner=self._gh_runner, operation_type="mutation")
