from __future__ import annotations

from types import SimpleNamespace

from startupai_controller.adapters.board_mutation import GitHubBoardMutationAdapter
from startupai_controller.runtime.wiring import build_github_port_bundle


def _config() -> SimpleNamespace:
    return SimpleNamespace(issue_prefixes={"crew": "StartupAI-site/startupai-crew"})


def test_set_issue_status_uses_board_info_and_field_option(monkeypatch) -> None:
    field_calls: list[tuple[str, str, str]] = []
    mutate_calls: list[tuple[str, str, str, str]] = []

    monkeypatch.setattr(
        GitHubBoardMutationAdapter,
        "_query_board_info",
        lambda self, issue_ref: SimpleNamespace(
            status="Ready", item_id="ITEM", project_id="PROJ"
        ),
    )
    monkeypatch.setattr(
        GitHubBoardMutationAdapter,
        "_query_single_select_field_option",
        lambda self, project_id, field_name, option_name: (
            field_calls.append((project_id, field_name, option_name)) or ("FIELD", "OPT")
        ),
    )
    monkeypatch.setattr(
        GitHubBoardMutationAdapter,
        "_set_project_single_select",
        lambda self, project_id, item_id, field_id, option_id: mutate_calls.append(
            (project_id, item_id, field_id, option_id)
        ),
    )
    adapter = GitHubBoardMutationAdapter(
        project_owner="StartupAI-site",
        project_number=1,
        config=_config(),
    )

    adapter.set_issue_status("crew#42", "Blocked")

    assert field_calls == [("PROJ", "Status", "Blocked")]
    assert mutate_calls == [("PROJ", "ITEM", "FIELD", "OPT")]


def test_set_issue_field_text_path_uses_text_field_mutation(monkeypatch) -> None:
    text_calls: list[tuple[str, str, str, str]] = []

    monkeypatch.setattr(
        GitHubBoardMutationAdapter,
        "_query_board_info",
        lambda self, issue_ref: SimpleNamespace(
            status="Ready", item_id="ITEM", project_id="PROJ"
        ),
    )
    monkeypatch.setattr(
        GitHubBoardMutationAdapter,
        "_query_field_id",
        lambda self, project_id, field_name: "FIELD",
    )
    monkeypatch.setattr(
        GitHubBoardMutationAdapter,
        "_set_project_text_field",
        lambda self, project_id, item_id, field_id, value: text_calls.append(
            (project_id, item_id, field_id, value)
        ),
    )
    adapter = GitHubBoardMutationAdapter(
        project_owner="StartupAI-site",
        project_number=1,
        config=_config(),
    )

    adapter.set_issue_field("crew#42", "Blocked Reason", "needs escalation")

    assert text_calls == [("PROJ", "ITEM", "FIELD", "needs escalation")]


def test_runtime_wiring_uses_dedicated_board_mutation_adapter() -> None:
    bundle = build_github_port_bundle(
        "StartupAI-site",
        1,
        config=_config(),
    )

    assert isinstance(bundle.board_mutations, GitHubBoardMutationAdapter)
