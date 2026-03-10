from __future__ import annotations

import json
from types import SimpleNamespace

from startupai_controller.adapters.github_cli import GitHubCliAdapter
from startupai_controller.board_io import _ProjectItemSnapshot
from startupai_controller.domain.models import IssueContext, IssueSnapshot
from startupai_controller.promote_ready import BoardInfo


def _config() -> SimpleNamespace:
    return SimpleNamespace(issue_prefixes={"crew": "StartupAI-site/startupai-crew"})


def test_list_open_prs_for_issue_searches_by_issue_number(monkeypatch) -> None:
    recorded: list[list[str]] = []

    def fake_run_gh(args: list[str], *, gh_runner=None, operation_type="query") -> str:
        recorded.append(args)
        return json.dumps(
            [
                {
                    "number": 7,
                    "url": "https://github.com/StartupAI-site/startupai-crew/pull/7",
                    "headRefName": "fix/example",
                    "isDraft": False,
                    "body": "Closes #42",
                    "author": {"login": "codex"},
                }
            ]
        )

    monkeypatch.setattr("startupai_controller.board_io._run_gh", fake_run_gh)
    adapter = GitHubCliAdapter(project_owner="StartupAI-site", project_number=1)

    prs = adapter.list_open_prs_for_issue("StartupAI-site/startupai-crew", 42)

    assert recorded[0][0:6] == [
        "pr",
        "list",
        "--repo",
        "StartupAI-site/startupai-crew",
        "--state",
        "open",
    ]
    assert "--search" in recorded[0]
    assert "Closes #42" in recorded[0]
    assert prs[0].number == 7
    assert prs[0].author == "codex"


def test_list_open_prs_delegates_to_query_open_pull_requests(monkeypatch) -> None:
    monkeypatch.setattr(
        "startupai_controller.adapters.github_cli.query_open_pull_requests",
        lambda repo, gh_runner=None: [
            SimpleNamespace(
                number=11,
                url=f"https://example.com/{repo}/11",
                head_ref_name="feat/example",
                is_draft=False,
                body="Closes #11",
                author="codex",
            )
        ],
    )
    adapter = GitHubCliAdapter(project_owner="StartupAI-site", project_number=1)

    prs = adapter.list_open_prs("StartupAI-site/startupai-crew")

    assert len(prs) == 1
    assert prs[0].number == 11
    assert prs[0].author == "codex"


def test_linked_issue_refs_uses_configured_query(monkeypatch) -> None:
    monkeypatch.setattr(
        "startupai_controller.adapters.github_cli.query_closing_issues",
        lambda owner, repo, pr_number, config, gh_runner=None: [
            SimpleNamespace(ref="crew#42"),
            SimpleNamespace(ref="crew#43"),
        ],
    )
    adapter = GitHubCliAdapter(
        project_owner="StartupAI-site",
        project_number=1,
        config=_config(),
    )

    refs = adapter.linked_issue_refs("StartupAI-site/startupai-crew", 42)

    assert refs == ("crew#42", "crew#43")


def test_has_copilot_review_signal_uses_payload_projection(monkeypatch) -> None:
    monkeypatch.setattr(
        "startupai_controller.adapters.github_cli.query_pull_request_view_payload",
        lambda repo, number, gh_runner=None: {"number": number},
    )
    monkeypatch.setattr(
        "startupai_controller.adapters.github_cli.has_copilot_review_signal_from_payload",
        lambda payload: payload == {"number": 42},
    )
    adapter = GitHubCliAdapter(project_owner="StartupAI-site", project_number=1)

    assert (
        adapter.has_copilot_review_signal(
            "StartupAI-site/startupai-crew",
            42,
        )
        is True
    )


def test_set_issue_status_uses_board_info_and_field_option(monkeypatch) -> None:
    board_info = BoardInfo(status="Ready", item_id="ITEM", project_id="PROJ")
    field_calls: list[tuple[str, str]] = []
    mutate_calls: list[tuple[str, str, str, str]] = []

    monkeypatch.setattr(
        GitHubCliAdapter,
        "_query_board_info",
        lambda self, issue_ref: board_info,
    )
    monkeypatch.setattr(
        "startupai_controller.board_io._query_status_field_option",
        lambda project_id, status, gh_runner=None: (
            field_calls.append((project_id, status)) or ("FIELD", "OPT")
        ),
    )
    monkeypatch.setattr(
        "startupai_controller.board_io._set_board_status",
        lambda project_id, item_id, field_id, option_id, gh_runner=None: mutate_calls.append(
            (project_id, item_id, field_id, option_id)
        ),
    )
    adapter = GitHubCliAdapter(
        project_owner="StartupAI-site",
        project_number=1,
        config=_config(),
    )

    adapter.set_issue_status("crew#42", "Blocked")

    assert field_calls == [("PROJ", "Blocked")]
    assert mutate_calls == [("PROJ", "ITEM", "FIELD", "OPT")]


def test_get_issue_context_returns_typed_context(monkeypatch) -> None:
    monkeypatch.setattr(
        "startupai_controller.adapters.github_cli._run_gh",
        lambda args, gh_runner=None: json.dumps(
            {
                "title": "Issue title",
                "body": "Issue body",
                "labels": ["bug", "urgent"],
                "updated_at": "2026-03-10T12:00:00+00:00",
            }
        ),
    )
    adapter = GitHubCliAdapter(project_owner="StartupAI-site", project_number=1)

    context = adapter.get_issue_context(
        "StartupAI-site",
        "startupai-crew",
        42,
    )

    assert context == IssueContext(
        title="Issue title",
        body="Issue body",
        labels=("bug", "urgent"),
        updated_at="2026-03-10T12:00:00+00:00",
    )


def test_set_issue_field_routes_single_select_fields(monkeypatch) -> None:
    board_info = BoardInfo(status="Ready", item_id="ITEM", project_id="PROJ")
    select_calls: list[tuple[str, str, str, str]] = []

    monkeypatch.setattr(
        GitHubCliAdapter,
        "_query_board_info",
        lambda self, issue_ref: board_info,
    )
    monkeypatch.setattr(
        "startupai_controller.adapters.github_cli._set_single_select_field",
        lambda project_id, item_id, field_name, option_name, gh_runner=None: select_calls.append(
            (project_id, item_id, field_name, option_name)
        ),
    )
    adapter = GitHubCliAdapter(
        project_owner="StartupAI-site",
        project_number=1,
        config=_config(),
    )

    adapter.set_issue_field("crew#42", "Handoff To", "claude")

    assert select_calls == [("PROJ", "ITEM", "Handoff To", "claude")]


def test_required_status_checks_delegates_to_query(monkeypatch) -> None:
    monkeypatch.setattr(
        "startupai_controller.adapters.github_cli.query_required_status_checks",
        lambda pr_repo, base_ref_name, gh_runner=None: {
            f"{pr_repo}:{base_ref_name}"
        },
    )
    adapter = GitHubCliAdapter(project_owner="StartupAI-site", project_number=1)

    required = adapter.required_status_checks(
        "StartupAI-site/startupai-crew",
        "main",
    )

    assert required == {"StartupAI-site/startupai-crew:main"}


def test_review_snapshots_batches_queries_by_repo(monkeypatch) -> None:
    payload_calls: list[tuple[str, tuple[int, ...]]] = []
    required_calls: list[tuple[str, str]] = []

    def fake_payloads(memo, pr_repo, pr_numbers, *, gh_runner=None):
        normalized = tuple(pr_numbers)
        payload_calls.append((pr_repo, normalized))
        return {
            number: SimpleNamespace(
                author="codex-bot",
                body=f"Closes #{number}",
                comments=(),
                base_ref_name="main",
            )
            for number in normalized
        }

    def fake_required(memo, pr_repo, base_ref_name, *, gh_runner=None):
        required_calls.append((pr_repo, base_ref_name))
        return {"ci"}

    monkeypatch.setattr(
        "startupai_controller.adapters.github_cli.memoized_query_pull_request_view_payloads",
        fake_payloads,
    )
    monkeypatch.setattr(
        "startupai_controller.adapters.github_cli.memoized_query_required_status_checks",
        fake_required,
    )
    monkeypatch.setattr(
        "startupai_controller.adapters.github_cli.has_copilot_review_signal_from_payload",
        lambda payload: False,
    )
    monkeypatch.setattr(
        "startupai_controller.adapters.github_cli.latest_codex_verdict_from_payload",
        lambda payload, trusted_actors: SimpleNamespace(
            decision="pass",
            source="comment",
            actor="codex",
        ),
    )
    monkeypatch.setattr(
        "startupai_controller.adapters.github_cli.build_pr_gate_status_from_payload",
        lambda payload, required: SimpleNamespace(
            required=set(required),
            checks={name: SimpleNamespace(result="pass") for name in required},
        ),
    )

    adapter = GitHubCliAdapter(project_owner="StartupAI-site", project_number=1)

    snapshots = adapter.review_snapshots(
        {
            ("StartupAI-site/startupai-crew", 210): ("crew#84",),
            ("StartupAI-site/startupai-crew", 211): ("crew#85",),
            ("StartupAI-site/app.startupai-site", 300): ("app#17",),
        },
        trusted_codex_actors=frozenset({"codex"}),
    )

    assert payload_calls == [
        ("StartupAI-site/app.startupai-site", (300,)),
        ("StartupAI-site/startupai-crew", (210, 211)),
    ]
    assert required_calls == [
        ("StartupAI-site/app.startupai-site", "main"),
        ("StartupAI-site/startupai-crew", "main"),
        ("StartupAI-site/startupai-crew", "main"),
    ]
    assert snapshots[("StartupAI-site/startupai-crew", 210)].review_refs == ("crew#84",)
    assert snapshots[("StartupAI-site/startupai-crew", 211)].review_refs == ("crew#85",)


def test_get_issue_fields_passes_config_to_field_queries(monkeypatch) -> None:
    calls: list[tuple[str, str, object, str, int]] = []
    values = {
        "Status": "Ready",
        "Priority": "P1",
        "Sprint": "S1",
        "Executor": "codex",
        "Owner": "codex:local-consumer",
        "Handoff To": "none",
        "Blocked Reason": "",
    }

    def fake_query(issue_ref, field_name, config, project_owner, project_number, *, gh_runner=None):
        calls.append((issue_ref, field_name, config, project_owner, project_number))
        return values[field_name]

    monkeypatch.setattr("startupai_controller.board_io._query_project_item_field", fake_query)
    cfg = _config()
    adapter = GitHubCliAdapter(
        project_owner="StartupAI-site",
        project_number=1,
        config=cfg,
    )

    fields = adapter.get_issue_fields("crew#42")

    assert fields.status == "Ready"
    assert fields.executor == "codex"
    assert all(call[2] is cfg for call in calls)
    assert all(call[3:] == ("StartupAI-site", 1) for call in calls)


def test_list_issues_by_status_maps_issue_refs_through_config(monkeypatch) -> None:
    snapshot = _ProjectItemSnapshot(
        issue_ref="StartupAI-site/startupai-crew#42",
        status="Review",
        executor="codex",
        handoff_to="none",
        priority="P0",
        item_id="ITEM",
        project_id="PROJ",
        title="Example issue",
    )
    monkeypatch.setattr(
        "startupai_controller.board_io._list_project_items_by_status",
        lambda status, project_owner, project_number, gh_runner=None: [snapshot],
    )
    adapter = GitHubCliAdapter(
        project_owner="StartupAI-site",
        project_number=1,
        config=_config(),
    )

    results = adapter.list_issues_by_status("Review")

    assert results == [
        IssueSnapshot(
            issue_ref="crew#42",
            status="Review",
            executor="codex",
            priority="P0",
            title="Example issue",
            item_id="ITEM",
            project_id="PROJ",
        )
    ]


def test_review_state_digests_batches_by_repo(monkeypatch) -> None:
    calls: list[tuple[str, tuple[int, ...]]] = []

    def fake_probes(memo, pr_repo, pr_numbers, *, gh_runner=None):
        calls.append((pr_repo, pr_numbers))
        return {
            number: {"repo": pr_repo, "number": number}
            for number in pr_numbers
        }

    monkeypatch.setattr(
        "startupai_controller.adapters.github_cli.memoized_query_pull_request_state_probes",
        fake_probes,
    )
    monkeypatch.setattr(
        "startupai_controller.adapters.github_cli.review_state_digest_from_probe",
        lambda probe: f"{probe['repo']}#{probe['number']}",
    )
    adapter = GitHubCliAdapter(project_owner="StartupAI-site", project_number=1)

    digests = adapter.review_state_digests(
        [
            ("StartupAI-site/startupai-crew", 7),
            ("StartupAI-site/startupai-crew", 9),
            ("StartupAI-site/app.startupai.site", 3),
        ]
    )

    assert calls == [
        ("StartupAI-site/app.startupai.site", (3,)),
        ("StartupAI-site/startupai-crew", (7, 9)),
    ]
    assert digests == {
        ("StartupAI-site/startupai-crew", 7): "StartupAI-site/startupai-crew#7",
        ("StartupAI-site/startupai-crew", 9): "StartupAI-site/startupai-crew#9",
        ("StartupAI-site/app.startupai.site", 3): "StartupAI-site/app.startupai.site#3",
    }


def test_post_codex_verdict_if_missing_checks_marker_first(monkeypatch) -> None:
    checker_calls: list[tuple[str, str, int, str]] = []
    poster_calls: list[tuple[str, str, int, str]] = []

    monkeypatch.setattr(
        "startupai_controller.adapters.github_cli._comment_exists",
        lambda owner, repo, number, marker, gh_runner=None: (
            checker_calls.append((owner, repo, number, marker)) or False
        ),
    )
    monkeypatch.setattr(
        "startupai_controller.adapters.github_cli._post_comment",
        lambda owner, repo, number, body, gh_runner=None: poster_calls.append(
            (owner, repo, number, body)
        ),
    )
    adapter = GitHubCliAdapter(project_owner="StartupAI-site", project_number=1)

    posted = adapter.post_codex_verdict_if_missing(
        "https://github.com/StartupAI-site/startupai-crew/pull/42",
        "session-123",
    )

    assert posted is True
    assert checker_calls == [
        (
            "StartupAI-site",
            "startupai-crew",
            42,
            "<!-- startupai-board-bot:codex-verdict:session=session-123 -->",
        )
    ]
    assert len(poster_calls) == 1
    assert poster_calls[0][0:3] == ("StartupAI-site", "startupai-crew", 42)
    assert "<!-- startupai-board-bot:codex-verdict:session=session-123 -->" in poster_calls[0][3]
    assert "codex-review: pass" in poster_calls[0][3]
