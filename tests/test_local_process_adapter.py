from __future__ import annotations

import subprocess

from startupai_controller.adapters.local_process import LocalProcessAdapter
from startupai_controller.domain.models import RepairBranchReconcileOutcome, WorktreeEntry


def test_run_gh_passes_args_list_without_splatting() -> None:
    recorded: list[tuple[list[str], bool]] = []

    def fake_runner(args: list[str], *, check: bool = True) -> str:
        recorded.append((args, check))
        return "ok"

    adapter = LocalProcessAdapter(gh_runner=fake_runner)

    result = adapter.run_gh(["api", "/rate_limit"], check=False)

    assert result == "ok"
    assert recorded == [(["api", "/rate_limit"], False)]


def test_run_delegates_to_subprocess_runner() -> None:
    recorded: list[tuple[list[str], dict[str, object]]] = []

    def fake_subprocess_runner(args, **kwargs):
        recorded.append((list(args), dict(kwargs)))
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout="ok",
            stderr="",
        )

    adapter = LocalProcessAdapter(subprocess_runner=fake_subprocess_runner)

    result = adapter.run(["git", "status"], capture_output=True, text=True)

    assert result.stdout == "ok"
    assert recorded == [
        (
            ["git", "status"],
            {"capture_output": True, "text": True},
        )
    ]


def test_list_worktrees_parses_porcelain_output() -> None:
    def fake_subprocess_runner(args, **kwargs):
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout=(
                "worktree /tmp/wt/one\n"
                "branch refs/heads/feat/one\n\n"
                "worktree /tmp/wt/two\n"
                "branch refs/heads/fix/two\n\n"
            ),
            stderr="",
        )

    adapter = LocalProcessAdapter(subprocess_runner=fake_subprocess_runner)

    records = adapter.list_worktrees("/tmp/repo")

    assert records == [
        WorktreeEntry(path="/tmp/wt/one", branch_name="feat/one"),
        WorktreeEntry(path="/tmp/wt/two", branch_name="fix/two"),
    ]


def test_reconcile_repair_branch_reports_conflicted_main_merge() -> None:
    calls: list[list[str]] = []

    def fake_subprocess_runner(args, **kwargs):
        calls.append(list(args))
        if args[-2:] == ["checkout", "fix/example"]:
            return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
        if args[-2:] == ["status", "--porcelain"]:
            return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
        if args[-3:] == ["origin", "main", "fix/example"]:
            return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
        if "rev-list" in args:
            return subprocess.CompletedProcess(args=args, returncode=0, stdout="0 0\n", stderr="")
        if args[-3:] == ["merge", "--no-edit", "origin/main"]:
            return subprocess.CompletedProcess(args=args, returncode=1, stdout="", stderr="conflict")
        if args[-3:] == ["-q", "--verify", "MERGE_HEAD"]:
            return subprocess.CompletedProcess(args=args, returncode=0, stdout="MERGE_HEAD\n", stderr="")
        raise AssertionError(args)

    adapter = LocalProcessAdapter(subprocess_runner=fake_subprocess_runner)

    outcome = adapter.reconcile_repair_branch("/tmp/wt", "fix/example")

    assert outcome == RepairBranchReconcileOutcome("conflicted_main_merge")
