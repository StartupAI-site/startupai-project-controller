from __future__ import annotations

from startupai_controller.adapters.local_process import LocalProcessAdapter


def test_run_gh_passes_args_list_without_splatting() -> None:
    recorded: list[tuple[list[str], bool]] = []

    def fake_runner(args: list[str], *, check: bool = True) -> str:
        recorded.append((args, check))
        return "ok"

    adapter = LocalProcessAdapter(gh_runner=fake_runner)

    result = adapter.run_gh(["api", "/rate_limit"], check=False)

    assert result == "ok"
    assert recorded == [(["api", "/rate_limit"], False)]
