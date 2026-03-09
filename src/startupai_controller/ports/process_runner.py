"""GhRunnerPort — typed gh CLI runner.

Port protocol replacing bare Callable for gh CLI execution.
"""

from __future__ import annotations

from typing import Protocol


class GhRunnerPort(Protocol):
    """Typed GitHub CLI runner."""

    def run_gh(self, args: list[str], *, check: bool = True) -> str:
        """Execute a gh CLI command and return stdout."""
        ...
