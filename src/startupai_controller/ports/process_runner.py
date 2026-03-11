"""Process and gh runner ports for local command execution."""

from __future__ import annotations

from typing import Any, Protocol


class ProcessRunnerPort(Protocol):
    """Typed local process runner."""

    def run(self, args: list[str], **kwargs: Any) -> Any:
        """Execute a local process command."""
        ...


class GhRunnerPort(Protocol):
    """Typed GitHub CLI runner."""

    def run_gh(self, args: list[str], *, check: bool = True) -> str:
        """Execute a gh CLI command and return stdout."""
        ...
