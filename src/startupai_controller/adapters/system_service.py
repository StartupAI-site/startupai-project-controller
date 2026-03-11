"""Local system-service adapter."""

from __future__ import annotations

import subprocess
from typing import Callable


class SystemServiceAdapter:
    """Read service state through the local service manager."""

    def __init__(
        self,
        *,
        subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    ) -> None:
        self._subprocess_runner = subprocess_runner or subprocess.run

    def is_active(self, service_name: str, *, user: bool = True) -> bool:
        args = ["systemctl"]
        if user:
            args.append("--user")
        args.extend(["is-active", service_name])
        result = self._subprocess_runner(
            args,
            capture_output=True,
            text=True,
            check=False,
        )
        return result.returncode == 0 and result.stdout.strip() == "active"
