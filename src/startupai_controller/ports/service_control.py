"""ServiceControlPort — query local service-manager state."""

from __future__ import annotations

from typing import Protocol


class ServiceControlPort(Protocol):
    """Read service state from the local service manager."""

    def is_active(self, service_name: str, *, user: bool = True) -> bool:
        """Return True when the named service is active."""
        ...
