"""GitHub HTTP adapter — wraps github_http.py for HTTP transport.

Thin re-export layer for the HTTP transport path. The existing
github_http.py module is already well-isolated; this adapter provides
a consistent import path within the adapters/ package.
"""

from __future__ import annotations

# Re-export the public interface from github_http for adapter consumers.
# The actual implementation stays in github_http.py.
from startupai_controller.github_http import (  # noqa: F401
    GitHubTransportError,
    begin_request_stats,
    end_request_stats,
    run_github_command,
)
