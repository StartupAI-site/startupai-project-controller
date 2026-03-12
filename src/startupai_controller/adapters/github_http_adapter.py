"""GitHub HTTP adapter facade for the canonical transport implementation.

Thin re-export layer for the canonical adapter implementation in
``github_http_transport.py``. The legacy top-level ``github_http.py`` module is
now only a deprecated compatibility facade.
"""

from __future__ import annotations

# Re-export the public interface for adapter consumers.
from startupai_controller.adapters.github_http_transport import (  # noqa: F401
    GitHubTransportError,
    begin_request_stats,
    end_request_stats,
    run_github_command,
)
