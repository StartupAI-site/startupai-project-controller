"""GitHub HTTP adapter facade for the canonical transport implementation.

Thin re-export layer for the canonical adapter implementation in
``github_http_transport.py``. The former top-level ``github_http.py`` shim has
been deleted, so adapter consumers should import this module or
``github_http_transport.py`` directly.
"""

from __future__ import annotations

# Re-export the public interface for adapter consumers.
from startupai_controller.adapters.github_http_transport import (  # noqa: F401
    GitHubTransportError,
    begin_request_stats,
    end_request_stats,
    record_transport_cli_fallback,
    record_transport_failure,
    record_transport_latency,
    record_transport_retry,
    run_github_command,
)
