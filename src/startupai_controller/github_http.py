"""Deprecated compatibility facade for the GitHub HTTP transport shim.

The canonical implementation now lives in
``startupai_controller.adapters.github_http_transport``. This module remains
only to preserve legacy import paths for tests and any external consumers that
have not yet migrated.
"""

from __future__ import annotations

from startupai_controller.adapters.github_http_transport import *  # noqa: F403
