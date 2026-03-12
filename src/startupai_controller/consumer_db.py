"""Deprecated compatibility facade for the legacy ConsumerDB import path.

The canonical SQLite persistence implementation now lives in
``startupai_controller.adapters.consumer_db_store``. This module remains only
to preserve legacy imports while internal runtime code uses adapter paths.
"""

from __future__ import annotations

from startupai_controller.adapters.consumer_db_store import *  # noqa: F403
from startupai_controller.domain.models import SessionInfo
