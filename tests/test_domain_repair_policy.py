"""Unit tests for repair-policy helpers."""

from __future__ import annotations

from startupai_controller.domain.repair_policy import marker_for


def test_marker_uniqueness() -> None:
    """Different refs produce different markers."""
    first = marker_for("promote-bridge", "crew#88")
    second = marker_for("promote-bridge", "crew#89")

    assert first != second
    assert "crew#88" in first
    assert "crew#89" in second
