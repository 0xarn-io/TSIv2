"""Tests for ErrorsPanel construction (no UI render)."""
from __future__ import annotations

from unittest.mock import MagicMock

from errors_panel import ErrorsPanel, _severity_text
from errors_store import ErrorsStore


def test_construction_wires_store() -> None:
    store = MagicMock(spec=ErrorsStore)
    panel = ErrorsPanel(store)
    assert panel.store is store
    assert panel._severity == "all"
    assert panel._device == ""
    # No DB access happens at construction.
    store.recent.assert_not_called()
    store.query.assert_not_called()


def test_severity_text_known_values():
    assert _severity_text("info").startswith("text-blue")
    assert _severity_text("warning").startswith("text-yellow")
    assert _severity_text("error").startswith("text-red")
    assert _severity_text("critical").startswith("text-red")


def test_severity_text_unknown_falls_back():
    assert _severity_text("debug") == "text-gray-600"
    assert _severity_text("") == "text-gray-600"


def test_refresh_swallows_dead_client_error_and_drops_stale_state() -> None:
    """A timer tick after the user navigated away must not crash —
    the container's client is gone, so .clear() raises RuntimeError.
    The panel must drop its stale refs and stop the timer so the
    cascade doesn't repeat every tick."""
    store = MagicMock(spec=ErrorsStore)
    panel = ErrorsPanel(store)

    dead_container = MagicMock()
    dead_container.clear.side_effect = RuntimeError(
        "The client this element belongs to has been deleted."
    )
    timer = MagicMock()
    panel._rows_container = dead_container
    panel._timer          = timer

    panel._refresh()                              # must not raise

    assert panel._rows_container is None
    assert panel._timer is None
    timer.delete.assert_called_once()
    store.recent.assert_not_called()
    store.query.assert_not_called()
