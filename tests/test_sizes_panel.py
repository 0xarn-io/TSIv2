"""Tests for SizesPanel: dead-client guard + bus subscription."""
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

from event_bus   import EventBus
from events      import SizesChanged, signals
from sizes_panel import SizesPanel
from sizes_store import SizesStore


def test_construction_without_bus_has_no_subscription() -> None:
    store = MagicMock(spec=SizesStore)
    panel = SizesPanel(store)
    assert panel._bus is None
    assert panel._bus_unsub is None


def test_construction_with_bus_subscribes_to_sizes_changed() -> None:
    """Bus is wired ⇒ a subscription is registered for sizes_changed.
    Verified by counting receivers on the blinker signal."""
    before = len(signals.sizes_changed.receivers)
    store = MagicMock(spec=SizesStore)
    bus   = EventBus()
    panel = SizesPanel(store, bus=bus)
    after = len(signals.sizes_changed.receivers)
    assert panel._bus is bus
    assert panel._bus_unsub is not None
    assert after == before + 1
    panel._bus_unsub()                            # cleanup so other tests are unaffected
    assert len(signals.sizes_changed.receivers) == before


def test_bus_event_triggers_refresh() -> None:
    """A SizesChanged publish drives _refresh() on the UI loop."""
    store = MagicMock(spec=SizesStore)
    bus   = EventBus(slow_handler_ms=None)        # silence timing in unit tests
    panel = SizesPanel(store, bus=bus)
    refresh_calls = []
    panel._refresh = lambda: refresh_calls.append(1)  # type: ignore[assignment]

    loop = asyncio.new_event_loop()
    try:
        bus.start(loop)
        bus.publish(signals.sizes_changed,
                    SizesChanged(slot=3, op="upsert",
                                 payload={"id": 1}, origin="robot_master"))
        # mode="async" goes through call_soon_threadsafe → drain the loop once.
        loop.call_soon(loop.stop)
        loop.run_forever()
    finally:
        bus.stop()
        loop.close()

    assert refresh_calls == [1]


def test_refresh_swallows_dead_client_error() -> None:
    """A bus tick or button event from a navigated-away client must not crash.
    .clear() raises RuntimeError → drop the stale container ref."""
    store = MagicMock(spec=SizesStore)
    panel = SizesPanel(store)

    dead_container = MagicMock()
    dead_container.clear.side_effect = RuntimeError(
        "The client this element belongs to has been deleted."
    )
    panel._container = dead_container

    panel._refresh()                              # must not raise

    assert panel._container is None
    store.list.assert_not_called()
