"""Tests for event_bus.EventBus dispatch modes and lifecycle."""
from __future__ import annotations

import asyncio
import threading
import time

import pytest
from blinker import Signal

from event_bus import EventBus


@pytest.fixture
def bus():
    b = EventBus()
    yield b
    b.stop()


def _wait(pred, timeout=2.0, interval=0.005):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred(): return True
        time.sleep(interval)
    return False


# ─── sync mode ────────────────────────────────────────────────────────────────

def test_sync_handler_runs_on_publisher_thread(bus):
    sig = Signal()
    seen = {}
    def h(payload):
        seen["thread"] = threading.current_thread().name
        seen["payload"] = payload
    bus.subscribe(sig, h, mode="sync")
    bus.publish(sig, {"x": 1})
    assert seen["thread"] == threading.current_thread().name
    assert seen["payload"] == {"x": 1}


def test_sync_handler_exception_does_not_break_others(bus):
    sig = Signal()
    calls: list[int] = []
    bus.subscribe(sig, lambda p: (_ for _ in ()).throw(RuntimeError("boom")), mode="sync")
    bus.subscribe(sig, lambda p: calls.append(p), mode="sync")
    bus.publish(sig, 42)
    assert calls == [42]


# ─── thread mode ──────────────────────────────────────────────────────────────

def test_thread_handler_runs_off_publisher_thread(bus):
    loop = asyncio.new_event_loop()
    try:
        bus.start(loop, workers=2)
        sig = Signal()
        seen: dict = {}
        done = threading.Event()
        def h(payload):
            seen["thread"] = threading.current_thread().name
            seen["payload"] = payload
            done.set()
        bus.subscribe(sig, h, mode="thread")
        bus.publish(sig, "hello")
        assert done.wait(2.0)
        assert seen["thread"] != threading.current_thread().name
        assert seen["thread"].startswith("bus-worker")
        assert seen["payload"] == "hello"
    finally:
        loop.close()


def test_thread_handler_dropped_when_bus_not_started(bus):
    sig = Signal()
    calls = []
    bus.subscribe(sig, lambda p: calls.append(p), mode="thread")
    # Bus was never started — should be a no-op, not a crash.
    bus.publish(sig, 1)
    time.sleep(0.05)
    assert calls == []


# ─── async mode ───────────────────────────────────────────────────────────────

def test_async_handler_runs_on_loop():
    bus = EventBus()
    loop = asyncio.new_event_loop()

    seen: dict = {}
    done = threading.Event()

    async def h(payload):
        seen["loop"] = asyncio.get_running_loop()
        seen["payload"] = payload
        done.set()

    sig = Signal()

    def runner():
        asyncio.set_event_loop(loop)
        bus.start(loop)
        bus.subscribe(sig, h, mode="async")
        loop.run_forever()

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    # Wait for loop to be running.
    assert _wait(lambda: loop.is_running())

    bus.publish(sig, "go")
    assert done.wait(2.0)
    assert seen["loop"] is loop
    assert seen["payload"] == "go"

    loop.call_soon_threadsafe(loop.stop)
    t.join(timeout=2.0)
    bus.stop()
    loop.close()


# ─── lifecycle ────────────────────────────────────────────────────────────────

def test_stop_disconnects_handlers_and_drains_executor(bus):
    loop = asyncio.new_event_loop()
    try:
        bus.start(loop)
        sig = Signal()
        calls = []
        bus.subscribe(sig, lambda p: calls.append(p), mode="sync")
        bus.publish(sig, 1)
        assert calls == [1]
        bus.stop()
        # After stop, blinker should have no receivers connected.
        bus.publish(sig, 2)
        assert calls == [1]
    finally:
        loop.close()


def test_unsubscribe(bus):
    sig = Signal()
    calls = []
    w = bus.subscribe(sig, lambda p: calls.append(p), mode="sync")
    bus.publish(sig, 1)
    bus.unsubscribe(sig, w)
    bus.publish(sig, 2)
    assert calls == [1]


def test_subscription_returns_zero_arg_unsub(bus):
    """subscription() — convenience handle that publishers actually want."""
    sig = Signal()
    calls = []
    undo = bus.subscription(sig, lambda p: calls.append(p), mode="sync")
    bus.publish(sig, 1)
    undo()                                  # 0-arg unsubscribe
    bus.publish(sig, 2)
    assert calls == [1]


def test_subscription_undo_idempotent(bus):
    """Calling the returned undo twice is a no-op (no exception)."""
    sig = Signal()
    undo = bus.subscription(sig, lambda p: None, mode="sync")
    undo()
    undo()                                  # must not raise


# ─── subscribe_filtered ───────────────────────────────────────────────────────

class _Payload:
    def __init__(self, **attrs):
        self.__dict__.update(attrs)


def test_subscribe_filtered_matches_only_when_all_filters_equal(bus):
    sig = Signal()
    seen = []
    bus.subscribe_filtered(
        sig, lambda p: seen.append(p.value), mode="sync",
        alias="recipe.code",
    )
    bus.publish(sig, _Payload(alias="recipe.code", value=7))
    bus.publish(sig, _Payload(alias="other",       value=99))
    bus.publish(sig, _Payload(alias="recipe.code", value=8))
    assert seen == [7, 8]


def test_subscribe_filtered_multiple_filters_all_must_match(bus):
    sig = Signal()
    seen = []
    bus.subscribe_filtered(
        sig, lambda p: seen.append(p.value), mode="sync",
        alias="a", kind="bool",
    )
    bus.publish(sig, _Payload(alias="a", kind="bool", value=1))   # match
    bus.publish(sig, _Payload(alias="a", kind="int",  value=2))   # kind mismatch
    bus.publish(sig, _Payload(alias="b", kind="bool", value=3))   # alias mismatch
    assert seen == [1]


def test_subscribe_filtered_missing_attribute_does_not_match(bus):
    """Payloads lacking a filtered attr are silently skipped, not errored."""
    sig = Signal()
    seen = []
    bus.subscribe_filtered(
        sig, lambda p: seen.append(p), mode="sync",
        alias="x",
    )
    bus.publish(sig, _Payload(value=1))         # no .alias attr at all
    bus.publish(sig, _Payload(alias="x", value=2))
    assert len(seen) == 1 and seen[0].value == 2


def test_subscribe_filtered_returns_zero_arg_undo(bus):
    sig = Signal()
    seen = []
    undo = bus.subscribe_filtered(
        sig, lambda p: seen.append(p.value), mode="sync",
        alias="x",
    )
    bus.publish(sig, _Payload(alias="x", value=1))
    undo()
    bus.publish(sig, _Payload(alias="x", value=2))
    assert seen == [1]


def test_subscribe_filtered_no_filters_passes_all_payloads(bus):
    """No kwargs ⇒ behaves like subscription() (every event delivered)."""
    sig = Signal()
    seen = []
    bus.subscribe_filtered(sig, lambda p: seen.append(p), mode="sync")
    bus.publish(sig, "a")
    bus.publish(sig, "b")
    assert seen == ["a", "b"]
