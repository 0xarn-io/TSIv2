"""tests/fakes.py — in-tree fakes that simulate the real comm-layer callback paths.

MagicMock is fine for asserting "this was called with these args" but it
doesn't simulate the round-trip from a hardware notification through the
EventBus to a subscriber. The refactor branch's bug class (subscriber
forgets to register an ADS notification → bus topic never publishes →
subscriber never fires) was missable with mocks because the mock didn't
care whether a notification was registered before fake-firing.

These fakes mirror the real subscribe/notify contract so integration
tests can assert end-to-end:
    plc.subscribe(alias, cb) registered  →  simulate_change(alias) fires  →
    PlcSignalChanged published on bus    →  bus subscriber sees it
"""
from __future__ import annotations

import time
from typing import Any, Callable


class FakeTwinCATComm:
    """Mimics twincat_comm.TwinCATComm's notification + bus-publish path.

    Used in integration tests to verify a subscriber actually receives
    bus events when a hardware change comes in. Like the real class:

    * `subscribe(alias, cb)` registers a callback per (alias, handle).
    * `ensure_published(alias)` registers a no-op callback purely for
       bus side-effects.
    * Both call paths publish PlcSignalChanged when an alias is changed.

    Test-only API:
    * `simulate_change(alias, value)` invokes every registered callback
       for that alias (which then publishes to the bus, matching real
       twincat_comm._cb).
    * `aliases_with_notifications()` — assert which aliases have at
       least one notification registered. Lets tests catch the
       "forgot to register" bug class.
    """

    def __init__(self, *, bus=None):
        self._bus = bus
        # alias -> list[(handle_id, callback)]
        self._notifications: dict[str, list[tuple[int, Callable]]] = {}
        self._next_handle = 1
        self.write_calls: list[tuple[str, Any]] = []

    # ---- API matching real TwinCATComm ----

    def validate(self, aliases: list[str]) -> None:
        """No-op — fakes don't enforce a TOML var list."""

    def subscribe(
        self,
        alias: str,
        callback: Callable[[str, Any], None],
        *,
        cycle_time_ms: int = 100,
        max_delay_ms: int = 100,
        on_change: bool = True,
    ) -> tuple[int, int]:
        handle = (self._next_handle, self._next_handle + 1)
        self._next_handle += 2
        self._notifications.setdefault(alias, []).append(
            (handle[0], callback),
        )
        return handle

    def unsubscribe(self, handles: tuple[int, int]) -> None:
        for alias, cbs in list(self._notifications.items()):
            self._notifications[alias] = [
                (h, cb) for (h, cb) in cbs if h != handles[0]
            ]
            if not self._notifications[alias]:
                del self._notifications[alias]

    def ensure_published(self, alias: str, *, cycle_time_ms: int = 100) -> None:
        if self._bus is None:
            return
        if alias in self._notifications:
            return
        self.subscribe(
            alias, lambda _a, _v: None,
            cycle_time_ms=cycle_time_ms, on_change=True,
        )

    def write(self, alias: str, value: Any) -> None:
        self.write_calls.append((alias, value))

    def read(self, alias: str) -> Any:
        return None

    # ---- test-only API ----

    def simulate_change(self, alias: str, value: Any) -> None:
        """Fire every registered callback for `alias` and publish to the bus.

        Mirrors twincat_comm._cb: legacy callback first, then bus publish.
        """
        cbs = list(self._notifications.get(alias, ()))
        for _h, cb in cbs:
            cb(alias, value)
        if self._bus is not None and cbs:
            from events import PlcSignalChanged, signals
            self._bus.publish(signals.plc_signal_changed, PlcSignalChanged(
                alias=alias, value=value, ts=time.time(),
            ))

    def aliases_with_notifications(self) -> set[str]:
        return set(self._notifications)
