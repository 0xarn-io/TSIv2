"""sick_ads_adapter.py — push SickBridge output to a TwinCAT PLC."""

from __future__ import annotations

import threading

import pyads
from pysickudt.ads import (
    ADSPublisher, default_event_mapping, default_live_mapping,
)
from sick_bridge import SickBridge


class _EnableWatcher:
    """Polls a PLC BOOL and mirrors it onto bridge.enable()/disable()."""

    def __init__(self, bridge, pub, symbol, poll_hz=10.0, default_enabled=False):
        self.bridge = bridge
        self.pub = pub
        self.symbol = symbol
        self.default_enabled = default_enabled
        self._poll_period = 1.0 / poll_hz
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        # Synchronous first read so the bridge starts in the right state.
        self._apply()
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _apply(self):
        try:
            enabled = bool(self.pub.read(self.symbol, "BOOL"))
        except Exception:
            enabled = self.default_enabled
        (self.bridge.enable if enabled else self.bridge.disable)()

    def _run(self):
        while not self._stop.is_set():
            self._apply()
            self._stop.wait(self._poll_period)


def attach_pyads(
    bridge: SickBridge,
    *,
    ams_net_id: str,
    ams_port: int = 851,
    prefix: str = "pyADS",
    enable_symbol: str | None = "pyADS.bEnable",   # set to None to disable the gate
    enable_poll_hz: float = 10.0,
    ignore_errors: bool = True,
):
    """Wire bridge → PLC via pyads. Returns (publisher, plc, watcher_or_None)."""
    plc = pyads.Connection(ams_net_id, ams_port)
    plc.open()
    pub = ADSPublisher(
        plc,
        event_mapping=default_event_mapping(prefix=f"{prefix}.SickEvent"),
        live_mapping=default_live_mapping(prefix=f"{prefix}.SickLive"),
        new_event_flag=f"{prefix}.SickEvent.bNew",
        ignore_errors=ignore_errors,
    )
    bridge.on_measurement(pub.publish_measurement)
    bridge.on_event(pub.publish_event)

    watcher = None
    if enable_symbol is not None:
        watcher = _EnableWatcher(bridge, pub, enable_symbol, poll_hz=enable_poll_hz)
        watcher.start()

    return pub, plc, watcher