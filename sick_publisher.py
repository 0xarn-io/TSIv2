"""sick_publisher.py — publish SickBridge data to TwinCAT via TwinCATComm.

Wiring (do once in main, then call .start() / .stop()):
    bridge.on_measurement → plc.write(live_alias,  {...mm DINTs...})
    bridge.on_event       → plc.write(event_alias, {...mm DINTs + flags...})
    plc.subscribe(enable_alias) ↔ bridge.enable()/disable()

All aliases come from `plc_signals.toml`. Aliases are validated at .start()
time so missing entries fail fast with a clear error.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

from sick_bridge import SickBridge
from twincat_comm import TwinCATComm

log = logging.getLogger(__name__)
M_TO_MM = 1000


@dataclass(frozen=True)
class PublisherConfig:
    event_alias:     str
    live_alias:      str
    enable_alias:    str
    enable_cycle_ms: int = 100


def _measurement_to_struct(m: dict) -> dict:
    """{width, height, offset} (metres, float) → ST_SickLive (mm, int)."""
    return {
        "nWidth":  int(round(m["width"]  * M_TO_MM)),
        "nHeight": int(round(m["height"] * M_TO_MM)),
        "nOffset": int(round(m["offset"] * M_TO_MM)),
    }


def _event_to_struct(ev) -> dict:
    """pysickudt.UnitEvent → ST_SickEvent (mm DINTs + REAL duration + bNew)."""
    return {
        "bNew":        True,
        "nLength":     int(round(ev.length_m      * M_TO_MM)),
        "nWidthMean":  int(round(ev.width_mean_m  * M_TO_MM)),
        "nWidthMin":   int(round(ev.width_min_m   * M_TO_MM)),
        "nWidthMax":   int(round(ev.width_max_m   * M_TO_MM)),
        "nHeightMean": int(round(ev.height_mean_m * M_TO_MM)),
        "nHeightMin":  int(round(ev.height_min_m  * M_TO_MM)),
        "nHeightMax":  int(round(ev.height_max_m  * M_TO_MM)),
        "nOffsetMean": int(round(ev.offset_mean_m * M_TO_MM)),
        "nOffsetMin":  int(round(ev.offset_min_m  * M_TO_MM)),
        "nOffsetMax":  int(round(ev.offset_max_m  * M_TO_MM)),
        "fDuration":   float(ev.duration_s),
        "nSamples":    int(ev.n_samples),
    }


class SickPublisher:
    """Glue between SickBridge and TwinCATComm. No state of its own."""

    def __init__(self, bridge: SickBridge, plc: TwinCATComm, cfg: PublisherConfig, *, bus=None):
        self.bridge = bridge
        self.plc = plc
        self.cfg = cfg
        self._bus = bus
        self._unsub_meas:  Callable[[], None] | None = None
        self._unsub_event: Callable[[], None] | None = None
        self._enable_handles: tuple[int, int] | None = None

    def start(self) -> None:
        self.plc.validate([
            self.cfg.live_alias,
            self.cfg.event_alias,
            self.cfg.enable_alias,
        ])
        self._unsub_meas  = self.bridge.on_measurement(self._on_measurement)
        self._unsub_event = self.bridge.on_event(self._on_event)

        try:
            self._apply_enable(bool(self.plc.read(self.cfg.enable_alias)))
        except Exception as e:
            log.warning("initial enable read failed: %s", e)

        try:
            self._enable_handles = self.plc.subscribe(
                self.cfg.enable_alias,
                lambda _alias, val: self._apply_enable(bool(val)),
                cycle_time_ms=self.cfg.enable_cycle_ms,
                on_change=True,
            )
        except Exception as e:
            log.warning(
                "enable subscription failed (%s); SICK publisher running "
                "without remote enable/disable from '%s'",
                e, self.cfg.enable_alias,
            )
            self._enable_handles = None

    def stop(self) -> None:
        if self._enable_handles is not None:
            try:
                self.plc.unsubscribe(self._enable_handles)
            except Exception as e:
                log.warning("unsubscribe enable failed: %s", e)
            self._enable_handles = None
        if self._unsub_meas:
            self._unsub_meas()
            self._unsub_meas = None
        if self._unsub_event:
            self._unsub_event()
            self._unsub_event = None

    # ---- internals ----

    def _apply_enable(self, enabled: bool) -> None:
        (self.bridge.enable if enabled else self.bridge.disable)()

    def _on_measurement(self, m: dict) -> None:
        try:
            self.plc.write(self.cfg.live_alias, _measurement_to_struct(m))
        except Exception as e:
            log.warning("live publish failed: %s", e)

    def _on_event(self, ev) -> None:
        s = _event_to_struct(ev)
        log.info("event w=%d h=%d off=%d", s["nWidthMean"], s["nHeightMean"], s["nOffsetMean"])
        try:
            self.plc.write(self.cfg.event_alias, s)
        except Exception as e:
            log.warning("event publish failed: %s", e)
