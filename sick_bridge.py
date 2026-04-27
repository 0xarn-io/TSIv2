"""sick_bridge.py — SICK twin-scanner measurement pipeline.

Knows about scanners, ROI, ScanProcessor, and UnitTracker. Has zero
knowledge of how/where measurements are consumed — register callbacks.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from pysickudt import (
    ROI, ScanProcessor, UnitEvent, UnitTracker,
    axis_position, height_range,
)
from pysickudt.stream import UDPReceiver


MeasurementCallback = Callable[[dict[str, Any]], None]
EventCallback       = Callable[[UnitEvent], None]


class SickBridge:
    """Two SICK scanners → per-scan + per-unit measurements via callbacks.

    Register callbacks with `on_measurement(cb)` / `on_event(cb)`. They
    fire from the receiver thread — keep them quick, or hand off to a
    queue.
    """

    def __init__(
        self,
        *,
        udp_port_a: int = 2111,
        udp_port_b: int = 2112,
        scanner_separation_m: float = 2.45,
        belt_speed_m_per_s: float = 0.254,
        belt_y: float = -1.48,
        roi: ROI | None = None,
        proc_a: ScanProcessor | None = None,
        proc_b: ScanProcessor | None = None,
    ) -> None:
        self.scanner_separation_m = scanner_separation_m
        self.belt_y = belt_y
        self._roi = roi or ROI(x_min=0.10, x_max=2.35, y_min=-1.45, y_max=+0.60)
        self._proc_a = proc_a or ScanProcessor(rotation_deg=0.0,
                          translation=(0.0, 0.0), roi=self._roi)
        self._proc_b = proc_b or ScanProcessor(rotation_deg=180.0,
                          translation=(scanner_separation_m, 0.018), roi=self._roi)

        self._tracker = UnitTracker(
            belt_speed_m_per_s=belt_speed_m_per_s,
            combine_callback=self._combine,
            on_event=self._fire_event,
        )

        self._rx_a = UDPReceiver(bind_ip="0.0.0.0", bind_port=udp_port_a, queue_max=1)
        self._rx_b = UDPReceiver(bind_ip="0.0.0.0", bind_port=udp_port_b, queue_max=1)
        self._state: dict[str, dict | None] = {"A": None, "B": None}

        self._measurement_cbs: list[MeasurementCallback] = []
        self._event_cbs: list[EventCallback] = []
        self._enabled = True
        self._lock = threading.Lock()

    # ---- subscriber API ----------------------------------------------

    def on_measurement(self, cb: MeasurementCallback) -> Callable[[], None]:
        self._measurement_cbs.append(cb)
        return lambda: self._measurement_cbs.remove(cb)

    def on_event(self, cb: EventCallback) -> Callable[[], None]:
        self._event_cbs.append(cb)
        return lambda: self._event_cbs.remove(cb)

    # ---- lifecycle ----------------------------------------------------

    def start(self) -> None:
        self._rx_a.on_scan(self._on_a)
        self._rx_b.on_scan(self._on_b)
        self._rx_a.start(); self._rx_b.start()

    def stop(self) -> None:
        self._rx_a.stop(); self._rx_b.stop()

    def __enter__(self):  self.start();  return self
    def __exit__(self, *_): self.stop()

    # ---- runtime control ---------------------------------------------

    def enable(self):  self._enabled = True
    def disable(self): self._enabled = False

    @property
    def is_enabled(self) -> bool:               return self._enabled
    @property
    def is_present(self) -> bool:               return self._tracker.is_present
    @property
    def last_event(self) -> UnitEvent | None:   return self._tracker.last_event

    def stats(self) -> dict:
        return {"rx_a": self._rx_a.stats(), "rx_b": self._rx_b.stats()}

    # ---- internals ----------------------------------------------------

    def _measure(self, scan, processor: ScanProcessor) -> dict | None:
        points = processor.process(scan)
        hr = height_range(points, axis="y", high_method="p95")
        if hr is None:
            return None
        y_low, y_top, _, _ = hr
        margin = min(0.05, (y_top - y_low) / 4)
        face_low, face_high = y_low + margin, y_top - margin
        where = (face_low, face_high) if face_high > face_low else None
        is_b_side = processor.translation[0] > self.scanner_separation_m / 2
        method = "p95" if is_b_side else "p5"
        pos = axis_position(points, axis="x", method=method, where=where)
        if pos is None:
            return None
        near, n = pos
        return {"near_face": near, "height": max(0.0, y_top - self.belt_y), "n": n}

    def _combine(self, m_a: dict | None, m_b: dict | None) -> dict | None:
        if m_a is None or m_b is None:
            return None
        centre = 0.5 * (m_a["near_face"] + m_b["near_face"])
        return {
            "width":  m_b["near_face"] - m_a["near_face"],
            "height": 0.5 * (m_a["height"] + m_b["height"]),
            "offset": centre - 0.5 * self.scanner_separation_m,
        }

    def _on_a(self, scan) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._state["A"] = self._measure(scan, self._proc_a)

    def _on_b(self, scan) -> None:
        if not self._enabled:
            return
        m_b = self._measure(scan, self._proc_b)
        with self._lock:
            self._state["B"] = m_b
            m_a = self._state["A"]
        c = self._combine(m_a, m_b)
        if c is not None:
            for cb in list(self._measurement_cbs):
                try: cb(c)
                except Exception: pass
        self._tracker.feed(m_a, m_b)

    def _fire_event(self, event: UnitEvent) -> None:
        for cb in list(self._event_cbs):
            try: cb(event)
            except Exception: pass