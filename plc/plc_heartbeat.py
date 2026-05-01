"""plc_heartbeat.py — increments a PLC counter so the PLC can detect
when Python is alive vs dead.

Usage in main.py:
    if cfg.plc.heartbeat:
        hb = PLCHeartbeat(plc, cfg.plc.heartbeat)
        hb.start()   # in @app.on_startup
        hb.stop()    # in @app.on_shutdown

PLC side (TwinCAT ST):
    tHB(IN := pyADS.nHeartbeat = nLastHB, PT := T#3S);
    nLastHB := pyADS.nHeartbeat;
    IF tHB.Q THEN
        // counter hasn't changed in 3s — Python is dead
    END_IF
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass

from twincat_comm import TwinCATComm

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class HeartbeatConfig:
    alias:     str        # e.g. "health.heartbeat"
    period_ms: int = 1000


class PLCHeartbeat:
    """Background thread: increments cfg.alias on the PLC every period_ms."""

    def __init__(self, plc: TwinCATComm, cfg: HeartbeatConfig):
        self.plc = plc
        self.cfg = cfg
        self._counter = 0
        self._stop: threading.Event | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self.plc.validate([self.cfg.alias])
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="plc-heartbeat",
        )
        self._thread.start()

    def stop(self) -> None:
        if self._stop is not None:
            self._stop.set()

    def _run(self) -> None:
        period_s = self.cfg.period_ms / 1000.0
        while not self._stop.wait(period_s):
            self._counter = (self._counter + 1) & 0xFFFFFFFF   # wrap UDINT
            try:
                self.plc.write(self.cfg.alias, self._counter)
            except Exception as e:
                log.warning("heartbeat write failed: %s", e)
