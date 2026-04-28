"""camera_publisher.py — fire camera snapshots from PLC bool rising edges.

Wiring (do once in main, then call .start() / .stop()):
    plc.subscribe(alias) → cameras.trigger(camera_name)  # on rising edge

Each entry in `cfg.plc.camera_triggers` maps a PLC bool alias (defined in
plc_signals.toml) to a camera name (defined in app_config.toml [[cameras]]).

Adding a new trigger = one entry in plc_signals.toml + one entry in
app_config.toml. No Python edits.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

from camera_panel import CameraManager
from twincat_comm import TwinCATComm

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CameraTriggerConfig:
    alias:  str   # PLC alias from plc_signals.toml (e.g. "camera.snap_entry")
    camera: str   # camera name from app_config.toml [[cameras]] (e.g. "entry")


class CameraPublisher:
    """Subscribe PLC bools to camera triggers. Rising edge fires a snapshot."""

    def __init__(
        self,
        cameras: CameraManager,
        plc: TwinCATComm,
        triggers: list[CameraTriggerConfig],
    ):
        self.cameras = cameras
        self.plc = plc
        self.triggers = list(triggers)
        self._handles: list[tuple[int, int]] = []
        self._unsub_done: list[Callable[[], None]] = []

    def start(self) -> None:
        # Fail fast — both ends of the mapping must exist.
        self.plc.validate([t.alias for t in self.triggers])
        unknown = [t.camera for t in self.triggers
                   if t.camera not in self.cameras.panels]
        if unknown:
            raise KeyError(
                f"camera_triggers reference unknown camera(s): {unknown}. "
                f"Known cameras: {sorted(self.cameras.panels)}"
            )

        for t in self.triggers:
            # Bind name in default arg so each subscription captures its own.
            def _cb(_alias, val, name=t.camera):
                if val:
                    log.info("snap %s", name)
                    self.cameras.trigger(name)

            handles = self.plc.subscribe(t.alias, _cb, on_change=True)
            self._handles.append(handles)

            # Handshake: clear the PLC bool once the snapshot completes.
            panel = self.cameras.panels[t.camera]
            unsub = panel.on_snapshot_done(
                lambda _name, _source, _ok, alias=t.alias: self._ack(alias)
            )
            self._unsub_done.append(unsub)

    def stop(self) -> None:
        for h in self._handles:
            try:
                self.plc.unsubscribe(h)
            except Exception as e:
                log.warning("camera trigger unsubscribe failed: %s", e)
        self._handles.clear()
        for u in self._unsub_done:
            u()
        self._unsub_done.clear()

    # ---- internals ----

    def _ack(self, alias: str) -> None:
        try:
            self.plc.write(alias, False)
        except Exception as e:
            log.warning("ack write %s failed: %s", alias, e)
