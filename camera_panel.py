"""camera_panel.py — NiceGUI camera panels with bool-triggered snapshots.

Usage in main.py:
    cameras = CameraManager.from_config(cfg.cameras)

    @ui.page("/")
    def index():
        cameras.build()

    # later, to snap from anywhere:
    cameras.trigger("entry")
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Optional

from nicegui import ui, run

from rtsp_capture import capture_as_base64

log = logging.getLogger(__name__)


@dataclass
class CameraConfig:
    name:     str
    rtsp_url: str


@dataclass
class CameraTrigger:
    """Boolean trigger; set .value=True from anywhere to fire on next poll."""
    value: bool = False

    def fire(self) -> None:
        self.value = True


class CameraPanel:
    """One camera panel: image + status + manual button + trigger flag."""

    def __init__(self, config: CameraConfig):
        self.config = config
        self.trigger = CameraTrigger()
        self._busy = False
        self._img:    Optional[ui.image] = None
        self._status: Optional[ui.label] = None
        self._done_cbs: list[Callable[[str, str, bool], None]] = []

    def on_snapshot_done(
        self, cb: Callable[[str, str, bool], None],
    ) -> Callable[[], None]:
        """Register cb(camera_name, source, ok) called after every snapshot."""
        self._done_cbs.append(cb)
        return lambda: self._done_cbs.remove(cb)

    def build(self) -> "CameraPanel":
        """Build the NiceGUI elements. Call inside a @ui.page function."""
        with ui.card().classes("w-[480px]"):
            ui.label(f"{self.config.name.capitalize()} Camera").classes(
                "text-lg font-bold"
            )
            self._img = ui.image().classes("w-full h-[360px] border bg-gray-100")
            self._status = ui.label("No snapshot yet").classes(
                "text-sm text-gray-500"
            )
            with ui.row():
                ui.button(
                    "Snapshot",
                    on_click=lambda: self.snapshot("manual"),
                ).props("icon=photo_camera")
        return self

    async def snapshot(self, source: str = "manual") -> None:
        """Capture and display a snapshot. Safe to call concurrently."""
        if self._busy:
            return
        self._busy = True
        ok = False
        try:
            self._status.text = f"Capturing ({source})..."
            data_url = await run.io_bound(capture_as_base64, self.config.rtsp_url)
            if data_url:
                self._img.set_source(data_url)
                kb = len(data_url) * 3 // 4 // 1024  # base64 ≈ 4/3 of raw bytes
                self._status.text = f"Captured via {source} ({kb} KB)"
                ok = True
            else:
                self._status.text = f"Capture failed ({source})"
                ui.notify(
                    f"{self.config.name.capitalize()} camera capture failed",
                    type="negative",
                )
        finally:
            self._busy = False
            for cb in list(self._done_cbs):
                try:
                    cb(self.config.name, source, ok)
                except Exception as e:
                    log.warning("snapshot done callback failed: %s", e)


class CameraManager:
    """Manages multiple camera panels and polls their bool triggers."""

    def __init__(self, configs: list[CameraConfig], poll_hz: float = 10.0):
        self.panels: dict[str, CameraPanel] = {
            cfg.name: CameraPanel(cfg) for cfg in configs
        }
        self.poll_hz = poll_hz
        self._poll_timer = None

    @classmethod
    def from_config(
        cls, configs: list[CameraConfig], poll_hz: float = 10.0,
    ) -> "CameraManager":
        return cls(configs, poll_hz=poll_hz)

    def build(self) -> "CameraManager":
        """Build all panels + Snap-All button + polling timer. Call in a page."""
        with ui.row().classes("gap-4"):
            for panel in self.panels.values():
                panel.build()

        async def snap_all():
            for panel in self.panels.values():
                await panel.snapshot("manual-all")

        ui.button("Snap All", on_click=snap_all).props(
            "icon=photo_library color=primary"
        )
        self._start_polling()
        return self

    def trigger(self, name: str) -> None:
        """Fire a snapshot trigger by camera name."""
        if name in self.panels:
            self.panels[name].trigger.fire()

    def get_trigger(self, name: str) -> CameraTrigger:
        """Direct reference to a camera's trigger bool."""
        return self.panels[name].trigger

    # ---- internals ----

    def _start_polling(self) -> None:
        """Watch each trigger and fire snapshots on the rising edge."""
        last_state = {name: False for name in self.panels}

        def poll():
            for name, panel in self.panels.items():
                current = panel.trigger.value
                if current and not last_state[name]:
                    panel.trigger.value = False  # auto-reset
                    ui.timer(
                        0,
                        lambda n=name: self.panels[n].snapshot(f"trigger:{n}"),
                        once=True,
                    )
                last_state[name] = current

        self._poll_timer = ui.timer(1.0 / self.poll_hz, poll)
