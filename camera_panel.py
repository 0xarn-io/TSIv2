"""camera_panel.py — NiceGUI camera panels with bool-triggered snapshots.

Usage in main.py:
    cameras = CameraManager.from_config(cfg.cameras, archive=archive)

    @ui.page("/")
    def index():
        cameras.build()

    # later, to snap from anywhere:
    cameras.trigger("entry")

If `archive` is provided, every successful capture also persists the JPEG
bytes to disk and forwards the path to `on_snapshot_done` callbacks.
"""
from __future__ import annotations

import base64
import logging
from dataclasses import dataclass
from typing import Callable, Optional

from nicegui import ui, run

from rtsp_capture import capture_as_jpeg_bytes

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

    def __init__(self, config: CameraConfig, archive=None):
        self.config = config
        self.archive = archive
        self.trigger = CameraTrigger()
        self._busy = False
        self._img:    Optional[ui.image] = None
        self._status: Optional[ui.label] = None
        self._done_cbs: list[Callable[[str, str, bool, str | None], None]] = []

    def on_snapshot_done(
        self, cb: Callable[[str, str, bool, str | None], None],
    ) -> Callable[[], None]:
        """Register cb(camera_name, source, ok, path) called after every snapshot.

        `path` is the saved-on-disk path (str) when an archive is configured
        and the capture succeeded, else None.
        """
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
        path: str | None = None
        try:
            self._status.text = f"Capturing ({source})..."
            jpeg_bytes = await run.io_bound(
                capture_as_jpeg_bytes, self.config.rtsp_url,
            )
            if jpeg_bytes:
                if self.archive is not None:
                    try:
                        path = self.archive.save(self.config.name, jpeg_bytes)
                    except Exception as e:
                        log.warning("snapshot archive save failed: %s", e)
                data_url = (
                    "data:image/jpeg;base64,"
                    + base64.b64encode(jpeg_bytes).decode("ascii")
                )
                self._img.set_source(data_url)
                kb = len(jpeg_bytes) // 1024
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
                    cb(self.config.name, source, ok, path)
                except Exception as e:
                    log.warning("snapshot done callback failed: %s", e)


class CameraManager:
    """Manages multiple camera panels and polls their bool triggers."""

    def __init__(
        self,
        configs: list[CameraConfig],
        poll_hz: float = 10.0,
        archive=None,
    ):
        self.archive = archive
        self.panels: dict[str, CameraPanel] = {
            cfg.name: CameraPanel(cfg, archive=archive) for cfg in configs
        }
        self.poll_hz = poll_hz
        self._poll_timer = None

    @classmethod
    def from_config(
        cls, configs: list[CameraConfig], poll_hz: float = 10.0, *,
        archive=None,
    ) -> "CameraManager":
        return cls(configs, poll_hz=poll_hz, archive=archive)

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
