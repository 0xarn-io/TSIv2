"""camera_panel.py — NiceGUI camera panels with PLC-triggered snapshots.

Usage in main.py:
    cameras = CameraManager.from_config(cfg.cameras, archive=archive)

    @app.on_startup
    def _startup():
        cameras.attach_loop(asyncio.get_running_loop())

    @ui.page("/")
    def index():
        cameras.build()

    # Fire from any thread (e.g. PLC subscription callback):
    cameras.snap("entry", source="trigger:entry")

If `archive` is provided, every successful capture also persists the JPEG
bytes to disk and forwards the path to `on_snapshot_done` callbacks. When
no client is connected, captures still run and archive — only the live UI
display is skipped.
"""
from __future__ import annotations

import asyncio
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


class CameraPanel:
    """One camera: capture pipeline + optional UI panel."""

    def __init__(self, config: CameraConfig, archive=None):
        self.config = config
        self.archive = archive
        self._busy = False
        self._img:    Optional[ui.image] = None
        self._status: Optional[ui.label] = None
        self._done_cbs: list[Callable[[str, str, bool, str | None], None]] = []

    def on_snapshot_done(
        self, cb: Callable[[str, str, bool, str | None], None],
    ) -> Callable[[], None]:
        """Register cb(camera_name, source, ok, path) called after every snapshot.

        Fires even when a snapshot was skipped (busy) or failed, so callers
        relying on it for handshakes (e.g. clearing a PLC bool) always run.
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
        """Capture a snapshot. Always fires done_cbs, including on busy/fail.

        Safe to call without a UI page open — capture + archive still run,
        only the live display is skipped.
        """
        if self._busy:
            # Drop overlapping triggers, but still ack so the caller's
            # handshake (e.g. PLC bool) doesn't stall.
            self._fire_done(source, ok=False, path=None)
            return
        self._busy = True
        ok = False
        path: str | None = None
        try:
            self._set_status(f"Capturing ({source})...")
            jpeg_bytes = await run.io_bound(
                capture_as_jpeg_bytes, self.config.rtsp_url,
            )
            if jpeg_bytes:
                if self.archive is not None:
                    try:
                        path = self.archive.save(self.config.name, jpeg_bytes)
                    except Exception as e:
                        log.warning("snapshot archive save failed: %s", e)
                self._set_image_from_bytes(jpeg_bytes)
                kb = len(jpeg_bytes) // 1024
                self._set_status(f"Captured via {source} ({kb} KB)")
                ok = True
            else:
                self._set_status(f"Capture failed ({source})")
                self._notify(
                    f"{self.config.name.capitalize()} camera capture failed",
                    "negative",
                )
        finally:
            self._busy = False
            self._fire_done(source, ok=ok, path=path)

    # ---- UI-safe helpers (tolerate missing/destroyed elements) -------------

    def _set_status(self, text: str) -> None:
        if self._status is None:
            return
        try: self._status.text = text
        except Exception as e: log.debug("camera status update skipped: %s", e)

    def _set_image_from_bytes(self, jpeg_bytes: bytes) -> None:
        if self._img is None:
            return
        try:
            data_url = (
                "data:image/jpeg;base64,"
                + base64.b64encode(jpeg_bytes).decode("ascii")
            )
            self._img.set_source(data_url)
        except Exception as e:
            log.debug("camera image update skipped: %s", e)

    def _notify(self, message: str, kind: str) -> None:
        try:
            ui.notify(message, type=kind)
        except Exception as e:
            log.debug("camera notify skipped: %s", e)

    def _fire_done(self, source: str, *, ok: bool, path: str | None) -> None:
        for cb in list(self._done_cbs):
            try:
                cb(self.config.name, source, ok, path)
            except Exception as e:
                log.warning("snapshot done callback failed: %s", e)


class CameraManager:
    """Owns N CameraPanels + thread-safe `snap()` for PLC callbacks."""

    def __init__(self, configs: list[CameraConfig], archive=None):
        self.archive = archive
        self.panels: dict[str, CameraPanel] = {
            cfg.name: CameraPanel(cfg, archive=archive) for cfg in configs
        }
        self._loop: asyncio.AbstractEventLoop | None = None

    @classmethod
    def from_config(
        cls, configs: list[CameraConfig], *, archive=None,
    ) -> "CameraManager":
        return cls(configs, archive=archive)

    def attach_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Capture the main asyncio loop so cross-thread snaps can schedule."""
        self._loop = loop

    def build(self) -> "CameraManager":
        """Build all panels + Snap-All button. Call inside a @ui.page function."""
        with ui.row().classes("gap-4"):
            for panel in self.panels.values():
                panel.build()

        async def snap_all():
            for panel in self.panels.values():
                await panel.snapshot("manual-all")

        ui.button("Snap All", on_click=snap_all).props(
            "icon=photo_library color=primary"
        )
        return self

    def snap(self, name: str, source: str = "trigger") -> bool:
        """Schedule a snapshot from any thread. Returns False if not dispatched.

        Used by `camera_publisher.CameraPublisher` when a PLC bool rises.
        Captures a Future on the main asyncio loop; doesn't wait for it.
        """
        panel = self.panels.get(name)
        if panel is None:
            log.warning("camera snap: unknown camera %r", name)
            return False
        if self._loop is None or not self._loop.is_running():
            log.warning(
                "camera snap %s: event loop not attached yet — dropped", name,
            )
            return False
        asyncio.run_coroutine_threadsafe(panel.snapshot(source), self._loop)
        return True
