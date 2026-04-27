"""
camera_panel.py - NiceGUI camera panels with bool-triggered snapshots.
"""

from dataclasses import dataclass, field
from typing import Callable, Optional
from nicegui import ui, run   # add `run`

from rtsp_capture import capture_as_jpeg_bytes, capture_as_base64


@dataclass
class CameraConfig:
    name: str
    rtsp_url: str


@dataclass
class CameraTrigger:
    """
    Boolean trigger for a camera. Set `.value = True` from anywhere to fire
    a snapshot on the next poll cycle. Auto-resets to False after firing.
    """
    value: bool = False

    def fire(self):
        self.value = True


class CameraPanel:
    """A single camera panel: UI + snapshot logic + bool trigger."""

    def __init__(self, config: CameraConfig):
        self.config = config
        self.trigger = CameraTrigger()
        self._busy = False
        self._img: Optional[ui.image] = None
        self._status: Optional[ui.label] = None

    def build(self):
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

    async def snapshot(self, source: str = "manual"):
        """Capture and display a snapshot. Safe to call concurrently."""
        if self._busy:
            return
        self._busy = True
        try:
            self._status.text = f"Capturing ({source})..."
            data_url = await run.io_bound(capture_as_base64, self.config.rtsp_url)
            if data_url:
                self._img.set_source(data_url)
                # rough size estimate: base64 is ~4/3 the original byte size
                kb = len(data_url) * 3 // 4 // 1024
                self._status.text = f"Captured via {source} ({kb} KB)"
            else:
                self._status.text = f"Capture failed ({source})"
                ui.notify(
                    f"{self.config.name.capitalize()} camera capture failed",
                    type="negative",
                )
        finally:
            self._busy = False


class CameraManager:
    """Manages multiple camera panels and polls their bool triggers."""

    def __init__(self, configs: list[CameraConfig], poll_hz: float = 10.0):
        self.panels: dict[str, CameraPanel] = {
            cfg.name: CameraPanel(cfg) for cfg in configs
        }
        self.poll_hz = poll_hz

    def build_ui(self):
        """Build all camera panels in a row."""
        with ui.row().classes("gap-4"):
            for panel in self.panels.values():
                panel.build()

        # Optional "Snap All" button
        async def snap_all():
            for panel in self.panels.values():
                await panel.snapshot("manual-all")

        ui.button("Snap All", on_click=snap_all).props(
            "icon=photo_library color=primary"
        )

        self._start_polling()
        return self

    def _start_polling(self):
        """Watch each panel's trigger bool and fire on rising edge."""
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

        ui.timer(1.0 / self.poll_hz, poll)

    def trigger(self, name: str):
        """Fire a snapshot trigger by camera name."""
        if name in self.panels:
            self.panels[name].trigger.fire()

    def get_trigger(self, name: str) -> CameraTrigger:
        """Get a direct reference to a camera's trigger bool."""
        return self.panels[name].trigger