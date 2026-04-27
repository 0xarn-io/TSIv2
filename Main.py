"""main.py — NiceGUI app: SICK + pyADS + cameras + 3D scene."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from nicegui import app, ui

from box_scene import mount_box_scene, on_box_click
from camera_panel import CameraManager
from config import AppConfig
from sick_ads_adapter import attach_pyads
from sick_bridge import SickBridge


CONFIG_PATH = Path(__file__).with_name("app_config.toml")
cfg = AppConfig.load(CONFIG_PATH)


# ─── shared state ─────────────────────────────────────────────────────────────

@dataclass
class State:
    bridge:  SickBridge | None    = None
    pub:     object | None        = None
    plc:     object | None        = None
    watcher: object | None        = None
    cameras: CameraManager | None = None
    latest:  dict | None          = None

state = State()


# ─── lifecycle ────────────────────────────────────────────────────────────────

def on_startup() -> None:
    bridge = SickBridge(
        scanner_separation_m=cfg.scanner.separation_m,
        belt_speed_m_per_s=cfg.scanner.belt_speed_mps,
        belt_y=cfg.scanner.belt_y,
    )
    pub, plc, watcher = attach_pyads(
        bridge,
        ams_net_id=cfg.plc.ams_net_id,
        enable_symbol=cfg.plc.enable_symbol,
    )
    bridge.on_measurement(lambda m: setattr(state, "latest", m))
    bridge.start()

    state.bridge  = bridge
    state.pub     = pub
    state.plc     = plc
    state.watcher = watcher
    state.cameras = CameraManager(cfg.cameras)


def on_shutdown() -> None:
    if state.watcher is not None:
        state.watcher.stop()
    if state.bridge is not None:
        state.bridge.stop()
    if state.plc is not None:
        try:
            state.plc.close()
        except Exception:
            pass


app.on_startup(on_startup)
app.on_shutdown(on_shutdown)


# ─── page ─────────────────────────────────────────────────────────────────────

@ui.page("/")
def index() -> None:
    pass


# ─── run ──────────────────────────────────────────────────────────────────────

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(host=cfg.ui.host, port=cfg.ui.port, title=cfg.ui.title)