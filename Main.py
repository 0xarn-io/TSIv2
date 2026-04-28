#
#     __      ___   ___    _   _  __  _____ ___ ___    ___          _   ___ 
#     \ \    / /_\ | _ \  /_\ | |/ / |_   _/ __|_ _|  / __|___ _ _ / | | __|
#      \ \/\/ / _ \|   / / _ \| ' <    | | \__ \| |  | (_ / -_) ' \| |_|__ \
#       \_/\_/_/ \_\_|_\/_/ \_\_|\_\   |_| |___/___|  \___\___|_||_|_(_)___/
#
#--------------------------------------------------------------------------------------
#   Main flow of non-RT python program for Warak TSI Gen 1.5 (Upgrade Pack)
#--------------------------------------------------------------------------------------
#   V1.1                amontplet                   27/04/2026


"""Main.py — NiceGUI app: SICK + TwinCAT + cameras + 3D scene.

Pattern:  build → page → lifecycle → run.
Adding a new module = one line in each section. Don't put logic here;
keep it in the module and expose a `.from_config(...)` constructor.
"""
from __future__ import annotations

import logging
from pathlib import Path

from nicegui import app, ui

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("watchfiles").setLevel(logging.WARNING)

from box_scene        import BoxScene
from camera_panel     import CameraManager
from camera_publisher import CameraPublisher
from config           import AppConfig
from db_orchestrator  import DBOrchestrator
from plc_heartbeat    import PLCHeartbeat
from robot_publisher  import RobotPublisher
from robot_status     import RobotMonitor
from sick_bridge      import SickBridge
from sick_publisher   import SickPublisher
from sizes_panel      import SizesPanel
from snapshot_archive import SnapshotArchive
from twincat_comm    import TwinCATComm


# ─── config ───────────────────────────────────────────────────────────────────

cfg = AppConfig.load(Path(__file__).with_name("app_config.toml"))


# ─── build ────────────────────────────────────────────────────────────────────

bridge    = SickBridge.from_config(cfg.scanner)
plc       = TwinCATComm.from_toml(cfg.plc.vars_file)
publisher = SickPublisher(bridge, plc, cfg.plc.publisher)
archive   = SnapshotArchive.from_config(cfg.snapshots) if cfg.snapshots else None
cameras   = CameraManager.from_config(cfg.cameras, archive=archive)
cam_pub   = CameraPublisher(cameras, plc, cfg.plc.camera_triggers)
heartbeat = PLCHeartbeat(plc, cfg.plc.heartbeat) if cfg.plc.heartbeat else None
robot     = RobotMonitor.from_config(cfg.robot) if cfg.robot else None
robot_pub = (RobotPublisher(robot, plc, cfg.plc.robot_status)
             if robot and cfg.plc.robot_status else None)
scene     = BoxScene.from_config(cfg.boxes)
db        = DBOrchestrator.from_config(
    cfg, plc=plc, bridge=bridge, archive=archive,
)
sizes_ui  = SizesPanel(db.sizes) if db.sizes else None


# ─── page ─────────────────────────────────────────────────────────────────────

@ui.page("/")
def index() -> None:
    if sizes_ui:
        ui.link("Sizes admin →", "/sizes").classes("text-sm")
    scene.mount()
    cameras.build()


@ui.page("/sizes")
def sizes_page() -> None:
    ui.link("← Back", "/").classes("text-sm mb-2")
    if sizes_ui:
        sizes_ui.mount()
    else:
        ui.label("Sizes store not configured (add [sizes] to app_config.toml)."
                ).classes("text-gray-500")


# ─── lifecycle ────────────────────────────────────────────────────────────────

@app.on_startup
def _startup() -> None:
    plc.open()
    if archive:   archive.start()        # one-shot prune
    bridge.start()
    publisher.start()
    cam_pub.start()
    if heartbeat: heartbeat.start()
    if robot:     robot.start()
    if robot_pub: robot_pub.start()
    db.start()                           # recipes / unit log / errors / recipe pub


@app.on_shutdown
def _shutdown() -> None:
    db.stop()
    if robot_pub: robot_pub.stop()
    if robot:     robot.stop()
    if heartbeat: heartbeat.stop()
    cam_pub.stop()
    publisher.stop()
    bridge.stop()
    if archive:   archive.stop()
    plc.close()


# ─── run ──────────────────────────────────────────────────────────────────────

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(host=cfg.ui.host, port=cfg.ui.port, title=cfg.ui.title)
