#
#     __      ___   ___    _   _  __  _____ ___ ___    ___          _   ___
#     \ \    / /_\ | _ \  /_\ | |/ / |_   _/ __|_ _|  / __|___ _ _ / | | __|
#      \ \/\/ / _ \|   / / _ \| ' <    | | \__ \| |  | (_ / -_) ' \| |_|__ \
#       \_/\_/_/ \_\_|_\/_/ \_\_|\_\   |_| |___/___|  \___\___|_||_|_(_)___/
#
#--------------------------------------------------------------------------------------
#   Main flow of non-RT python program for Warak TSI Gen 1.5 (Upgrade Pack)
#--------------------------------------------------------------------------------------
#   V1.2                amontplet                   28/04/2026


"""Main.py — composition root.

Build → register routes → run. No UI primitives here; UI lives in
`dashboard.py` and the per-feature panels. The data + hardware layers
have no nicegui dependency, so the system can also be run headless by
not constructing the Dashboard.
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

from camera_panel     import CameraManager
from camera_publisher import CameraPublisher
from config           import AppConfig
from dashboard        import Dashboard
from db_orchestrator  import DBOrchestrator
from plc_heartbeat    import PLCHeartbeat
from robot_publisher  import RobotPublisher
from robot_status     import RobotMonitor
from sick_bridge      import SickBridge
from sick_publisher   import SickPublisher
from snapshot_archive import SnapshotArchive
from twincat_comm     import TwinCATComm


# ─── config ───────────────────────────────────────────────────────────────────

cfg = AppConfig.load(Path(__file__).with_name("app_config.toml"))


# ─── build (data + hardware) ─────────────────────────────────────────────────

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
db        = DBOrchestrator.from_config(
    cfg, plc=plc, bridge=bridge, archive=archive,
)


# ─── ui ──────────────────────────────────────────────────────────────────────

# Serve TTF (or any) static assets from ./static — used by theme.py for the
# Magistral wordmark font. Empty dir is fine; missing files fall back to Muli.
app.add_static_files("/static", str(Path(__file__).with_name("static")))

dashboard = Dashboard.build(
    cameras       = cameras,
    robot_monitor = robot,
    recipes_store = db.recipes,
    sizes_store   = db.sizes,
    errors_store  = db.errors,
    title         = cfg.ui.title,
)
dashboard.register_routes()


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
    db.start()


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
