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


"""Main.py — composition root

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


# NiceGUI's Timer task can fire one extra tick after its parent slot
# is torn down (race during page navigation), then logs the resulting
# RuntimeError + traceback. The error is cosmetic — the task is already
# disposed. Drop it so the console isn't spammed.
class _DropParentSlotDeletedFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if "parent slot of the element has been deleted" in record.getMessage():
            return False
        if record.exc_info and record.exc_info[1] is not None:
            if "parent slot" in str(record.exc_info[1]):
                return False
        return True


for _name in ("", "nicegui", "nicegui.background_tasks"):
    logging.getLogger(_name).addFilter(_DropParentSlotDeletedFilter())

from camera_panel     import CameraManager
from camera_publisher import CameraPublisher
from config           import AppConfig
from dashboard        import Dashboard
from db_orchestrator  import DBOrchestrator
from event_bus        import EventBus
from plc_heartbeat    import PLCHeartbeat
from robot_errors     import RobotElogPoller
from robot_master     import RobotMasterMonitor
from robot_publisher  import RobotPublisher
from robot_status     import RobotMonitor
from robot_variables  import RobotVariablesMonitor
from sick_bridge      import SickBridge
from sick_publisher   import SickPublisher
from snapshot_archive import SnapshotArchive
from twincat_comm     import TwinCATComm


# ─── config ───────────────────────────────────────────────────────────────────

cfg = AppConfig.load(Path(__file__).with_name("app_config.toml"))


# ─── build (data + hardware) ─────────────────────────────────────────────────

# Single in-process event bus. Constructed eagerly so any module can hold
# a reference at build time; started/stopped inside the NiceGUI lifecycle
# hooks below (loop ref needed for "async" subscribers).
bus       = EventBus()

bridge    = SickBridge.from_config(cfg.scanner, bus=bus)
plc       = TwinCATComm.from_toml(cfg.plc.vars_file, bus=bus)
publisher = SickPublisher(bridge, plc, cfg.plc.publisher, bus=bus)
archive   = SnapshotArchive.from_config(cfg.snapshots) if cfg.snapshots else None
cameras   = CameraManager.from_config(cfg.cameras, archive=archive)
cam_pub   = CameraPublisher(cameras, plc, cfg.plc.camera_triggers, bus=bus)
heartbeat = PLCHeartbeat(plc, cfg.plc.heartbeat) if cfg.plc.heartbeat else None
robot     = RobotMonitor.from_config(cfg.robot, bus=bus) if cfg.robot else None
robot_pub = (RobotPublisher(robot, plc, cfg.plc.robot_status, bus=bus)
             if robot and cfg.plc.robot_status else None)
db        = DBOrchestrator.from_config(
    cfg, plc=plc, bridge=bridge, archive=archive, bus=bus, robot=robot,
)
robot_vars = (
    RobotVariablesMonitor(
        robot.client, list(cfg.robot.vars),
        errors_store=db.errors, plc=plc, bus=bus,
    )
    if robot and cfg.robot and cfg.robot.vars else None
)
robot_elog = (
    RobotElogPoller(
        robot, db.errors,
        poll_ms      = cfg.robot.elog_poll_ms,
        domain       = cfg.robot.elog_domain,
        limit        = cfg.robot.elog_limit,
        include_info = cfg.robot.elog_include_info,
        bus          = bus,
    )
    if robot and db.errors and cfg.robot and cfg.robot.elog_poll_ms > 0 else None
)
robot_master = (
    RobotMasterMonitor(
        robot.client, db.sizes,
        task          = cfg.robot.master_task,
        module        = cfg.robot.master_module,
        master_symbol = cfg.robot.master_symbol,
        dims_symbol   = cfg.robot.master_dims_symbol,
        poll_ms       = cfg.robot.master_poll_ms,
        bus           = bus,
    )
    if robot and db.sizes and cfg.robot and cfg.robot.master_poll_ms > 0 else None
)


# ─── ui ──────────────────────────────────────────────────────────────────────

# Serve TTF (or any) static assets from ./static — used by theme.py for the
# Magistral wordmark font. Empty dir is fine; missing files fall back to Muli.
app.add_static_files("/static", str(Path(__file__).with_name("static")))

dashboard = Dashboard.build(
    cameras            = cameras,
    robot_monitor      = robot,
    robot_vars_monitor = robot_vars,
    recipes_store      = db.recipes,
    sizes_store        = db.sizes,
    errors_store       = db.errors,
    robot_status_log   = db.robot_status_log,
    title              = cfg.ui.title,
    bus                = bus,
)
dashboard.register_routes()


# ─── lifecycle ────────────────────────────────────────────────────────────────

@app.on_startup
def _startup() -> None:
    # Cameras need the running asyncio loop so PLC callbacks (on the
    # AmsRouter thread) can schedule snapshots cross-thread. on_startup
    # runs inside the NiceGUI loop, so get_event_loop() returns it.
    import asyncio
    loop = asyncio.get_event_loop()
    cameras.attach_loop(loop)
    # 4 workers: camera/recipe/unit-logger/robot_master each subscribe
    # in mode='thread'; 2 was the old default and is too tight for the
    # current set of bus subscribers.
    bus.start(loop, workers=4)              # before any producer publishes
    plc.open()
    if archive:    archive.start()        # one-shot prune
    bridge.start()
    publisher.start()
    cam_pub.start()
    if heartbeat:  heartbeat.start()
    if robot:      robot.start()
    if robot_pub:  robot_pub.start()
    db.start()
    if robot_vars:   robot_vars.start()     # depends on db.errors being open
    if robot_elog:   robot_elog.start()     # mirror RWS event log into errors_store
    if robot_master: robot_master.start()   # mirror Master arrays ↔ sizes DB


@app.on_shutdown
def _shutdown() -> None:
    if robot_master: robot_master.stop()
    if robot_elog:   robot_elog.stop()
    if robot_vars:   robot_vars.stop()
    db.stop()
    if robot_pub:  robot_pub.stop()
    if robot:      robot.stop()
    if heartbeat:  heartbeat.stop()
    cam_pub.stop()
    publisher.stop()
    bridge.stop()
    if archive:    archive.stop()
    plc.close()
    bus.stop()                              # after every producer is silent


# ─── run ──────────────────────────────────────────────────────────────────────

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(host=cfg.ui.host, port=cfg.ui.port, title=cfg.ui.title)
