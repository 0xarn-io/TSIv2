"""config.py — load app settings from app_config.toml.

Two TOMLs split the concerns:
    app_config.toml    — UI, scanner, cameras, PLC connection
    plc_signals.toml   — TwinCAT structs + var aliases (loaded by TwinCATComm)

Add a new field: 1) add it to the matching dataclass, 2) read it in load().
"""
from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path

from camera_panel     import CameraConfig
from camera_publisher import CameraTriggerConfig
from errors_store     import ErrorsConfig
from plc_heartbeat    import HeartbeatConfig
from recipe_publisher import RecipePublisherConfig
from recipes_store    import RecipesConfig
from robot_publisher  import RobotStatusConfig
from robot_status     import RobotConfig
from robot_status_log  import RobotStatusLogConfig
from robot_variables  import RobotVariableConfig
from sick_publisher   import PublisherConfig
from sizes_store      import SizesConfig
from snapshot_archive import SnapshotArchiveConfig
from unit_logger      import UnitLoggerConfig


@dataclass(frozen=True)
class PLCSettings:
    vars_file:       str
    publisher:       PublisherConfig
    camera_triggers: list[CameraTriggerConfig]
    heartbeat:       HeartbeatConfig | None
    robot_status:    RobotStatusConfig | None
    recipe:          RecipePublisherConfig | None


@dataclass(frozen=True)
class ScannerSettings:
    udp_port_a:     int
    udp_port_b:     int
    separation_m:   float
    belt_speed_mps: float
    belt_y:         float


@dataclass(frozen=True)
class UISettings:
    refresh_hz: float
    host:       str
    port:       int
    title:      str


@dataclass(frozen=True)
class AppConfig:
    plc:        PLCSettings
    scanner:    ScannerSettings
    ui:         UISettings
    cameras:    list[CameraConfig]
    robot:      RobotConfig | None
    recipes:    RecipesConfig | None
    sizes:      SizesConfig | None
    unit_log:   UnitLoggerConfig | None
    errors_log: ErrorsConfig | None
    snapshots:  SnapshotArchiveConfig | None
    robot_status_log: RobotStatusLogConfig | None

    @classmethod
    def load(cls, path: str | Path) -> "AppConfig":
        path = Path(path)
        with path.open("rb") as f:
            d = tomllib.load(f)
        base = path.parent

        # PLC vars_file is relative to app_config.toml's directory.
        vars_file = str((base / d["plc"]["vars_file"]).resolve())

        return cls(
            plc=PLCSettings(
                vars_file=vars_file,
                publisher=PublisherConfig(**d["plc"]["publisher"]),
                camera_triggers=[
                    CameraTriggerConfig(**t)
                    for t in d["plc"].get("camera_triggers", [])
                ],
                heartbeat=(
                    HeartbeatConfig(**d["plc"]["heartbeat"])
                    if "heartbeat" in d["plc"] else None
                ),
                robot_status=(
                    RobotStatusConfig(**d["plc"]["robot_status"])
                    if "robot_status" in d["plc"] else None
                ),
                recipe=(
                    RecipePublisherConfig(**d["plc"]["recipe"])
                    if "recipe" in d["plc"] else None
                ),
            ),
            scanner=ScannerSettings(**d["scanner"]),
            ui=UISettings(**d["ui"]),
            cameras=[CameraConfig(name=c["name"], rtsp_url=c["url"])
                     for c in d["cameras"]],
            robot=_load_robot(d.get("robot"), base),
            recipes=(
                RecipesConfig(**d["recipes"]) if "recipes" in d else None
            ),
            sizes=(
                SizesConfig(**d["sizes"]) if "sizes" in d else None
            ),
            unit_log=(
                UnitLoggerConfig(**d["unit_log"]) if "unit_log" in d else None
            ),
            errors_log=(
                ErrorsConfig(**d["errors_log"]) if "errors_log" in d else None
            ),
            snapshots=(
                SnapshotArchiveConfig(**d["snapshots"])
                if "snapshots" in d else None
            ),
            robot_status_log=(
                RobotStatusLogConfig(**d["robot_status_log"])
                if "robot_status_log" in d else None
            ),
        )


def _load_robot(d: dict | None, base: Path) -> RobotConfig | None:
    """[robot] section.

    The variable list is loaded from a separate file pointed at by
    `vars_file` (path is resolved relative to app_config.toml). Inline
    `[[robot.vars]]` blocks are still honored as a legacy escape hatch.
    """
    if not d:
        return None

    # vars_file (preferred) — load [[vars]] entries from a sibling TOML.
    vars_file = d.get("vars_file")
    raw_vars: list[dict] = []
    if vars_file:
        p = (base / vars_file).resolve()
        if p.is_file():
            with p.open("rb") as f:
                raw_vars = list(tomllib.load(f).get("vars", []))
    # Legacy: inline [[robot.vars]] blocks.
    raw_vars += list(d.get("vars", []))

    fields = {k: v for k, v in d.items() if k not in ("vars", "vars_file")}
    vars_tuple = tuple(
        RobotVariableConfig(
            **{
                **v,
                "targets": tuple(v.get("targets", ("ui",))),
            }
        )
        for v in raw_vars
    )
    return RobotConfig(**fields, vars=vars_tuple)
