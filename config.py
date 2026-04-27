"""config.py — load app settings from app_config.toml.

Two TOMLs split the concerns:
    app_config.toml    — UI, scanner, cameras, boxes, PLC connection
    plc_signals.toml   — TwinCAT structs + var aliases (loaded by TwinCATComm)

Add a new field: 1) add it to the matching dataclass, 2) read it in load().
"""
from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path

from box_scene        import BoxConfig
from camera_panel     import CameraConfig
from camera_publisher import CameraTriggerConfig
from sick_publisher   import PublisherConfig


@dataclass(frozen=True)
class PLCSettings:
    vars_file:       str
    publisher:       PublisherConfig
    camera_triggers: list[CameraTriggerConfig]


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
    plc:     PLCSettings
    scanner: ScannerSettings
    ui:      UISettings
    cameras: list[CameraConfig]
    boxes:   list[BoxConfig]

    @classmethod
    def load(cls, path: str | Path) -> "AppConfig":
        path = Path(path)
        with path.open("rb") as f:
            d = tomllib.load(f)

        # PLC vars_file is relative to app_config.toml's directory.
        vars_file = str((path.parent / d["plc"]["vars_file"]).resolve())

        return cls(
            plc=PLCSettings(
                vars_file=vars_file,
                publisher=PublisherConfig(**d["plc"]["publisher"]),
                camera_triggers=[
                    CameraTriggerConfig(**t)
                    for t in d["plc"].get("camera_triggers", [])
                ],
            ),
            scanner=ScannerSettings(**d["scanner"]),
            ui=UISettings(**d["ui"]),
            cameras=[CameraConfig(name=c["name"], rtsp_url=c["url"])
                     for c in d["cameras"]],
            boxes=[BoxConfig(**b) for b in d["boxes"]],
        )
