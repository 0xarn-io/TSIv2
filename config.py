"""config.py — load app settings from a TOML file."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path

from camera_panel import CameraConfig


@dataclass(frozen=True)
class PLCSettings:
    ams_net_id: str
    enable_symbol: str | None


@dataclass(frozen=True)
class ScannerSettings:
    separation_m: float
    belt_speed_mps: float
    belt_y: float


@dataclass(frozen=True)
class UISettings:
    refresh_hz: float
    host: str
    port: int
    title: str


@dataclass(frozen=True)
class AppConfig:
    plc: PLCSettings
    scanner: ScannerSettings
    ui: UISettings
    cameras: list[CameraConfig]

    @classmethod
    def load(cls, path: str | Path) -> "AppConfig":
        with Path(path).open("rb") as f:
            d = tomllib.load(f)
        return cls(
            plc=PLCSettings(
                ams_net_id=d["plc"]["ams_net_id"],
                enable_symbol=d["plc"].get("enable_symbol") or None,
            ),
            scanner=ScannerSettings(**d["scanner"]),
            ui=UISettings(**d["ui"]),
            cameras=[CameraConfig(c["name"], c["url"]) for c in d["cameras"]],
        )