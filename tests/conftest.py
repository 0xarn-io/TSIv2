"""Shared pytest fixtures + pysickudt mock so tests can import the project.

pysickudt is not on PyPI, so we install a minimal stand-in into sys.modules
before any project module imports it.
"""
from __future__ import annotations

import sys
import types
from dataclasses import dataclass


def _install_pysickudt_mock() -> None:
    if "pysickudt" in sys.modules:
        return

    pkg = types.ModuleType("pysickudt")
    stream = types.ModuleType("pysickudt.stream")

    class ROI:
        def __init__(self, **kw): self.__dict__.update(kw)

    class ScanProcessor:
        def __init__(self, *, rotation_deg=0.0, translation=(0.0, 0.0), roi=None):
            self.rotation_deg = rotation_deg
            self.translation  = translation
            self.roi = roi
        def process(self, scan): return scan

    @dataclass
    class UnitEvent:
        # Match pysickudt.tracking.UnitEvent (https://github.com/0xarn-io/SickUDT)
        entered_at:    float = 0.0
        exited_at:     float = 0.0
        duration_s:    float = 0.0
        n_samples:     int   = 0
        length_m:      float = 0.0
        width_mean_m:  float = 0.0
        width_min_m:   float = 0.0
        width_max_m:   float = 0.0
        height_mean_m: float = 0.0
        height_min_m:  float = 0.0
        height_max_m:  float = 0.0
        offset_mean_m: float = 0.0
        offset_min_m:  float = 0.0
        offset_max_m:  float = 0.0

    class UnitTracker:
        def __init__(self, **kw):
            self.is_present = False
            self.last_event = None
        def feed(self, *_): pass

    def axis_position(*_a, **_kw): return None
    def height_range(*_a, **_kw):  return None

    class UDPReceiver:
        def __init__(self, **kw): self._cb = None
        def on_scan(self, cb):    self._cb = cb
        def start(self):          pass
        def stop(self):           pass
        def stats(self):          return {}

    pkg.ROI            = ROI
    pkg.ScanProcessor  = ScanProcessor
    pkg.UnitEvent      = UnitEvent
    pkg.UnitTracker    = UnitTracker
    pkg.axis_position  = axis_position
    pkg.height_range   = height_range
    stream.UDPReceiver = UDPReceiver

    sys.modules["pysickudt"]        = pkg
    sys.modules["pysickudt.stream"] = stream


_install_pysickudt_mock()


import pytest

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def signals_toml(tmp_path: Path) -> Path:
    p = tmp_path / "plc_signals.toml"
    p.write_text("""
[ams]
net_id = "1.2.3.4.1.1"
port = 851

[structs.ST_SickEvent]
bNew         = "BOOL"
nLength      = "DINT"
nWidthMean   = "DINT"
nWidthMin    = "DINT"
nWidthMax    = "DINT"
nHeightMean  = "DINT"
nHeightMin   = "DINT"
nHeightMax   = "DINT"
nOffsetMean  = "DINT"
nOffsetMin   = "DINT"
nOffsetMax   = "DINT"
fDuration    = "REAL"
nSamples     = "DINT"

[structs.ST_SickLive]
nWidth  = "DINT"
nHeight = "DINT"
nOffset = "DINT"

[structs.ST_RobotStatus]
bReady       = "BOOL"
bMotorsOn    = "BOOL"
bAutoMode    = "BOOL"
bRunning     = "BOOL"
bGuardStop   = "BOOL"
bEStop       = "BOOL"
nSpeedRatio  = "DINT"

[structs.ST_RecipeSetpoints]
nWidth   = "DINT"
nHeight  = "DINT"
nDepth   = "DINT"
nX1Pos   = "DINT"
nX2Pos   = "DINT"
nX3Pos   = "DINT"
nY1Pos   = "DINT"
nY2Pos   = "DINT"
nY3Pos   = "DINT"

[groups.sick]
prefix = ""

[groups.sick.vars]
event   = { name = "GVL_Sick.SickEvent", type = "ST_SickEvent" }
live    = { name = "GVL_Sick.SickLive",  type = "ST_SickLive" }
enable  = { name = "GVL_Sick.bEnable",   type = "BOOL" }

[groups.health]
prefix = ""

[groups.health.vars]
heartbeat = { name = "GVL.nHeartbeat", type = "UDINT" }

[groups.robot]
prefix = ""

[groups.robot.vars]
status = { name = "GVL.RobotStatus", type = "ST_RobotStatus" }

[groups.recipe]
prefix = ""

[groups.recipe.vars]
code      = { name = "GVL.nRecipeCode",     type = "DINT" }
setpoints = { name = "GVL.RecipeSetpoints", type = "ST_RecipeSetpoints" }
""")
    return p


@pytest.fixture
def app_toml(tmp_path: Path, signals_toml: Path) -> Path:
    p = tmp_path / "app_config.toml"
    p.write_text(f"""
[plc]
vars_file = "{signals_toml.name}"

[plc.publisher]
event_alias     = "sick.event"
live_alias      = "sick.live"
enable_alias    = "sick.enable"
enable_cycle_ms = 100

[scanner]
udp_port_a     = 2111
udp_port_b     = 2112
separation_m   = 2.45
belt_speed_mps = 0.254
belt_y         = -1.48

[ui]
refresh_hz = 10.0
host       = "0.0.0.0"
port       = 8080
title      = "Test"

[[cameras]]
name = "entry"
url  = "rtsp://x/1"

[[boxes]]
width_mm  = 711
height_mm = 1800
depth_mm  = 1778
x_pos     = 0.0
""")
    return p
