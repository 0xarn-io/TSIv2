"""Tests for AppConfig.load + dataclass shape."""
from __future__ import annotations

from pathlib import Path

from box_scene       import BoxConfig
from camera_panel    import CameraConfig
from config          import AppConfig, PLCSettings, ScannerSettings, UISettings
from plc_heartbeat   import HeartbeatConfig
from robot_publisher import RobotStatusConfig
from robot_status    import RobotConfig
from sick_publisher  import PublisherConfig


def test_load_full_config(app_toml: Path):
    cfg = AppConfig.load(app_toml)
    assert isinstance(cfg.plc, PLCSettings)
    assert isinstance(cfg.scanner, ScannerSettings)
    assert isinstance(cfg.ui, UISettings)


def test_vars_file_resolves_relative_to_app_toml(app_toml: Path):
    cfg = AppConfig.load(app_toml)
    assert Path(cfg.plc.vars_file).is_absolute()
    assert Path(cfg.plc.vars_file).exists()


def test_publisher_loaded(app_toml: Path):
    cfg = AppConfig.load(app_toml)
    pub = cfg.plc.publisher
    assert isinstance(pub, PublisherConfig)
    assert pub.event_alias  == "sick.event"
    assert pub.live_alias   == "sick.live"
    assert pub.enable_alias == "sick.enable"
    assert pub.enable_cycle_ms == 100


def test_scanner_settings(app_toml: Path):
    cfg = AppConfig.load(app_toml)
    assert cfg.scanner.udp_port_a == 2111
    assert cfg.scanner.udp_port_b == 2112
    assert cfg.scanner.separation_m == 2.45


def test_cameras_list(app_toml: Path):
    cfg = AppConfig.load(app_toml)
    assert len(cfg.cameras) == 1
    cam = cfg.cameras[0]
    assert isinstance(cam, CameraConfig)
    assert cam.name == "entry"
    assert cam.rtsp_url == "rtsp://x/1"


def test_boxes_list(app_toml: Path):
    cfg = AppConfig.load(app_toml)
    assert len(cfg.boxes) == 1
    b = cfg.boxes[0]
    assert isinstance(b, BoxConfig)
    assert b.width_mm == 711
    assert b.height_mm == 1800
    assert b.depth_mm == 1778


def test_heartbeat_optional_absent_means_none(tmp_path: Path, signals_toml: Path):
    """If [plc.heartbeat] is omitted, cfg.plc.heartbeat is None."""
    p = tmp_path / "no_hb.toml"
    p.write_text(f"""
[plc]
vars_file = "{signals_toml.name}"
[plc.publisher]
event_alias = "sick.event"
live_alias = "sick.live"
enable_alias = "sick.enable"
[scanner]
udp_port_a = 2111
udp_port_b = 2112
separation_m = 2.45
belt_speed_mps = 0.254
belt_y = -1.48
[ui]
refresh_hz = 10.0
host = "0.0.0.0"
port = 8080
title = "Test"
[[cameras]]
name = "x"
url = "rtsp://x"
[[boxes]]
width_mm = 1
height_mm = 1
depth_mm = 1
x_pos = 0.0
""")
    cfg = AppConfig.load(p)
    assert cfg.plc.heartbeat is None


def test_robot_optional_absent(app_toml: Path):
    """Default fixture has no [robot]; cfg.robot should be None."""
    cfg = AppConfig.load(app_toml)
    assert cfg.robot is None
    assert cfg.plc.robot_status is None


def test_robot_loads_when_present(tmp_path: Path, signals_toml: Path):
    p = tmp_path / "with_robot.toml"
    p.write_text(f"""
[plc]
vars_file = "{signals_toml.name}"
[plc.publisher]
event_alias = "sick.event"
live_alias = "sick.live"
enable_alias = "sick.enable"
[plc.robot_status]
status_alias = "robot.status"
[scanner]
udp_port_a = 2111
udp_port_b = 2112
separation_m = 2.45
belt_speed_mps = 0.254
belt_y = -1.48
[ui]
refresh_hz = 10.0
host = "0.0.0.0"
port = 8080
title = "Test"
[[cameras]]
name = "x"
url = "rtsp://x"
[[boxes]]
width_mm = 1
height_mm = 1
depth_mm = 1
x_pos = 0.0
[robot]
ip = "192.168.125.1"
poll_ms = 1500
""")
    cfg = AppConfig.load(p)
    assert isinstance(cfg.robot, RobotConfig)
    assert cfg.robot.ip == "192.168.125.1"
    assert cfg.robot.poll_ms == 1500
    assert isinstance(cfg.plc.robot_status, RobotStatusConfig)
    assert cfg.plc.robot_status.status_alias == "robot.status"


def test_heartbeat_loads_when_present(tmp_path: Path, signals_toml: Path):
    p = tmp_path / "with_hb.toml"
    p.write_text(f"""
[plc]
vars_file = "{signals_toml.name}"
[plc.publisher]
event_alias = "sick.event"
live_alias = "sick.live"
enable_alias = "sick.enable"
[plc.heartbeat]
alias = "health.heartbeat"
period_ms = 500
[scanner]
udp_port_a = 2111
udp_port_b = 2112
separation_m = 2.45
belt_speed_mps = 0.254
belt_y = -1.48
[ui]
refresh_hz = 10.0
host = "0.0.0.0"
port = 8080
title = "Test"
[[cameras]]
name = "x"
url = "rtsp://x"
[[boxes]]
width_mm = 1
height_mm = 1
depth_mm = 1
x_pos = 0.0
""")
    cfg = AppConfig.load(p)
    assert isinstance(cfg.plc.heartbeat, HeartbeatConfig)
    assert cfg.plc.heartbeat.alias == "health.heartbeat"
    assert cfg.plc.heartbeat.period_ms == 500
