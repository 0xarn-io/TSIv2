"""Tests for AppConfig.load + dataclass shape."""
from __future__ import annotations

from pathlib import Path

from box_scene      import BoxConfig
from camera_panel   import CameraConfig
from config         import AppConfig, PLCSettings, ScannerSettings, UISettings
from sick_publisher import PublisherConfig


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
