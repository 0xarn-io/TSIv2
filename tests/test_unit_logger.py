"""Tests for UnitLogger — schema, event subscribe, recipe code, archive paths, drain."""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pysickudt import UnitEvent       # mock from conftest
from unit_logger import UnitLogger, UnitLoggerConfig


def _evt(**overrides) -> UnitEvent:
    base = dict(
        entered_at=1.0, exited_at=2.0, duration_s=1.0, n_samples=42,
        length_m=0.250,
        width_mean_m=0.711, width_min_m=0.700, width_max_m=0.720,
        height_mean_m=1.800, height_min_m=1.790, height_max_m=1.810,
        offset_mean_m=0.005, offset_min_m=-0.001, offset_max_m=0.012,
    )
    base.update(overrides)
    return UnitEvent(**base)


def _make(tmp_path: Path, *, recipe_alias=None, archive=None,
          plc=None, bridge=None) -> UnitLogger:
    cfg = UnitLoggerConfig(db_path=str(tmp_path / "units.db"))
    if bridge is None:
        bridge = MagicMock()
        bridge.on_event = MagicMock(side_effect=lambda cb: cb)  # returns cb as unsub
    if plc is None:
        plc = MagicMock()
    return UnitLogger(cfg, bridge, plc,
                      recipe_alias=recipe_alias, archive=archive)


def _drain(logger: UnitLogger, expected: int, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if len(logger.recent(limit=expected + 5)) >= expected:
            return
        time.sleep(0.01)
    raise AssertionError(f"writer didn't drain {expected} rows in {timeout}s")


# ---- schema + lifecycle -----------------------------------------------------

def test_start_creates_schema(tmp_path: Path):
    u = _make(tmp_path); u.start()
    try:
        with sqlite3.connect(u.cfg.db_path) as c:
            tables = {r[0] for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
        assert "unit" in tables
    finally:
        u.stop()


def test_start_subscribes_to_bridge(tmp_path: Path):
    bridge = MagicMock()
    bridge.on_event = MagicMock(return_value=MagicMock())
    u = _make(tmp_path, bridge=bridge); u.start()
    try:
        bridge.on_event.assert_called_once()
    finally:
        u.stop()


def test_stop_unsubscribes(tmp_path: Path):
    unsub = MagicMock()
    bridge = MagicMock()
    bridge.on_event = MagicMock(return_value=unsub)
    u = _make(tmp_path, bridge=bridge); u.start()
    u.stop()
    unsub.assert_called_once()


# ---- event handling ---------------------------------------------------------

def test_event_writes_row_with_mm_conversion(tmp_path: Path):
    captured = {}
    bridge = MagicMock()
    def fake_on_event(cb):
        captured["cb"] = cb
        return MagicMock()
    bridge.on_event.side_effect = fake_on_event

    u = _make(tmp_path, bridge=bridge); u.start()
    try:
        captured["cb"](_evt())
        _drain(u, 1)
        row = u.recent()[0]
        assert row["width_mm"]      == 711
        assert row["height_mm"]     == 1800
        assert row["offset_mm"]     == 5
        assert row["length_mm"]     == 250
        assert row["width_min_mm"]  == 700
        assert row["width_max_mm"]  == 720
        assert row["samples"]       == 42
        assert row["duration_s"]    == pytest.approx(1.0)
        assert row["raw_event_json"]                        # JSON populated
    finally:
        u.stop()


def test_recipe_code_read_when_alias_given(tmp_path: Path):
    captured = {}
    bridge = MagicMock()
    bridge.on_event.side_effect = (
        lambda cb: captured.setdefault("cb", cb) or MagicMock()
    )
    plc = MagicMock(); plc.read.return_value = 7
    u = _make(tmp_path, bridge=bridge, plc=plc, recipe_alias="recipe.code")
    u.start()
    try:
        captured["cb"](_evt())
        _drain(u, 1)
        plc.read.assert_called_with("recipe.code")
        assert u.recent()[0]["recipe_code"] == 7
    finally:
        u.stop()


def test_recipe_code_null_when_no_alias(tmp_path: Path):
    captured = {}
    bridge = MagicMock()
    bridge.on_event.side_effect = (
        lambda cb: captured.setdefault("cb", cb) or MagicMock()
    )
    plc = MagicMock()
    u = _make(tmp_path, bridge=bridge, plc=plc)        # no recipe_alias
    u.start()
    try:
        captured["cb"](_evt())
        _drain(u, 1)
        assert u.recent()[0]["recipe_code"] is None
        plc.read.assert_not_called()
    finally:
        u.stop()


def test_recipe_code_read_failure_falls_back_to_null(tmp_path: Path):
    captured = {}
    bridge = MagicMock()
    bridge.on_event.side_effect = (
        lambda cb: captured.setdefault("cb", cb) or MagicMock()
    )
    plc = MagicMock(); plc.read.side_effect = RuntimeError("ADS down")
    u = _make(tmp_path, bridge=bridge, plc=plc, recipe_alias="recipe.code")
    u.start()
    try:
        captured["cb"](_evt())
        _drain(u, 1)
        assert u.recent()[0]["recipe_code"] is None
    finally:
        u.stop()


def test_camera_paths_from_archive(tmp_path: Path):
    captured = {}
    bridge = MagicMock()
    bridge.on_event.side_effect = (
        lambda cb: captured.setdefault("cb", cb) or MagicMock()
    )
    archive = MagicMock()
    archive.latest_path.side_effect = lambda name: f"/snaps/{name}.jpg"
    u = _make(tmp_path, bridge=bridge, archive=archive)
    u.start()
    try:
        captured["cb"](_evt())
        _drain(u, 1)
        row = u.recent()[0]
        assert row["snap_entry_path"] == "/snaps/entry.jpg"
        assert row["snap_exit_path"]  == "/snaps/exit.jpg"
    finally:
        u.stop()


def test_camera_paths_null_when_no_archive(tmp_path: Path):
    captured = {}
    bridge = MagicMock()
    bridge.on_event.side_effect = (
        lambda cb: captured.setdefault("cb", cb) or MagicMock()
    )
    u = _make(tmp_path, bridge=bridge)                  # no archive
    u.start()
    try:
        captured["cb"](_evt())
        _drain(u, 1)
        row = u.recent()[0]
        assert row["snap_entry_path"] is None
        assert row["snap_exit_path"]  is None
    finally:
        u.stop()


def test_writer_drains_on_stop(tmp_path: Path):
    captured = {}
    bridge = MagicMock()
    bridge.on_event.side_effect = (
        lambda cb: captured.setdefault("cb", cb) or MagicMock()
    )
    u = _make(tmp_path, bridge=bridge); u.start()
    for i in range(15):
        captured["cb"](_evt(n_samples=i))
    u.stop()
    with sqlite3.connect(u.cfg.db_path) as c:
        n = c.execute("SELECT COUNT(*) FROM unit").fetchone()[0]
    assert n == 15


def test_recent_orders_newest_first(tmp_path: Path):
    captured = {}
    bridge = MagicMock()
    bridge.on_event.side_effect = (
        lambda cb: captured.setdefault("cb", cb) or MagicMock()
    )
    u = _make(tmp_path, bridge=bridge); u.start()
    try:
        for i in range(5):
            captured["cb"](_evt(n_samples=i))
        _drain(u, 5)
        rows = u.recent()
        sample_order = [r["samples"] for r in rows]
        assert sample_order == sorted(sample_order, reverse=True)
    finally:
        u.stop()


def test_db_dir_auto_created(tmp_path: Path):
    cfg = UnitLoggerConfig(db_path=str(tmp_path / "deep" / "nested" / "units.db"))
    bridge = MagicMock(); bridge.on_event = MagicMock(return_value=MagicMock())
    u = UnitLogger(cfg, bridge, MagicMock())
    u.start()
    try:
        assert Path(cfg.db_path).is_file()
    finally:
        u.stop()


# ---- bus-mode integration ----
# Real EventBus + FakeTwinCATComm + real UnitLogger. Verifies the
# recipe-code cache stays fresh via PlcSignalChanged events without a
# per-unit ADS read on the SICK receiver thread.

import asyncio
import threading
from event_bus import EventBus
from tests.fakes import FakeTwinCATComm


def _bus_loop():
    loop = asyncio.new_event_loop()
    started = threading.Event()
    def _run():
        asyncio.set_event_loop(loop)
        loop.call_soon(started.set)
        loop.run_forever()
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    started.wait(timeout=1.0)
    return loop, t


def test_bus_mode_registers_recipe_alias_notification(tmp_path: Path):
    """ensure_published must be called for the recipe alias in bus mode."""
    bus = EventBus()
    loop, t = _bus_loop()
    bus.start(loop)
    plc = FakeTwinCATComm(bus=bus)
    plc.read = lambda alias: 0
    bridge = MagicMock(); bridge.on_event = MagicMock(return_value=MagicMock())
    cfg = UnitLoggerConfig(db_path=str(tmp_path / "u.db"))
    u = UnitLogger(cfg, bridge, plc, recipe_alias="recipe.code", bus=bus)
    try:
        u.start()
        assert "recipe.code" in plc.aliases_with_notifications()
    finally:
        u.stop()
        bus.stop()
        loop.call_soon_threadsafe(loop.stop)
        t.join(timeout=2.0)
        loop.close()


def test_bus_mode_recipe_cache_updates_on_plc_change(tmp_path: Path):
    """A PlcSignalChanged for the recipe alias updates the cache;
    subsequent unit events get the new code without a sync ADS read."""
    bus = EventBus()
    loop, t = _bus_loop()
    bus.start(loop)
    plc = FakeTwinCATComm(bus=bus)
    plc.read = lambda alias: 0   # initial seed = 0
    captured = {}
    bridge = MagicMock()
    def on_event(cb):
        captured["cb"] = cb
        return MagicMock()
    bridge.on_event = MagicMock(side_effect=on_event)
    cfg = UnitLoggerConfig(db_path=str(tmp_path / "u.db"))
    u = UnitLogger(cfg, bridge, plc, recipe_alias="recipe.code", bus=bus)
    try:
        u.start()
        # PLC pushes new recipe code 7; bus subscriber updates the cache.
        plc.simulate_change("recipe.code", 7)
        for _ in range(50):
            if u._recipe_code_cache == 7:
                break
            time.sleep(0.01)
        assert u._recipe_code_cache == 7
        # Now a unit event arrives; row should pick up code 7 from cache,
        # not call plc.read again (which would still return 0).
        captured["cb"](_evt())
        _drain(u, 1)
        assert u.recent()[0]["recipe_code"] == 7
    finally:
        u.stop()
        bus.stop()
        loop.call_soon_threadsafe(loop.stop)
        t.join(timeout=2.0)
        loop.close()
