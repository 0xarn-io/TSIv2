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


# ---- bus-mode subscription -------------------------------------------------

def test_bus_path_writes_row_and_uses_cached_recipe_code(tmp_path: Path):
    """When a bus is supplied, unit events arrive via the bus and the
    recipe code is the last value seen on PlcSignalChanged — never read
    synchronously inside the event path."""
    import asyncio
    from event_bus import EventBus
    from events import (
        PlcSignalChanged, SickUnitEvent, signals,
    )

    bus = EventBus()
    loop = asyncio.new_event_loop()
    bus.start(loop)
    try:
        cfg = UnitLoggerConfig(db_path=str(tmp_path / "units.db"))
        bridge = MagicMock()  # should not be subscribed when bus is used
        plc = MagicMock(); plc.read.return_value = 3   # bootstrap value
        u = UnitLogger(cfg, bridge, plc,
                       recipe_alias="recipe.code", bus=bus)
        u.start()
        try:
            # Recipe code update arrives via the bus.
            bus.publish(signals.plc_signal_changed,
                        PlcSignalChanged(alias="recipe.code",
                                         value=11, ts=0.0))
            # Unit event arrives via the bus.
            bus.publish(signals.sick_unit_event,
                        SickUnitEvent(event=_evt()))
            _drain(u, 1, timeout=2.0)
            row = u.recent()[0]
            assert row["recipe_code"] == 11
            assert row["width_mm"]    == 711
            # bridge.on_event must NOT have been used in bus mode.
            bridge.on_event.assert_not_called()
        finally:
            u.stop()
    finally:
        bus.stop()
        loop.close()


def test_bus_path_ignores_unrelated_alias(tmp_path: Path):
    """Recipe-code subscription only updates the cache for our alias."""
    import asyncio
    from event_bus import EventBus
    from events import PlcSignalChanged, SickUnitEvent, signals

    bus = EventBus()
    loop = asyncio.new_event_loop()
    bus.start(loop)
    try:
        cfg = UnitLoggerConfig(db_path=str(tmp_path / "units.db"))
        plc = MagicMock(); plc.read.return_value = 5  # bootstrap
        u = UnitLogger(cfg, MagicMock(), plc,
                       recipe_alias="recipe.code", bus=bus)
        u.start()
        try:
            # Different alias — should NOT update the recipe cache.
            bus.publish(signals.plc_signal_changed,
                        PlcSignalChanged(alias="some.other.signal",
                                         value=99, ts=0.0))
            bus.publish(signals.sick_unit_event,
                        SickUnitEvent(event=_evt()))
            _drain(u, 1, timeout=2.0)
            assert u.recent()[0]["recipe_code"] == 5  # bootstrap value
        finally:
            u.stop()
    finally:
        bus.stop()
        loop.close()
