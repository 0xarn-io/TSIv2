"""Tests for RobotStatusLog — schema, bus subscription, tick sampling,
retention prune, and the three aggregation queries."""
from __future__ import annotations

import asyncio
import sqlite3
import time
from pathlib import Path

import pytest

from event_bus        import EventBus
from events           import RobotStatusChanged, signals
from robot_status     import RobotStatus
from robot_status_log import RobotStatusLog, RobotStatusLogConfig


def _drain(store: RobotStatusLog, expected: int, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if len(store.recent(limit=expected + 5)) >= expected:
            return
        time.sleep(0.01)
    raise AssertionError(
        f"writer thread didn't drain {expected} rows in {timeout}s"
    )


@pytest.fixture
def bus():
    b = EventBus()
    loop = asyncio.new_event_loop()
    b.start(loop, workers=2)
    yield b
    b.stop()
    loop.close()


def _store(tmp_path: Path, *, bus=None, monitor=None, **overrides) -> RobotStatusLog:
    cfg = RobotStatusLogConfig(
        db_path=str(tmp_path / "rsl.db"), **overrides,
    )
    return RobotStatusLog.from_config(cfg, bus=bus, monitor=monitor)


# ─── schema ──────────────────────────────────────────────────────────────────

def test_start_creates_schema(tmp_path: Path):
    s = _store(tmp_path, tick_period_s=0)
    s.start()
    try:
        with sqlite3.connect(s.cfg.db_path) as c:
            tables = {r[0] for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
        assert "robot_status_log" in tables
    finally:
        s.stop()


# ─── bus subscription ────────────────────────────────────────────────────────

def test_bus_change_event_logged(tmp_path: Path, bus):
    s = _store(tmp_path, bus=bus, tick_period_s=0)
    s.start()
    try:
        bus.publish(
            signals.robot_status_changed,
            RobotStatusChanged(status=RobotStatus(
                opmode="AUTO", ctrl_state="motoron",
                exec_state="running", speed_ratio=100,
            )),
        )
        _drain(s, 1)
        rows = s.recent()
        assert len(rows) == 1
        r = rows[0]
        assert r["opmode"]      == "AUTO"
        assert r["ctrl_state"]  == "motoron"
        assert r["exec_state"]  == "running"
        assert r["speed_ratio"] == 100
        assert r["is_ready"]    == 1
        assert r["source"]      == "change"
    finally:
        s.stop()


def test_bus_unsubscribe_on_stop(tmp_path: Path, bus):
    """After stop(), further publishes must NOT add rows."""
    s = _store(tmp_path, bus=bus, tick_period_s=0)
    s.start()
    bus.publish(
        signals.robot_status_changed,
        RobotStatusChanged(status=RobotStatus(opmode="AUTO")),
    )
    _drain(s, 1)
    n_before = len(s.recent())
    s.stop()

    # Re-open a read conn to count rows on disk after stop.
    bus.publish(
        signals.robot_status_changed,
        RobotStatusChanged(status=RobotStatus(opmode="MANR")),
    )
    time.sleep(0.2)
    with sqlite3.connect(s.cfg.db_path) as c:
        n_after = c.execute("SELECT COUNT(*) FROM robot_status_log").fetchone()[0]
    assert n_after == n_before


# ─── heartbeat tick ──────────────────────────────────────────────────────────

class _FakeMonitor:
    def __init__(self, status: RobotStatus):
        self._s = status
    def status(self) -> RobotStatus:
        return self._s


def test_tick_samples_monitor(tmp_path: Path):
    fake = _FakeMonitor(RobotStatus(
        opmode="AUTO", ctrl_state="motoron",
        exec_state="running", speed_ratio=80,
    ))
    s = _store(tmp_path, monitor=fake, tick_period_s=0.1)
    s.start()
    try:
        _drain(s, 2, timeout=3.0)
        rows = s.recent(limit=10, source="tick")
        assert len(rows) >= 2
        assert all(r["source"] == "tick" for r in rows)
        assert rows[0]["opmode"] == "AUTO"
    finally:
        s.stop()


def test_tick_disabled_when_period_zero(tmp_path: Path):
    fake = _FakeMonitor(RobotStatus(opmode="AUTO"))
    s = _store(tmp_path, monitor=fake, tick_period_s=0.0)
    s.start()
    try:
        time.sleep(0.3)
        assert s.recent() == []
    finally:
        s.stop()


# ─── retention prune ─────────────────────────────────────────────────────────

def test_retention_prune_on_start(tmp_path: Path):
    s = _store(tmp_path, keep_days=1, tick_period_s=0)
    s.start()
    try:
        # Insert directly via writer thread, then back-date some rows.
        for _ in range(5):
            s._enqueue(RobotStatus(opmode="AUTO"), source="tick")
        _drain(s, 5)
    finally:
        s.stop()

    with sqlite3.connect(str(tmp_path / "rsl.db")) as c:
        c.execute(
            "UPDATE robot_status_log SET ts = datetime('now','-3 days') "
            "WHERE id <= 3"
        )
        c.commit()

    s2 = _store(tmp_path, keep_days=1, tick_period_s=0)
    s2.start()
    try:
        rows = s2.recent(limit=20)
        assert len(rows) == 2
    finally:
        s2.stop()


# ─── aggregation queries ─────────────────────────────────────────────────────

def _seed(store: RobotStatusLog, samples: list[tuple[str, str, str, int]]) -> None:
    """Enqueue a sequence of (opmode, ctrl, exec, speed) tuples and wait."""
    for opmode, ctrl, exec_state, speed in samples:
        store._enqueue(
            RobotStatus(opmode=opmode, ctrl_state=ctrl,
                        exec_state=exec_state, speed_ratio=speed),
            source="change",
        )
    _drain(store, len(samples))


def test_recent_filter_by_opmode(tmp_path: Path):
    s = _store(tmp_path, tick_period_s=0)
    s.start()
    try:
        _seed(s, [
            ("AUTO", "motoron", "running", 100),
            ("MANR", "motoron", "stopped", 25),
            ("AUTO", "motoron", "running", 100),
        ])
        rows = s.recent(opmode="AUTO")
        assert len(rows) == 2
        assert all(r["opmode"] == "AUTO" for r in rows)
    finally:
        s.stop()


def test_time_in_state_returns_per_opmode_seconds(tmp_path: Path):
    s = _store(tmp_path, tick_period_s=0)
    s.start()
    try:
        _seed(s, [
            ("AUTO", "motoron", "running", 100),
            ("MANR", "motoron", "stopped",  25),
            ("AUTO", "motoron", "running", 100),
        ])
        # Window covering everything.
        rows = s.time_in_state("1970-01-01 00:00:00")
        opmodes = {r["opmode"] for r in rows}
        assert "AUTO" in opmodes
        # All rows have non-negative durations.
        for r in rows:
            assert (r.get("seconds") or 0) >= 0
    finally:
        s.stop()


def test_transitions_only_returns_change_rows(tmp_path: Path):
    s = _store(tmp_path, tick_period_s=0)
    s.start()
    try:
        # Mix of source values.
        s._enqueue(RobotStatus(opmode="AUTO"), source="tick")
        s._enqueue(RobotStatus(opmode="MANR"), source="change")
        s._enqueue(RobotStatus(opmode="AUTO"), source="tick")
        _drain(s, 3)
        rows = s.transitions("1970-01-01 00:00:00")
        assert len(rows) == 1
        assert rows[0]["opmode"] == "MANR"
    finally:
        s.stop()


def test_daily_summary_aggregates_today(tmp_path: Path):
    s = _store(tmp_path, tick_period_s=0)
    s.start()
    try:
        _seed(s, [
            ("AUTO", "motoron", "running", 100),
            ("AUTO", "motoron", "running", 100),
            ("MANR", "motoroff", "stopped", 0),
        ])
        rows = s.daily_summary(days=1)
        # At least today's row.
        assert len(rows) >= 1
        today = rows[0]
        # The schema columns we care about exist with non-negative values.
        for col in ("auto_minutes", "manr_minutes", "manf_minutes",
                    "running_minutes", "motors_on_minutes",
                    "estop_minutes"):
            assert today[col] is not None
            assert today[col] >= 0
        assert today["num_stops"] >= 0
    finally:
        s.stop()


# ─── shift_summary ───────────────────────────────────────────────────────────

def test_shift_summary_returns_three_rows_with_labels(tmp_path: Path):
    from datetime import date
    s = _store(tmp_path, tick_period_s=0)
    s.start()
    try:
        _seed(s, [
            ("AUTO", "motoron", "running", 100),
            ("MANR", "motoroff", "stopped", 0),
        ])
        rows = s.shift_summary(date.today().isoformat())
        assert len(rows) == 3
        assert [r["shift"] for r in rows] == ["S1 06–14", "S2 14–22", "S3 22–06"]
        for r in rows:
            for col in ("auto_minutes", "manr_minutes", "manf_minutes",
                        "running_minutes", "motors_on_minutes",
                        "estop_minutes", "total_minutes"):
                assert r[col] is not None
                assert r[col] >= 0
            assert r["num_stops"] >= 0
            # start_ts < end_ts and both are in the right calendar day(s).
            assert r["start_ts"] < r["end_ts"]
    finally:
        s.stop()


def test_shift_summary_attributes_estop_minutes(tmp_path: Path):
    """Insert a span with `emergencystop` ctrl_state and confirm it lands
    in `estop_minutes` for whichever shift contains the timestamp."""
    from datetime import datetime
    s = _store(tmp_path, tick_period_s=0)
    s.start()
    try:
        # Direct INSERTs with explicit ts so the spans clearly fall inside
        # one shift and are wide enough to register at minute precision.
        today = datetime.now().strftime("%Y-%m-%d")
        with sqlite3.connect(s.cfg.db_path) as c:
            c.executemany(
                """INSERT INTO robot_status_log
                   (ts, opmode, ctrl_state, exec_state, speed_ratio,
                    is_ready, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                [
                    (f"{today} 07:00:00", "AUTO", "emergencystop",
                     "stopped", 0, 0, "change"),
                    (f"{today} 07:30:00", "AUTO", "motoron",
                     "running", 100, 1, "change"),
                ],
            )
            c.commit()
        rows = s.shift_summary(today)
        s1 = next(r for r in rows if r["shift"].startswith("S1"))
        # ~30 minutes of estop in S1.
        assert s1["estop_minutes"] >= 25
        # S2 should see no estop time.
        s2 = next(r for r in rows if r["shift"].startswith("S2"))
        assert s2["estop_minutes"] == 0
    finally:
        s.stop()


def test_shift_summary_rejects_bad_date(tmp_path: Path):
    s = _store(tmp_path, tick_period_s=0)
    s.start()
    try:
        with pytest.raises(ValueError):
            s.shift_summary("not-a-date")
    finally:
        s.stop()


def test_shift_summary_s3_crosses_midnight(tmp_path: Path):
    """S3 starts at 22:00 of date_iso and ends at 06:00 of date_iso+1."""
    from datetime import date, timedelta
    s = _store(tmp_path, tick_period_s=0)
    s.start()
    try:
        d = date(2025, 1, 1).isoformat()
        next_d = (date(2025, 1, 1) + timedelta(days=1)).isoformat()
        rows = s.shift_summary(d)
        s3 = next(r for r in rows if r["shift"].startswith("S3"))
        assert s3["start_ts"].startswith(d)
        assert s3["end_ts"].startswith(next_d)
        assert s3["start_ts"].endswith("22:00:00")
        assert s3["end_ts"].endswith("06:00:00")
    finally:
        s.stop()
