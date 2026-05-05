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


def test_time_in_state_returns_safety_categories(tmp_path: Path):
    """time_in_state buckets spans into estop / bypass / enabled
    (mutually exclusive, severity-ranked) rather than by opmode."""
    s = _store(tmp_path, tick_period_s=0)
    s.start()
    try:
        with sqlite3.connect(s.cfg.db_path) as c:
            c.executemany(
                """INSERT INTO robot_status_log
                   (ts, opmode, ctrl_state, exec_state, speed_ratio,
                    is_ready, bypass, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    # 30 min enabled, then 30 min bypass, then 30 min estop.
                    ("2025-01-01 06:00:00", "AUTO", "motoron",
                     "running", 100, 1, 0, "change"),
                    ("2025-01-01 06:30:00", "AUTO", "motoron",
                     "running", 100, 1, 1, "change"),
                    ("2025-01-01 07:00:00", "AUTO", "emergencystop",
                     "stopped", 0, 0, 1, "change"),
                    ("2025-01-01 07:30:00", "AUTO", "motoron",
                     "running", 100, 1, 0, "change"),
                ],
            )
            c.commit()

        rows = s.time_in_state("2025-01-01 06:00:00")
        states = {r["state"]: r["seconds"] for r in rows}
        # All three categories present, all non-negative.
        assert set(states.keys()).issuperset({"estop", "bypass", "enabled"})
        for v in states.values():
            assert v >= 0
        # Returned in severity order: estop, bypass, enabled.
        assert [r["state"] for r in rows[:3]] == ["estop", "bypass", "enabled"]
    finally:
        s.stop()


def test_time_in_state_estop_wins_over_bypass(tmp_path: Path):
    """A span that is both estop and bypass must be attributed to estop —
    estop dominates because it's a stricter safety condition."""
    s = _store(tmp_path, tick_period_s=0)
    s.start()
    try:
        with sqlite3.connect(s.cfg.db_path) as c:
            c.executemany(
                """INSERT INTO robot_status_log
                   (ts, opmode, ctrl_state, exec_state, speed_ratio,
                    is_ready, bypass, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    ("2025-01-01 06:00:00", "AUTO", "emergencystop",
                     "stopped", 0, 0, 1, "change"),
                    ("2025-01-01 06:30:00", "AUTO", "motoron",
                     "running", 100, 1, 0, "change"),
                ],
            )
            c.commit()

        rows = s.time_in_state("2025-01-01 06:00:00")
        seen = {r["state"]: r["seconds"] for r in rows}
        assert seen.get("estop", 0) >= 25 * 60      # ~30 min estop
        assert seen.get("bypass", 0) == 0
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
                    "estop_minutes", "bypass_minutes"):
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
            for col in ("ready_minutes", "enabled_minutes",
                        "bypass_minutes", "estop_minutes", "total_minutes"):
                assert r[col] is not None
                assert r[col] >= 0
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
                    is_ready, bypass, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    (f"{today} 07:00:00", "AUTO", "emergencystop",
                     "stopped", 0, 0, 0, "change"),
                    (f"{today} 07:30:00", "AUTO", "motoron",
                     "running", 100, 1, 0, "change"),
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


def test_shift_summary_attributes_bypass_minutes(tmp_path: Path):
    """A span with bypass=1 inside a shift window must be attributed to
    `bypass_minutes`; while bypass=0 spans land in `enabled_minutes`."""
    from datetime import datetime
    s = _store(tmp_path, tick_period_s=0)
    s.start()
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        with sqlite3.connect(s.cfg.db_path) as c:
            c.executemany(
                """INSERT INTO robot_status_log
                   (ts, opmode, ctrl_state, exec_state, speed_ratio,
                    is_ready, bypass, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    (f"{today} 07:00:00", "AUTO", "motoron",
                     "running", 100, 1, 1, "change"),
                    (f"{today} 07:30:00", "AUTO", "motoron",
                     "running", 100, 1, 0, "change"),
                ],
            )
            c.commit()
        rows = s.shift_summary(today)
        s1 = next(r for r in rows if r["shift"].startswith("S1"))
        # Roughly 30 min of bypass; the rest of S1 is enabled.
        assert s1["bypass_minutes"] >= 25
        assert s1["enabled_minutes"] > s1["bypass_minutes"]
    finally:
        s.stop()


def test_bypass_column_persisted_via_bus(tmp_path: Path, bus):
    """Publishing plc_signal_changed for the configured alias should
    flip the cached bypass flag and the next robot_status_changed row
    must carry bypass=1."""
    from events import PlcSignalChanged, RobotStatusChanged, signals
    from robot_status import RobotStatus

    s = _store(tmp_path, bus=bus, tick_period_s=0,
               bypass_alias="robot.bypass")
    s.start()
    try:
        bus.publish(
            signals.plc_signal_changed,
            PlcSignalChanged(alias="robot.bypass", value=True, ts=0.0),
        )
        # Wait for the bypass-row to drain (the handler enqueues one).
        _drain(s, 1)

        bus.publish(
            signals.robot_status_changed,
            RobotStatusChanged(status=RobotStatus(opmode="AUTO")),
        )
        _drain(s, 2)

        rows = s.recent(limit=5)
        assert len(rows) >= 2
        # The freshest row is from the robot_status_changed publish above.
        latest = rows[0]
        assert latest["bypass"] == 1
    finally:
        s.stop()


def test_bypass_alias_unset_skips_subscription(tmp_path: Path, bus):
    """When `bypass_alias` is None, plc_signal_changed publishes for any
    alias must be ignored — the bypass column stays 0."""
    from events import PlcSignalChanged, signals
    from robot_status import RobotStatus

    s = _store(tmp_path, bus=bus, tick_period_s=0, bypass_alias=None)
    s.start()
    try:
        bus.publish(
            signals.plc_signal_changed,
            PlcSignalChanged(alias="robot.bypass", value=True, ts=0.0),
        )
        time.sleep(0.2)
        # No bypass-driven row should land.
        assert s.recent() == []
        # And a normal status change still records bypass=0.
        from events import RobotStatusChanged
        bus.publish(
            signals.robot_status_changed,
            RobotStatusChanged(status=RobotStatus(opmode="AUTO")),
        )
        _drain(s, 1)
        assert s.recent()[0]["bypass"] == 0
    finally:
        s.stop()


def test_bypass_via_rws_var_changed(tmp_path: Path, bus):
    """A robot_var_changed publish for the configured RWS alias flips
    bypass on subsequent rows — same shape as the PLC source."""
    from events import RobotStatusChanged, RobotVarChanged, signals
    from robot_status import RobotStatus

    s = _store(tmp_path, bus=bus, tick_period_s=0,
               bypass_rws_alias="robot.bypass_rws")
    s.start()
    try:
        bus.publish(
            signals.robot_var_changed,
            RobotVarChanged(alias="robot.bypass_rws", value=True, prev=False),
        )
        _drain(s, 1)

        bus.publish(
            signals.robot_status_changed,
            RobotStatusChanged(status=RobotStatus(opmode="AUTO")),
        )
        _drain(s, 2)
        assert s.recent()[0]["bypass"] == 1
    finally:
        s.stop()


def test_bypass_or_fusion_either_source_high(tmp_path: Path, bus):
    """When both PLC and RWS aliases are configured, the effective bypass
    is the OR of the two — dropping one source doesn't clear the column
    if the other is still high."""
    from events import (PlcSignalChanged, RobotStatusChanged,
                        RobotVarChanged, signals)
    from robot_status import RobotStatus

    s = _store(tmp_path, bus=bus, tick_period_s=0,
               bypass_alias="robot.bypass",
               bypass_rws_alias="robot.bypass_rws")
    s.start()
    try:
        # PLC says bypassed.
        bus.publish(
            signals.plc_signal_changed,
            PlcSignalChanged(alias="robot.bypass", value=True, ts=0.0),
        )
        # RWS says bypassed too.
        bus.publish(
            signals.robot_var_changed,
            RobotVarChanged(alias="robot.bypass_rws", value=True, prev=False),
        )
        # Now PLC drops — RWS still high → effective bypass must remain 1.
        bus.publish(
            signals.plc_signal_changed,
            PlcSignalChanged(alias="robot.bypass", value=False, ts=0.0),
        )
        _drain(s, 3)

        bus.publish(
            signals.robot_status_changed,
            RobotStatusChanged(status=RobotStatus(opmode="AUTO")),
        )
        _drain(s, 4)
        assert s.recent()[0]["bypass"] == 1

        # Now RWS drops too → effective bypass = 0 on the next row.
        bus.publish(
            signals.robot_var_changed,
            RobotVarChanged(alias="robot.bypass_rws", value=False, prev=True),
        )
        bus.publish(
            signals.robot_status_changed,
            RobotStatusChanged(status=RobotStatus(opmode="AUTO")),
        )
        _drain(s, 6)
        assert s.recent()[0]["bypass"] == 0
    finally:
        s.stop()


def test_bypass_rws_string_value_is_coerced(tmp_path: Path, bus):
    """RAPID values often come back as strings ('TRUE'/'FALSE'). The
    handler must coerce them like RobotVariablesMonitor does."""
    from events import RobotStatusChanged, RobotVarChanged, signals
    from robot_status import RobotStatus

    s = _store(tmp_path, bus=bus, tick_period_s=0,
               bypass_rws_alias="rapid.bypass")
    s.start()
    try:
        bus.publish(
            signals.robot_var_changed,
            RobotVarChanged(alias="rapid.bypass", value="TRUE", prev="FALSE"),
        )
        bus.publish(
            signals.robot_status_changed,
            RobotStatusChanged(status=RobotStatus(opmode="AUTO")),
        )
        _drain(s, 2)
        assert s.recent()[0]["bypass"] == 1

        bus.publish(
            signals.robot_var_changed,
            RobotVarChanged(alias="rapid.bypass", value="false", prev="TRUE"),
        )
        bus.publish(
            signals.robot_status_changed,
            RobotStatusChanged(status=RobotStatus(opmode="AUTO")),
        )
        _drain(s, 4)
        assert s.recent()[0]["bypass"] == 0
    finally:
        s.stop()


def test_alter_table_migration_adds_bypass_column(tmp_path: Path):
    """A pre-bypass DB file (no `bypass` column) must gain the column
    on next start() without losing rows."""
    db_path = tmp_path / "rsl.db"
    with sqlite3.connect(db_path) as c:
        c.executescript("""
            CREATE TABLE robot_status_log (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ts           TEXT    NOT NULL DEFAULT (datetime('now')),
                opmode       TEXT    NOT NULL,
                ctrl_state   TEXT    NOT NULL,
                exec_state   TEXT    NOT NULL,
                speed_ratio  INTEGER NOT NULL,
                is_ready     INTEGER NOT NULL,
                source       TEXT    NOT NULL
            );
        """)
        c.execute(
            "INSERT INTO robot_status_log "
            "(opmode, ctrl_state, exec_state, speed_ratio, is_ready, source) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("AUTO", "motoron", "running", 100, 1, "change"),
        )
        c.commit()

    s = _store(tmp_path, tick_period_s=0)
    s.start()
    try:
        # The legacy row is preserved with bypass defaulted to 0.
        rows = s.recent()
        assert len(rows) == 1
        assert rows[0]["bypass"] == 0
        assert rows[0]["opmode"] == "AUTO"
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
