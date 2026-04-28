"""Tests for ErrorsStore — schema, writer thread, log(), retention, robustness."""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from errors_store import ErrorsConfig, ErrorsStore


def _drain(store: ErrorsStore, expected: int, timeout: float = 2.0) -> None:
    """Wait until at least `expected` rows have been written."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if len(store.recent(limit=expected + 5)) >= expected:
            return
        time.sleep(0.01)
    raise AssertionError(f"writer thread didn't drain {expected} rows in {timeout}s")


def _store(tmp_path: Path, **overrides) -> ErrorsStore:
    cfg = ErrorsConfig(db_path=str(tmp_path / "errors.db"), **overrides)
    return ErrorsStore.from_config(cfg)


def test_start_creates_schema(tmp_path: Path):
    store = _store(tmp_path)
    store.start()
    try:
        with sqlite3.connect(store.cfg.db_path) as c:
            tables = {r[0] for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
        assert "error_log" in tables
    finally:
        store.stop()


def test_log_round_trip_all_severities(tmp_path: Path):
    store = _store(tmp_path)
    store.start()
    try:
        for sev in ("info", "warning", "error", "critical"):
            store.log("python", code=1, title=f"t-{sev}", severity=sev)
        _drain(store, 4)

        rows = store.recent(limit=10)
        assert len(rows) == 4
        assert {r["severity"] for r in rows} == {"info", "warning", "error", "critical"}
        assert all(r["device"] == "python" for r in rows)
    finally:
        store.stop()


def test_log_captures_full_payload(tmp_path: Path):
    store = _store(tmp_path)
    store.start()
    try:
        store.log(
            "plc", code=42, title="oops",
            subsystem="recipe",
            description="something broke",
            severity="warning",
            message="ADS error 1234",
            context={"alias": "recipe.code", "value": 7},
            recipe_code=7,
        )
        _drain(store, 1)

        row = store.recent()[0]
        assert row["device"]      == "plc"
        assert row["subsystem"]   == "recipe"
        assert row["code"]        == 42
        assert row["title"]       == "oops"
        assert row["description"] == "something broke"
        assert row["severity"]    == "warning"
        assert row["message"]     == "ADS error 1234"
        assert row["recipe_code"] == 7
        assert "recipe.code" in row["raw_context_json"]
        assert row["ts"]          # populated by SQLite default
    finally:
        store.stop()


def test_invalid_severity_coerced_to_error(tmp_path: Path, caplog):
    store = _store(tmp_path)
    store.start()
    try:
        import logging
        with caplog.at_level(logging.WARNING, logger="errors_store"):
            store.log("python", code=1, title="t", severity="catastrophic")
        _drain(store, 1)
        assert store.recent()[0]["severity"] == "error"
        assert any("invalid severity" in r.message for r in caplog.records)
    finally:
        store.stop()


def test_recent_filters(tmp_path: Path):
    store = _store(tmp_path)
    store.start()
    try:
        store.log("plc",    code=1, title="a", severity="warning")
        store.log("python", code=2, title="b", severity="error")
        store.log("plc",    code=3, title="c", severity="error")
        _drain(store, 3)

        plc_rows = store.recent(device="plc")
        assert {r["code"] for r in plc_rows} == {1, 3}

        warn_rows = store.recent(severity="warning")
        assert len(warn_rows) == 1 and warn_rows[0]["code"] == 1

        both = store.recent(device="plc", severity="error")
        assert len(both) == 1 and both[0]["code"] == 3
    finally:
        store.stop()


def test_writer_drains_on_stop(tmp_path: Path):
    store = _store(tmp_path)
    store.start()
    for i in range(20):
        store.log("python", code=i, title=f"t{i}")
    store.stop()
    # After stop, the writer thread should have flushed every queued row.
    with sqlite3.connect(store.cfg.db_path) as c:
        n = c.execute("SELECT COUNT(*) FROM error_log").fetchone()[0]
    assert n == 20


def test_log_never_raises_when_not_started(tmp_path: Path):
    """Calling .log() before .start() must not raise (it just gets queued)."""
    store = _store(tmp_path)
    # Don't start — queue exists, but no writer.
    store.log("python", code=1, title="t")          # must not raise
    # Cleanup: drain the sentinel slot by starting then stopping.
    store.start()
    store.stop()


def test_retention_prunes_on_start(tmp_path: Path):
    """keep_days > 0 deletes rows older than the threshold at start()."""
    db_path = tmp_path / "errors.db"
    # Seed an old row directly.
    with sqlite3.connect(db_path) as c:
        c.executescript("""
            CREATE TABLE error_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL DEFAULT (datetime('now')),
                device TEXT NOT NULL, subsystem TEXT,
                code INTEGER NOT NULL, title TEXT NOT NULL,
                description TEXT,
                severity TEXT NOT NULL DEFAULT 'error'
                    CHECK (severity IN ('info','warning','error','critical')),
                message TEXT, raw_context_json TEXT, recipe_code INTEGER
            );
        """)
        c.execute(
            "INSERT INTO error_log (ts, device, code, title) "
            "VALUES (datetime('now', '-30 days'), 'python', 1, 'old')"
        )
        c.execute(
            "INSERT INTO error_log (device, code, title) "
            "VALUES ('python', 2, 'new')"
        )
        c.commit()

    store = ErrorsStore.from_config(ErrorsConfig(db_path=str(db_path), keep_days=7))
    store.start()
    try:
        rows = store.recent()
        titles = {r["title"] for r in rows}
        assert titles == {"new"}                    # 'old' was pruned
    finally:
        store.stop()


def test_recent_returns_empty_when_not_started(tmp_path: Path):
    store = _store(tmp_path)
    assert store.recent() == []


def test_recent_orders_newest_first(tmp_path: Path):
    store = _store(tmp_path)
    store.start()
    try:
        for i in range(5):
            store.log("python", code=i, title=f"t{i}")
        _drain(store, 5)

        rows = store.recent()
        codes = [r["code"] for r in rows]
        assert codes == sorted(codes, reverse=True)
    finally:
        store.stop()


def test_db_dir_auto_created(tmp_path: Path):
    """start() should create the DB's parent directory."""
    nested = tmp_path / "deep" / "nested" / "errors.db"
    store = ErrorsStore.from_config(ErrorsConfig(db_path=str(nested)))
    store.start()
    try:
        assert nested.parent.is_dir()
        assert nested.is_file()
    finally:
        store.stop()
