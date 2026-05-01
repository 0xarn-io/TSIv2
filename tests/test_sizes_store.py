"""Tests for SizesStore — single `sizes` table with station3 boolean."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from sizes_store import Size, SizesChange, SizesConfig, SizesStore, TABLE


def _store(tmp_path: Path) -> SizesStore:
    return SizesStore.from_config(SizesConfig(db_path=str(tmp_path / "sizes.db")))


def _size(name: str = "A4", **overrides) -> Size:
    base = dict(name=name, width_mm=210, length_mm=297)
    base.update(overrides)
    return Size(**base)


def test_start_creates_table(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        with sqlite3.connect(s.cfg.db_path) as c:
            tables = {r[0] for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
        assert TABLE in tables
    finally:
        s.stop()


def test_add_and_get_round_trip(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        sid = s.add(_size(name="Foo", width_mm=300, length_mm=400, station3=True))
        got = s.get(sid)
        assert got is not None
        assert got.id == sid
        assert got.name == "Foo"
        assert got.width_mm == 300
        assert got.length_mm == 400
        assert got.station3 is True
    finally:
        s.stop()


def test_list_orders_by_id(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        ids = [s.add(_size(name=n)) for n in ("a", "b", "c")]
        rows = s.list()
        assert [r.id for r in rows] == ids
        assert [r.name for r in rows] == ["a", "b", "c"]
    finally:
        s.stop()


def test_get_missing_returns_none(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        assert s.get(999) is None
    finally:
        s.stop()


def test_update_modifies_row_and_bumps_updated_at(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        sid = s.add(_size(name="orig"))
        with sqlite3.connect(s.cfg.db_path) as c:
            assert c.execute(
                f"SELECT updated_at FROM {TABLE} WHERE id = ?", (sid,)
            ).fetchone()[0] is None

        s.update(Size(id=sid, name="changed", width_mm=1, length_mm=2))
        got = s.get(sid)
        assert got.name == "changed"
        assert got.width_mm == 1

        with sqlite3.connect(s.cfg.db_path) as c:
            assert c.execute(
                f"SELECT updated_at FROM {TABLE} WHERE id = ?", (sid,)
            ).fetchone()[0] is not None
    finally:
        s.stop()


def test_update_without_id_raises(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        with pytest.raises(ValueError, match="requires Size.id"):
            s.update(_size())
    finally:
        s.stop()


def test_update_missing_id_raises(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        with pytest.raises(KeyError):
            s.update(Size(id=999, name="x", width_mm=1, length_mm=1))
    finally:
        s.stop()


def test_delete_removes_row(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        sid = s.add(_size(name="Tmp"))
        assert s.get(sid) is not None
        s.delete(sid)
        assert s.get(sid) is None
    finally:
        s.stop()


def test_use_before_start_raises(tmp_path: Path):
    s = _store(tmp_path)
    with pytest.raises(RuntimeError, match="not started"):
        s.list()
    with pytest.raises(RuntimeError, match="not started"):
        s.get(1)
    with pytest.raises(RuntimeError, match="not started"):
        s.add(_size())


def test_stop_is_idempotent(tmp_path: Path):
    s = _store(tmp_path); s.start()
    s.stop(); s.stop()                              # must not raise


def test_db_dir_auto_created(tmp_path: Path):
    nested = tmp_path / "deep" / "nested" / "sizes.db"
    s = SizesStore.from_config(SizesConfig(db_path=str(nested)))
    s.start()
    try:
        assert nested.is_file()
    finally:
        s.stop()


def test_migrate_folds_legacy_tables(tmp_path: Path):
    """Legacy DBs with separate `cardboard` + `others` tables must fold into
    `sizes`, with station3 set from the source table (cardboard=0, others=1).
    """
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as legacy:
        legacy.executescript("""
            CREATE TABLE cardboard (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                name      TEXT NOT NULL,
                width_mm  INTEGER NOT NULL,
                length_mm INTEGER NOT NULL,
                slot      INTEGER
            );
            CREATE TABLE others (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                name      TEXT NOT NULL,
                width_mm  INTEGER NOT NULL,
                length_mm INTEGER NOT NULL,
                slot      INTEGER
            );
        """)
        legacy.execute(
            "INSERT INTO cardboard (name, width_mm, length_mm) "
            "VALUES ('paper', 100, 200)"
        )
        legacy.execute(
            "INSERT INTO others (name, width_mm, length_mm) "
            "VALUES ('wood', 1000, 1000)"
        )
        legacy.commit()

    s = SizesStore.from_config(SizesConfig(db_path=str(db_path)))
    s.start()
    try:
        rows = {r.name: r for r in s.list()}
        assert "paper" in rows and rows["paper"].station3 is False
        assert "wood"  in rows and rows["wood"].station3  is True
        # Legacy tables are gone.
        with sqlite3.connect(db_path) as c:
            tables = {r[0] for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
            assert "cardboard" not in tables
            assert "others"    not in tables
    finally:
        s.stop()


def test_migrate_is_idempotent_on_fresh_dbs(tmp_path: Path):
    """Running on a brand-new DB (no legacy tables) must be a no-op."""
    s = _store(tmp_path); s.start(); s.stop()
    s2 = _store(tmp_path); s2.start()                # second start, same file
    try:
        s2.add(_size())                              # writes still work
    finally:
        s2.stop()


# ---- slot helpers -----------------------------------------------------------

def test_slot_unique(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.add(_size(name="A", slot=3))
        with pytest.raises(ValueError, match="already used"):
            s.add(_size(name="B", slot=3))
    finally:
        s.stop()


def test_slot_out_of_range_rejected(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        with pytest.raises(ValueError, match="out of range"):
            s.add(_size(slot=99))
    finally:
        s.stop()


def test_get_slot_finds_owner(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.add(_size(name="A", slot=2))
        s.add(_size(name="B", slot=4, station3=True))
        out = s.get_slot(2)
        assert out is not None and out.name == "A" and out.station3 is False
        out = s.get_slot(4)
        assert out is not None and out.name == "B" and out.station3 is True
        assert s.get_slot(0) is None
    finally:
        s.stop()


def test_upsert_slot_creates_row(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        sid = s.upsert_slot(0, "Box", 100, 200, station3=False)
        got = s.get(sid)
        assert got is not None
        assert got.station3 is False
        assert got.slot == 0
    finally:
        s.stop()


def test_upsert_slot_flips_station3(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.upsert_slot(0, "Box", 100, 200, station3=False)
        # Same slot, but now station3=True — row updates in place.
        s.upsert_slot(0, "Box", 100, 200, station3=True)
        got = s.get_slot(0)
        assert got is not None and got.station3 is True
    finally:
        s.stop()


def test_upsert_slot_idempotent_on_unchanged_data(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        events: list = []
        s.on_change(events.append)
        sid1 = s.upsert_slot(3, "X", 100, 200, station3=False)
        events.clear()
        sid2 = s.upsert_slot(3, "X", 100, 200, station3=False)
        assert sid1 == sid2
        assert events == []                 # no-op shouldn't fire on_change
    finally:
        s.stop()


def test_clear_slot_returns_true_when_deleted(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.add(_size(slot=7))
        assert s.clear_slot(7) is True
        assert s.clear_slot(7) is False     # already gone
    finally:
        s.stop()


# ---- on_change subscription -------------------------------------------------

def test_on_change_fires_on_add_update_delete(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        events: list[SizesChange] = []
        s.on_change(events.append)

        sid = s.add(_size(name="A"))
        assert events[-1].op == "add"

        s.update(Size(id=sid, name="A2", width_mm=1, length_mm=2))
        assert events[-1].op == "update"

        s.delete(sid)
        assert events[-1].op == "delete"
        assert events[-1].size is None
    finally:
        s.stop()


def test_silent_block_suppresses_on_change(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        events: list = []
        s.on_change(events.append)
        with s.silent():
            s.add(_size())
        assert events == []
        s.add(_size(name="loud"))   # post-silent
        assert len(events) == 1
    finally:
        s.stop()
