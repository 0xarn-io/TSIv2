"""Tests for SizesStore — schema (2 tables), CRUD, validation."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from sizes_store import Size, SizesConfig, SizesStore, TABLES


def _store(tmp_path: Path) -> SizesStore:
    return SizesStore.from_config(SizesConfig(db_path=str(tmp_path / "sizes.db")))


def _size(name: str = "A4", **overrides) -> Size:
    base = dict(name=name, width_mm=210, length_mm=297)
    base.update(overrides)
    return Size(**base)


def test_start_creates_both_tables(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        with sqlite3.connect(s.cfg.db_path) as c:
            tables = {r[0] for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
        for t in TABLES:
            assert t in tables
    finally:
        s.stop()


@pytest.mark.parametrize("table", TABLES)
def test_add_and_get_round_trip(tmp_path: Path, table: str):
    s = _store(tmp_path); s.start()
    try:
        sid = s.add(table, _size(name="Foo", width_mm=300, length_mm=400))
        got = s.get(table, sid)
        assert got is not None
        assert got.id == sid
        assert got.name == "Foo"
        assert got.width_mm == 300
        assert got.length_mm == 400
    finally:
        s.stop()


def test_tables_are_independent(tmp_path: Path):
    """A row added to one table must not appear in the other."""
    s = _store(tmp_path); s.start()
    try:
        cid = s.add("cardboard", _size(name="Box-A"))
        oid = s.add("others",    _size(name="Wood-A"))

        assert s.get("cardboard", cid).name == "Box-A"
        assert s.get("others",    oid).name == "Wood-A"
        assert s.get("cardboard", oid) is None or s.get("cardboard", oid).name != "Wood-A"
        assert [r.name for r in s.list("cardboard")] == ["Box-A"]
        assert [r.name for r in s.list("others")]    == ["Wood-A"]
    finally:
        s.stop()


def test_list_orders_by_id(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        ids = [s.add("cardboard", _size(name=n)) for n in ("a", "b", "c")]
        rows = s.list("cardboard")
        assert [r.id for r in rows] == ids
        assert [r.name for r in rows] == ["a", "b", "c"]
    finally:
        s.stop()


def test_get_missing_returns_none(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        assert s.get("cardboard", 999) is None
    finally:
        s.stop()


def test_update_modifies_row_and_bumps_updated_at(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        sid = s.add("cardboard", _size(name="orig"))
        with sqlite3.connect(s.cfg.db_path) as c:
            assert c.execute(
                "SELECT updated_at FROM cardboard WHERE id = ?", (sid,)
            ).fetchone()[0] is None

        s.update("cardboard", Size(id=sid, name="changed",
                                   width_mm=1, length_mm=2))
        got = s.get("cardboard", sid)
        assert got.name == "changed"
        assert got.width_mm == 1

        with sqlite3.connect(s.cfg.db_path) as c:
            assert c.execute(
                "SELECT updated_at FROM cardboard WHERE id = ?", (sid,)
            ).fetchone()[0] is not None
    finally:
        s.stop()


def test_update_without_id_raises(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        with pytest.raises(ValueError, match="requires Size.id"):
            s.update("cardboard", _size())
    finally:
        s.stop()


def test_update_missing_id_raises(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        with pytest.raises(KeyError):
            s.update("cardboard", Size(id=999, name="x", width_mm=1, length_mm=1))
    finally:
        s.stop()


def test_delete_removes_row(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        sid = s.add("others", _size(name="Tmp"))
        assert s.get("others", sid) is not None
        s.delete("others", sid)
        assert s.get("others", sid) is None
    finally:
        s.stop()


def test_unknown_table_rejected(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        with pytest.raises(ValueError, match="unknown table"):
            s.list("does_not_exist")
        with pytest.raises(ValueError, match="unknown table"):
            s.add("does_not_exist", _size())
    finally:
        s.stop()


def test_use_before_start_raises(tmp_path: Path):
    s = _store(tmp_path)
    with pytest.raises(RuntimeError, match="not started"):
        s.list("cardboard")
    with pytest.raises(RuntimeError, match="not started"):
        s.get("cardboard", 1)
    with pytest.raises(RuntimeError, match="not started"):
        s.add("cardboard", _size())


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


def test_migrate_drops_legacy_inch_columns(tmp_path: Path):
    """Legacy DBs with width_in/length_in NOT NULL must accept new inserts."""
    db_path = tmp_path / "legacy.db"
    # Build a DB matching the OLD shape (still has width_in/length_in NOT NULL).
    with sqlite3.connect(db_path) as legacy:
        legacy.executescript("""
            CREATE TABLE cardboard (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                name      TEXT NOT NULL,
                width_mm  INTEGER NOT NULL,
                length_mm INTEGER NOT NULL,
                width_in  INTEGER NOT NULL,
                length_in INTEGER NOT NULL
            );
            CREATE TABLE others (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                name      TEXT NOT NULL,
                width_mm  INTEGER NOT NULL,
                length_mm INTEGER NOT NULL,
                width_in  INTEGER NOT NULL,
                length_in INTEGER NOT NULL
            );
        """)
        legacy.execute(
            "INSERT INTO cardboard (name, width_mm, length_mm, width_in, length_in) "
            "VALUES ('legacy', 100, 200, 4, 8)"
        )
        legacy.commit()

    s = SizesStore.from_config(SizesConfig(db_path=str(db_path)))
    s.start()
    try:
        # Legacy row still readable.
        rows = s.list("cardboard")
        assert any(r.name == "legacy" for r in rows)
        # New inserts (without width_in/length_in) must succeed now.
        sid = s.add("cardboard", _size(name="post-migration"))
        assert s.get("cardboard", sid).name == "post-migration"
        # Legacy columns are gone from both tables.
        with sqlite3.connect(db_path) as c:
            for table in TABLES:
                cols = {r[1] for r in c.execute(f"PRAGMA table_info({table})")}
                assert "width_in" not in cols
                assert "length_in" not in cols
    finally:
        s.stop()


def test_migrate_is_idempotent_on_fresh_dbs(tmp_path: Path):
    """Running on a brand-new DB (no legacy columns) must be a no-op."""
    s = _store(tmp_path); s.start(); s.stop()
    s2 = _store(tmp_path); s2.start()                # second start, same file
    try:
        s2.add("cardboard", _size())                 # writes still work
    finally:
        s2.stop()


# ---- slot helpers -----------------------------------------------------------

from sizes_store import SizesChange


def test_slot_unique_within_table(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.add("cardboard", _size(name="A", slot=3))
        with pytest.raises(ValueError, match="already used"):
            s.add("cardboard", _size(name="B", slot=3))
    finally:
        s.stop()


def test_slot_unique_across_tables(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.add("cardboard", _size(name="A", slot=5))
        with pytest.raises(ValueError, match="already used"):
            s.add("others", _size(name="B", slot=5))
    finally:
        s.stop()


def test_slot_out_of_range_rejected(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        with pytest.raises(ValueError, match="out of range"):
            s.add("cardboard", _size(slot=99))
    finally:
        s.stop()


def test_get_slot_finds_owner(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.add("cardboard", _size(name="A", slot=2))
        s.add("others",    _size(name="B", slot=4))
        out = s.get_slot(2)
        assert out is not None and out[0] == "cardboard" and out[1].name == "A"
        out = s.get_slot(4)
        assert out is not None and out[0] == "others" and out[1].name == "B"
        assert s.get_slot(0) is None
    finally:
        s.stop()


def test_upsert_slot_creates_in_correct_table(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        # wood=False → cardboard
        t1, _ = s.upsert_slot(0, "Box", 100, 200, wood=False)
        assert t1 == "cardboard"
        # wood=True → others
        t2, _ = s.upsert_slot(1, "Wood", 1000, 1000, wood=True)
        assert t2 == "others"
    finally:
        s.stop()


def test_upsert_slot_moves_across_tables_on_wood_flip(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.upsert_slot(0, "Box", 100, 200, wood=False)
        # Same slot, but now wood=True — row must move tables.
        s.upsert_slot(0, "Box", 100, 200, wood=True)
        assert s.get_slot(0)[0] == "others"
        # Old table is empty.
        assert all(r.slot != 0 for r in s.list("cardboard"))
    finally:
        s.stop()


def test_upsert_slot_idempotent_on_unchanged_data(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        events: list = []
        s.on_change(events.append)
        t1, sid1 = s.upsert_slot(3, "X", 100, 200, wood=False)
        events.clear()
        t2, sid2 = s.upsert_slot(3, "X", 100, 200, wood=False)
        assert (t1, sid1) == (t2, sid2)
        assert events == []                 # no-op shouldn't fire on_change
    finally:
        s.stop()


def test_clear_slot_returns_true_when_deleted(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.add("cardboard", _size(slot=7))
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

        sid = s.add("cardboard", _size(name="A"))
        assert events[-1].op == "add"

        s.update("cardboard", Size(id=sid, name="A2", width_mm=1, length_mm=2))
        assert events[-1].op == "update"

        s.delete("cardboard", sid)
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
            s.add("cardboard", _size())
        assert events == []
        s.add("cardboard", _size(name="loud"))   # post-silent
        assert len(events) == 1
    finally:
        s.stop()
