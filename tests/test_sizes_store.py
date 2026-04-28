"""Tests for SizesStore — schema (2 tables), CRUD, validation."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from sizes_store import Size, SizesConfig, SizesStore, TABLES


def _store(tmp_path: Path) -> SizesStore:
    return SizesStore.from_config(SizesConfig(db_path=str(tmp_path / "sizes.db")))


def _size(name: str = "A4", **overrides) -> Size:
    base = dict(name=name, width_mm=210, length_mm=297,
                width_in=8, length_in=12)
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
        sid = s.add(table, _size(name="Foo", width_mm=300, length_mm=400,
                                  width_in=12, length_in=16))
        got = s.get(table, sid)
        assert got is not None
        assert got.id == sid
        assert got.name == "Foo"
        assert got.width_mm == 300
        assert got.length_mm == 400
        assert got.width_in == 12
        assert got.length_in == 16
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
                                   width_mm=1, length_mm=2,
                                   width_in=1, length_in=2))
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
            s.update("cardboard", Size(id=999, name="x", width_mm=1, length_mm=1,
                                        width_in=1, length_in=1))
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
