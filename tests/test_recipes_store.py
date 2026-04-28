"""Tests for RecipesStore — schema, CRUD, upsert, soft-delete."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from recipes_store import Recipe, RecipesConfig, RecipesStore


def _store(tmp_path: Path) -> RecipesStore:
    return RecipesStore.from_config(
        RecipesConfig(db_path=str(tmp_path / "recipes.db"))
    )


def _box(code: int = 1, **overrides) -> Recipe:
    base = dict(code=code)
    base.update(overrides)
    return Recipe(**base)


def test_start_creates_schema(tmp_path: Path):
    s = _store(tmp_path)
    s.start()
    try:
        with sqlite3.connect(s.cfg.db_path) as c:
            tables = {r[0] for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
        assert "recipe" in tables
    finally:
        s.stop()


def test_save_and_get_round_trip(tmp_path: Path):
    s = _store(tmp_path)
    s.start()
    try:
        r = _box(code=7,
                 x_topsheet_length=711, x_topsheet_width=400, x_units=2,
                 x1_pos=100, x2_pos=110, x3_pos=120, x_folding=True,
                 y_topsheet_length=1800, y_topsheet_width=900, y_units=3,
                 y1_pos=200, y2_pos=210, y3_pos=220, y_folding=False,
                 wood=True, wood_x_pos=50, wood_y_pos=60)
        s.save(r)
        got = s.get(7)
        assert got is not None
        assert got.code == 7
        assert got.x_topsheet_length == 711
        assert got.x_topsheet_width == 400
        assert got.x_units == 2
        assert got.x1_pos == 100
        assert got.x_folding is True
        assert got.y_topsheet_length == 1800
        assert got.y1_pos == 200
        assert got.y_folding is False
        assert got.wood is True
        assert got.wood_x_pos == 50
        assert got.wood_y_pos == 60
        assert got.active is True
    finally:
        s.stop()


def test_get_missing_returns_none(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        assert s.get(99) is None
    finally:
        s.stop()


def test_save_upserts_by_code(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.save(_box(code=3, x_topsheet_length=100, y_topsheet_length=200))
        s.save(_box(code=3, x_topsheet_length=999, y_topsheet_length=200))
        got = s.get(3)
        assert got.x_topsheet_length == 999
        # Only one row with code=3.
        with sqlite3.connect(s.cfg.db_path) as c:
            assert c.execute(
                "SELECT COUNT(*) FROM recipe WHERE code = 3"
            ).fetchone()[0] == 1
    finally:
        s.stop()


def test_list_active_only_default(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.save(_box(code=1)); s.save(_box(code=2)); s.save(_box(code=3))
        s.deactivate(2)

        active_codes  = [r.code for r in s.list()]
        all_codes     = [r.code for r in s.list(active_only=False)]
        assert active_codes == [1, 3]
        assert all_codes    == [1, 2, 3]
    finally:
        s.stop()


def test_deactivate_keeps_row(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.save(_box(code=5))
        s.deactivate(5)
        got = s.get(5)
        assert got is not None
        assert got.active is False
    finally:
        s.stop()


def test_delete_removes_row(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.save(_box(code=5))
        assert s.get(5) is not None
        s.delete(5)
        assert s.get(5) is None
    finally:
        s.stop()


def test_delete_missing_is_silent(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.delete(999)                                # must not raise
    finally:
        s.stop()


def test_rename_moves_row_preserves_created_at(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.save(_box(code=7, x_topsheet_length=100))
        with sqlite3.connect(s.cfg.db_path) as c:
            created = c.execute(
                "SELECT created_at FROM recipe WHERE code = 7"
            ).fetchone()[0]

        s.rename(7, 9)

        assert s.get(7) is None
        moved = s.get(9)
        assert moved is not None
        assert moved.x_topsheet_length == 100
        with sqlite3.connect(s.cfg.db_path) as c:
            assert c.execute(
                "SELECT created_at FROM recipe WHERE code = 9"
            ).fetchone()[0] == created
            assert c.execute(
                "SELECT updated_at FROM recipe WHERE code = 9"
            ).fetchone()[0] is not None
    finally:
        s.stop()


def test_rename_to_existing_code_raises(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.save(_box(code=1)); s.save(_box(code=2))
        with pytest.raises(ValueError, match="already exists"):
            s.rename(1, 2)
        # Originals are untouched.
        assert s.get(1) is not None
        assert s.get(2) is not None
    finally:
        s.stop()


def test_rename_missing_old_code_raises(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        with pytest.raises(KeyError, match="not found"):
            s.rename(99, 100)
    finally:
        s.stop()


def test_rename_same_code_is_noop(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.save(_box(code=3))
        s.rename(3, 3)                              # must not raise
        assert s.get(3) is not None
    finally:
        s.stop()


def test_save_bumps_updated_at(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.save(_box(code=1, x_topsheet_length=100))
        with sqlite3.connect(s.cfg.db_path) as c:
            first_updated = c.execute(
                "SELECT updated_at FROM recipe WHERE code = 1"
            ).fetchone()[0]
        # First insert leaves updated_at NULL; the upsert path sets it.
        assert first_updated is None
        s.save(_box(code=1, x_topsheet_length=200))
        with sqlite3.connect(s.cfg.db_path) as c:
            second_updated = c.execute(
                "SELECT updated_at FROM recipe WHERE code = 1"
            ).fetchone()[0]
        assert second_updated is not None
    finally:
        s.stop()


def test_default_field_values(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.save(_box(code=1))                        # only code supplied
        got = s.get(1)
        assert got.x_topsheet_length == 0
        assert got.y_topsheet_length == 0
        assert got.x_folding is False
        assert got.y_folding is False
        assert got.wood is False
        assert got.wood_x_pos == 0
        assert got.wood_y_pos == 0
    finally:
        s.stop()


def test_bools_persist_round_trip(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.save(_box(code=1, x_folding=True, y_folding=True, wood=True))
        got = s.get(1)
        assert got.x_folding is True
        assert got.y_folding is True
        assert got.wood is True
        s.save(_box(code=1, x_folding=False, y_folding=False, wood=False))
        got = s.get(1)
        assert got.x_folding is False
        assert got.y_folding is False
        assert got.wood is False
    finally:
        s.stop()


def test_use_before_start_raises(tmp_path: Path):
    s = _store(tmp_path)
    with pytest.raises(RuntimeError, match="not started"):
        s.get(1)
    with pytest.raises(RuntimeError, match="not started"):
        s.save(_box())
    with pytest.raises(RuntimeError, match="not started"):
        s.list()


def test_stop_is_idempotent(tmp_path: Path):
    s = _store(tmp_path); s.start()
    s.stop(); s.stop()       # must not raise


def test_db_dir_auto_created(tmp_path: Path):
    nested = tmp_path / "deep" / "nested" / "recipes.db"
    s = RecipesStore.from_config(RecipesConfig(db_path=str(nested)))
    s.start()
    try:
        assert nested.is_file()
    finally:
        s.stop()
