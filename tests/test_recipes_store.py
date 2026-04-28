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
    base = dict(code=code, name=f"Box-{code}",
                width_mm=711, height_mm=1800, depth_mm=1778)
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
        r = _box(code=7, name="Tall", width_mm=711, height_mm=1800,
                 x1_pos=100, y1_pos=200, rapid_program="MainProc",
                 description="seven")
        s.save(r)
        got = s.get(7)
        assert got is not None
        assert got.code == 7
        assert got.name == "Tall"
        assert got.width_mm == 711
        assert got.x1_pos == 100
        assert got.y1_pos == 200
        assert got.rapid_program == "MainProc"
        assert got.description == "seven"
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
        s.save(_box(code=3, name="A", width_mm=100, height_mm=200, depth_mm=300))
        s.save(_box(code=3, name="B", width_mm=999, height_mm=200, depth_mm=300))
        got = s.get(3)
        assert got.name == "B"
        assert got.width_mm == 999
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


def test_save_bumps_updated_at(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.save(_box(code=1, name="first"))
        with sqlite3.connect(s.cfg.db_path) as c:
            first_updated = c.execute(
                "SELECT updated_at FROM recipe WHERE code = 1"
            ).fetchone()[0]
        # First insert leaves updated_at NULL; the upsert path sets it.
        assert first_updated is None
        s.save(_box(code=1, name="second"))
        with sqlite3.connect(s.cfg.db_path) as c:
            second_updated = c.execute(
                "SELECT updated_at FROM recipe WHERE code = 1"
            ).fetchone()[0]
        assert second_updated is not None
    finally:
        s.stop()


def test_default_tolerances(tmp_path: Path):
    s = _store(tmp_path); s.start()
    try:
        s.save(_box(code=1))                        # no tolerances supplied
        got = s.get(1)
        assert got.width_tol == 5
        assert got.height_tol == 5
        assert got.depth_tol == 5
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
