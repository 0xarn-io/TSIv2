"""Tests for DBOrchestrator — registration, ordered start/stop, rollback."""
from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from pathlib import Path

import pytest

from db_orchestrator   import DBOrchestrator
from errors_store      import ErrorsConfig
from recipe_publisher  import RecipePublisherConfig
from recipes_store     import RecipesConfig
from unit_logger       import UnitLoggerConfig


def _cfg(tmp_path: Path, *,
         with_errors=True, with_recipes=True,
         with_recipe_pub=True, with_unit_log=True) -> SimpleNamespace:
    """Tiny stand-in for AppConfig — only carries the fields the orchestrator reads."""
    plc_ns = SimpleNamespace(
        recipe=(RecipePublisherConfig(
            code_alias="recipe.code", setpoints_alias="recipe.setpoints",
        ) if with_recipe_pub else None),
    )
    return SimpleNamespace(
        plc        = plc_ns,
        errors_log = (ErrorsConfig(db_path=str(tmp_path / "e.db"))
                      if with_errors else None),
        recipes    = (RecipesConfig(db_path=str(tmp_path / "r.db"))
                      if with_recipes else None),
        unit_log   = (UnitLoggerConfig(db_path=str(tmp_path / "u.db"))
                      if with_unit_log else None),
    )


def _deps():
    plc = MagicMock(); plc.read.return_value = 0
    plc.subscribe.return_value = ("n", "u")
    bridge = MagicMock()
    bridge.on_event = MagicMock(return_value=MagicMock())
    return plc, bridge


# ---- registration ------------------------------------------------------------

def test_full_config_registers_all_four(tmp_path: Path):
    plc, bridge = _deps()
    db = DBOrchestrator.from_config(_cfg(tmp_path), plc=plc, bridge=bridge)
    assert db.services() == [
        "ErrorsStore", "RecipesStore", "RecipePublisher", "UnitLogger",
    ]


def test_missing_config_sections_skipped(tmp_path: Path):
    plc, bridge = _deps()
    cfg = _cfg(tmp_path, with_errors=False, with_unit_log=False)
    db = DBOrchestrator.from_config(cfg, plc=plc, bridge=bridge)
    assert db.errors is None
    assert db.unit_log is None
    assert db.services() == ["RecipesStore", "RecipePublisher"]


def test_recipe_pub_skipped_when_recipes_absent(tmp_path: Path):
    """Without recipes, recipe_pub cannot exist (it has no DB to look in)."""
    plc, bridge = _deps()
    cfg = _cfg(tmp_path, with_recipes=False)
    db = DBOrchestrator.from_config(cfg, plc=plc, bridge=bridge)
    assert db.recipes is None
    assert db.recipe_pub is None
    assert "RecipePublisher" not in db.services()


def test_recipe_pub_skipped_when_plc_recipe_absent(tmp_path: Path):
    plc, bridge = _deps()
    cfg = _cfg(tmp_path, with_recipe_pub=False)
    db = DBOrchestrator.from_config(cfg, plc=plc, bridge=bridge)
    assert db.recipes is not None
    assert db.recipe_pub is None


# ---- lifecycle ---------------------------------------------------------------

def test_start_calls_each_service_in_order(tmp_path: Path):
    plc, bridge = _deps()
    db = DBOrchestrator.from_config(_cfg(tmp_path), plc=plc, bridge=bridge)

    order = []
    for svc in db._services:
        svc.start = MagicMock(side_effect=lambda n=svc.name: order.append(n))

    db.start()
    assert order == ["ErrorsStore", "RecipesStore", "RecipePublisher", "UnitLogger"]


def test_stop_reverses_start_order(tmp_path: Path):
    plc, bridge = _deps()
    db = DBOrchestrator.from_config(_cfg(tmp_path), plc=plc, bridge=bridge)

    order = []
    for svc in db._services:
        svc.start = MagicMock()
        svc.stop  = MagicMock(side_effect=lambda n=svc.name: order.append(n))

    db.start()
    db.stop()
    assert order == ["UnitLogger", "RecipePublisher", "RecipesStore", "ErrorsStore"]


def test_partial_start_failure_rolls_back(tmp_path: Path):
    plc, bridge = _deps()
    db = DBOrchestrator.from_config(_cfg(tmp_path), plc=plc, bridge=bridge)

    started, stopped = [], []
    def make_start(n, fail=False):
        def _start():
            if fail: raise RuntimeError(f"{n} failed")
            started.append(n)
        return _start
    def make_stop(n):
        def _stop(): stopped.append(n)
        return _stop

    db._services[0].start = make_start("ErrorsStore")
    db._services[0].stop  = make_stop("ErrorsStore")
    db._services[1].start = make_start("RecipesStore")
    db._services[1].stop  = make_stop("RecipesStore")
    db._services[2].start = make_start("RecipePublisher", fail=True)
    db._services[2].stop  = make_stop("RecipePublisher")
    db._services[3].start = make_start("UnitLogger")
    db._services[3].stop  = make_stop("UnitLogger")

    with pytest.raises(RuntimeError, match="RecipePublisher failed"):
        db.start()

    # The two services that started should be rolled back; the failing one
    # never appended to _started, and the last one was never reached.
    assert started == ["ErrorsStore", "RecipesStore"]
    assert stopped == ["RecipesStore", "ErrorsStore"]


def test_stop_swallows_per_service_errors(tmp_path: Path):
    plc, bridge = _deps()
    db = DBOrchestrator.from_config(_cfg(tmp_path), plc=plc, bridge=bridge)
    for svc in db._services:
        svc.start = MagicMock()
    db.start()
    db._services[0].stop = MagicMock(side_effect=RuntimeError("boom"))
    for svc in db._services[1:]:
        svc.stop = MagicMock()
    db.stop()                                   # must not raise
    for svc in db._services[1:]:
        svc.stop.assert_called_once()


def test_real_lifecycle_smoke(tmp_path: Path):
    """End-to-end: real stores actually start, run, and stop on three SQLite files."""
    plc, bridge = _deps()
    cfg = _cfg(tmp_path)
    db = DBOrchestrator.from_config(cfg, plc=plc, bridge=bridge)
    db.start()
    try:
        # Each store created its DB file.
        assert Path(cfg.errors_log.db_path).is_file()
        assert Path(cfg.recipes.db_path).is_file()
        assert Path(cfg.unit_log.db_path).is_file()
        # The PLC publisher validated its aliases and subscribed.
        plc.validate.assert_called_with(["recipe.code", "recipe.setpoints"])
        plc.subscribe.assert_called_once()
        # The unit logger subscribed to the bridge.
        bridge.on_event.assert_called_once()
    finally:
        db.stop()


def test_services_method_reflects_built_set(tmp_path: Path):
    plc, bridge = _deps()
    cfg = _cfg(tmp_path, with_errors=False, with_recipe_pub=False, with_unit_log=False)
    db = DBOrchestrator.from_config(cfg, plc=plc, bridge=bridge)
    assert db.services() == ["RecipesStore"]


def test_started_list_cleared_on_stop(tmp_path: Path):
    plc, bridge = _deps()
    db = DBOrchestrator.from_config(_cfg(tmp_path), plc=plc, bridge=bridge)
    for svc in db._services:
        svc.start = MagicMock(); svc.stop = MagicMock()
    db.start(); assert len(db._started) == 4
    db.stop();  assert db._started == []
