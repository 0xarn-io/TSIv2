"""Tests for RecipePublisher: subscribe → DB lookup → setpoints struct write.

Writes run on a worker thread, so tests block until the queue is drained
(via _drain) before asserting on plc.write side effects.
"""
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from recipe_publisher import (
    RecipePublisher, RecipePublisherConfig, _recipe_to_struct,
)
from recipes_store import Recipe


def _drain(pub: RecipePublisher, timeout: float = 1.0) -> None:
    """Wait until the writer queue is empty (worker has consumed everything)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pub._queue.empty():
            time.sleep(0.02)        # let worker finish the current item
            return
        time.sleep(0.01)
    raise AssertionError("worker did not drain in time")


def _recipe(code: int = 1, **overrides) -> Recipe:
    base = dict(code=code)
    base.update(overrides)
    return Recipe(**base)


def _make(initial_code: int = 0):
    recipes = MagicMock()
    plc = MagicMock()
    plc.read.return_value = initial_code
    plc.subscribe.return_value = ("notif", "user")
    cfg = RecipePublisherConfig(
        code_alias="recipe.code",
        setpoints_alias="recipe.setpoints",
    )
    return RecipePublisher(recipes, plc, cfg), recipes, plc


# ---- translation -------------------------------------------------------------

def test_recipe_to_struct_maps_all_fields():
    r = _recipe(
        x_topsheet_length=100, x_topsheet_width=110, x_units=2,
        x1_pos=11, x2_pos=22, x3_pos=33, x_folding=True,
        y_topsheet_length=200, y_topsheet_width=210, y_units=3,
        y1_pos=44, y2_pos=55, y3_pos=66, y_folding=False,
        wood=True, wood_x_pos=77, wood_y_pos=88,
    )
    s = _recipe_to_struct(r)
    assert s == {
        "nXTopsheetLength": 100, "nXTopsheetWidth": 110, "nXUnits": 2,
        "nX1Pos": 11, "nX2Pos": 22, "nX3Pos": 33, "bXFolding": True,
        "nYTopsheetLength": 200, "nYTopsheetWidth": 210, "nYUnits": 3,
        "nY1Pos": 44, "nY2Pos": 55, "nY3Pos": 66, "bYFolding": False,
        "bWood": True, "nWoodXPos": 77, "nWoodYPos": 88,
    }


# ---- lifecycle ---------------------------------------------------------------

def test_start_validates_aliases():
    pub, recipes, plc = _make()
    pub.start()
    try:
        plc.validate.assert_called_once_with(["recipe.code", "recipe.setpoints"])
    finally:
        pub.stop()


def test_start_pushes_initial_recipe():
    """If a recipe is already selected on the PLC, push it once at boot."""
    pub, recipes, plc = _make(initial_code=7)
    recipes.get.return_value = _recipe(code=7, x_topsheet_length=999)

    pub.start()
    try:
        _drain(pub)
        recipes.get.assert_called_with(7)
        plc.write.assert_called_once()
        alias, struct = plc.write.call_args.args
        assert alias == "recipe.setpoints"
        assert struct["nXTopsheetLength"] == 999
    finally:
        pub.stop()


def test_start_handles_unknown_initial_code(caplog):
    """Initial DB miss should log a warning, not raise."""
    pub, recipes, plc = _make(initial_code=42)
    recipes.get.return_value = None
    import logging
    with caplog.at_level(logging.WARNING, logger="recipe_publisher"):
        pub.start()
        try:
            _drain(pub)
        finally:
            pub.stop()
    plc.write.assert_not_called()
    assert any("recipe code 42 not found" in r.message for r in caplog.records)


def test_start_skips_code_zero_silently(caplog):
    """code=0 means 'no selection' — must not query the DB or warn."""
    pub, recipes, plc = _make(initial_code=0)
    import logging
    with caplog.at_level(logging.WARNING, logger="recipe_publisher"):
        pub.start()
        try:
            _drain(pub)
        finally:
            pub.stop()
    recipes.get.assert_not_called()
    plc.write.assert_not_called()
    assert not any("not found" in r.message for r in caplog.records)


def test_start_handles_initial_read_failure(caplog):
    pub, recipes, plc = _make()
    plc.read.side_effect = RuntimeError("ADS down")
    import logging
    with caplog.at_level(logging.WARNING, logger="recipe_publisher"):
        pub.start()
        try:
            # Subscription still registered despite the read failure.
            plc.subscribe.assert_called_once()
        finally:
            pub.stop()
    assert any("initial recipe read failed" in r.message for r in caplog.records)


def test_start_handles_subscription_failure(caplog):
    """Missing PLC symbol on subscribe must NOT propagate — log + continue."""
    pub, recipes, plc = _make()
    plc.subscribe.side_effect = RuntimeError("symbol not found (1808)")
    import logging
    with caplog.at_level(logging.WARNING, logger="recipe_publisher"):
        pub.start()
        try:
            assert pub._handles is None
        finally:
            pub.stop()
    assert any("subscription failed" in r.message for r in caplog.records)


def test_subscribe_callback_writes_setpoints():
    """The PLC notification path: code change → DB lookup → struct write."""
    pub, recipes, plc = _make()
    captured = {}
    def fake_subscribe(alias, cb, **_):
        captured["cb"] = cb
        return ("notif", "user")
    plc.subscribe.side_effect = fake_subscribe

    pub.start()
    try:
        _drain(pub)                                  # initial code=0 path
        plc.reset_mock()
        recipes.get.return_value = _recipe(
            code=5, x_topsheet_length=500, y_topsheet_length=1500, wood=True,
        )

        captured["cb"]("recipe.code", 5)
        _drain(pub)

        recipes.get.assert_called_with(5)
        plc.write.assert_called_once()
        alias, struct = plc.write.call_args.args
        assert alias == "recipe.setpoints"
        assert struct["nXTopsheetLength"] == 500
        assert struct["nYTopsheetLength"] == 1500
        assert struct["bWood"] is True
    finally:
        pub.stop()


def test_callback_unknown_code_is_warned_not_written(caplog):
    pub, recipes, plc = _make()
    captured = {}
    plc.subscribe.side_effect = (
        lambda alias, cb, **_: captured.setdefault("cb", cb) or ("n", "u")
    )
    pub.start()
    try:
        _drain(pub)
        plc.reset_mock()
        recipes.get.return_value = None

        import logging
        with caplog.at_level(logging.WARNING, logger="recipe_publisher"):
            captured["cb"]("recipe.code", 999)
            _drain(pub)

        plc.write.assert_not_called()
        assert any("recipe code 999 not found" in r.message for r in caplog.records)
    finally:
        pub.stop()


def test_write_failure_swallowed():
    pub, recipes, plc = _make()
    captured = {}
    plc.subscribe.side_effect = (
        lambda alias, cb, **_: captured.setdefault("cb", cb) or ("n", "u")
    )
    pub.start()
    try:
        recipes.get.return_value = _recipe(code=1)
        plc.write.side_effect = RuntimeError("ADS gone")
        captured["cb"]("recipe.code", 1)            # must not raise
        _drain(pub)                                 # worker must not die either
        # Worker is still alive after a write failure.
        assert pub._worker is not None and pub._worker.is_alive()
    finally:
        pub.stop()


def test_stop_unsubscribes():
    pub, recipes, plc = _make()
    pub.start()
    pub.stop()
    plc.unsubscribe.assert_called_once_with(("notif", "user"))
    assert pub._handles is None


def test_stop_idempotent():
    pub, recipes, plc = _make()
    pub.start()
    pub.stop()
    pub.stop()                                      # must not raise
    plc.unsubscribe.assert_called_once()


def test_stop_joins_worker():
    pub, recipes, plc = _make()
    pub.start()
    assert pub._worker is not None
    pub.stop()
    assert pub._worker is None
