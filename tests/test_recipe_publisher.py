"""Tests for RecipePublisher: subscribe → DB lookup → setpoints struct write."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from recipe_publisher import (
    RecipePublisher, RecipePublisherConfig, _recipe_to_struct,
)
from recipes_store import Recipe


def _recipe(code: int = 1, **overrides) -> Recipe:
    base = dict(code=code, name=f"R{code}",
                width_mm=711, height_mm=1800, depth_mm=1778)
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
    r = _recipe(width_mm=100, height_mm=200, depth_mm=300,
                x1_pos=11, x2_pos=22, x3_pos=33,
                y1_pos=44, y2_pos=55, y3_pos=66)
    s = _recipe_to_struct(r)
    assert s == {
        "nWidth":  100, "nHeight": 200, "nDepth":  300,
        "nX1Pos":  11,  "nX2Pos":  22,  "nX3Pos":  33,
        "nY1Pos":  44,  "nY2Pos":  55,  "nY3Pos":  66,
    }


# ---- lifecycle ---------------------------------------------------------------

def test_start_validates_aliases():
    pub, recipes, plc = _make()
    pub.start()
    plc.validate.assert_called_once_with(["recipe.code", "recipe.setpoints"])


def test_start_pushes_initial_recipe():
    """If a recipe is already selected on the PLC, push it once at boot."""
    pub, recipes, plc = _make(initial_code=7)
    recipes.get.return_value = _recipe(code=7, width_mm=999)

    pub.start()

    recipes.get.assert_called_with(7)
    plc.write.assert_called_once()
    alias, struct = plc.write.call_args.args
    assert alias == "recipe.setpoints"
    assert struct["nWidth"] == 999


def test_start_handles_unknown_initial_code(caplog):
    """Initial DB miss should log a warning, not raise."""
    pub, recipes, plc = _make(initial_code=42)
    recipes.get.return_value = None
    import logging
    with caplog.at_level(logging.WARNING, logger="recipe_publisher"):
        pub.start()
    plc.write.assert_not_called()
    assert any("recipe code 42 not found" in r.message for r in caplog.records)


def test_start_handles_initial_read_failure(caplog):
    pub, recipes, plc = _make()
    plc.read.side_effect = RuntimeError("ADS down")
    import logging
    with caplog.at_level(logging.WARNING, logger="recipe_publisher"):
        pub.start()
    # Subscription still registered despite the read failure.
    plc.subscribe.assert_called_once()
    assert any("initial recipe read failed" in r.message for r in caplog.records)


def test_subscribe_callback_writes_setpoints():
    """The PLC notification path: code change → DB lookup → struct write."""
    pub, recipes, plc = _make()
    captured = {}
    def fake_subscribe(alias, cb, **_):
        captured["cb"] = cb
        return ("notif", "user")
    plc.subscribe.side_effect = fake_subscribe

    pub.start()
    plc.reset_mock()
    recipes.get.return_value = _recipe(code=5, width_mm=500, height_mm=1500, depth_mm=900)

    captured["cb"]("recipe.code", 5)

    recipes.get.assert_called_with(5)
    plc.write.assert_called_once()
    alias, struct = plc.write.call_args.args
    assert alias == "recipe.setpoints"
    assert struct["nWidth"]  == 500
    assert struct["nHeight"] == 1500
    assert struct["nDepth"]  == 900


def test_callback_unknown_code_is_warned_not_written(caplog):
    pub, recipes, plc = _make()
    captured = {}
    plc.subscribe.side_effect = (
        lambda alias, cb, **_: captured.setdefault("cb", cb) or ("n", "u")
    )
    pub.start()
    plc.reset_mock()
    recipes.get.return_value = None

    import logging
    with caplog.at_level(logging.WARNING, logger="recipe_publisher"):
        captured["cb"]("recipe.code", 999)

    plc.write.assert_not_called()
    assert any("recipe code 999 not found" in r.message for r in caplog.records)


def test_write_failure_swallowed():
    pub, recipes, plc = _make()
    captured = {}
    plc.subscribe.side_effect = (
        lambda alias, cb, **_: captured.setdefault("cb", cb) or ("n", "u")
    )
    pub.start()
    recipes.get.return_value = _recipe(code=1)
    plc.write.side_effect = RuntimeError("ADS gone")
    captured["cb"]("recipe.code", 1)            # must not raise


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
    pub.stop()                                  # must not raise
    plc.unsubscribe.assert_called_once()
