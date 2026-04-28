"""Tests for RecipesPanel construction and pure helpers (no UI render)."""
from __future__ import annotations

from unittest.mock import MagicMock

from recipes_panel import RecipesPanel, _summary, _subline
from recipes_store import Recipe, RecipesStore


def test_construction_wires_store() -> None:
    store = MagicMock(spec=RecipesStore)
    panel = RecipesPanel(store)
    assert panel.store is store
    # No DB access happens at construction.
    store.list.assert_not_called()
    store.get.assert_not_called()


def test_summary_includes_dimensions_and_units():
    r = Recipe(
        code=1,
        x_topsheet_length=600, x_topsheet_width=400, x_units=3,
        y_topsheet_length=800, y_topsheet_width=500, y_units=2,
    )
    out = _summary(r)
    assert "X 600×400 ×3" in out
    assert "Y 800×500 ×2" in out


def test_subline_omits_optional_fields_when_off():
    r = Recipe(code=1)
    out = _subline(r)
    assert "X pos 0/0/0" in out
    assert "Y pos 0/0/0" in out
    assert "fold X" not in out
    assert "fold Y" not in out
    assert "wood" not in out


def test_subline_lists_active_options():
    r = Recipe(
        code=1, x_folding=True, y_folding=True,
        wood=True, wood_x_pos=10, wood_y_pos=20,
    )
    out = _subline(r)
    assert "fold X" in out
    assert "fold Y" in out
    assert "wood @ (10, 20)" in out
