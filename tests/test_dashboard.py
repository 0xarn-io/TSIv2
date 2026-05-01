"""Tests for Dashboard composition (no NiceGUI render)."""
from __future__ import annotations

from unittest.mock import MagicMock

from dashboard import Dashboard


def test_build_with_all_stores_populates_all_panels() -> None:
    d = Dashboard.build(
        cameras            = MagicMock(),
        robot_monitor      = MagicMock(),
        robot_vars_monitor = MagicMock(vars={"a": object()}),
        recipes_store      = MagicMock(),
        sizes_store        = MagicMock(),
        errors_store       = MagicMock(),
    )
    assert d.cameras    is not None
    assert d.robot      is not None
    assert d.robot_vars is not None
    assert d.recipes    is not None
    assert d.sizes      is not None
    assert d.errors     is not None
    available = [t.name for t in d._available_tabs()]
    # "errors" tab spec is temporarily commented out in dashboard.py.
    assert available == [
        "cameras", "robot", "vars", "recipes", "sizes",
    ]


def test_build_skips_panels_for_missing_stores() -> None:
    d = Dashboard.build(
        cameras       = None,
        robot_monitor = None,
        recipes_store = MagicMock(),
        sizes_store   = None,
        errors_store  = None,
    )
    assert d.cameras is None
    assert d.robot   is None
    assert d.recipes is not None
    assert d.sizes   is None
    assert d.errors  is None
    assert [t.name for t in d._available_tabs()] == ["recipes"]


def test_build_with_no_stores_yields_no_tabs() -> None:
    d = Dashboard.build()
    assert d._available_tabs() == []


def test_title_is_propagated() -> None:
    d = Dashboard.build(title="Custom Title")
    assert d.title == "Custom Title"
