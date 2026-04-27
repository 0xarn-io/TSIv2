"""Tests for BoxScene state management (no nicegui rendering)."""
from __future__ import annotations

from box_scene import BoxConfig, BoxScene


def _cfgs():
    return [
        BoxConfig(width_mm=711, height_mm=1800, depth_mm=1778, x_pos=-2.0),
        BoxConfig(width_mm=899, height_mm=1300, depth_mm=1778, x_pos=0.0),
    ]


def test_scene_holds_configs():
    s = BoxScene.from_config(_cfgs())
    assert len(s._cfgs) == 2


def test_set_size_updates_config():
    s = BoxScene.from_config(_cfgs())
    s.set_size(0, width_mm=999)
    assert s._cfgs[0].width_mm == 999
    assert s._cfgs[0].height_mm == 1800  # unchanged


def test_set_size_partial_args():
    s = BoxScene.from_config(_cfgs())
    s.set_size(1, height_mm=500, depth_mm=600)
    assert s._cfgs[1].height_mm == 500
    assert s._cfgs[1].depth_mm == 600
    assert s._cfgs[1].width_mm == 899   # unchanged


def test_on_click_unsubscribe():
    s = BoxScene.from_config(_cfgs())
    cb = lambda i: None
    unsub = s.on_click(cb)
    assert cb in s._click_cbs
    unsub()
    assert cb not in s._click_cbs


def test_two_scenes_have_independent_state():
    """Regression: module-level state used to leak between instances."""
    a = BoxScene.from_config(_cfgs())
    b = BoxScene.from_config(_cfgs())
    a.set_size(0, width_mm=111)
    assert b._cfgs[0].width_mm == 711
