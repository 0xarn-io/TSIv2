"""Tests for CameraPublisher: PLC bool → camera trigger wiring."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from camera_publisher import CameraPublisher, CameraTriggerConfig


def _make(trigger_count: int = 2):
    cameras = MagicMock()
    cameras.panels = {"entry": MagicMock(), "exit": MagicMock()}
    plc = MagicMock()
    plc.subscribe.side_effect = lambda *_a, **_kw: (10, 20)

    triggers = [
        CameraTriggerConfig(alias="camera.snap_entry", camera="entry"),
        CameraTriggerConfig(alias="camera.snap_exit",  camera="exit"),
    ][:trigger_count]
    return CameraPublisher(cameras, plc, triggers), cameras, plc


def test_start_validates_aliases():
    pub, cameras, plc = _make()
    pub.start()
    plc.validate.assert_called_once_with([
        "camera.snap_entry", "camera.snap_exit",
    ])


def test_start_subscribes_each_trigger():
    pub, cameras, plc = _make()
    pub.start()
    assert plc.subscribe.call_count == 2


def test_start_rejects_unknown_camera():
    cameras = MagicMock()
    cameras.panels = {"entry": MagicMock()}   # no "exit"
    plc = MagicMock()
    pub = CameraPublisher(cameras, plc, [
        CameraTriggerConfig(alias="camera.snap_exit", camera="exit"),
    ])
    with pytest.raises(KeyError, match="unknown camera"):
        pub.start()


def test_rising_edge_fires_camera_trigger():
    """The PLC callback should call cameras.trigger(name) when val=True."""
    pub, cameras, plc = _make()
    captured_callbacks = []
    plc.subscribe.side_effect = lambda alias, cb, **_kw: (
        captured_callbacks.append((alias, cb)) or (1, 2)
    )
    pub.start()

    # Find the entry callback and fire it with True
    entry_cb = next(cb for a, cb in captured_callbacks if a == "camera.snap_entry")
    entry_cb("camera.snap_entry", True)
    cameras.trigger.assert_called_with("entry")


def test_falling_edge_does_nothing():
    """val=False must NOT trigger a snapshot."""
    pub, cameras, plc = _make()
    captured = []
    plc.subscribe.side_effect = lambda alias, cb, **_kw: (
        captured.append((alias, cb)) or (1, 2)
    )
    pub.start()

    entry_cb = next(cb for a, cb in captured if a == "camera.snap_entry")
    entry_cb("camera.snap_entry", False)
    cameras.trigger.assert_not_called()


def test_each_callback_binds_its_own_camera():
    """Loop closure regression: each subscription must trigger its own camera."""
    pub, cameras, plc = _make()
    captured = []
    plc.subscribe.side_effect = lambda alias, cb, **_kw: (
        captured.append((alias, cb)) or (len(captured), len(captured))
    )
    pub.start()

    for alias, cb in captured:
        cb(alias, True)

    # Both cameras should have been triggered exactly once each.
    triggered = [c.args[0] for c in cameras.trigger.call_args_list]
    assert sorted(triggered) == ["entry", "exit"]


def test_stop_unsubscribes_all():
    pub, cameras, plc = _make()
    plc.subscribe.side_effect = [(1, 2), (3, 4)]
    pub.start()
    pub.stop()
    assert plc.unsubscribe.call_count == 2
    assert pub._handles == []


def test_stop_swallows_unsubscribe_errors():
    pub, cameras, plc = _make()
    plc.subscribe.side_effect = [(1, 2), (3, 4)]
    plc.unsubscribe.side_effect = RuntimeError("ADS gone")
    pub.start()
    pub.stop()  # must not raise
    assert pub._handles == []


def test_empty_triggers_list_is_a_noop():
    pub, cameras, plc = _make(trigger_count=0)
    pub.start()
    plc.validate.assert_called_once_with([])
    plc.subscribe.assert_not_called()
