"""Tests for RobotPublisher: mirror RobotMonitor.is_ready to a PLC bool."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from robot_publisher import RobotPublisher, RobotStatusConfig
from robot_status    import RobotStatus


def _make(initial_ready: bool = False):
    monitor = MagicMock()
    monitor.status.return_value = RobotStatus(
        ctrl_state="motoron" if initial_ready else "motoroff",
        opmode="AUTO",
        exec_state="running" if initial_ready else "stopped",
    )
    plc = MagicMock()
    cfg = RobotStatusConfig(ready_alias="robot.ready")
    return RobotPublisher(monitor, plc, cfg), monitor, plc


def test_start_validates_alias():
    pub, monitor, plc = _make()
    pub.start()
    plc.validate.assert_called_once_with(["robot.ready"])


def test_start_publishes_initial_state():
    pub, monitor, plc = _make(initial_ready=True)
    pub.start()
    plc.write.assert_called_with("robot.ready", True)


def test_start_subscribes_to_monitor():
    pub, monitor, plc = _make()
    pub.start()
    monitor.on_change.assert_called_once()


def test_change_callback_writes_to_plc():
    pub, monitor, plc = _make()
    captured = {}
    monitor.on_change.side_effect = (
        lambda cb: captured.setdefault("cb", cb) or MagicMock()
    )
    pub.start()
    plc.reset_mock()  # clear the initial publish

    captured["cb"](RobotStatus(
        ctrl_state="motoron", opmode="AUTO", exec_state="running",
    ))
    plc.write.assert_called_once_with("robot.ready", True)


def test_publish_swallows_write_errors():
    pub, monitor, plc = _make()
    plc.write.side_effect = RuntimeError("ADS down")
    pub.start()  # initial publish raises internally — must not propagate


def test_stop_unsubscribes():
    pub, monitor, plc = _make()
    unsub = MagicMock()
    monitor.on_change.return_value = unsub
    pub.start()
    pub.stop()
    unsub.assert_called_once()
    assert pub._unsub is None
