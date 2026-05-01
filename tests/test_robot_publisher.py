"""Tests for RobotPublisher: write ST_RobotStatus struct on every change."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from robot_publisher import RobotPublisher, RobotStatusConfig, _status_to_struct
from robot_status    import RobotStatus


def _make(initial: RobotStatus | None = None):
    monitor = MagicMock()
    monitor.status.return_value = initial or RobotStatus()
    plc = MagicMock()
    cfg = RobotStatusConfig(status_alias="robot.status")
    return RobotPublisher(monitor, plc, cfg), monitor, plc


# ---- struct translation ----

def test_status_to_struct_ready():
    s = RobotStatus(
        ctrl_state="motoron", opmode="AUTO", exec_state="running",
        speed_ratio=80,
    )
    out = _status_to_struct(s)
    assert out == {
        "bReady":      True,
        "bMotorsOn":   True,
        "bAutoMode":   True,
        "bRunning":    True,
        "bGuardStop":  False,
        "bEStop":      False,
        "nSpeedRatio": 80,
    }


def test_status_to_struct_guard_stop():
    s = RobotStatus(ctrl_state="guardstop", opmode="AUTO", exec_state="stopped")
    out = _status_to_struct(s)
    assert out["bGuardStop"] is True
    assert out["bMotorsOn"]  is False
    assert out["bReady"]     is False


def test_status_to_struct_estop():
    s = RobotStatus(ctrl_state="emergencystop")
    out = _status_to_struct(s)
    assert out["bEStop"] is True


# ---- lifecycle ----

def test_start_validates_alias():
    pub, monitor, plc = _make()
    pub.start()
    plc.validate.assert_called_once_with(["robot.status"])


def test_start_publishes_initial_state():
    """Initial publish on start so the PLC bool isn't stale at boot."""
    pub, monitor, plc = _make(RobotStatus(
        ctrl_state="motoron", opmode="AUTO", exec_state="running", speed_ratio=100,
    ))
    pub.start()
    plc.write.assert_called_once_with("robot.status", _status_to_struct(
        RobotStatus(ctrl_state="motoron", opmode="AUTO", exec_state="running",
                    speed_ratio=100)
    ))


def test_start_subscribes_to_monitor():
    pub, monitor, plc = _make()
    pub.start()
    monitor.on_change.assert_called_once()


def test_change_callback_writes_struct():
    pub, monitor, plc = _make()
    captured = {}
    monitor.on_change.side_effect = (
        lambda cb: captured.setdefault("cb", cb) or MagicMock()
    )
    pub.start()
    plc.reset_mock()        # clear initial publish

    captured["cb"](RobotStatus(
        ctrl_state="guardstop", opmode="MANR", exec_state="stopped",
        speed_ratio=10,
    ))
    plc.write.assert_called_once()
    alias, struct = plc.write.call_args.args
    assert alias == "robot.status"
    assert struct["bGuardStop"]  is True
    assert struct["bAutoMode"]   is False
    assert struct["nSpeedRatio"] == 10


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


# ---- bus mode --------------------------------------------------------------

def test_bus_mode_subscribes_via_bus_not_monitor():
    import asyncio, time
    from event_bus import EventBus
    from events import RobotStatusChanged, signals

    bus = EventBus()
    loop = asyncio.new_event_loop()
    bus.start(loop)
    try:
        monitor = MagicMock()
        monitor.status.return_value = RobotStatus()
        plc = MagicMock()
        cfg = RobotStatusConfig(status_alias="robot.status")
        pub = RobotPublisher(monitor, plc, cfg, bus=bus)
        pub.start()
        try:
            # Boot push happened (synchronous).
            assert plc.write.called
            monitor.on_change.assert_not_called()

            plc.write.reset_mock()
            new = RobotStatus(
                ctrl_state="motoron", opmode="AUTO", exec_state="running",
                speed_ratio=100,
            )
            bus.publish(signals.robot_status_changed,
                        RobotStatusChanged(status=new))
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and not plc.write.called:
                time.sleep(0.005)
            alias, struct = plc.write.call_args.args
            assert alias == "robot.status"
            assert struct["bReady"] is True
        finally:
            pub.stop()
    finally:
        bus.stop()
        loop.close()
