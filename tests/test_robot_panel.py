"""Tests for RobotPanel construction + helpers (no UI render)."""
from __future__ import annotations

from unittest.mock import MagicMock

from robot_panel  import RobotPanel
from robot_status import RobotMonitor, RobotStatus


def test_construction_wires_monitor() -> None:
    monitor = MagicMock(spec=RobotMonitor)
    panel = RobotPanel(monitor)
    assert panel.monitor is monitor
    monitor.status.assert_not_called()


def test_summary_text_estop_first():
    s = RobotStatus(ctrl_state="emergencystop")
    assert RobotPanel._summary_text(s) == "Emergency Stop"


def test_summary_text_guard_stop():
    s = RobotStatus(ctrl_state="guardstop")
    assert RobotPanel._summary_text(s) == "Guard Stop"


def test_summary_text_motors_off():
    s = RobotStatus(ctrl_state="motoroff")
    assert RobotPanel._summary_text(s) == "Motors off"


def test_summary_text_manual_mode():
    s = RobotStatus(ctrl_state="motoron", opmode="MANR")
    assert RobotPanel._summary_text(s) == "Manual mode"


def test_summary_text_rapid_stopped():
    s = RobotStatus(ctrl_state="motoron", opmode="AUTO", exec_state="stopped")
    assert RobotPanel._summary_text(s) == "RAPID stopped"
