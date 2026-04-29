"""Tests for RobotVarsPanel construction (no UI render)."""
from __future__ import annotations

from unittest.mock import MagicMock

from robot_variables  import RobotVariablesMonitor
from robot_vars_panel import RobotVarsPanel, _format_value


def test_construction_wires_monitor():
    monitor = MagicMock(spec=RobotVariablesMonitor)
    panel = RobotVarsPanel(monitor)
    assert panel.monitor is monitor
    monitor.get.assert_not_called()


def test_format_value_bool():
    assert _format_value(True)  == "TRUE"
    assert _format_value(False) == "FALSE"


def test_format_value_float_uses_g_format():
    assert _format_value(1.5)   == "1.5"
    assert _format_value(1.000) == "1"


def test_format_value_int_and_string():
    assert _format_value(42)    == "42"
    assert _format_value("hi")  == "hi"
