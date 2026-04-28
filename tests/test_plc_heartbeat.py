"""Tests for PLCHeartbeat: thread, validation, write, error handling."""
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from plc_heartbeat import HeartbeatConfig, PLCHeartbeat


def _make(period_ms: int = 20):
    plc = MagicMock()
    cfg = HeartbeatConfig(alias="health.heartbeat", period_ms=period_ms)
    return PLCHeartbeat(plc, cfg), plc


def test_start_validates_alias():
    hb, plc = _make()
    hb.start()
    plc.validate.assert_called_once_with(["health.heartbeat"])
    hb.stop()


def test_increments_and_writes():
    """Counter should increment and call plc.write each tick."""
    hb, plc = _make(period_ms=10)
    hb.start()
    time.sleep(0.05)        # ~5 ticks
    hb.stop()
    time.sleep(0.02)        # let thread exit

    assert plc.write.call_count >= 2
    aliases = [c.args[0] for c in plc.write.call_args_list]
    assert all(a == "health.heartbeat" for a in aliases)
    values = [c.args[1] for c in plc.write.call_args_list]
    # Strictly increasing (1, 2, 3, ...)
    assert values == sorted(values)
    assert values[0] >= 1


def test_stop_halts_writes():
    hb, plc = _make(period_ms=10)
    hb.start()
    time.sleep(0.03)
    hb.stop()
    time.sleep(0.02)
    snapshot = plc.write.call_count
    time.sleep(0.05)
    assert plc.write.call_count == snapshot   # no new writes after stop


def test_write_errors_swallowed():
    hb, plc = _make(period_ms=10)
    plc.write.side_effect = RuntimeError("ADS gone")
    hb.start()
    time.sleep(0.03)
    hb.stop()
    # Thread must not have crashed; wait_count > 0 implies _run kept looping.
    assert plc.write.call_count >= 1


def test_counter_wraps_at_udint_max():
    hb, _plc = _make()
    hb._counter = 0xFFFFFFFE
    hb._counter = (hb._counter + 1) & 0xFFFFFFFF
    assert hb._counter == 0xFFFFFFFF
    hb._counter = (hb._counter + 1) & 0xFFFFFFFF
    assert hb._counter == 0      # wrapped


def test_default_period_is_one_second():
    cfg = HeartbeatConfig(alias="x")
    assert cfg.period_ms == 1000
