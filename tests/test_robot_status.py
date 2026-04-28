"""Tests for robot_status: RobotStatus.is_ready, fetch_errors, _poll, helpers."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from robot_status import (
    RobotConfig, RobotMonitor, RobotStatus, _state, _state_int,
)


# ---- RobotConfig defaults ----

def test_robot_config_defaults():
    cfg = RobotConfig(ip="1.2.3.4")
    assert cfg.username == "Admin"
    assert cfg.password == "robotics"
    assert cfg.verify_ssl is False
    assert cfg.poll_ms == 2000
    assert cfg.timeout_s == 2.0


# ---- is_ready logic ----

@pytest.mark.parametrize(
    "ctrl, opmode, exec_, expected",
    [
        ("motoron",  "AUTO", "running", True),
        ("motoron",  "AUTO", "stopped", False),
        ("motoroff", "AUTO", "running", False),
        ("motoron",  "MANR", "running", False),
        ("guardstop","AUTO", "running", False),
    ],
)
def test_is_ready(ctrl, opmode, exec_, expected):
    s = RobotStatus(ctrl_state=ctrl, opmode=opmode, exec_state=exec_)
    assert s.is_ready is expected


def test_individual_status_flags():
    s = RobotStatus(ctrl_state="motoron", opmode="AUTO", exec_state="running")
    assert s.motors_on  is True
    assert s.auto_mode  is True
    assert s.running    is True
    assert s.guard_stop is False
    assert s.estop      is False

    s = RobotStatus(ctrl_state="guardstop", opmode="MANR", exec_state="stopped")
    assert s.motors_on  is False
    assert s.auto_mode  is False
    assert s.running    is False
    assert s.guard_stop is True

    s = RobotStatus(ctrl_state="emergencystop")
    assert s.estop is True
    s = RobotStatus(ctrl_state="emergencystopreset")
    assert s.estop is True


# ---- HAL+JSON helpers ----

def test_state_extracts_first_match():
    obj = {"state": [{"ctrlstate": "motoron"}]}
    assert _state(obj, "ctrlstate") == "motoron"


def test_state_returns_default_when_missing():
    assert _state({"state": [{}]}, "x") == "unknown"
    assert _state(None, "x") == "unknown"
    assert _state({}, "x") == "unknown"


def test_state_int_parses_or_defaults():
    obj = {"state": [{"speedratio": "75"}]}
    assert _state_int(obj, "speedratio") == 75
    assert _state_int({"state": [{"speedratio": "abc"}]}, "speedratio") == 0
    assert _state_int(None, "x") == 0


# ---- _poll: build a monitor without making real HTTP calls ----

def _make_monitor(get_responses: dict[str, dict] | None = None):
    """Build a RobotMonitor whose _get returns canned responses by path."""
    m = RobotMonitor(RobotConfig(ip="1.2.3.4"))
    responses = get_responses or {}
    m._get = lambda path, params=None: responses.get(path)
    return m


def test_poll_updates_status():
    m = _make_monitor({
        "/rw/panel/ctrlstate":   {"state": [{"ctrlstate": "motoron"}]},
        "/rw/panel/opmode":      {"state": [{"opmode": "AUTO"}]},
        "/rw/rapid/execution":   {"state": [{"ctrlexecstate": "running"}]},
        "/rw/panel/speedratio":  {"state": [{"speedratio": "100"}]},
    })
    m._poll()
    s = m.status()
    assert s.ctrl_state == "motoron"
    assert s.opmode     == "AUTO"
    assert s.exec_state == "running"
    assert s.speed_ratio == 100
    assert s.is_ready is True


def test_poll_handles_missing_responses():
    m = _make_monitor({})   # every _get returns None
    m._poll()
    s = m.status()
    assert s.ctrl_state == "unknown"
    assert s.is_ready is False


def test_on_change_fires_on_any_field_change():
    """on_change fires whenever any tracked field changes, not just is_ready."""
    state = {
        "/rw/panel/ctrlstate":   {"state": [{"ctrlstate": "motoron"}]},
        "/rw/panel/opmode":      {"state": [{"opmode": "AUTO"}]},
        "/rw/rapid/execution":   {"state": [{"ctrlexecstate": "running"}]},
        "/rw/panel/speedratio":  {"state": [{"speedratio": "100"}]},
    }
    m = _make_monitor(state)
    seen = []
    m.on_change(lambda s: seen.append((s.is_ready, s.speed_ratio)))

    m._poll()       # initial: unknown → motoron/AUTO/running/100 — fires
    m._poll()       # no change — no fire
    state["/rw/panel/speedratio"] = {"state": [{"speedratio": "50"}]}
    m._poll()       # speed dropped — fires (even though is_ready unchanged)

    assert seen == [(True, 100), (True, 50)]


def test_on_change_unsubscribe():
    m = _make_monitor()
    cb = lambda _s: None
    unsub = m.on_change(cb)
    assert cb in m._change_cbs
    unsub()
    assert cb not in m._change_cbs


# ---- fetch_errors ----

def _elog_response():
    return {
        "_embedded": {
            "resources": [
                {"_title": "1", "msgtype": "1", "code": "10400",
                 "tstamp": "ts1", "title": "logged on"},
                {"_title": "2", "msgtype": "2", "code": "12700",
                 "tstamp": "ts2", "title": "missing tz", "desc": "..."},
                {"_title": "3", "msgtype": "3", "code": "10106",
                 "tstamp": "ts3", "title": "service due", "desc": "..."},
            ]
        }
    }


def test_fetch_errors_filters_to_warn_and_error_by_default():
    m = _make_monitor({"/rw/elog/0": _elog_response()})
    out = m.fetch_errors()
    assert [e["type"] for e in out] == ["WARN", "ERROR"]
    assert out[0]["code"] == "12700"
    assert out[1]["code"] == "10106"


def test_fetch_errors_include_info_keeps_everything():
    m = _make_monitor({"/rw/elog/0": _elog_response()})
    out = m.fetch_errors(include_info=True)
    assert [e["type"] for e in out] == ["INFO", "WARN", "ERROR"]


def test_fetch_errors_returns_empty_on_no_response():
    m = _make_monitor({})
    assert m.fetch_errors() == []


def test_fetch_errors_passes_limit_to_request():
    captured = {}
    def fake_get(path, params=None):
        captured["path"] = path
        captured["params"] = params
        return _elog_response()
    m = RobotMonitor(RobotConfig(ip="1.2.3.4"))
    m._get = fake_get
    m.fetch_errors(domain=2, limit=10)
    assert captured["path"] == "/rw/elog/2"
    assert captured["params"]["lim"] == 10
