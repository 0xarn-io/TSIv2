"""Tests for RobotVariablesMonitor — coercion, dispatch, set + read paths."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from robot_variables import (
    RobotVariableConfig, RobotVariablesMonitor, _coerce, _encode,
)


def _cfg(alias="x", **overrides) -> RobotVariableConfig:
    base = dict(alias=alias, task="T_ROB1", module="M", symbol=alias)
    base.update(overrides)
    return RobotVariableConfig(**base)


# ---- config validation ------------------------------------------------------

def test_config_rejects_unknown_type():
    with pytest.raises(ValueError, match="type"):
        RobotVariableConfig(alias="a", task="t", module="m", symbol="s", type="float")


def test_config_rejects_unknown_target():
    with pytest.raises(ValueError, match="target"):
        _cfg(targets=("ui", "csv"))


def test_config_requires_plc_alias_when_plc_target():
    with pytest.raises(ValueError, match="plc_alias"):
        _cfg(targets=("plc",))


# ---- coerce / encode --------------------------------------------------------

def test_coerce_num_int():
    assert _coerce(_cfg(type="num"), "42") == 42


def test_coerce_num_float():
    assert _coerce(_cfg(type="num"), "1.5") == 1.5


def test_coerce_bool_true_variants():
    cfg = _cfg(type="bool")
    assert _coerce(cfg, "TRUE")  is True
    assert _coerce(cfg, "true")  is True
    assert _coerce(cfg, "1")     is True
    assert _coerce(cfg, "FALSE") is False
    assert _coerce(cfg, "0")     is False


def test_coerce_string_strips_quotes():
    assert _coerce(_cfg(type="string"), '"hello"') == "hello"
    assert _coerce(_cfg(type="string"), "hello")  == "hello"


def test_encode_round_trip():
    cfg_n = _cfg(type="num")
    assert _encode(cfg_n, 42) == "42"
    assert _encode(cfg_n, 1.5) == "1.5"
    cfg_b = _cfg(type="bool")
    assert _encode(cfg_b, True) == "TRUE"
    assert _encode(cfg_b, False) == "FALSE"
    cfg_s = _cfg(type="string")
    assert _encode(cfg_s, "hi") == '"hi"'


# ---- monitor public API -----------------------------------------------------

def _monitor(vars_, *, errors_store=None, plc=None):
    client = MagicMock()
    return RobotVariablesMonitor(
        client, vars_, errors_store=errors_store, plc=plc,
    ), client


def test_get_returns_none_before_first_poll():
    m, _ = _monitor([_cfg("a")])
    assert m.get("a") is None


def test_set_blocked_for_read_only():
    m, c = _monitor([_cfg("a", mode="r")])
    with pytest.raises(PermissionError):
        m.set("a", 1)
    c.write_rapid.assert_not_called()


def test_set_writes_through_and_caches():
    m, c = _monitor([_cfg("a", type="num", mode="rw")])
    c.write_rapid.return_value = True
    m.set("a", 5)
    c.write_rapid.assert_called_once_with("T_ROB1", "M", "a", "5")
    assert m.get("a") == 5


def test_set_raises_when_rws_fails():
    m, c = _monitor([_cfg("a", type="num", mode="rw")])
    c.write_rapid.return_value = False
    with pytest.raises(RuntimeError, match="RWS write failed"):
        m.set("a", 5)


def test_observe_fires_ui_callback_only_on_change():
    m, _ = _monitor([_cfg("a", type="num")])
    seen = []
    m.on_change("a", seen.append)
    cfg = m.config("a")
    m._observe(cfg, 1)
    m._observe(cfg, 1)        # same value — no second fire
    m._observe(cfg, 2)
    assert seen == [1, 2]


def test_log_target_calls_errors_store_on_change():
    store = MagicMock()
    m, _ = _monitor(
        [_cfg("a", type="num", targets=("ui", "log"))],
        errors_store=store,
    )
    m._observe(m.config("a"), 1)
    store.log.assert_called_once()
    kwargs = store.log.call_args.kwargs
    assert kwargs["device"] == "robot"
    assert kwargs["subsystem"] == "vars"
    assert kwargs["title"] == "a"
    assert kwargs["severity"] == "info"


def test_plc_target_writes_to_plc_on_change():
    plc = MagicMock()
    m, _ = _monitor(
        [_cfg("a", type="num", targets=("plc",), plc_alias="robot.a")],
        plc=plc,
    )
    m._observe(m.config("a"), 7)
    plc.write.assert_called_once_with("robot.a", 7)


def test_dispatch_swallows_target_failures():
    plc = MagicMock(); plc.write.side_effect = RuntimeError("ADS down")
    store = MagicMock(); store.log.side_effect = RuntimeError("DB down")
    m, _ = _monitor(
        [_cfg("a", type="num", targets=("ui", "log", "plc"),
              plc_alias="robot.a")],
        errors_store=store, plc=plc,
    )
    seen = []
    m.on_change("a", seen.append)
    # Must not raise even though log + plc both fail.
    m._observe(m.config("a"), 1)
    assert seen == [1]


def test_aliases_lists_in_order():
    m, _ = _monitor([_cfg("a"), _cfg("b"), _cfg("c")])
    assert m.aliases() == ["a", "b", "c"]


def test_duplicate_alias_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        RobotVariablesMonitor(MagicMock(), [_cfg("a"), _cfg("a")])
