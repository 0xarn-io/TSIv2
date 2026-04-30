"""Tests for RWSClient — session composition + RAPID read/write paths."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from rws_client import RWSClient, _extract_rapid_value


def _cfg():
    return SimpleNamespace(
        ip="1.2.3.4", username="u", password="p",
        verify_ssl=False, timeout_s=2.0,
    )


def _client_with_mock_session(get=None, post=None):
    c = RWSClient(_cfg())
    sess = MagicMock()
    if get is not None:   sess.get  = MagicMock(side_effect=get)
    if post is not None:  sess.post = MagicMock(side_effect=post)
    c._session = sess
    return c, sess


def _ok(json_body=None, status=200, ok=True):
    r = MagicMock()
    r.status_code = status
    r.ok = ok
    r.json.return_value = json_body or {}
    r.text = ""
    return r


# ---- _extract_rapid_value ---------------------------------------------------

def test_extract_value_hal_state_shape():
    assert _extract_rapid_value(
        {"_embedded": {"_state": [{"value": "42"}]}}
    ) == "42"


def test_extract_value_alt_state_key():
    assert _extract_rapid_value(
        {"_embedded": {"state": [{"value": "TRUE"}]}}
    ) == "TRUE"


def test_extract_value_flat_shape():
    assert _extract_rapid_value({"value": "hello"}) == "hello"


def test_extract_value_missing_returns_none():
    assert _extract_rapid_value({"_embedded": {}}) is None


# ---- read_rapid -------------------------------------------------------------

def test_read_rapid_url_and_value():
    c, sess = _client_with_mock_session(
        get=lambda url, **kw: _ok({"_embedded": {"_state": [{"value": "5"}]}}),
    )
    out = c.read_rapid("T_ROB1", "MainModule", "n")
    assert out == "5"
    # OmniCore RWS path: plain slashes, /data subresource at the end.
    # Matches the omnicore-sdk.js getSymbolUrl shape.
    call = sess.get.call_args
    assert call.args[0] == (
        "https://1.2.3.4/rw/rapid/symbol/RAPID/T_ROB1/MainModule/n/data"
    )


def test_read_rapid_failure_returns_none():
    c, _ = _client_with_mock_session(get=lambda *a, **kw: _ok(status=404, ok=False))
    assert c.read_rapid("T_ROB1", "Mod", "x") is None


# ---- write_rapid ------------------------------------------------------------

def test_write_rapid_acquires_and_releases_mastership():
    calls: list[tuple[str, str | None]] = []
    def post(url, params=None, data=None, headers=None, timeout=None):
        # action may live in either params or data depending on which
        # mastership URI shape we ended up using.
        action = (params or {}).get("action") or (data or {}).get("action")
        calls.append((url, action))
        return _ok()
    c, _ = _client_with_mock_session(post=post)

    assert c.write_rapid("T_ROB1", "MainModule", "n", "42") is True

    # First POST acquires mastership (some flavour of /rw/mastership[/edit]).
    assert "rw/mastership" in calls[0][0]
    assert calls[0][1] == "request"
    # Then the data POST.
    assert calls[1] == (
        "https://1.2.3.4/rw/rapid/symbol/RAPID/T_ROB1/MainModule/n/data",
        None,
    )
    # Finally, release on the same URI we locked onto.
    assert calls[-1][0] == calls[0][0]
    assert calls[-1][1] == "release"


def test_write_rapid_releases_mastership_even_when_set_fails():
    """Even if the data POST fails, mastership must still be released."""
    posts: list[str] = []
    def post(url, params=None, data=None, headers=None, timeout=None):
        action = (params or {}).get("action") or (data or {}).get("action")
        if "/rw/rapid/symbol/" in url:
            posts.append("write")
            return _ok(status=500, ok=False)
        posts.append(action)
        return _ok()
    c, _ = _client_with_mock_session(post=post)

    assert c.write_rapid("T_ROB1", "Mod", "n", "1") is False
    assert posts == ["request", "write", "release"]


def test_write_rapid_aborts_when_mastership_request_fails():
    """Every mastership probe shape returns 403 → write must not run."""
    def post(url, params=None, data=None, headers=None, timeout=None):
        action = (params or {}).get("action") or (data or {}).get("action")
        if "/rw/mastership" in url and action in ("request", "release"):
            return _ok(status=403, ok=False)
        pytest.fail(f"unexpected POST {url} (action={action})")
    c, _ = _client_with_mock_session(post=post)
    assert c.write_rapid("T_ROB1", "Mod", "n", "1") is False


# ---- RAPID array codec ------------------------------------------------------

from rws_client import format_rapid_array, parse_rapid_array


def test_parse_string_array():
    assert parse_rapid_array('[["35x70"],["28x70"],[""]]') == [
        ["35x70"], ["28x70"], [""],
    ]


def test_parse_nested_num_array():
    out = parse_rapid_array("[[889,1778,1],[711,1778,1],[0,0,0]]")
    assert out == [[889, 1778, 1], [711, 1778, 1], [0, 0, 0]]


def test_parse_handles_floats_and_bools():
    assert parse_rapid_array("[[1.5, TRUE], [2, FALSE]]") == [
        [1.5, True], [2, False],
    ]


def test_parse_handles_escaped_quotes():
    assert parse_rapid_array(r'[["he said \"hi\""]]') == [['he said "hi"']]


def test_parse_rejects_garbage():
    import pytest
    with pytest.raises(ValueError):
        parse_rapid_array("not an array")
    with pytest.raises(ValueError):
        parse_rapid_array("[1,]")               # trailing comma
    with pytest.raises(ValueError):
        parse_rapid_array("[")                  # unterminated


def test_format_round_trip_strings():
    src = [["35x70"], ["28x70"], [""]]
    dumped = format_rapid_array(src)
    assert dumped == '[["35x70"],["28x70"],[""]]'
    assert parse_rapid_array(dumped) == src


def test_format_round_trip_nums():
    src = [[889, 1778, 1], [0, 0, 0]]
    dumped = format_rapid_array(src)
    assert dumped == "[[889,1778,1],[0,0,0]]"
    assert parse_rapid_array(dumped) == src


def test_format_escapes_quotes_in_strings():
    assert format_rapid_array(['a"b']) == r'["a\"b"]'


def test_format_floats_keep_decimal_when_fractional():
    assert format_rapid_array([1.5]) == "[1.5]"
    assert format_rapid_array([2.0]) == "[2]"
