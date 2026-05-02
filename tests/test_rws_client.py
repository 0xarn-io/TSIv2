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

def _classify(url: str) -> str:
    """Map a POST url to a short label for write_rapid test assertions."""
    if "/rw/rapid/symbol/" in url:
        return "write"
    if url.endswith("/rw/mastership/edit/request"):
        return "request"
    if url.endswith("/rw/mastership/edit/release"):
        return "release"
    return f"unknown:{url}"


def test_write_rapid_succeeds_without_mastership_when_direct_write_ok():
    """OmniCore in AUTO mode usually allows the write directly — no master."""
    calls: list[str] = []
    def post(url, params=None, data=None, headers=None, timeout=None):
        calls.append(_classify(url))
        return _ok()
    c, _ = _client_with_mock_session(post=post)

    assert c.write_rapid("T_ROB1", "MainModule", "n", "42") is True
    assert calls == ["write"]   # single POST, no mastership round-trip


def test_write_rapid_falls_back_to_mastership_when_direct_fails():
    """Direct write fails → acquire mastership, retry, release."""
    posts: list[str] = []
    def post(url, params=None, data=None, headers=None, timeout=None):
        kind = _classify(url)
        posts.append(kind)
        if kind == "write":
            # First write fails (no mastership), second succeeds.
            return _ok(status=500, ok=False) if posts.count("write") == 1 else _ok()
        return _ok()
    c, _ = _client_with_mock_session(post=post)

    assert c.write_rapid("T_ROB1", "Mod", "n", "1") is True
    assert posts == ["write", "request", "write", "release"]


def test_write_rapid_releases_mastership_even_when_retry_fails():
    """Direct + retried write both fail → mastership must still be released."""
    posts: list[str] = []
    def post(url, params=None, data=None, headers=None, timeout=None):
        kind = _classify(url)
        posts.append(kind)
        return _ok(status=500, ok=False) if kind == "write" else _ok()
    c, _ = _client_with_mock_session(post=post)

    assert c.write_rapid("T_ROB1", "Mod", "n", "1") is False
    assert posts == ["write", "request", "write", "release"]


def test_write_rapid_returns_false_when_direct_and_mastership_unavailable():
    """Direct write fails AND mastership endpoint 404 → return False, no retry."""
    posts: list[str] = []
    def post(url, params=None, data=None, headers=None, timeout=None):
        kind = _classify(url)
        posts.append(kind)
        if kind == "write":
            return _ok(status=500, ok=False)
        return _ok(status=404, ok=False)   # mastership not available
    c, _ = _client_with_mock_session(post=post)

    assert c.write_rapid("T_ROB1", "Mod", "n", "1") is False
    assert posts.count("write") == 1   # no retry once mastership fails
    assert "release" not in posts


def test_release_mastership_posts_release_endpoint():
    """Public release endpoint hits POST /rw/mastership/edit/release."""
    posts: list[str] = []
    def post(url, params=None, data=None, headers=None, timeout=None):
        posts.append(_classify(url))
        return _ok()
    c, _ = _client_with_mock_session(post=post)

    assert c.release_mastership() is True
    assert posts == ["release"]


def test_release_mastership_silent_on_failure():
    """A failed release returns False but never raises — pollers call this
    every cycle as insurance and a network blip mustn't crash the loop."""
    def post(url, params=None, data=None, headers=None, timeout=None):
        return _ok(status=500, ok=False)
    c, _ = _client_with_mock_session(post=post)

    assert c.release_mastership() is False         # must not raise


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


# ─── lenient JSON parse + one-shot diagnostic ─────────────────────────────────

import json as _json
import logging as _logging

from rws_client import _parse_lenient_json


def _resp(text: str, *, status: int = 200, ct: str = "application/json"):
    """Build a fake requests.Response with raw .text but a strict r.json()
    that fails — exercising the lenient fallback path in RWSClient.get()."""
    r = MagicMock()
    r.status_code = status
    r.headers = {"Content-Type": ct}
    r.text = text
    def _strict_json():
        return _json.loads(text)            # raises on malformed
    r.json.side_effect = _strict_json
    return r


def test_parse_lenient_strict_passthrough():
    assert _parse_lenient_json('{"a": 1}') == {"a": 1}


def test_parse_lenient_handles_utf8_bom():
    assert _parse_lenient_json('﻿{"a": 1}') == {"a": 1}


def test_parse_lenient_handles_unescaped_control_chars():
    """A localized message containing \\x01 inside a string parses via the
    strict=False fallback (which tolerates control chars within strings).
    The char is preserved in the value — we just don't reject the doc."""
    bad = '{"msg": "hello\x01world", "code": 5}'
    out = _parse_lenient_json(bad)
    assert out == {"msg": "hello\x01world", "code": 5}


def test_parse_lenient_strips_control_chars_when_strict_false_also_fails():
    """If a control char sits OUTSIDE strings (between tokens) where even
    strict=False rejects it, the final pass strips and retries."""
    bad = '{\x01"a": 1}'                   # \x01 between '{' and the key
    out = _parse_lenient_json(bad)
    assert out == {"a": 1}


def test_parse_lenient_returns_none_for_truly_malformed():
    assert _parse_lenient_json("not json at all") is None
    assert _parse_lenient_json("") is None


def test_get_recovers_via_lenient_parse_no_diagnostic(caplog):
    """Strict json fails, lenient succeeds → caller gets the dict and no
    diagnostic line is logged."""
    text = '﻿{"_embedded": {"resources": [{"code": "1"}]}}'   # BOM + valid JSON
    c, _ = _client_with_mock_session(get=lambda *a, **kw: _resp(text))
    with caplog.at_level(_logging.WARNING, logger="rws_client"):
        out = c.get("/rw/elog/0", silent=True)
    assert out == {"_embedded": {"resources": [{"code": "1"}]}}
    assert not any("parse-failed" in r.message for r in caplog.records)


def test_get_logs_diagnostic_once_on_unparseable(caplog):
    """A response strict + lenient both reject is logged exactly once per
    (path, content-type) — subsequent calls stay silent."""
    text = "<html>not json at all</html>"
    c, _ = _client_with_mock_session(
        get=lambda *a, **kw: _resp(text, ct="text/html"),
    )
    with caplog.at_level(_logging.WARNING, logger="rws_client"):
        for _ in range(5):
            assert c.get("/rw/elog/0", silent=True) is None
    diag = [r for r in caplog.records if "parse-failed" in r.message]
    assert len(diag) == 1
    assert "/rw/elog/0" in diag[0].message
    assert "ct=text/html" in diag[0].message
    assert "html" in diag[0].message.lower()       # body snippet present


def test_get_diagnostic_clears_on_recovery_then_relogs(caplog):
    """After a parse-recovered transition the diagnostic is armed again —
    a subsequent failure with the same signature logs anew."""
    bad = "<html>error</html>"
    good = '{"ok": true}'
    responses = [bad, good, bad]
    def _next(*_a, **_kw):
        return _resp(responses.pop(0), ct="text/html")
    c, _ = _client_with_mock_session(get=_next)

    with caplog.at_level(_logging.WARNING, logger="rws_client"):
        with caplog.at_level(_logging.INFO, logger="rws_client"):
            assert c.get("/rw/elog/0", silent=True) is None        # bad → diag #1
            assert c.get("/rw/elog/0", silent=True) == {"ok": True} # good → recovered
            assert c.get("/rw/elog/0", silent=True) is None        # bad → diag #2

    diag = [r for r in caplog.records if "parse-failed" in r.message]
    recovered = [r for r in caplog.records if "parse-recovered" in r.message]
    assert len(diag) == 2
    assert len(recovered) == 1
