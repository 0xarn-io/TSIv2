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
    url = sess.get.call_args.args[0]
    assert url == "https://1.2.3.4/rw/rapid/symbol/data/RAPID/T_ROB1/MainModule/n"


def test_read_rapid_failure_returns_none():
    c, _ = _client_with_mock_session(get=lambda *a, **kw: _ok(status=404, ok=False))
    assert c.read_rapid("T_ROB1", "Mod", "x") is None


# ---- write_rapid ------------------------------------------------------------

def test_write_rapid_acquires_and_releases_mastership():
    calls: list[tuple[str, dict]] = []
    def post(url, params=None, data=None, timeout=None):
        calls.append((url, dict(params or {})))
        return _ok()
    c, _ = _client_with_mock_session(post=post)

    assert c.write_rapid("T_ROB1", "MainModule", "n", "42") is True

    paths = [(url, p.get("action")) for url, p in calls]
    assert paths[0]  == ("https://1.2.3.4/rw/mastership/edit", "request")
    assert paths[1]  == ("https://1.2.3.4/rw/rapid/symbol/data/RAPID/T_ROB1/MainModule/n", "set")
    assert paths[-1] == ("https://1.2.3.4/rw/mastership/edit", "release")


def test_write_rapid_releases_mastership_even_when_set_fails():
    posts: list[str] = []
    def post(url, params=None, data=None, timeout=None):
        action = (params or {}).get("action")
        posts.append(action)
        if action == "set":
            return _ok(status=500, ok=False)
        return _ok()
    c, _ = _client_with_mock_session(post=post)

    assert c.write_rapid("T_ROB1", "Mod", "n", "1") is False
    assert posts == ["request", "set", "release"]


def test_write_rapid_aborts_when_mastership_request_fails():
    def post(url, params=None, data=None, timeout=None):
        if (params or {}).get("action") == "request":
            return _ok(status=403, ok=False)
        pytest.fail("should not POST set after mastership failure")
    c, _ = _client_with_mock_session(post=post)
    assert c.write_rapid("T_ROB1", "Mod", "n", "1") is False
