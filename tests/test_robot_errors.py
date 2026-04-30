"""Tests for RobotElogPoller — dedup, severity mapping, seen-seq pre-load."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from robot_errors import (
    RobotElogPoller, _ELOG_TYPE_TO_SEVERITY,
    _safe_int, _sorted_oldest_first,
)


def _entry(seq, type_="ERROR", code=10001, title="t", desc="d", action=""):
    return {"seq": seq, "type": type_, "code": code, "ts": "2026-04-28",
            "title": title, "desc": desc, "action": action}


def _make(monitor=None, store=None):
    if monitor is None:
        monitor = MagicMock()
        monitor.fetch_errors.return_value = []
    if store is None:
        store = MagicMock()
        store.query.return_value = []
    return RobotElogPoller(monitor, store, poll_ms=1000), monitor, store


# ---- helpers ---------------------------------------------------------------

def test_severity_mapping_known_values():
    assert _ELOG_TYPE_TO_SEVERITY["INFO"]  == "info"
    assert _ELOG_TYPE_TO_SEVERITY["WARN"]  == "warning"
    assert _ELOG_TYPE_TO_SEVERITY["ERROR"] == "error"


def test_safe_int_handles_garbage():
    assert _safe_int(42)    == 42
    assert _safe_int("42")  == 42
    assert _safe_int("abc") == 0
    assert _safe_int(None)  == 0


def test_sorted_oldest_first_uses_trailing_seq_int():
    out = _sorted_oldest_first([
        _entry("0/30"), _entry("0/5"), _entry("0/100"),
    ])
    assert [e["seq"] for e in out] == ["0/5", "0/30", "0/100"]


def test_sorted_oldest_first_falls_back_to_string_compare():
    out = _sorted_oldest_first([
        _entry("alpha"), _entry("beta"), _entry("0/3"),
    ])
    assert [e["seq"] for e in out] == ["0/3", "alpha", "beta"]


# ---- _load_seen ------------------------------------------------------------

def test_load_seen_extracts_seq_from_context_json():
    store = MagicMock()
    store.query.return_value = [
        {"raw_context_json": json.dumps({"seq": "0/1", "ts": "x"})},
        {"raw_context_json": json.dumps({"seq": "0/7"})},
        {"raw_context_json": None},
        {"raw_context_json": "{not json"},
        {"raw_context_json": json.dumps({"ts": "x"})},      # no seq
    ]
    p, _, _ = _make(store=store)
    assert p._load_seen() == {"0/1", "0/7"}


def test_load_seen_swallows_query_failure():
    store = MagicMock()
    store.query.side_effect = RuntimeError("DB locked")
    p, _, _ = _make(store=store)
    assert p._load_seen() == set()


# ---- _poll_once ------------------------------------------------------------

def test_poll_once_logs_only_unseen_entries():
    monitor = MagicMock()
    monitor.fetch_errors.return_value = [
        _entry("0/1", "ERROR"),
        _entry("0/2", "WARN"),
        _entry("0/3", "ERROR"),
    ]
    store = MagicMock(); store.query.return_value = []
    p = RobotElogPoller(monitor, store, poll_ms=1000)
    p._seen_seqs = {"0/1"}                    # already mirrored

    p._poll_once()

    assert store.log.call_count == 2
    seqs_logged = [
        json.loads(c.kwargs["context"] and "{}" or "")  # placate linter
        for c in []
    ]
    # Inspect kwargs directly:
    logged_seqs = [c.kwargs["context"]["seq"] for c in store.log.call_args_list]
    assert logged_seqs == ["0/2", "0/3"]
    severities = [c.kwargs["severity"] for c in store.log.call_args_list]
    assert severities == ["warning", "error"]
    # Both new seqs are now remembered.
    assert {"0/1", "0/2", "0/3"} == p._seen_seqs


def test_poll_once_skips_entries_without_seq():
    monitor = MagicMock()
    monitor.fetch_errors.return_value = [
        _entry("", "ERROR"),
        _entry("0/9", "ERROR"),
    ]
    store = MagicMock(); store.query.return_value = []
    p = RobotElogPoller(monitor, store, poll_ms=1000)
    p._poll_once()

    assert store.log.call_count == 1
    assert store.log.call_args.kwargs["context"]["seq"] == "0/9"


def test_poll_once_logs_in_chronological_order():
    monitor = MagicMock()
    monitor.fetch_errors.return_value = [
        _entry("0/30"), _entry("0/5"), _entry("0/100"),
    ]
    store = MagicMock(); store.query.return_value = []
    p = RobotElogPoller(monitor, store, poll_ms=1000)
    p._poll_once()
    seqs = [c.kwargs["context"]["seq"] for c in store.log.call_args_list]
    assert seqs == ["0/5", "0/30", "0/100"]


def test_poll_once_dedupes_across_calls():
    monitor = MagicMock()
    monitor.fetch_errors.return_value = [_entry("0/1"), _entry("0/2")]
    store = MagicMock(); store.query.return_value = []
    p = RobotElogPoller(monitor, store, poll_ms=1000)

    p._poll_once()
    assert store.log.call_count == 2
    p._poll_once()                         # same data, second tick
    assert store.log.call_count == 2       # nothing new


def test_poll_once_swallows_fetch_failure():
    monitor = MagicMock()
    monitor.fetch_errors.side_effect = RuntimeError("RWS down")
    store = MagicMock(); store.query.return_value = []
    p = RobotElogPoller(monitor, store, poll_ms=1000)
    p._poll_once()                         # must not raise
    store.log.assert_not_called()


def test_poll_once_swallows_log_failure_per_entry():
    monitor = MagicMock()
    monitor.fetch_errors.return_value = [_entry("0/1"), _entry("0/2")]
    store = MagicMock(); store.query.return_value = []
    store.log.side_effect = [RuntimeError("DB full"), None]
    p = RobotElogPoller(monitor, store, poll_ms=1000)

    p._poll_once()
    # First entry's seq should NOT be in seen (log failed) so we'll retry.
    assert "0/1" not in p._seen_seqs
    assert "0/2" in p._seen_seqs


def test_poll_once_unknown_type_defaults_to_error():
    monitor = MagicMock()
    monitor.fetch_errors.return_value = [_entry("0/1", type_="UNKNOWN")]
    store = MagicMock(); store.query.return_value = []
    p = RobotElogPoller(monitor, store, poll_ms=1000)
    p._poll_once()
    assert store.log.call_args.kwargs["severity"] == "error"


def test_poll_once_passes_through_fetch_kwargs():
    monitor = MagicMock()
    monitor.fetch_errors.return_value = []
    store = MagicMock(); store.query.return_value = []
    p = RobotElogPoller(
        monitor, store, poll_ms=1000,
        domain=2, limit=20, include_info=True,
    )
    p._poll_once()
    monitor.fetch_errors.assert_called_once_with(
        domain=2, limit=20, include_info=True,
    )
