"""Tests for SizesPublisher — multi-table subscribe → DB lookup → struct write.

Writes run on a worker thread; tests block until the queue drains before
asserting on plc.write side effects.
"""
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from sizes_publisher import (
    SizesPublisher, SizeSetpointConfig, _size_to_struct,
)
from sizes_store import Size


def _drain(pub: SizesPublisher, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pub._queue.empty():
            time.sleep(0.02)        # let worker finish the current item
            return
        time.sleep(0.01)
    raise AssertionError("worker did not drain in time")


def _size(**overrides) -> Size:
    base = dict(name="A4", width_mm=210, length_mm=297)
    base.update(overrides)
    return Size(**base)


def _mappings() -> list[SizeSetpointConfig]:
    return [
        SizeSetpointConfig(table="cardboard",
                            code_alias="size.cardboard_id",
                            setpoints_alias="size.cardboard_setpoints"),
        SizeSetpointConfig(table="others",
                            code_alias="size.others_id",
                            setpoints_alias="size.others_setpoints"),
    ]


def _make(initial: dict[str, int] | None = None):
    """initial maps code_alias → value returned by plc.read()."""
    sizes = MagicMock()
    plc   = MagicMock()
    initial = initial or {}
    plc.read.side_effect = lambda alias: initial.get(alias, 0)
    plc.subscribe.return_value = ("n", "u")
    return SizesPublisher(sizes, plc, _mappings()), sizes, plc


# ---- translation -------------------------------------------------------------

def test_size_to_struct_maps_all_fields():
    s = _size(width_mm=300, length_mm=400)
    assert _size_to_struct(s) == {"nWidthMm": 300, "nLengthMm": 400}


# ---- start -------------------------------------------------------------------

def test_start_validates_all_aliases():
    pub, sizes, plc = _make()
    pub.start()
    try:
        plc.validate.assert_called_once_with([
            "size.cardboard_id", "size.cardboard_setpoints",
            "size.others_id",    "size.others_setpoints",
        ])
    finally:
        pub.stop()


def test_start_pushes_initial_for_each_table():
    pub, sizes, plc = _make(initial={
        "size.cardboard_id": 7,
        "size.others_id":    3,
    })
    sizes.get.side_effect = lambda table, sid: {
        ("cardboard", 7): _size(name="C7", width_mm=999),
        ("others",    3): _size(name="O3", length_mm=888),
    }[(table, sid)]

    pub.start()
    try:
        _drain(pub)
        sizes.get.assert_any_call("cardboard", 7)
        sizes.get.assert_any_call("others",    3)
        written = {c.args[0]: c.args[1] for c in plc.write.call_args_list}
        assert written["size.cardboard_setpoints"]["nWidthMm"]  == 999
        assert written["size.others_setpoints"]["nLengthMm"] == 888
    finally:
        pub.stop()


def test_start_skips_id_zero_silently():
    """id=0 means 'no selection' — must not warn or query the DB."""
    pub, sizes, plc = _make()                       # both initial reads return 0
    pub.start()
    try:
        _drain(pub)
        sizes.get.assert_not_called()
        plc.write.assert_not_called()
    finally:
        pub.stop()


def test_start_handles_unknown_id(caplog):
    pub, sizes, plc = _make(initial={"size.cardboard_id": 42})
    sizes.get.return_value = None

    import logging
    with caplog.at_level(logging.WARNING, logger="sizes_publisher"):
        pub.start()
        try:
            _drain(pub)
        finally:
            pub.stop()

    plc.write.assert_not_called()
    assert any("cardboard size id 42 not found" in r.message
               for r in caplog.records)


def test_start_handles_initial_read_failure(caplog):
    pub, sizes, plc = _make()
    plc.read.side_effect = RuntimeError("ADS down")

    import logging
    with caplog.at_level(logging.WARNING, logger="sizes_publisher"):
        pub.start()
        try:
            assert plc.subscribe.call_count == 2
        finally:
            pub.stop()

    msgs = [r.message for r in caplog.records]
    assert any("initial cardboard size read failed" in m for m in msgs)
    assert any("initial others size read failed"    in m for m in msgs)


def test_start_handles_subscription_failure(caplog):
    """Missing PLC symbol on subscribe must NOT raise — log + continue."""
    pub, sizes, plc = _make()
    plc.subscribe.side_effect = RuntimeError("symbol not found (1808)")

    import logging
    with caplog.at_level(logging.WARNING, logger="sizes_publisher"):
        pub.start()                                  # must not raise
        try:
            assert pub._handles == []
        finally:
            pub.stop()

    msgs = [r.message for r in caplog.records]
    assert any("cardboard size subscription failed" in m for m in msgs)
    assert any("others size subscription failed"    in m for m in msgs)


# ---- runtime callback --------------------------------------------------------

def test_callback_writes_correct_table_struct():
    """Each PLC notification must look up in the matching table and write."""
    pub, sizes, plc = _make()
    captured: dict[str, callable] = {}
    def fake_subscribe(alias, cb, **_):
        captured[alias] = cb
        return ("n", "u")
    plc.subscribe.side_effect = fake_subscribe

    pub.start()
    try:
        _drain(pub)                                  # initial both 0 → no-ops
        plc.reset_mock()
        sizes.get.return_value = _size(name="X", width_mm=500, length_mm=700)

        captured["size.cardboard_id"]("size.cardboard_id", 5)
        _drain(pub)
        sizes.get.assert_called_with("cardboard", 5)
        alias, struct = plc.write.call_args.args
        assert alias == "size.cardboard_setpoints"
        assert struct["nWidthMm"]  == 500
        assert struct["nLengthMm"] == 700

        plc.reset_mock()
        captured["size.others_id"]("size.others_id", 9)
        _drain(pub)
        sizes.get.assert_called_with("others", 9)
        alias, _ = plc.write.call_args.args
        assert alias == "size.others_setpoints"
    finally:
        pub.stop()


def test_callback_unknown_id_warns(caplog):
    pub, sizes, plc = _make()
    captured = {}
    plc.subscribe.side_effect = (
        lambda alias, cb, **_: captured.setdefault(alias, cb) or ("n", "u")
    )
    pub.start()
    try:
        _drain(pub)
        plc.reset_mock()
        sizes.get.return_value = None

        import logging
        with caplog.at_level(logging.WARNING, logger="sizes_publisher"):
            captured["size.others_id"]("size.others_id", 999)
            _drain(pub)

        plc.write.assert_not_called()
        assert any("others size id 999 not found" in r.message for r in caplog.records)
    finally:
        pub.stop()


def test_callback_write_failure_swallowed():
    pub, sizes, plc = _make()
    captured = {}
    plc.subscribe.side_effect = (
        lambda alias, cb, **_: captured.setdefault(alias, cb) or ("n", "u")
    )
    pub.start()
    try:
        sizes.get.return_value = _size()
        plc.write.side_effect = RuntimeError("ADS gone")
        captured["size.cardboard_id"]("size.cardboard_id", 1)    # must not raise
        _drain(pub)
        # Worker must survive a write failure.
        assert pub._worker is not None and pub._worker.is_alive()
    finally:
        pub.stop()


# ---- stop --------------------------------------------------------------------

def test_stop_unsubscribes_all_handles():
    pub, sizes, plc = _make()
    plc.subscribe.side_effect = [("a", "1"), ("b", "2")]
    pub.start()
    pub.stop()

    assert plc.unsubscribe.call_count == 2
    assert pub._handles == []


def test_stop_idempotent():
    pub, sizes, plc = _make()
    pub.start()
    pub.stop()
    pub.stop()                                      # must not raise


def test_stop_joins_worker():
    pub, sizes, plc = _make()
    pub.start()
    assert pub._worker is not None
    pub.stop()
    assert pub._worker is None


def test_empty_mappings_means_no_validate_or_subscribe():
    sizes = MagicMock()
    plc   = MagicMock()
    pub = SizesPublisher(sizes, plc, [])
    pub.start()
    try:
        plc.validate.assert_called_once_with([])
        plc.subscribe.assert_not_called()
    finally:
        pub.stop()
