"""Tests for sick_publisher translation + lifecycle."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sick_publisher import (
    PublisherConfig,
    SickPublisher,
    _event_to_struct,
    _measurement_to_struct,
)


# ---- pure translation ----

def test_measurement_to_struct_rounds_metres_to_mm():
    m = {"width": 0.711, "height": 1.8005, "offset": -0.012}
    out = _measurement_to_struct(m)
    assert out == {"nWidth": 711, "nHeight": 1800, "nOffset": -12}


def test_measurement_to_struct_handles_zero():
    out = _measurement_to_struct({"width": 0.0, "height": 0.0, "offset": 0.0})
    assert out == {"nWidth": 0, "nHeight": 0, "nOffset": 0}


def test_event_to_struct_uses_pysickudt_attrs():
    """Mock UnitEvent: every named field translates field-by-field."""
    from pysickudt import UnitEvent
    ev = UnitEvent(
        length_m=2.5,
        width_mean_m=0.711, width_min_m=0.700, width_max_m=0.722,
        height_mean_m=1.800, height_min_m=1.795, height_max_m=1.812,
        offset_mean_m=0.012, offset_min_m=-0.001, offset_max_m=0.024,
        duration_s=1.234, n_samples=42,
    )
    out = _event_to_struct(ev)
    assert out["bNew"] is True
    assert out["nLength"]     == 2500
    assert out["nWidthMean"]  == 711
    assert out["nWidthMin"]   == 700
    assert out["nWidthMax"]   == 722
    assert out["nHeightMean"] == 1800
    assert out["nOffsetMin"]  == -1
    assert out["fDuration"]   == pytest.approx(1.234)
    assert out["nSamples"]    == 42


# ---- start() validation + wiring ----

def _make_publisher():
    bridge = MagicMock()
    plc = MagicMock()
    plc.read.return_value = True
    plc.subscribe.return_value = (1, 2)

    cfg = PublisherConfig(
        event_alias="sick.event",
        live_alias="sick.live",
        enable_alias="sick.enable",
    )
    return SickPublisher(bridge, plc, cfg), bridge, plc


def test_start_validates_aliases():
    pub, bridge, plc = _make_publisher()
    pub.start()
    plc.validate.assert_called_once_with([
        "sick.live", "sick.event", "sick.enable",
    ])


def test_start_subscribes_to_bridge_and_plc():
    pub, bridge, plc = _make_publisher()
    pub.start()
    bridge.on_measurement.assert_called_once()
    bridge.on_event.assert_called_once()
    plc.subscribe.assert_called_once()


def test_start_applies_initial_enable_true():
    pub, bridge, plc = _make_publisher()
    plc.read.return_value = True
    pub.start()
    bridge.enable.assert_called_once()
    bridge.disable.assert_not_called()


def test_start_applies_initial_enable_false():
    pub, bridge, plc = _make_publisher()
    plc.read.return_value = False
    pub.start()
    bridge.disable.assert_called_once()
    bridge.enable.assert_not_called()


def test_start_continues_if_initial_read_fails():
    pub, bridge, plc = _make_publisher()
    plc.read.side_effect = RuntimeError("ADS down")
    pub.start()  # must not raise
    plc.subscribe.assert_called_once()


def test_stop_unsubscribes_everything():
    pub, bridge, plc = _make_publisher()
    unsub_meas = MagicMock()
    unsub_event = MagicMock()
    bridge.on_measurement.return_value = unsub_meas
    bridge.on_event.return_value = unsub_event
    pub.start()
    pub.stop()
    plc.unsubscribe.assert_called_once_with((1, 2))
    unsub_meas.assert_called_once()
    unsub_event.assert_called_once()


def test_measurement_write_routes_through_plc():
    pub, bridge, plc = _make_publisher()
    pub.start()
    pub._on_measurement({"width": 0.7, "height": 1.8, "offset": 0.0})
    plc.write.assert_called_with(
        "sick.live", {"nWidth": 700, "nHeight": 1800, "nOffset": 0},
    )


def test_measurement_write_swallows_errors():
    """A PLC write failure must not propagate into the SICK callback thread."""
    pub, bridge, plc = _make_publisher()
    plc.write.side_effect = RuntimeError("disconnected")
    pub.start()
    pub._on_measurement({"width": 0.5, "height": 1.0, "offset": 0.0})  # no raise
