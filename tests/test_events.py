"""Smoke tests: every event payload type has a matching signal and round-trips."""
from __future__ import annotations

import dataclasses

import pytest

from events import (
    PlcSignalChanged, SickMeasurement, SickUnitEvent,
    RobotStatusChanged, RobotVarChanged, RobotErrorLogged, MasterArrayChanged,
    CameraTriggered, CameraSnapshotTaken,
    RecipeCodeChanged, RecipeSetpointsPushed,
    SizesChanged,
    TOPICS, signals,
)
from event_bus import EventBus


SAMPLE_PAYLOADS = {
    PlcSignalChanged:       PlcSignalChanged(alias="x", value=1, ts=0.0),
    SickMeasurement:        SickMeasurement(width_m=0.5, height_m=0.2, offset_m=0.0, ts=0.0),
    SickUnitEvent:          SickUnitEvent(event=object()),
    RobotStatusChanged:     RobotStatusChanged(status=object()),
    RobotVarChanged:        RobotVarChanged(alias="v", value=1, prev=0),
    RobotErrorLogged:       RobotErrorLogged(entry={"seq": 1}),
    MasterArrayChanged:     MasterArrayChanged(slots=[]),
    CameraTriggered:        CameraTriggered(slot="entry", edge_ts=0.0),
    CameraSnapshotTaken:    CameraSnapshotTaken(slot="entry", path="/tmp/x.jpg", ts=0.0),
    RecipeCodeChanged:      RecipeCodeChanged(code=10, prev=9),
    RecipeSetpointsPushed:  RecipeSetpointsPushed(code=10, values={}),
    SizesChanged:           SizesChanged(slot=0, op="upsert", payload={}),
}


def test_topics_covers_every_payload_type():
    """Every dataclass in this module is registered in TOPICS."""
    assert set(TOPICS.keys()) == set(SAMPLE_PAYLOADS.keys())


def test_payloads_are_frozen_dataclasses():
    for cls in TOPICS:
        assert dataclasses.is_dataclass(cls), cls
        assert cls.__dataclass_params__.frozen, f"{cls.__name__} should be frozen"


@pytest.mark.parametrize("cls,payload", list(SAMPLE_PAYLOADS.items()))
def test_signal_round_trip(cls, payload):
    """A subscriber on the signal for `cls` receives the exact payload."""
    bus = EventBus()
    sig = TOPICS[cls]
    received: list = []
    bus.subscribe(sig, lambda p: received.append(p), mode="sync")
    bus.publish(sig, payload)
    assert received == [payload]
    bus.stop()


def test_signals_object_exposes_all_topics():
    """Every signal in TOPICS is reachable as an attribute on `signals`."""
    exposed = {getattr(signals, n) for n in dir(signals) if not n.startswith("_")}
    for sig in TOPICS.values():
        assert sig in exposed
