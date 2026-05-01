"""events.py — typed event payloads and named blinker signals.

Single source of truth for what kinds of events flow through the internal
EventBus (event_bus.py). Each event has:

* a frozen `@dataclass` payload — named fields, no dicts, fails loudly on
  shape drift between publishers and subscribers.
* a `blinker.Signal` named after the event's topic. Publishers do
  `bus.publish(signals.<name>, Payload(...))`; subscribers do
  `bus.subscribe(signals.<name>, handler, mode=...)`.

Topic naming follows `<source>_<verb>` so signals sort visually by source
when introspecting (`plc_*`, `sick_*`, `robot_*`, `camera_*`, `recipe_*`,
`sizes_*`, `master_*`).

This module deliberately knows nothing about the bus or threading model —
it's just data classes and signal handles. event_bus.py owns the dispatch
strategy.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from blinker import Signal


# ─── PLC ──────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class PlcSignalChanged:
    """A subscribed PLC alias (twincat_comm) reported a new value."""
    alias: str
    value: Any
    ts:    float


# ─── SICK scanner ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SickMeasurement:
    """Live geometric measurement frame from the SICK twin scanner."""
    width_m:  float
    height_m: float
    offset_m: float
    ts:       float


@dataclass(frozen=True)
class SickUnitEvent:
    """Completed unit traversal from pysickudt's UnitTracker.

    `event` is the raw `pysickudt.UnitEvent` dataclass — kept opaque so
    this module doesn't import pysickudt. Subscribers that need fields
    access them via attribute names (entered_at, length_m, …)."""
    event: Any


# ─── Robot status / vars / errors / master ───────────────────────────────────

@dataclass(frozen=True)
class RobotStatusChanged:
    """RobotMonitor detected a controller-state change."""
    status: Any   # robot_status.RobotStatus


@dataclass(frozen=True)
class RobotVarChanged:
    """A RAPID symbol monitored by RobotVariablesMonitor changed value."""
    alias: str
    value: Any
    prev:  Any


@dataclass(frozen=True)
class RobotErrorLogged:
    """A new entry was fetched from the controller event log."""
    entry: Mapping[str, Any]   # {seq, type, code, ts, title, desc, action}


@dataclass(frozen=True)
class MasterArrayChanged:
    """RobotMaster polled the controller and saw a new slot configuration."""
    slots: Sequence[Mapping[str, Any]]


# ─── Camera ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class CameraTriggered:
    """A PLC trigger bool went high; a snapshot is being requested."""
    slot:    str   # 'entry' / 'exit'
    edge_ts: float


@dataclass(frozen=True)
class CameraSnapshotTaken:
    """A snapshot completed and was written to disk (or archived)."""
    slot: str
    path: str
    ts:   float


# ─── Recipe ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class RecipeCodeChanged:
    """The PLC's active recipe code changed."""
    code: int
    prev: int | None


@dataclass(frozen=True)
class RecipeSetpointsPushed:
    """RecipePublisher wrote a new setpoint struct to the PLC."""
    code:   int
    values: Mapping[str, Any]


# ─── Sizes ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SizesChanged:
    """A row in the sizes store was inserted, updated, or cleared.

    `op` is one of 'upsert' | 'clear' | 'reload'. `payload` is the row
    (or None for clear)."""
    slot:    int
    op:      str
    payload: Mapping[str, Any] | None


# ─── Signals ──────────────────────────────────────────────────────────────────
# One blinker.Signal per payload type. The signal name matches the topic.

class _Signals:
    plc_signal_changed     = Signal("plc_signal_changed")
    sick_measurement       = Signal("sick_measurement")
    sick_unit_event        = Signal("sick_unit_event")
    robot_status_changed   = Signal("robot_status_changed")
    robot_var_changed      = Signal("robot_var_changed")
    robot_error_logged     = Signal("robot_error_logged")
    master_array_changed   = Signal("master_array_changed")
    camera_triggered       = Signal("camera_triggered")
    camera_snapshot_taken  = Signal("camera_snapshot_taken")
    recipe_code_changed    = Signal("recipe_code_changed")
    recipe_setpoints_pushed = Signal("recipe_setpoints_pushed")
    sizes_changed          = Signal("sizes_changed")


signals = _Signals()


# Pairing between payload class and signal — useful for tests and
# introspection (e.g. logging every event type uniformly).
TOPICS: dict[type, Signal] = {
    PlcSignalChanged:       signals.plc_signal_changed,
    SickMeasurement:        signals.sick_measurement,
    SickUnitEvent:          signals.sick_unit_event,
    RobotStatusChanged:     signals.robot_status_changed,
    RobotVarChanged:        signals.robot_var_changed,
    RobotErrorLogged:       signals.robot_error_logged,
    MasterArrayChanged:     signals.master_array_changed,
    CameraTriggered:        signals.camera_triggered,
    CameraSnapshotTaken:    signals.camera_snapshot_taken,
    RecipeCodeChanged:      signals.recipe_code_changed,
    RecipeSetpointsPushed:  signals.recipe_setpoints_pushed,
    SizesChanged:           signals.sizes_changed,
}
