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

Source-of-truth rule: each topic has exactly ONE publisher class — see
`OWNERS` below. Subscribers must NEVER publish to a topic they don't own;
that's how loop-backs sneak in. When a subscriber needs to react to a
change but mustn't echo back, it filters by `payload.origin` against its
own publisher tag.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Mapping, Sequence

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
    """A row in the sizes store was inserted, updated, or deleted.

    `op` matches the SizesStore vocabulary: 'add' | 'update' | 'delete'.
    `payload` is the row dict (or None for delete). `slot` is the row's
    pinned slot when known, else -1. `origin` tags who triggered the
    change so subscribers can self-filter — e.g. `robot_master` writes
    rows tagged 'robot_master' and ignores those events to avoid echoing
    the change back to the controller.
    """
    slot:    int
    op:      Literal["add", "update", "delete"]
    payload: Mapping[str, Any] | None
    origin:  str = "user"


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


# Source-of-truth ownership: which class is allowed to publish each
# signal. Anything else doing so is a refactor mistake — subscribers
# never publish to topics they don't own (that's how loop-backs sneak in).
# Strings (not classes) keep the dict importable without circular deps.
OWNERS: dict[Signal, str] = {
    signals.plc_signal_changed:      "twincat_comm.TwinCATComm",
    signals.sick_measurement:        "sick_bridge.SickBridge",
    signals.sick_unit_event:         "sick_bridge.SickBridge",
    signals.robot_status_changed:    "robot_status.RobotMonitor",
    signals.robot_var_changed:       "robot_variables.RobotVariablesMonitor",
    signals.robot_error_logged:      "robot_errors.RobotElogPoller",
    signals.master_array_changed:    "robot_master.RobotMasterMonitor",
    signals.camera_triggered:        "camera_publisher.CameraPublisher",
    signals.camera_snapshot_taken:   "camera_panel.CameraPanel",
    signals.recipe_code_changed:     "recipe_publisher.RecipePublisher",
    signals.recipe_setpoints_pushed: "recipe_publisher.RecipePublisher",
    signals.sizes_changed:           "sizes_store.SizesStore",
}
