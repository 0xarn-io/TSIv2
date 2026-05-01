"""Tests for CameraPublisher: PLC bool → camera trigger wiring."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from camera_publisher import CameraPublisher, CameraTriggerConfig


def _make(trigger_count: int = 2):
    cameras = MagicMock()
    panels = {"entry": MagicMock(), "exit": MagicMock()}
    for p in panels.values():
        p.on_snapshot_done.return_value = MagicMock()  # unsub fn
    cameras.panels = panels
    plc = MagicMock()
    plc.subscribe.side_effect = lambda *_a, **_kw: (10, 20)

    triggers = [
        CameraTriggerConfig(alias="camera.snap_entry", camera="entry"),
        CameraTriggerConfig(alias="camera.snap_exit",  camera="exit"),
    ][:trigger_count]
    return CameraPublisher(cameras, plc, triggers), cameras, plc


def test_start_validates_aliases():
    pub, cameras, plc = _make()
    pub.start()
    plc.validate.assert_called_once_with([
        "camera.snap_entry", "camera.snap_exit",
    ])


def test_start_subscribes_each_trigger():
    pub, cameras, plc = _make()
    pub.start()
    assert plc.subscribe.call_count == 2


def test_start_rejects_unknown_camera():
    cameras = MagicMock()
    cameras.panels = {"entry": MagicMock()}   # no "exit"
    plc = MagicMock()
    pub = CameraPublisher(cameras, plc, [
        CameraTriggerConfig(alias="camera.snap_exit", camera="exit"),
    ])
    with pytest.raises(KeyError, match="unknown camera"):
        pub.start()


def test_rising_edge_fires_camera_snap():
    """The PLC callback should call cameras.snap(name, source=...) when val=True."""
    pub, cameras, plc = _make()
    captured_callbacks = []
    plc.subscribe.side_effect = lambda alias, cb, **_kw: (
        captured_callbacks.append((alias, cb)) or (1, 2)
    )
    pub.start()

    # Find the entry callback and fire it with True
    entry_cb = next(cb for a, cb in captured_callbacks if a == "camera.snap_entry")
    entry_cb("camera.snap_entry", True)
    cameras.snap.assert_called_with("entry", source="trigger:entry")


def test_falling_edge_does_nothing():
    """val=False must NOT trigger a snapshot."""
    pub, cameras, plc = _make()
    captured = []
    plc.subscribe.side_effect = lambda alias, cb, **_kw: (
        captured.append((alias, cb)) or (1, 2)
    )
    pub.start()

    entry_cb = next(cb for a, cb in captured if a == "camera.snap_entry")
    entry_cb("camera.snap_entry", False)
    cameras.snap.assert_not_called()


def test_each_callback_binds_its_own_camera():
    """Loop closure regression: each subscription must trigger its own camera."""
    pub, cameras, plc = _make()
    captured = []
    plc.subscribe.side_effect = lambda alias, cb, **_kw: (
        captured.append((alias, cb)) or (len(captured), len(captured))
    )
    pub.start()

    for alias, cb in captured:
        cb(alias, True)

    # Both cameras should have been triggered exactly once each.
    triggered = [c.args[0] for c in cameras.snap.call_args_list]
    assert sorted(triggered) == ["entry", "exit"]


def test_stop_unsubscribes_all():
    pub, cameras, plc = _make()
    plc.subscribe.side_effect = [(1, 2), (3, 4)]
    pub.start()
    pub.stop()
    assert plc.unsubscribe.call_count == 2
    assert pub._handles == []


def test_stop_swallows_unsubscribe_errors():
    pub, cameras, plc = _make()
    plc.subscribe.side_effect = [(1, 2), (3, 4)]
    plc.unsubscribe.side_effect = RuntimeError("ADS gone")
    pub.start()
    pub.stop()  # must not raise
    assert pub._handles == []


def test_empty_triggers_list_is_a_noop():
    pub, cameras, plc = _make(trigger_count=0)
    pub.start()
    plc.validate.assert_called_once_with([])
    plc.subscribe.assert_not_called()


# ---- ack handshake (snapshot_done → plc.write(alias, False)) ----

def test_start_registers_done_callback_per_trigger():
    pub, cameras, plc = _make()
    pub.start()
    cameras.panels["entry"].on_snapshot_done.assert_called_once()
    cameras.panels["exit"].on_snapshot_done.assert_called_once()


def test_snapshot_done_writes_false_to_plc():
    pub, cameras, plc = _make()
    captured = {}
    cameras.panels["entry"].on_snapshot_done.side_effect = (
        lambda cb: captured.setdefault("entry", cb) or MagicMock()
    )
    cameras.panels["exit"].on_snapshot_done.side_effect = (
        lambda cb: captured.setdefault("exit", cb) or MagicMock()
    )
    pub.start()

    # Simulate the entry panel finishing a snapshot.
    captured["entry"]("entry", "trigger:entry", True, None)
    plc.write.assert_called_once_with("camera.snap_entry", False)


def test_each_done_cb_acks_its_own_alias():
    """Loop closure regression: each panel's done cb must clear its own alias."""
    pub, cameras, plc = _make()
    captured = {}
    cameras.panels["entry"].on_snapshot_done.side_effect = (
        lambda cb: captured.setdefault("entry", cb) or MagicMock()
    )
    cameras.panels["exit"].on_snapshot_done.side_effect = (
        lambda cb: captured.setdefault("exit", cb) or MagicMock()
    )
    pub.start()

    captured["entry"]("entry", "x", True, "/snaps/entry.jpg")
    captured["exit"]("exit",   "x", True, None)

    written = [c.args for c in plc.write.call_args_list]
    assert ("camera.snap_entry", False) in written
    assert ("camera.snap_exit",  False) in written


def test_ack_swallows_write_errors():
    pub, cameras, plc = _make()
    captured = {}
    cameras.panels["entry"].on_snapshot_done.side_effect = (
        lambda cb: captured.setdefault("entry", cb) or MagicMock()
    )
    cameras.panels["exit"].on_snapshot_done.side_effect = (
        lambda cb: captured.setdefault("exit", cb) or MagicMock()
    )
    pub.start()
    plc.write.side_effect = RuntimeError("ADS down")

    # Must not raise — failed acks shouldn't tear down the snapshot pipeline.
    captured["entry"]("entry", "x", True, None)


def test_stop_removes_done_callbacks():
    pub, cameras, plc = _make()
    unsubs = [MagicMock(), MagicMock()]
    cameras.panels["entry"].on_snapshot_done.return_value = unsubs[0]
    cameras.panels["exit"].on_snapshot_done.return_value = unsubs[1]
    pub.start()
    pub.stop()
    unsubs[0].assert_called_once()
    unsubs[1].assert_called_once()


# ---- bus mode --------------------------------------------------------------

def test_bus_mode_subscribes_via_bus_not_plc():
    """When a bus is supplied, camera_publisher subscribes to
    PlcSignalChanged on the bus instead of plc.subscribe()."""
    import asyncio, time
    from event_bus import EventBus
    from events import PlcSignalChanged, signals

    bus = EventBus()
    loop = asyncio.new_event_loop()
    bus.start(loop)
    try:
        cameras = MagicMock()
        cameras.panels = {"entry": MagicMock(), "exit": MagicMock()}
        for p in cameras.panels.values():
            p.on_snapshot_done.return_value = MagicMock()
        plc = MagicMock()
        triggers = [
            CameraTriggerConfig(alias="camera.snap_entry", camera="entry"),
            CameraTriggerConfig(alias="camera.snap_exit",  camera="exit"),
        ]
        pub = CameraPublisher(cameras, plc, triggers, bus=bus)
        pub.start()
        try:
            plc.subscribe.assert_not_called()

            # Rising edge on entry → cameras.snap("entry", ...)
            bus.publish(signals.plc_signal_changed,
                        PlcSignalChanged(alias="camera.snap_entry",
                                         value=True, ts=0.0))
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and not cameras.snap.called:
                time.sleep(0.005)
            cameras.snap.assert_called_with("entry", source="trigger:entry")

            # Falling edge: no new snap.
            cameras.snap.reset_mock()
            bus.publish(signals.plc_signal_changed,
                        PlcSignalChanged(alias="camera.snap_entry",
                                         value=False, ts=0.0))
            time.sleep(0.05)
            cameras.snap.assert_not_called()
        finally:
            pub.stop()
    finally:
        bus.stop()
        loop.close()
