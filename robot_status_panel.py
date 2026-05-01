"""robot_status_panel.py — read-only NiceGUI viewer for RobotStatusLog.

Inner tabs:
  * History         — paginated raw rows with opmode + source filters
  * Time in state   — opmode minutes over a window selector
  * Transitions     — chronological change-rows
  * Daily summary   — per-day minutes table for the last N days

All NiceGUI imports stay inside this module.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from nicegui import ui

from robot_status_log import RobotStatusLog
from theme            import card

log = logging.getLogger(__name__)

_REFRESH_INTERVAL_S = 5.0
_HISTORY_LIMIT      = 200
_TRANSITIONS_LIMIT  = 500
_OPMODES_FILTER     = ("all", "AUTO", "MANR", "MANF", "unknown")
_SOURCES_FILTER     = ("all", "tick", "change")
_WINDOWS = (
    ("Last 1h",  3600),
    ("Last 8h",  8 * 3600),
    ("Last 24h", 24 * 3600),
    ("Last 7d",  7 * 24 * 3600),
)
_OPMODE_COLORS = {
    "AUTO":     "bg-green-50",
    "MANR":     "bg-yellow-50",
    "MANF":     "bg-orange-50",
    "unknown":  "bg-gray-50",
}


class RobotStatusPanel:
    """Live robot-status log viewer (read-only)."""

    def __init__(self, store: RobotStatusLog):
        self.store = store
        self._opmode = "all"
        self._source = "all"
        self._window_s = 24 * 3600
        self._summary_days = 14
        self._history_box:     ui.column | None = None
        self._timeinstate_box: ui.column | None = None
        self._transitions_box: ui.column | None = None
        self._summary_box:     ui.column | None = None
        self._timer: ui.timer | None = None

    # ---- mount --------------------------------------------------------------

    def mount(self) -> None:
        with ui.column().classes("w-full gap-4 p-6"):
            with ui.tabs().props("dense").classes("text-[#0053A1]") as tabs:
                t_hist  = ui.tab("History",       icon="list")
                t_state = ui.tab("Time in state", icon="pie_chart")
                t_trans = ui.tab("Transitions",   icon="timeline")
                t_sum   = ui.tab("Daily summary", icon="calendar_month")
            with ui.tab_panels(tabs, value=t_hist).classes(
                "w-full bg-transparent"
            ):
                with ui.tab_panel(t_hist).classes("p-0"):
                    self._render_filters_history()
                    self._history_box = ui.column().classes("w-full gap-1")
                with ui.tab_panel(t_state).classes("p-0"):
                    self._render_filters_window()
                    self._timeinstate_box = ui.column().classes("w-full gap-1")
                with ui.tab_panel(t_trans).classes("p-0"):
                    self._transitions_box = ui.column().classes("w-full gap-1")
                with ui.tab_panel(t_sum).classes("p-0"):
                    self._render_filters_summary()
                    self._summary_box = ui.column().classes("w-full gap-1")

            self._timer = ui.timer(_REFRESH_INTERVAL_S, self._refresh_all)
        self._refresh_all()

    # ---- filter rows --------------------------------------------------------

    def _render_filters_history(self) -> None:
        with card():
            with ui.row().classes("items-center gap-3 w-full"):
                ui.select(
                    list(_OPMODES_FILTER),
                    value=self._opmode,
                    label="Opmode",
                    on_change=lambda e: self._set_opmode(e.value),
                ).classes("w-40")
                ui.select(
                    list(_SOURCES_FILTER),
                    value=self._source,
                    label="Source",
                    on_change=lambda e: self._set_source(e.value),
                ).classes("w-40")
                ui.space()
                ui.button(
                    "Refresh", icon="refresh",
                    on_click=self._refresh_all,
                ).props("flat color=primary")

    def _render_filters_window(self) -> None:
        with card():
            with ui.row().classes("items-center gap-3 w-full"):
                ui.select(
                    {sec: lbl for lbl, sec in _WINDOWS},
                    value=self._window_s,
                    label="Window",
                    on_change=lambda e: self._set_window(int(e.value)),
                ).classes("w-40")
                ui.space()
                ui.button(
                    "Refresh", icon="refresh",
                    on_click=self._refresh_all,
                ).props("flat color=primary")

    def _render_filters_summary(self) -> None:
        with card():
            with ui.row().classes("items-center gap-3 w-full"):
                ui.number(
                    label="Days",
                    value=self._summary_days, min=1, max=90, step=1,
                    on_change=lambda e: self._set_summary_days(int(e.value or 14)),
                ).classes("w-32")
                ui.space()
                ui.button(
                    "Refresh", icon="refresh",
                    on_click=self._refresh_all,
                ).props("flat color=primary")

    def _set_opmode(self, v: str) -> None:
        self._opmode = v or "all"; self._refresh_all()

    def _set_source(self, v: str) -> None:
        self._source = v or "all"; self._refresh_all()

    def _set_window(self, v: int) -> None:
        self._window_s = int(v); self._refresh_all()

    def _set_summary_days(self, v: int) -> None:
        self._summary_days = max(1, int(v)); self._refresh_all()

    # ---- refresh ------------------------------------------------------------

    def _refresh_all(self) -> None:
        self._refresh_history()
        self._refresh_time_in_state()
        self._refresh_transitions()
        self._refresh_summary()

    def _since_iso(self) -> str:
        return (datetime.now(timezone.utc).replace(tzinfo=None)
                - timedelta(seconds=self._window_s)).strftime("%Y-%m-%d %H:%M:%S")

    # ---- history ------------------------------------------------------------

    def _refresh_history(self) -> None:
        box = self._history_box
        if box is None:
            return
        try:
            opmode = None if self._opmode == "all" else self._opmode
            source = None if self._source == "all" else self._source
            rows = self.store.recent(
                limit=_HISTORY_LIMIT, opmode=opmode, source=source,
            )
        except Exception as e:
            box.clear()
            with box:
                ui.label(f"Failed to load history: {e}").classes("text-red-500")
            return

        box.clear()
        with box:
            self._history_header()
            if not rows:
                ui.label("No samples logged yet.").classes(
                    "text-gray-500 italic px-3 py-4"
                )
                return
            for r in rows:
                self._history_row(r)

    def _history_header(self) -> None:
        with ui.row().classes(
            "w-full items-center gap-3 px-3 py-2 text-xs font-semibold "
            "uppercase tracking-wider text-gray-500 border-b border-[#E5E9EE]"
        ):
            ui.label("Time").classes("w-44")
            ui.label("Opmode").classes("w-20")
            ui.label("Ctrl").classes("w-28")
            ui.label("Exec").classes("w-24")
            ui.label("Speed").classes("w-16 text-right")
            ui.label("Ready").classes("w-16 text-center")
            ui.label("Source").classes("w-20")

    def _history_row(self, r: dict) -> None:
        tint = _OPMODE_COLORS.get(r.get("opmode") or "", "")
        with ui.row().classes(
            f"w-full items-center gap-3 px-3 py-1.5 text-sm "
            f"border-b border-[#E5E9EE] {tint}"
        ):
            ui.label(r.get("ts") or "").classes("w-44 text-gray-700 font-mono text-xs")
            ui.label(r.get("opmode") or "").classes("w-20 font-semibold")
            ui.label(r.get("ctrl_state") or "").classes("w-28 font-mono text-xs")
            ui.label(r.get("exec_state") or "").classes("w-24 font-mono text-xs")
            ui.label(f"{int(r.get('speed_ratio') or 0)}%").classes(
                "w-16 text-right font-mono"
            )
            ui.label("✓" if r.get("is_ready") else "·").classes(
                "w-16 text-center "
                + ("text-green-700 font-semibold" if r.get("is_ready")
                   else "text-gray-400")
            )
            ui.label(r.get("source") or "").classes("w-20 text-xs text-gray-500")

    # ---- time in state ------------------------------------------------------

    def _refresh_time_in_state(self) -> None:
        box = self._timeinstate_box
        if box is None:
            return
        try:
            rows = self.store.time_in_state(self._since_iso())
        except Exception as e:
            box.clear()
            with box:
                ui.label(f"Failed to load: {e}").classes("text-red-500")
            return

        box.clear()
        with box:
            if not rows:
                ui.label("No data in window.").classes(
                    "text-gray-500 italic px-3 py-4"
                )
                return

            total = max(sum(r.get("seconds") or 0 for r in rows), 1.0)
            with ui.column().classes("w-full gap-2 p-2"):
                for r in rows:
                    secs = float(r.get("seconds") or 0)
                    pct  = secs / total * 100.0
                    label = r.get("opmode") or "unknown"
                    with ui.row().classes("w-full items-center gap-3"):
                        ui.label(label).classes(
                            "w-24 font-semibold text-sm"
                        )
                        with ui.element("div").classes(
                            "flex-grow h-5 rounded bg-[#E5E9EE] overflow-hidden"
                        ):
                            tint = _OPMODE_COLORS.get(label, "bg-blue-200")
                            ui.element("div").classes(
                                f"h-full {tint}"
                            ).style(
                                f"width: {pct:.1f}%; "
                                f"background-color: #0053A1; opacity: 0.85;"
                            )
                        ui.label(f"{_fmt_dur(secs)}  ({pct:.1f}%)").classes(
                            "w-44 text-right font-mono text-xs"
                        )

    # ---- transitions --------------------------------------------------------

    def _refresh_transitions(self) -> None:
        box = self._transitions_box
        if box is None:
            return
        try:
            rows = self.store.transitions(
                self._since_iso(), limit=_TRANSITIONS_LIMIT,
            )
        except Exception as e:
            box.clear()
            with box:
                ui.label(f"Failed to load: {e}").classes("text-red-500")
            return

        box.clear()
        with box:
            with ui.row().classes(
                "w-full items-center gap-3 px-3 py-2 text-xs font-semibold "
                "uppercase tracking-wider text-gray-500 border-b border-[#E5E9EE]"
            ):
                ui.label("Time").classes("w-44")
                ui.label("Opmode").classes("w-20")
                ui.label("Ctrl").classes("w-28")
                ui.label("Exec").classes("w-24")
                ui.label("Speed").classes("w-16 text-right")
            if not rows:
                ui.label("No transitions in window.").classes(
                    "text-gray-500 italic px-3 py-4"
                )
                return
            for r in rows:
                tint = _OPMODE_COLORS.get(r.get("opmode") or "", "")
                with ui.row().classes(
                    f"w-full items-center gap-3 px-3 py-1.5 text-sm "
                    f"border-b border-[#E5E9EE] {tint}"
                ):
                    ui.label(r.get("ts") or "").classes(
                        "w-44 text-gray-700 font-mono text-xs"
                    )
                    ui.label(r.get("opmode") or "").classes(
                        "w-20 font-semibold"
                    )
                    ui.label(r.get("ctrl_state") or "").classes(
                        "w-28 font-mono text-xs"
                    )
                    ui.label(r.get("exec_state") or "").classes(
                        "w-24 font-mono text-xs"
                    )
                    ui.label(f"{int(r.get('speed_ratio') or 0)}%").classes(
                        "w-16 text-right font-mono"
                    )

    # ---- daily summary ------------------------------------------------------

    def _refresh_summary(self) -> None:
        box = self._summary_box
        if box is None:
            return
        try:
            rows = self.store.daily_summary(days=self._summary_days)
        except Exception as e:
            box.clear()
            with box:
                ui.label(f"Failed to load: {e}").classes("text-red-500")
            return

        box.clear()
        with box:
            with ui.row().classes(
                "w-full items-center gap-3 px-3 py-2 text-xs font-semibold "
                "uppercase tracking-wider text-gray-500 border-b border-[#E5E9EE]"
            ):
                ui.label("Day").classes("w-28")
                ui.label("AUTO").classes("w-24 text-right")
                ui.label("MANR").classes("w-24 text-right")
                ui.label("MANF").classes("w-24 text-right")
                ui.label("Running").classes("w-24 text-right")
                ui.label("Motors on").classes("w-24 text-right")
                ui.label("Avg speed").classes("w-24 text-right")
                ui.label("Stops").classes("w-16 text-right")
            if not rows:
                ui.label("No data.").classes(
                    "text-gray-500 italic px-3 py-4"
                )
                return
            for r in rows:
                with ui.row().classes(
                    "w-full items-center gap-3 px-3 py-1.5 text-sm "
                    "border-b border-[#E5E9EE]"
                ):
                    ui.label(r.get("day") or "").classes(
                        "w-28 font-mono text-xs"
                    )
                    ui.label(_fmt_min(r.get("auto_minutes"))).classes(
                        "w-24 text-right font-mono text-xs"
                    )
                    ui.label(_fmt_min(r.get("manr_minutes"))).classes(
                        "w-24 text-right font-mono text-xs"
                    )
                    ui.label(_fmt_min(r.get("manf_minutes"))).classes(
                        "w-24 text-right font-mono text-xs"
                    )
                    ui.label(_fmt_min(r.get("running_minutes"))).classes(
                        "w-24 text-right font-mono text-xs"
                    )
                    ui.label(_fmt_min(r.get("motors_on_minutes"))).classes(
                        "w-24 text-right font-mono text-xs"
                    )
                    ui.label(f"{r.get('avg_speed_ratio') or 0:.0f}%").classes(
                        "w-24 text-right font-mono text-xs"
                    )
                    ui.label(str(r.get("num_stops") or 0)).classes(
                        "w-16 text-right font-mono text-xs"
                    )


def _fmt_dur(seconds: float) -> str:
    s = int(round(seconds))
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def _fmt_min(minutes) -> str:
    if minutes is None:
        return "—"
    m = float(minutes)
    if m >= 60:
        return f"{m / 60:.1f}h"
    return f"{m:.0f}m"
