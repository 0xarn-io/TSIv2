"""robot_status_panel.py — read-only NiceGUI viewer for RobotStatusLog.

Inner tabs:
  * History         — paginated raw rows with opmode + source filters
  * Time in state   — opmode minutes over a window selector
  * Transitions     — chronological change-rows
  * Shifts          — per-shift (06–14 / 14–22 / 22–06) totals for a date

All NiceGUI imports stay inside this module.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone

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
_ESTOP_TINT = "bg-red-100"


def _row_tint(r: dict) -> str:
    """Estop wins over opmode — a green AUTO row should turn red on
    emergency stop, since the operational state is dominated by the
    safety condition, not the mode dial."""
    if "emergencystop" in (r.get("ctrl_state") or "").lower():
        return _ESTOP_TINT
    return _OPMODE_COLORS.get(r.get("opmode") or "", "")


class RobotStatusPanel:
    """Live robot-status log viewer (read-only)."""

    def __init__(self, store: RobotStatusLog):
        self.store = store
        self._opmode = "all"
        self._source = "all"
        self._window_s = 24 * 3600
        self._shift_date = date.today().isoformat()
        self._date_input: ui.input | None = None
        self._history_box:     ui.column | None = None
        self._timeinstate_box: ui.column | None = None
        self._transitions_box: ui.column | None = None
        self._shifts_box:      ui.column | None = None
        self._timer: ui.timer | None = None

    # ---- mount --------------------------------------------------------------

    def mount(self) -> None:
        with ui.column().classes("w-full gap-4 p-6"):
            with ui.tabs().props("dense").classes("text-[#0053A1]") as tabs:
                t_hist   = ui.tab("History",       icon="list")
                t_state  = ui.tab("Time in state", icon="pie_chart")
                t_trans  = ui.tab("Transitions",   icon="timeline")
                t_shifts = ui.tab("Shifts",        icon="schedule")
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
                with ui.tab_panel(t_shifts).classes("p-0"):
                    self._render_filters_shifts()
                    self._shifts_box = ui.column().classes("w-full gap-1")

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

    def _render_filters_shifts(self) -> None:
        with card():
            with ui.row().classes("items-center gap-3 w-full"):
                ui.button(icon="chevron_left",
                          on_click=lambda: self._shift_day(-1)
                          ).props("flat dense color=primary")
                inp = ui.input(
                    label="Date",
                    value=self._shift_date,
                    on_change=lambda e: self._set_shift_date(e.value or ""),
                ).classes("w-40")
                with inp.add_slot("append"):
                    ui.icon("event").classes("cursor-pointer").on(
                        "click",
                        lambda _e: menu.open(),  # noqa: F821 - menu defined below
                    )
                with ui.menu() as menu:
                    ui.date(
                        value=self._shift_date,
                        on_change=lambda e: self._set_shift_date(e.value or ""),
                    ).bind_value(inp)
                self._date_input = inp
                ui.button(icon="chevron_right",
                          on_click=lambda: self._shift_day(+1)
                          ).props("flat dense color=primary")
                ui.button("Today", icon="today",
                          on_click=lambda: self._set_shift_date(
                              date.today().isoformat()
                          )
                          ).props("flat dense color=primary")
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

    def _set_shift_date(self, v: str) -> None:
        v = (v or "").strip()
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            return
        self._shift_date = v
        if self._date_input is not None:
            self._date_input.value = v
        self._refresh_all()

    def _shift_day(self, delta_days: int) -> None:
        try:
            d = datetime.strptime(self._shift_date, "%Y-%m-%d").date()
        except ValueError:
            d = date.today()
        self._set_shift_date((d + timedelta(days=delta_days)).isoformat())

    # ---- refresh ------------------------------------------------------------

    def _refresh_all(self) -> None:
        self._refresh_history()
        self._refresh_time_in_state()
        self._refresh_transitions()
        self._refresh_shifts()

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
            ui.label("Bypass").classes("w-20 text-center")
            ui.label("Source").classes("w-20")

    def _history_row(self, r: dict) -> None:
        tint = _row_tint(r)
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
            byp = bool(r.get("bypass"))
            ui.label("BYPASS" if byp else "·").classes(
                "w-20 text-center text-xs "
                + ("text-orange-700 font-semibold" if byp
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
                tint = _row_tint(r)
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

    # ---- shifts -------------------------------------------------------------

    def _refresh_shifts(self) -> None:
        box = self._shifts_box
        if box is None:
            return
        try:
            rows = self.store.shift_summary(self._shift_date)
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
                ui.label("Shift").classes("w-28")
                ui.label("Ready").classes("w-24 text-right")
                ui.label("Enabled").classes("w-24 text-right")
                ui.label("Bypass").classes("w-24 text-right")
                ui.label("E-stop").classes("w-24 text-right")
            if not rows:
                ui.label("No data.").classes(
                    "text-gray-500 italic px-3 py-4"
                )
                return
            for r in rows:
                ready_min  = float(r.get("ready_minutes")   or 0)
                bypass_min = float(r.get("bypass_minutes")  or 0)
                estop_min  = float(r.get("estop_minutes")   or 0)
                with ui.row().classes(
                    "w-full items-center gap-3 px-3 py-1.5 text-sm "
                    "border-b border-[#E5E9EE]"
                ):
                    ui.label(r.get("shift") or "").classes(
                        "w-28 font-semibold text-sm"
                    )
                    ui.label(_fmt_min(ready_min)).classes(
                        "w-24 text-right font-mono text-xs "
                        + ("text-green-700 font-semibold" if ready_min > 0
                           else "text-gray-400")
                    )
                    ui.label(_fmt_min(r.get("enabled_minutes"))).classes(
                        "w-24 text-right font-mono text-xs"
                    )
                    ui.label(_fmt_min(bypass_min)).classes(
                        "w-24 text-right font-mono text-xs "
                        + ("text-red-700 font-semibold" if bypass_min > 0
                           else "text-gray-400")
                    )
                    ui.label(_fmt_min(estop_min)).classes(
                        "w-24 text-right font-mono text-xs "
                        + ("text-red-700 font-semibold" if estop_min > 0
                           else "text-gray-400")
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
