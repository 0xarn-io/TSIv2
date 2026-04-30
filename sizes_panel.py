"""sizes_panel.py — NiceGUI admin page for the SizesStore.

Single table; rows show ID / Slot / Name / W / L / St3 plus edit/delete.
"+ Add" opens a dialog with mm fields, station3 checkbox, and a non-stored
inches calculator.

Usage in dashboard.py:
    panel = SizesPanel(db.sizes)
    panel.mount()                     # call inside @ui.page
"""
from __future__ import annotations

import logging

from nicegui import ui

from sizes_store import Size, SizesStore
from theme       import card

log = logging.getLogger(__name__)


def _mm_to_in(mm: int) -> int:
    """Display-only conversion. Round to nearest inch."""
    return int(round(mm / 25.4))


def _in_to_mm(inches: int) -> int:
    return int(round(float(inches) * 25.4))


class SizesPanel:
    """Admin UI for the size catalog."""

    def __init__(self, store: SizesStore):
        self.store = store
        self._container: ui.column | None = None

    # ---- top-level mount ----------------------------------------------------

    def mount(self) -> None:
        with ui.column().classes("w-full gap-4 p-6"):
            with ui.row().classes("items-center mb-1 gap-2"):
                ui.button(
                    "New size", icon="add",
                    on_click=lambda: self._open_dialog(),
                ).props("color=primary unelevated")
                ui.button(
                    "Refresh", icon="refresh",
                    on_click=self._refresh,
                ).props("flat color=primary")
            self._container = ui.column().classes("w-full gap-1")
        self._refresh()

    # ---- list ---------------------------------------------------------------

    def _refresh(self) -> None:
        if self._container is None:
            return
        self._container.clear()
        with self._container:
            self._render_header()
            try:
                rows = self.store.list()
            except Exception as e:
                ui.label(f"Failed to load: {e}").classes("text-red-500")
                return
            if not rows:
                ui.label("No entries yet.").classes(
                    "text-gray-500 italic py-3"
                )
                return
            for s in rows:
                self._render_row(s)

    @staticmethod
    def _render_header() -> None:
        with ui.row().classes(
            "w-full items-center gap-3 px-3 py-2 text-xs font-semibold "
            "uppercase tracking-wider text-gray-500 border-b border-[#E5E9EE]"
        ):
            ui.label("ID").classes("w-12")
            ui.label("Slot").classes("w-14 text-right")
            ui.label("Name").classes("w-40")
            ui.label("W (mm)").classes("w-24 text-right")
            ui.label("L (mm)").classes("w-24 text-right")
            ui.label("≈ W (in)").classes("w-20 text-right text-gray-400")
            ui.label("≈ L (in)").classes("w-20 text-right text-gray-400")
            ui.label("St 3").classes("w-12 text-center")
            ui.label("").classes("flex-grow")

    def _render_row(self, s: Size) -> None:
        with ui.row().classes(
            "w-full items-center gap-3 px-3 py-2 text-sm "
            "border-b border-[#E5E9EE] hover:bg-[#F4F6F8]"
        ):
            ui.label(str(s.id)).classes(
                "w-12 font-mono text-xs text-gray-500"
            )
            slot_text = "—" if s.slot is None else str(s.slot)
            ui.label(slot_text).classes(
                "w-14 text-right font-mono text-xs text-[#0053A1]"
            )
            ui.label(s.name).classes("w-40 font-medium")
            ui.label(str(s.width_mm)).classes("w-24 text-right font-mono")
            ui.label(str(s.length_mm)).classes("w-24 text-right font-mono")
            ui.label(str(_mm_to_in(s.width_mm))).classes(
                "w-20 text-right font-mono text-gray-400"
            )
            ui.label(str(_mm_to_in(s.length_mm))).classes(
                "w-20 text-right font-mono text-gray-400"
            )
            ui.label("✓" if s.station3 else "—").classes(
                f"w-12 text-center font-mono "
                f"{'text-[#118938]' if s.station3 else 'text-gray-300'}"
            )
            with ui.row().classes("flex-grow justify-end gap-1"):
                ui.button(
                    icon="edit",
                    on_click=lambda s=s: self._open_dialog(s),
                ).props("dense flat color=primary").tooltip("Edit")
                ui.button(
                    icon="delete",
                    on_click=lambda sid=s.id: self._delete(sid),
                ).props("dense flat color=negative").tooltip("Delete")

    # ---- dialog (add / edit) ------------------------------------------------

    def _open_dialog(self, existing: Size | None = None) -> None:
        title = (f"Edit size #{existing.id}") if existing else "New size"

        with ui.dialog() as dialog, ui.card().classes(
            "min-w-[460px] p-0 overflow-hidden"
        ):
            with ui.element("div").classes(
                "bg-[#0053A1] text-white px-5 py-3"
            ):
                ui.label(title).classes("text-base font-semibold")

            with ui.column().classes(
                "gap-4 p-5 w-full max-h-[65vh] overflow-y-auto"
            ):
                with ui.row().classes("gap-3 w-full items-center"):
                    name = ui.input(
                        "Name",
                        value=existing.name if existing else "",
                    ).classes("flex-grow")
                    slot = ui.number(
                        "Slot (0–19, blank = none)",
                        value=(existing.slot if existing and existing.slot is not None
                               else None),
                        format="%d", min=0, max=19,
                    ).classes("w-48")

                with card("Dimensions (mm)"):
                    with ui.grid(columns=2).classes("w-full gap-3"):
                        wmm = ui.number(
                            "Width (mm)",
                            value=existing.width_mm if existing else 0,
                            format="%d",
                        )
                        lmm = ui.number(
                            "Length (mm)",
                            value=existing.length_mm if existing else 0,
                            format="%d",
                        )

                with card("Station 3"):
                    station3 = ui.checkbox(
                        "Selectable at station 3",
                        value=bool(existing.station3) if existing else False,
                    )

                with card("Inches calculator (not stored)"):
                    with ui.grid(columns=2).classes("w-full gap-3"):
                        win = ui.number(
                            "Width (in)",
                            value=_mm_to_in(existing.width_mm) if existing else 0,
                            format="%d",
                        )
                        lin = ui.number(
                            "Length (in)",
                            value=_mm_to_in(existing.length_mm) if existing else 0,
                            format="%d",
                        )

                    def fill_mm_from_inches() -> None:
                        wmm.value = _in_to_mm(win.value or 0)
                        lmm.value = _in_to_mm(lin.value or 0)

                    ui.button(
                        "Fill mm from inches", icon="swap_horiz",
                        on_click=fill_mm_from_inches,
                    ).props("flat dense color=primary").classes("mt-2")

            def submit() -> None:
                raw_slot = slot.value
                slot_val: int | None = None
                if raw_slot not in (None, ""):
                    try:
                        slot_val = int(raw_slot)
                    except (TypeError, ValueError):
                        ui.notify("Slot must be 0–19 or blank", type="warning")
                        return
                try:
                    s = Size(
                        id=existing.id if existing else None,
                        name=(name.value or "").strip(),
                        width_mm=int(wmm.value or 0),
                        length_mm=int(lmm.value or 0),
                        slot=slot_val,
                        station3=bool(station3.value),
                    )
                    if not s.name:
                        ui.notify("Name is required", type="warning")
                        return
                    if existing:
                        self.store.update(s)
                    else:
                        self.store.add(s)
                except Exception as e:
                    log.exception("sizes save failed")
                    ui.notify(f"Save failed: {e}", type="negative")
                    return
                dialog.close()
                self._refresh()

            with ui.row().classes(
                "justify-end gap-2 w-full px-5 py-3 "
                "border-t border-[#E5E9EE] bg-white"
            ):
                ui.button("Cancel", on_click=dialog.close).props("flat")
                ui.button(
                    "Save", icon="save", on_click=submit,
                ).props("color=primary unelevated")

        dialog.open()

    # ---- delete -------------------------------------------------------------

    def _delete(self, sid: int) -> None:
        with ui.dialog() as dialog, ui.card().classes("min-w-[360px]"):
            ui.label(f"Delete size #{sid}?").classes(
                "text-base font-semibold"
            )
            ui.label("This permanently removes the row.").classes(
                "text-sm text-gray-500"
            )
            with ui.row().classes("justify-end gap-2 w-full pt-2"):
                ui.button("Cancel", on_click=dialog.close).props("flat")

                def go() -> None:
                    try:
                        self.store.delete(sid)
                    except Exception as e:
                        ui.notify(f"Delete failed: {e}", type="negative")
                        return
                    dialog.close()
                    self._refresh()

                ui.button(
                    "Delete", icon="delete", on_click=go,
                ).props("color=negative unelevated")
        dialog.open()
