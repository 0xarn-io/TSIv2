"""theme.py — Warak corporate theme for the NiceGUI UI layer.

Confines all colour / typography / chrome decisions to one module so panels
just call `apply_theme()` and `card()` without sprinkling brand constants
around.

Brand:
    primary  Pantone 2728  #0053A1   (deep blue)
    accent   Pantone 362   #4DA32F   (corporate green)
    info     Pantone 2925  #0698D6   (light blue)
    dark     Pantone 2746  #283273   (navy)

Fonts:
    Muli           — body type. Loaded from Google Fonts.
    Magistral      — display type for the WARAK wordmark only. Drop a TTF
                     into `static/Magistral.ttf` to enable; the @font-face
                     falls back to Muli silently if the file is absent.

Place TTFs (Magistral-Regular.ttf, Magistral-Medium.ttf, Magistral-Bold.ttf)
under `static/` in the repo root and serve via:
    from nicegui import app
    app.add_static_files("/static", "static")
in Main.py.
"""
from __future__ import annotations

from contextlib import contextmanager

from nicegui import ui


# Brand palette (hex). Use these names rather than the hex values directly
# so that a future palette tweak only edits this module.
PRIMARY   = "#0053A1"
ACCENT    = "#4DA32F"
INFO      = "#0698D6"
DARK      = "#283273"
NEGATIVE  = "#C0392B"
BG_PAGE   = "#F4F6F8"   # page background — soft cool gray
BG_CARD   = "#FFFFFF"


_HEAD_HTML = f"""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Mulish:wght@300;400;600;700;800&display=swap" rel="stylesheet">

<style>
@font-face {{
    font-family: 'Magistral';
    src: url('/static/Magistral-Regular.ttf') format('truetype');
    font-weight: 400;
    font-display: swap;
}}
@font-face {{
    font-family: 'Magistral';
    src: url('/static/Magistral-Medium.ttf') format('truetype');
    font-weight: 600;
    font-display: swap;
}}
@font-face {{
    font-family: 'Magistral';
    src: url('/static/Magistral-Bold.ttf') format('truetype');
    font-weight: 800;
    font-display: swap;
}}

html, body {{
    font-family: 'Mulish', 'Muli', -apple-system, BlinkMacSystemFont, sans-serif;
    background: {BG_PAGE};
    color: #1f2937;
}}

.warak-title {{
    font-family: 'Magistral', 'Mulish', sans-serif;
    font-weight: 800;
    letter-spacing: 0.04em;
    color: {PRIMARY};
    text-transform: uppercase;
}}

.warak-card {{
    background: {BG_CARD};
    border: 1px solid #E5E9EE;
    border-radius: 12px;
    box-shadow: 0 1px 2px rgba(15, 39, 80, 0.04),
                0 4px 12px rgba(15, 39, 80, 0.04);
}}

/* Quasar tab indicator: thicker, brand colour */
.q-tab__indicator {{ height: 3px; }}

/* NiceGUI default page padding is too tight for this layout */
.nicegui-content {{
    padding: 0 !important;
}}
</style>
"""


def apply_theme() -> None:
    """Install brand palette + fonts. Call inside every @ui.page."""
    ui.colors(
        primary  = PRIMARY,
        secondary= INFO,
        accent   = ACCENT,
        dark     = DARK,
        positive = ACCENT,
        negative = NEGATIVE,
        info     = INFO,
        warning  = "#E8A33D",
    )
    ui.dark_mode().disable()
    ui.add_head_html(_HEAD_HTML)


def warak_header(subtitle: str = "") -> None:
    """Render the WARAK wordmark + optional subtitle as a top bar."""
    with ui.row().classes(
        "w-full items-baseline gap-3 px-6 py-4 bg-white border-b "
        "border-[#E5E9EE]"
    ):
        ui.label("WARAK").classes("warak-title text-3xl")
        if subtitle:
            ui.label(subtitle).classes("text-sm text-gray-500 tracking-wide")


@contextmanager
def card(title: str | None = None, *, padding: str = "p-5"):
    """Styled card wrapper used by every panel section.

    Usage:
        with card("Section title"):
            ui.label("body")
    """
    with ui.element("div").classes(f"warak-card {padding} w-full"):
        if title:
            ui.label(title).classes(
                "text-sm font-semibold uppercase tracking-wider "
                "text-[#0053A1] mb-3"
            )
        yield


def severity_classes(severity: str) -> str:
    """Tailwind row-tint classes by error severity."""
    return {
        "info":     "bg-blue-50",
        "warning":  "bg-yellow-50",
        "error":    "bg-red-50",
        "critical": "bg-red-200 font-semibold",
    }.get(severity, "")
