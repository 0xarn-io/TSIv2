"""Self-contained 3D scene with three boxes sized from external variables (mm).

Usage:
    from nicegui import ui
    from box_scene import mount_box_scene, set_box_size, on_box_click

    @ui.page("/")
    def index():
        mount_box_scene()
        on_box_click(lambda i: print(f"Clicked box {i}"))
"""
from nicegui import ui
from typing import Callable, Optional

MM_TO_M = 0.001

_sizes_mm = [
    {"w": 711, "h": 1800, "d": 1778},
    {"w": 899, "h": 1300, "d": 1778},
    {"w": 635, "h": 1000, "d": 1778},
]

_x_positions = [-2.0, 0.0, 2.0]
_colors = ["#a1845a", "#a1845a", "#a1845a"]

_boxes = []
_click_callbacks: list[Callable[[int], None]] = []


def mount_box_scene(width=800, height=500):
    """Mount the 3D canvas. Call once inside a @ui.page function."""
    _boxes.clear()

    def handle_click(e):
        # e.hits is a list of objects under the click ray, nearest first
        if not e.hits:
            return
        hit = e.hits[0]
        # Match the clicked three.js object back to our box index
        for i, box in enumerate(_boxes):
            if hit.object_id == box.id:
                for cb in _click_callbacks:
                    try:
                        cb(i)
                    except Exception as ex:
                        print(f"[WARN] box click callback failed: {ex}")
                return

    with ui.scene(width=width, height=height, on_click=handle_click).classes(
        "border rounded"
    ) as scene:
        for size, x, color in zip(_sizes_mm, _x_positions, _colors):
            box = scene.box(1, 1, 1).material(color)
            _apply_size(box, size, x)
            _boxes.append(box)


def on_box_click(callback: Callable[[int], None]):
    """Register a callback that fires when any box is clicked.
    The callback receives the box index (0, 1, or 2).
    """
    _click_callbacks.append(callback)
    return lambda: _click_callbacks.remove(callback)


def set_box_size(index, width_mm=None, height_mm=None, depth_mm=None):
    """Update one box's dimensions live. Pass any subset."""
    s = _sizes_mm[index]
    if width_mm  is not None: s["w"] = width_mm
    if height_mm is not None: s["h"] = height_mm
    if depth_mm  is not None: s["d"] = depth_mm
    if index < len(_boxes):
        _apply_size(_boxes[index], s, _x_positions[index])


def _apply_size(box, size_mm, x_pos):
    w = size_mm["w"] * MM_TO_M
    h = size_mm["h"] * MM_TO_M
    d = size_mm["d"] * MM_TO_M
    box.scale(w, h, d)
    box.move(x=x_pos, y=h / 2)