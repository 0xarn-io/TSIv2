"""Guard test: the data + hardware layer must not import `nicegui`.

The UI layer (theme, dashboard, *_panel, camera_panel + camera_publisher
which transitively pulls it in) is allowed to depend on NiceGUI;
everything else has to be runnable headless. This test scans the data /
hardware modules' source text for any `nicegui` import statement.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

# Modules that are part of the data + hardware layer. Camera UI modules
# are excluded (they're inherently NiceGUI-bound). For truly-headless
# camera capture, use rtsp_capture.py + snapshot_archive directly.
DATA_LAYER_MODULES = (
    "recipes_store.py",
    "recipe_publisher.py",
    "errors_store.py",
    "sizes_store.py",
    "unit_logger.py",
    "db_orchestrator.py",
    "twincat_comm.py",
    "sick_bridge.py",
    "sick_publisher.py",
    "snapshot_archive.py",
    "robot_errors.py",
    "robot_master.py",
    "robot_publisher.py",
    "robot_status.py",
    "robot_variables.py",
    "rws_client.py",
    "plc_heartbeat.py",
    "rtsp_capture.py",
    "config.py",
)

_NICEGUI_IMPORT = re.compile(
    r"^\s*(?:from\s+nicegui\b|import\s+nicegui\b)", re.MULTILINE,
)


_REPO_ROOT = Path(__file__).resolve().parent.parent


def _find_module(name: str) -> Path | None:
    """Locate `name.py` anywhere under the repo (root or topical subdir).

    After the folder restructure modules live under core/, plc/, robot/,
    stores/, ui/, camera/ — pytest's pythonpath resolves the imports, but
    this guard reads source text directly so it has to walk the tree.
    """
    for path in _REPO_ROOT.rglob(name):
        # Skip caches, venv copies, and tests-as-fixtures.
        parts = set(path.parts)
        if parts & {"__pycache__", ".git", "tests", "static"}:
            continue
        return path
    return None


@pytest.mark.parametrize("module", DATA_LAYER_MODULES)
def test_module_does_not_import_nicegui(module: str) -> None:
    path = _find_module(module)
    if path is None:
        pytest.skip(f"{module} not present")
    src = path.read_text(encoding="utf-8")
    matches = _NICEGUI_IMPORT.findall(src)
    assert not matches, (
        f"{module} imports nicegui — UI deps must stay in panel/dashboard/theme "
        f"modules so the data layer can run headless."
    )
