# Warak TSI

![tests](https://github.com/0xarn-io/tsiv2/actions/workflows/tests.yml/badge.svg)

Top Sheet Inserter — non-real-time control plane for the Warak TSI Gen 1.5
upgrade pack. A single Python process owns the SICK scanners, the Beckhoff
TwinCAT PLC link, the ABB OmniCore robot, the cameras, and the operator UI.

The hardware-facing layers run headless; the NiceGUI dashboard is opt-in
(don't construct it and you have a service).

## Features

- **Live dashboard** — Cameras, Robot status, RAPID variables, Recipes,
  Sizes catalog, and Robot status log, served at `http://<host>:8080/`.
- **TwinCAT PLC link** over ADS — typed signals declared in
  `plc_signals.toml`, fan-out via the in-process EventBus.
- **SICK UDP scanners** with per-scan event publication and live PLC mirror.
- **ABB OmniCore RWS client** — RAPID read/write, edit-mastership lifecycle,
  RAPID-array codec, and a polling Master-array mirror that survives
  controller-side edits.
- **Per-tab SQLite stores** for sizes, recipes, errors, units, and robot
  status — opened lazily, async-safe, with bus-driven live updates.
- **Snapshot archive** — disk-backed image archive with auto-prune.
- **EventBus** — sync / thread / async dispatch modes, payload-attribute
  filters, slow-handler timing alarms.

## Quick start

```bash
git clone https://github.com/0xarn-io/tsiv2.git
cd tsiv2
pip install -r requirements.txt        # plus pysickudt from your vendor channel
python Main.py
```

Open `http://localhost:8080/`.

> **Note** — `pysickudt` ships from the SICK vendor and isn't on PyPI.
> Tests stub it via `tests/conftest.py`, but production needs the real
> wheel installed locally.

## Configuration

| File | Purpose |
|---|---|
| `app_config.toml` | Runtime config: cameras, scanners, PLC publisher cadences, robot RWS address, UI title. |
| `plc_signals.toml` | TwinCAT alias → symbol/type map. Add a row here before any code references the alias. |
| `robot_vars.toml` | RAPID variables exposed in the Vars tab (alias, task/module/symbol, mode, poll cadence). |

Each module documents its config section in its docstring; check there for
the exact keys.

## Architecture

```
                         +--------------------+
                         |     EventBus       |
                         |  (sync/thread/async|
                         |   dispatch modes)  |
                         +---------+----------+
                                   |
   +---------------+   +-----------+-----------+   +----------------+
   | TwinCATComm   |   |   SickBridge / Pub    |   |  RWS Client    |
   |  (ADS link)   |   |   (UDP receiver +     |   |  (HTTP to ABB  |
   |  publishes    |   |    PLC mirror)        |   |   OmniCore)    |
   |  PlcSignal    |   |                       |   |                |
   |  Changed      |   |                       |   |                |
   +-------+-------+   +-----------+-----------+   +-------+--------+
           |                       |                       |
           v                       v                       v
   +---------------+       +---------------+       +-------------------+
   | Cam/Recipe/   |       |  UnitLogger   |       | RobotMaster /     |
   | Vars publish- |       |  (per-scan    |       | Vars / Elog       |
   | ers (filter   |       |   row)        |       | (poll + mirror)   |
   |  by alias)    |       |               |       |                   |
   +-------+-------+       +-------+-------+       +---------+---------+
           |                       |                         |
           v                       v                         v
   +---------------------------------------------------------------+
   |  DBOrchestrator → SizesStore / RecipesStore / ErrorsStore /   |
   |                   UnitsStore / RobotStatusLog (SQLite)        |
   +-------------------------------+-------------------------------+
                                   |
                                   v (sizes_changed, ...)
                          +-----------------+
                          |    Dashboard    |
                          |  (NiceGUI tabs) |
                          +-----------------+
```

`Main.py` is the composition root: it builds every component, wires the bus,
constructs the dashboard, and registers NiceGUI lifecycle hooks. No module
besides `Main.py`, `dashboard.py`, and the `*_panel.py` files imports
`nicegui`, so the data + hardware layers stay testable without a UI loop.

## Running the tests

```bash
pytest
```

CI runs the same command on Ubuntu/Python 3.13 against every push to `main`
and every PR — see `.github/workflows/tests.yml`. The `pysickudt` import is
stubbed in `tests/conftest.py`; nothing else is mocked at import time.

## Project layout

```
Main.py                  composition root (no business logic)
dashboard.py             NiceGUI route + tab wiring
event_bus.py             pub/sub dispatch (sync/thread/async)
events.py                typed payloads + signal registry
config.py                TOML loader + dataclasses

# hardware adapters
twincat_comm.py          TwinCAT ADS read/write + notifications
rws_client.py            ABB OmniCore RWS HTTP client
sick_bridge.py           SICK UDP receiver
rtsp_capture.py          RTSP camera capture

# producers (read hardware → publish events / push state)
sick_publisher.py        SICK measurements → PLC + bus
camera_publisher.py      PLC trigger → camera snap
recipe_publisher.py      PLC recipe code → setpoints write
robot_publisher.py       Robot status → PLC mirror
robot_master.py          Two-way sync: Sizes DB ↔ robot Master arrays
robot_variables.py       RAPID var poll + write
robot_status.py          OmniCore controller-state monitor
robot_status_log.py      Persistent robot-status timeline
robot_errors.py          OmniCore elog mirror
plc_heartbeat.py         Periodic ALIVE write
unit_logger.py           Per-scan row → units DB

# stores (SQLite, lazy-open)
sizes_store.py
recipes_store.py
errors_store.py
db_orchestrator.py       Builds + opens the configured stores
snapshot_archive.py      On-disk image archive with prune

# UI panels (each owns its tab)
camera_panel.py
robot_panel.py
robot_vars_panel.py
recipes_panel.py
sizes_panel.py
errors_panel.py
robot_status_panel.py
theme.py                 Brand palette, typography, card chrome

tests/                   357+ unit tests (pytest)
```

## License

Proprietary — Warak Group internal use.
