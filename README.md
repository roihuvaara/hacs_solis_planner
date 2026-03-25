# Solis Planner

Home Assistant custom integration that plans Solis battery charge and hold
slots from a fixed 15-minute price horizon, rolling usage profile, solar
forecast, and current inverter slot state.

This repository is packaged as a HACS integration:

- domain: `solis_planner`
- install path: `custom_components/solis_planner/`

## What It Does

- computes a deterministic future period plan in Python
- compiles that plan into Solis charge and discharge slots
- preserves the live current-period strategy during recompilation
- exposes one service action, `solis_planner.plan_schedule`, for Home
  Assistant automations

## Installation Model

1. Install the repository as a HACS custom integration.
2. Add the `Solis Planner` integration in Home Assistant.
3. Call `solis_planner.plan_schedule` from your automation with the planner
   state payload.
4. Feed the returned slots into a thin apply script that writes Solis entities.

Installation notes are in
[docs/installation.md](/home/jukka/work/hacs_solis_planner/docs/installation.md).

## Development

Run the local test suite with:

```bash
python3 -m unittest discover -s tests
```
