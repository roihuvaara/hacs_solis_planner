# Installation

## HACS

1. Add this repository as a custom integration repository in HACS.
2. Install `Solis Planner`.
3. Restart Home Assistant if required.
4. Add the `Solis Planner` integration from the UI so the
   `solis_planner.plan_schedule` service is registered.

## Wiring

Use `solis_planner.plan_schedule` from an automation that gathers:

- current battery SOC
- reserve SOC
- the quarter-hour price horizon
- the rolling 7-day usage profile
- current charge and discharge slot state

Then pass the returned `charge_slots` and `discharge_slots` into a thin apply
script that writes Solis slot entities.
