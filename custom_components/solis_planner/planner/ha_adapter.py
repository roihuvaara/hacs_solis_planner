from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Mapping

from .core import PeriodPrice, PlannerInputs, PlannerResult, SolisSlot, plan_solis_schedule
from .usage import decode_usage_buckets


def parse_slots(payload: str | list[dict[str, Any]] | None) -> list[SolisSlot]:
    if payload is None:
        return []
    data = json.loads(payload) if isinstance(payload, str) else payload
    return [
        SolisSlot(
            time=item["time"],
            enabled=bool(item["enabled"]),
            current=int(item["current"]),
            soc=int(item["soc"]),
        )
        for item in data
    ]


def planner_inputs_from_hass_state(state: Mapping[str, Any]) -> PlannerInputs:
    price_horizon_raw = json.loads(str(state["price_horizon"]))
    solar_forecast_by_period = json.loads(str(state["solar_forecast_by_period_kwh"])) if state.get("solar_forecast_by_period_kwh") else None
    return PlannerInputs(
        now=datetime.fromisoformat(str(state["now"])),
        battery_soc_pct=float(state["battery_soc_pct"]),
        battery_capacity_kwh=float(state["battery_capacity_kwh"]),
        usable_battery_kwh=float(state["usable_battery_kwh"]),
        reserve_soc_pct=float(state["reserve_soc_pct"]),
        max_charge_current_setting=int(state["max_charge_current_setting"]),
        solar_forecast_tomorrow_kwh=float(state["solar_forecast_tomorrow_kwh"]),
        solar_forecast_by_period_kwh=solar_forecast_by_period,
        price_horizon=[
            PeriodPrice(
                start_ts=datetime.fromisoformat(item["start_ts"]),
                price_cents_per_kwh=float(item["price_cents_per_kwh"]),
            )
            for item in price_horizon_raw
        ],
        rolling_usage_7d=decode_usage_buckets(str(state["rolling_usage_7d"])),
        current_charge_slots=parse_slots(state.get("current_charge_slots")),
        current_discharge_slots=parse_slots(state.get("current_discharge_slots")),
    )


def slot_payload(slots: list[SolisSlot]) -> list[dict[str, Any]]:
    return [
        {
            "time": slot.time,
            "enabled": slot.enabled,
            "current": slot.current,
            "soc": slot.soc,
        }
        for slot in slots
    ]


def planner_result_to_hass_payload(result: PlannerResult) -> dict[str, Any]:
    return {
        "target_soc_pct": result.target_soc_pct,
        "hold_soc_pct": result.hold_soc_pct,
        "morning_value_window_start": result.morning_value_window_start.isoformat() if result.morning_value_window_start else None,
        "morning_value_window_end": result.morning_value_window_end.isoformat() if result.morning_value_window_end else None,
        "expected_morning_load_kwh": result.expected_morning_load_kwh,
        "expected_morning_solar_kwh": result.expected_morning_solar_kwh,
        "debug_status": result.debug_status,
        "debug_summary": result.debug_summary,
        "charge_slots": slot_payload(result.charge_slots),
        "discharge_slots": slot_payload(result.discharge_slots),
    }


def run_planner_from_hass_state(state: Mapping[str, Any]) -> PlannerResult:
    return plan_solis_schedule(planner_inputs_from_hass_state(state))
