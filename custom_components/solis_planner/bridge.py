from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .planner.ha_adapter import (
    planner_inputs_from_hass_state,
    planner_result_to_hass_payload,
)
from .planner.forecast import (
    LoadForecastResult,
    TemperatureSample,
    build_load_forecast_for_periods,
)
from .planner.core import plan_solis_schedule
from .planner.usage import UsageSample


def normalize_planner_state(planner_state: Mapping[str, Any] | str) -> dict[str, Any]:
    if isinstance(planner_state, str):
        return json.loads(planner_state)
    return dict(planner_state)


def plan_schedule_payload(planner_state: Mapping[str, Any] | str) -> dict[str, Any]:
    normalized_state = normalize_planner_state(planner_state)
    inputs = planner_inputs_from_hass_state(normalized_state)
    result = plan_solis_schedule(inputs)
    return planner_result_to_hass_payload(result)


def _parse_usage_samples(payload: list[dict[str, Any]] | str | None) -> list[UsageSample]:
    if payload is None:
        return []
    items = json.loads(payload) if isinstance(payload, str) else payload
    return [
        UsageSample(
            start_ts=datetime.fromisoformat(item["start_ts"]),
            kwh=float(item["kwh"]),
        )
        for item in items
    ]


def _parse_temperature_samples(payload: list[dict[str, Any]] | str | None) -> list[TemperatureSample]:
    if payload is None:
        return []
    items = json.loads(payload) if isinstance(payload, str) else payload
    return [
        TemperatureSample(
            start_ts=datetime.fromisoformat(item["start_ts"]),
            temperature_c=float(item["temperature_c"]),
        )
        for item in items
    ]


def forecast_result_to_payload(result: LoadForecastResult) -> dict[str, Any]:
    return {
        "load_forecast_by_period_kwh": result.load_forecast_by_period_kwh,
        "packed_profile": result.packed_profile,
        "baseline_bucket_count": result.baseline_bucket_count,
        "weather_adjusted_bucket_count": result.weather_adjusted_bucket_count,
        "recent_residual_bucket_count": result.recent_residual_bucket_count,
        "recent_only_bucket_count": result.recent_only_bucket_count,
        "missing_bucket_count": result.missing_bucket_count,
    }


def build_load_forecast_payload(forecast_state: Mapping[str, Any] | str) -> dict[str, Any]:
    normalized_state = normalize_planner_state(forecast_state)
    result = build_load_forecast_for_periods(
        target_period_starts=[
            datetime.fromisoformat(item["start_ts"])
            for item in json.loads(str(normalized_state["price_horizon"]))
        ],
        load_samples=_parse_usage_samples(normalized_state.get("load_samples")),
        historical_temperature_samples=_parse_temperature_samples(
            normalized_state.get("historical_temperature_samples")
        ),
        future_temperature_samples=_parse_temperature_samples(
            normalized_state.get("future_temperature_samples")
        ),
        target_time=datetime.fromisoformat(str(normalized_state["now"])),
        baseline_days=int(normalized_state.get("baseline_days", 30)),
        recent_days=int(normalized_state.get("recent_days", 7)),
        bucket_minutes=int(normalized_state.get("bucket_minutes", 15)),
    )
    return forecast_result_to_payload(result)
