from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

try:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant, ServiceCall, ServiceResponse
    from homeassistant.helpers.dispatcher import async_dispatcher_send
except ModuleNotFoundError:  # pragma: no cover - local unit tests run without HA installed.
    ConfigEntry = Any  # type: ignore[misc,assignment]
    HomeAssistant = Any  # type: ignore[misc,assignment]
    ServiceCall = Any  # type: ignore[misc,assignment]
    ServiceResponse = dict[str, Any]  # type: ignore[misc,assignment]

    def async_dispatcher_send(*args: Any, **kwargs: Any) -> None:
        return None

from .bridge import build_load_forecast_payload, forecast_result_to_payload, plan_schedule_payload
from .const import (
    DATA_LATEST_PLAN,
    DOMAIN,
    PLATFORMS,
    SERVICE_BUILD_LOAD_FORECAST,
    SERVICE_PLAN_SCHEDULE,
    planner_update_signal,
)
from .planner.forecast import TemperatureSample, build_load_forecast_for_periods
from .planner.usage import UsageSample

type SolisPlannerConfigEntry = ConfigEntry


async def async_setup(hass: HomeAssistant, config: Mapping[str, Any]) -> bool:
    hass.data.setdefault(DOMAIN, {})
    await _async_register_services(hass)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: SolisPlannerConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = {}
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    await _async_register_services(hass)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: SolisPlannerConfigEntry) -> bool:
    await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
    return True


async def _async_register_services(hass: HomeAssistant) -> None:
    if not hass.services.has_service(DOMAIN, SERVICE_PLAN_SCHEDULE):
        async def handle_plan_schedule(call: ServiceCall) -> ServiceResponse:
            planner_state = call.data.get("planner_state", {})
            if not isinstance(planner_state, Mapping):
                raise ValueError("planner_state must be a mapping")
            response = plan_schedule_payload(dict(planner_state))
            _store_latest_plan_payload(hass, response)
            return response

        hass.services.async_register(
            DOMAIN,
            SERVICE_PLAN_SCHEDULE,
            handle_plan_schedule,
            supports_response="only",
        )

    if hass.services.has_service(DOMAIN, SERVICE_BUILD_LOAD_FORECAST):
        return

    async def handle_build_load_forecast(call: ServiceCall) -> ServiceResponse:
        forecast_state = call.data.get("forecast_state")
        if forecast_state is not None:
            if not isinstance(forecast_state, Mapping):
                raise ValueError("forecast_state must be a mapping")
            return build_load_forecast_payload(dict(forecast_state))

        return await _build_load_forecast_from_hass(hass, call)

    hass.services.async_register(
        DOMAIN,
        SERVICE_BUILD_LOAD_FORECAST,
        handle_build_load_forecast,
        supports_response="only",
    )


async def _build_load_forecast_from_hass(
    hass: HomeAssistant,
    call: ServiceCall,
) -> ServiceResponse:
    energy_entity_id = call.data.get("energy_entity_id")
    if not energy_entity_id:
        raise ValueError("energy_entity_id is required when forecast_state is not provided")

    planner_state = call.data.get("planner_state", {})
    if not isinstance(planner_state, Mapping):
        raise ValueError("planner_state must be a mapping")

    now = datetime.fromisoformat(str(planner_state["now"]))
    price_horizon_raw = json.loads(str(planner_state["price_horizon"]))
    target_period_starts = [
        datetime.fromisoformat(item["start_ts"])
        for item in price_horizon_raw
    ]
    baseline_days = int(call.data.get("baseline_days", 30))
    recent_days = int(call.data.get("recent_days", 7))
    bucket_minutes = int(call.data.get("bucket_minutes", 15))

    load_samples = await _async_fetch_energy_samples(
        hass=hass,
        entity_id=str(energy_entity_id),
        end_time=now,
        baseline_days=baseline_days,
    )
    temperature_entity_id = call.data.get("temperature_entity_id")
    historical_temperature_samples = (
        await _async_fetch_temperature_samples(
            hass=hass,
            entity_id=str(temperature_entity_id),
            end_time=now,
            baseline_days=baseline_days,
        )
        if temperature_entity_id
        else []
    )
    weather_entity_id = call.data.get("weather_entity_id")
    future_temperature_samples = (
        await _async_fetch_weather_forecast_samples(
            hass=hass,
            entity_id=str(weather_entity_id),
            target_period_starts=target_period_starts,
        )
        if weather_entity_id
        else []
    )

    result = build_load_forecast_for_periods(
        target_period_starts=target_period_starts,
        load_samples=load_samples,
        historical_temperature_samples=historical_temperature_samples,
        future_temperature_samples=future_temperature_samples,
        target_time=now,
        baseline_days=baseline_days,
        recent_days=recent_days,
        bucket_minutes=bucket_minutes,
    )
    return forecast_result_to_payload(result)


def _store_latest_plan_payload(hass: HomeAssistant, payload: Mapping[str, Any]) -> None:
    for entry_id, entry_state in hass.data.get(DOMAIN, {}).items():
        if not isinstance(entry_state, dict):
            continue
        entry_state[DATA_LATEST_PLAN] = dict(payload)
        async_dispatcher_send(hass, planner_update_signal(entry_id))


async def _async_fetch_energy_samples(
    hass: HomeAssistant,
    *,
    entity_id: str,
    end_time: datetime,
    baseline_days: int,
) -> list[UsageSample]:
    try:
        from homeassistant.components.recorder.statistics import statistics_during_period
    except ModuleNotFoundError as err:  # pragma: no cover - only available in HA runtime
        raise RuntimeError("Recorder statistics are unavailable in this environment") from err

    start_time = end_time - timedelta(days=baseline_days)
    response = await hass.async_add_executor_job(
        statistics_during_period,
        hass,
        start_time,
        end_time,
        {entity_id},
        "5minute",
        None,
        {"change"},
    )
    rows = response.get(entity_id, [])
    return [
        UsageSample(
            start_ts=_coerce_stat_start(row["start"], end_time.tzinfo),
            kwh=max(0.0, float(row.get("change") or 0.0)),
        )
        for row in rows
        if row.get("change") is not None
    ]


async def _async_fetch_temperature_samples(
    hass: HomeAssistant,
    *,
    entity_id: str,
    end_time: datetime,
    baseline_days: int,
) -> list[TemperatureSample]:
    try:
        from homeassistant.components.recorder.statistics import statistics_during_period
    except ModuleNotFoundError as err:  # pragma: no cover - only available in HA runtime
        raise RuntimeError("Recorder statistics are unavailable in this environment") from err

    start_time = end_time - timedelta(days=baseline_days)
    response = await hass.async_add_executor_job(
        statistics_during_period,
        hass,
        start_time,
        end_time,
        {entity_id},
        "5minute",
        None,
        {"mean"},
    )
    rows = response.get(entity_id, [])
    return [
        TemperatureSample(
            start_ts=_coerce_stat_start(row["start"], end_time.tzinfo),
            temperature_c=float(row["mean"]),
        )
        for row in rows
        if row.get("mean") is not None
    ]


async def _async_fetch_weather_forecast_samples(
    hass: HomeAssistant,
    *,
    entity_id: str,
    target_period_starts: list[datetime],
) -> list[TemperatureSample]:
    response = await hass.services.async_call(
        "weather",
        "get_forecasts",
        {"entity_id": [entity_id], "type": "hourly"},
        blocking=True,
        return_response=True,
    )
    forecast_rows = response.get(entity_id, {}).get("forecast", []) if isinstance(response, Mapping) else []
    hourly_temperatures = {
        datetime.fromisoformat(item["datetime"]).replace(minute=0, second=0, microsecond=0): float(item["temperature"])
        for item in forecast_rows
        if item.get("datetime") and item.get("temperature") is not None
    }
    samples: list[TemperatureSample] = []
    for period_start in target_period_starts:
        period_hour = period_start.replace(minute=0, second=0, microsecond=0)
        if period_hour in hourly_temperatures:
            samples.append(
                TemperatureSample(
                    start_ts=period_start,
                    temperature_c=hourly_temperatures[period_hour],
                )
            )
    return samples


def _coerce_stat_start(value: Any, tzinfo: Any) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=tzinfo)
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=tzinfo)
    return parsed
