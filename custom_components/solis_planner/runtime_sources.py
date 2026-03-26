from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

try:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers import device_registry as dr
    from homeassistant.helpers import entity_registry as er
except ModuleNotFoundError:  # pragma: no cover - imported only in HA runtime.
    ConfigEntry = Any  # type: ignore[misc,assignment]
    HomeAssistant = Any  # type: ignore[misc,assignment]
    dr = None  # type: ignore[assignment]
    er = None  # type: ignore[assignment]

from .planner.usage import UsageSample


def power_rows_to_usage_samples(
    rows: list[dict[str, Any]],
    *,
    tzinfo: Any,
    sample_minutes: int = 5,
) -> list[UsageSample]:
    period_hours = sample_minutes / 60.0
    samples: list[UsageSample] = []
    for row in rows:
        mean_watts = row.get("mean")
        if mean_watts is None:
            continue
        samples.append(
            UsageSample(
                start_ts=_coerce_stat_start(row["start"], tzinfo),
                kwh=max(0.0, float(mean_watts) * period_hours / 1000.0),
            )
        )
    return samples


def solar_series_from_wh_period(
    *,
    target_period_starts: list[datetime],
    wh_period: dict[datetime, int],
    period_minutes: int = 15,
) -> list[float]:
    if not target_period_starts:
        return []
    if not wh_period:
        return [0.0] * len(target_period_starts)

    sorted_periods = sorted(wh_period.items(), key=lambda item: item[0])
    resolution = _detect_resolution(sorted_periods)
    target_period = timedelta(minutes=period_minutes)
    target_end = target_period_starts[-1] + target_period
    values = [0.0] * len(target_period_starts)

    for period_start, watt_hours in sorted_periods:
        interval_start = period_start
        interval_end = interval_start + resolution
        if interval_end <= target_period_starts[0] or interval_start >= target_end:
            continue

        interval_seconds = resolution.total_seconds()
        if interval_seconds <= 0:
            continue
        watt_hours_per_second = float(watt_hours) / interval_seconds

        for index, target_start in enumerate(target_period_starts):
            overlap_seconds = _overlap_seconds(
                interval_start,
                interval_end,
                target_start,
                target_start + target_period,
            )
            if overlap_seconds <= 0:
                continue
            values[index] += watt_hours_per_second * overlap_seconds / 1000.0

    return [round(max(0.0, value), 4) for value in values]


async def async_build_solar_forecast_series(
    hass: HomeAssistant,
    *,
    source_entity_id: str,
    target_period_starts: list[datetime],
) -> tuple[list[float] | None, str]:
    entry = _resolve_config_entry_from_entity(hass, source_entity_id)
    if entry is None:
        return None, "daily_total_fallback"
    if entry.domain != "forecast_solar":
        return None, "daily_total_fallback"

    estimate = getattr(getattr(entry, "runtime_data", None), "data", None)
    wh_period = getattr(estimate, "wh_period", None)
    if not wh_period:
        return None, "daily_total_fallback"

    return (
        solar_series_from_wh_period(
            target_period_starts=target_period_starts,
            wh_period=wh_period,
        ),
        "forecast_solar_provider",
    )


def _resolve_config_entry_from_entity(
    hass: HomeAssistant,
    entity_id: str,
) -> ConfigEntry | None:
    if er is None or dr is None:
        return None

    entity_registry = er.async_get(hass)
    entity_entry = entity_registry.async_get(entity_id)
    if entity_entry is None:
        return None

    config_entry_ids: list[str] = []
    config_entry_id = getattr(entity_entry, "config_entry_id", None)
    if config_entry_id:
        config_entry_ids.append(config_entry_id)

    if entity_entry.device_id:
        device = dr.async_get(hass).async_get(entity_entry.device_id)
        if device is not None:
            config_entry_ids.extend(device.config_entries)

    for entry_id in dict.fromkeys(config_entry_ids):
        entry = hass.config_entries.async_get_entry(entry_id)
        if entry is not None:
            return entry
    return None


def _detect_resolution(sorted_periods: list[tuple[datetime, int]]) -> timedelta:
    deltas = [
        current[0] - previous[0]
        for previous, current in zip(sorted_periods, sorted_periods[1:])
        if current[0] > previous[0]
    ]
    if not deltas:
        return timedelta(hours=1)
    return min(deltas)


def _overlap_seconds(
    start_a: datetime,
    end_a: datetime,
    start_b: datetime,
    end_b: datetime,
) -> float:
    overlap_start = max(start_a, start_b)
    overlap_end = min(end_a, end_b)
    if overlap_end <= overlap_start:
        return 0.0
    return (overlap_end - overlap_start).total_seconds()


def _coerce_stat_start(value: Any, tzinfo: Any) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=tzinfo)
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=tzinfo)
    return parsed
