from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Mapping

try:
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.storage import Store
except ModuleNotFoundError:  # pragma: no cover - imported only in HA runtime.
    HomeAssistant = Any  # type: ignore[misc,assignment]
    Store = Any  # type: ignore[misc,assignment]

from .const import DOMAIN


SOLAR_BIAS_STORAGE_VERSION = 1
SOLAR_BIAS_STORAGE_KEY = f"{DOMAIN}_solar_bias"
SOLAR_BIAS_MONTH_MIN_OBSERVATIONS = 5
SOLAR_BIAS_WEEK_MIN_OBSERVATIONS = 20
SOLAR_BIAS_WEEK_ACTIVATION_DAYS = 365
SOLAR_BIAS_MAX_FACTOR = 1.1


def normalize_weather_condition(condition: str | None) -> str:
    normalized = (condition or "").strip().lower()
    if normalized in {"sunny", "clear-night", "clear"}:
        return "clear"
    if normalized in {"partlycloudy", "partly-cloudy", "partly cloudy"}:
        return "partly_cloudy"
    if normalized in {"cloudy", "overcast"}:
        return "cloudy"
    if normalized in {"fog", "hazy"}:
        return "fog"
    if normalized in {"rainy", "pouring", "lightning-rainy"}:
        return "rain"
    if normalized in {"snowy", "snowy-rainy"}:
        return "snow"
    return "other"


def empty_solar_bias_store() -> dict[str, Any]:
    return {
        "version": SOLAR_BIAS_STORAGE_VERSION,
        "first_observation_at": None,
        "last_reconciled_hour": None,
        "pending_forecasts": {},
        "stats": {
            "weather_hour": {},
            "weather_month_hour": {},
            "weather_week_hour": {},
        },
    }


async def async_load_solar_bias_store(hass: HomeAssistant) -> dict[str, Any]:
    domain_data = hass.data.setdefault(f"{DOMAIN}_runtime", {})
    cached = domain_data.get("solar_bias_store")
    if cached is not None:
        return cached

    store = Store(hass, SOLAR_BIAS_STORAGE_VERSION, SOLAR_BIAS_STORAGE_KEY)
    loaded = await store.async_load()
    data = loaded if isinstance(loaded, dict) else empty_solar_bias_store()
    data.setdefault("pending_forecasts", {})
    stats = data.setdefault("stats", {})
    stats.setdefault("weather_hour", {})
    stats.setdefault("weather_month_hour", {})
    stats.setdefault("weather_week_hour", {})
    data.setdefault("first_observation_at", None)
    data.setdefault("last_reconciled_hour", None)
    domain_data["solar_bias_store_handle"] = store
    domain_data["solar_bias_store"] = data
    return data


async def async_save_solar_bias_store(hass: HomeAssistant, data: Mapping[str, Any]) -> None:
    domain_data = hass.data.setdefault(f"{DOMAIN}_runtime", {})
    store = domain_data.get("solar_bias_store_handle")
    if store is None:
        store = Store(hass, SOLAR_BIAS_STORAGE_VERSION, SOLAR_BIAS_STORAGE_KEY)
        domain_data["solar_bias_store_handle"] = store
    domain_data["solar_bias_store"] = dict(data)
    await store.async_save(dict(data))


def period_series_to_hourly_kwh(
    *,
    target_period_starts: list[datetime],
    values_kwh: list[float],
) -> dict[datetime, float]:
    hourly: dict[datetime, float] = defaultdict(float)
    for start_ts, value in zip(target_period_starts, values_kwh):
        hour_start = start_ts.replace(minute=0, second=0, microsecond=0)
        hourly[hour_start] += max(0.0, float(value))
    return {hour: round(value, 4) for hour, value in hourly.items()}


def reconcile_solar_bias_store(
    data: dict[str, Any],
    *,
    now: datetime,
    actual_hourly_kwh: Mapping[datetime, float],
) -> bool:
    pending = data.setdefault("pending_forecasts", {})
    completed_hours = sorted(
        hour_start
        for hour_start in (_parse_hour(key) for key in pending.keys())
        if hour_start + timedelta(hours=1) <= now
    )
    if not completed_hours:
        return False

    changed = False
    for hour_start in completed_hours:
        if hour_start not in actual_hourly_kwh:
            continue
        entry = pending.pop(hour_start.isoformat(), None)
        if not isinstance(entry, dict):
            continue
        forecast_kwh = max(0.0, float(entry.get("forecast_kwh") or 0.0))
        actual_kwh = max(0.0, float(actual_hourly_kwh.get(hour_start, 0.0)))
        weather_bucket = str(entry.get("weather_bucket") or "other")
        _update_bucket_family(
            data,
            family="weather_hour",
            key=f"{weather_bucket}|{hour_start.hour:02d}",
            forecast_kwh=forecast_kwh,
            actual_kwh=actual_kwh,
            hour_start=hour_start,
        )
        _update_bucket_family(
            data,
            family="weather_month_hour",
            key=f"{weather_bucket}|{hour_start.month:02d}|{hour_start.hour:02d}",
            forecast_kwh=forecast_kwh,
            actual_kwh=actual_kwh,
            hour_start=hour_start,
        )
        _update_bucket_family(
            data,
            family="weather_week_hour",
            key=f"{weather_bucket}|{hour_start.isocalendar().week:02d}|{hour_start.hour:02d}",
            forecast_kwh=forecast_kwh,
            actual_kwh=actual_kwh,
            hour_start=hour_start,
        )
        data["last_reconciled_hour"] = hour_start.isoformat()
        changed = True

    return changed


def record_pending_solar_forecasts(
    data: dict[str, Any],
    *,
    captured_at: datetime,
    hourly_forecast_kwh: Mapping[datetime, float],
    hourly_weather_buckets: Mapping[datetime, str],
) -> bool:
    changed = False
    current_hour = captured_at.replace(minute=0, second=0, microsecond=0)
    pending = data.setdefault("pending_forecasts", {})

    for hour_start, forecast_kwh in hourly_forecast_kwh.items():
        if hour_start <= current_hour:
            continue
        entry = {
            "captured_at": captured_at.isoformat(),
            "forecast_kwh": round(max(0.0, float(forecast_kwh)), 4),
            "weather_bucket": str(hourly_weather_buckets.get(hour_start, "other")),
        }
        existing = pending.get(hour_start.isoformat())
        if not isinstance(existing, dict):
            pending[hour_start.isoformat()] = entry
            changed = True
            continue
        existing_captured_at = datetime.fromisoformat(str(existing["captured_at"]))
        if captured_at >= existing_captured_at:
            pending[hour_start.isoformat()] = entry
            changed = True

    return changed


def apply_solar_bias_correction(
    data: Mapping[str, Any],
    *,
    now: datetime,
    target_period_starts: list[datetime],
    raw_series_kwh: list[float],
    hourly_weather_buckets: Mapping[datetime, str],
) -> tuple[list[float], list[dict[str, Any]], dict[str, Any]]:
    corrected: list[float] = []
    period_debug: list[dict[str, Any]] = []
    source_counts: dict[str, int] = defaultdict(int)

    for start_ts, raw_value in zip(target_period_starts, raw_series_kwh):
        hour_start = start_ts.replace(minute=0, second=0, microsecond=0)
        weather_bucket = str(hourly_weather_buckets.get(hour_start, "other"))
        factor, source, observations = select_solar_bias_factor(
            data,
            now=now,
            hour_start=hour_start,
            weather_bucket=weather_bucket,
        )
        corrected_value = round(max(0.0, float(raw_value) * factor), 4)
        corrected.append(corrected_value)
        source_counts[source] += 1
        period_debug.append(
            {
                "raw_solar_forecast_kwh": round(max(0.0, float(raw_value)), 4),
                "corrected_solar_forecast_kwh": corrected_value,
                "solar_correction_factor": round(factor, 4),
                "solar_correction_source": source,
                "solar_weather_bucket": weather_bucket,
                "solar_correction_observations": observations,
            }
        )

    summary = {
        "week_bias_active": week_bias_enabled(data, now=now),
        "pending_forecast_hours": len(data.get("pending_forecasts", {})),
        "last_reconciled_hour": data.get("last_reconciled_hour"),
        "source_counts": dict(source_counts),
    }
    return corrected, period_debug, summary


def select_solar_bias_factor(
    data: Mapping[str, Any],
    *,
    now: datetime,
    hour_start: datetime,
    weather_bucket: str,
) -> tuple[float, str, int]:
    weather_hour_key = f"{weather_bucket}|{hour_start.hour:02d}"
    weather_month_hour_key = f"{weather_bucket}|{hour_start.month:02d}|{hour_start.hour:02d}"
    weather_week_hour_key = f"{weather_bucket}|{hour_start.isocalendar().week:02d}|{hour_start.hour:02d}"

    if week_bias_enabled(data, now=now):
        weekly = _resolved_bucket(
            data,
            family="weather_week_hour",
            key=weather_week_hour_key,
            min_observations=SOLAR_BIAS_WEEK_MIN_OBSERVATIONS,
        )
        if weekly is not None:
            return weekly[0], "weather_week_hour", weekly[1]

    monthly = _resolved_bucket(
        data,
        family="weather_month_hour",
        key=weather_month_hour_key,
        min_observations=SOLAR_BIAS_MONTH_MIN_OBSERVATIONS,
    )
    if monthly is not None:
        return monthly[0], "weather_month_hour", monthly[1]

    weather_only = _resolved_bucket(
        data,
        family="weather_hour",
        key=weather_hour_key,
        min_observations=SOLAR_BIAS_MONTH_MIN_OBSERVATIONS,
    )
    if weather_only is not None:
        return weather_only[0], "weather_hour", weather_only[1]

    return 1.0, "raw_provider", 0


def week_bias_enabled(data: Mapping[str, Any], *, now: datetime) -> bool:
    first_observation_at = data.get("first_observation_at")
    if not first_observation_at:
        return False
    return now - datetime.fromisoformat(str(first_observation_at)) >= timedelta(days=SOLAR_BIAS_WEEK_ACTIVATION_DAYS)


def _resolved_bucket(
    data: Mapping[str, Any],
    *,
    family: str,
    key: str,
    min_observations: int,
) -> tuple[float, int] | None:
    stats = data.get("stats", {}).get(family, {}).get(key)
    if not isinstance(stats, dict):
        return None
    observations = int(stats.get("observations", 0))
    if observations < min_observations:
        return None
    forecast_sum = float(stats.get("forecast_sum_kwh", 0.0))
    actual_sum = float(stats.get("actual_sum_kwh", 0.0))
    if forecast_sum <= 1e-6:
        return None
    factor = max(0.0, min(SOLAR_BIAS_MAX_FACTOR, actual_sum / forecast_sum))
    return round(factor, 4), observations


def _update_bucket_family(
    data: dict[str, Any],
    *,
    family: str,
    key: str,
    forecast_kwh: float,
    actual_kwh: float,
    hour_start: datetime,
) -> None:
    family_stats = data.setdefault("stats", {}).setdefault(family, {})
    bucket = family_stats.setdefault(
        key,
        {
            "observations": 0,
            "forecast_sum_kwh": 0.0,
            "actual_sum_kwh": 0.0,
            "delta_sum_kwh": 0.0,
        },
    )
    bucket["observations"] = int(bucket.get("observations", 0)) + 1
    bucket["forecast_sum_kwh"] = round(float(bucket.get("forecast_sum_kwh", 0.0)) + forecast_kwh, 4)
    bucket["actual_sum_kwh"] = round(float(bucket.get("actual_sum_kwh", 0.0)) + actual_kwh, 4)
    bucket["delta_sum_kwh"] = round(float(bucket.get("delta_sum_kwh", 0.0)) + (actual_kwh - forecast_kwh), 4)
    if not data.get("first_observation_at"):
        data["first_observation_at"] = hour_start.isoformat()


def _parse_hour(value: str) -> datetime:
    return datetime.fromisoformat(value)
