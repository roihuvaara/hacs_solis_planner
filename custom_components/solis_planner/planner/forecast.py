from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from statistics import mean

from .usage import UsageSample, encode_quarter_hour_profile


@dataclass(frozen=True)
class TemperatureSample:
    start_ts: datetime
    temperature_c: float


@dataclass(frozen=True)
class LoadForecastResult:
    load_forecast_by_period_kwh: list[float]
    packed_profile: str | None
    baseline_bucket_count: int
    weather_adjusted_bucket_count: int
    recent_residual_bucket_count: int
    recent_only_bucket_count: int
    missing_bucket_count: int


@dataclass(frozen=True)
class _BaselinePair:
    load_kwh: float
    temperature_c: float | None


def _floor_to_bucket(value: datetime, bucket_minutes: int) -> datetime:
    minute = (value.minute // bucket_minutes) * bucket_minutes
    return value.replace(minute=minute, second=0, microsecond=0)


def _day_start(value: datetime) -> datetime:
    localized = value.astimezone(value.tzinfo)
    return localized.replace(hour=0, minute=0, second=0, microsecond=0)


def _temperature_lookup(
    samples: list[TemperatureSample],
    bucket_minutes: int,
) -> dict[datetime, float]:
    lookup: dict[datetime, float] = {}
    for sample in samples:
        lookup[_floor_to_bucket(sample.start_ts, bucket_minutes)] = float(sample.temperature_c)
    return lookup


def _build_paired_samples(
    load_samples: list[UsageSample],
    temperature_by_bucket: dict[datetime, float],
    bucket_minutes: int,
) -> list[tuple[datetime, _BaselinePair]]:
    paired: list[tuple[datetime, _BaselinePair]] = []
    for sample in load_samples:
        bucket_start = _floor_to_bucket(sample.start_ts, bucket_minutes)
        paired.append(
            (
                bucket_start,
                _BaselinePair(
                    load_kwh=float(sample.kwh),
                    temperature_c=temperature_by_bucket.get(bucket_start),
                ),
            )
        )
    return paired


def _select_pairs(
    paired_samples: list[tuple[datetime, _BaselinePair]],
    *,
    minute_of_day: int,
    weekday: int | None = None,
) -> list[_BaselinePair]:
    selected: list[_BaselinePair] = []
    for bucket_start, pair in paired_samples:
        localized = bucket_start.astimezone(bucket_start.tzinfo)
        if localized.hour * 60 + localized.minute != minute_of_day:
            continue
        if weekday is not None and localized.weekday() != weekday:
            continue
        selected.append(pair)
    return selected


def _predict_load(pairs: list[_BaselinePair], target_temperature: float | None) -> float:
    if not pairs:
        return 0.0

    loads = [pair.load_kwh for pair in pairs]
    temperatures = [pair.temperature_c for pair in pairs if pair.temperature_c is not None]
    if target_temperature is None or len(temperatures) < 2 or len(set(temperatures)) < 2:
        return mean(loads)

    temp_mean = mean(temperatures)
    load_mean = mean(loads)
    numerator = 0.0
    denominator = 0.0
    for pair in pairs:
        if pair.temperature_c is None:
            continue
        numerator += (pair.temperature_c - temp_mean) * (pair.load_kwh - load_mean)
        denominator += (pair.temperature_c - temp_mean) ** 2
    if denominator == 0:
        return load_mean
    slope = numerator / denominator
    intercept = load_mean - slope * temp_mean
    return intercept + slope * target_temperature


def build_load_forecast_for_periods(
    *,
    target_period_starts: list[datetime],
    load_samples: list[UsageSample],
    historical_temperature_samples: list[TemperatureSample],
    future_temperature_samples: list[TemperatureSample],
    target_time: datetime,
    baseline_days: int = 30,
    recent_days: int = 7,
    bucket_minutes: int = 15,
) -> LoadForecastResult:
    if not target_period_starts:
        return LoadForecastResult([], None, 0, 0, 0, 0, 0)

    target_day_start = _day_start(target_time)
    recent_start = target_day_start - timedelta(days=recent_days)
    baseline_start = target_day_start - timedelta(days=baseline_days)

    historical_temperature_by_bucket = _temperature_lookup(
        historical_temperature_samples,
        bucket_minutes,
    )
    future_temperature_by_bucket = _temperature_lookup(
        future_temperature_samples,
        bucket_minutes,
    )

    baseline_pairs = _build_paired_samples(
        [
            sample
            for sample in load_samples
            if baseline_start <= _floor_to_bucket(sample.start_ts, bucket_minutes) < recent_start
        ],
        historical_temperature_by_bucket,
        bucket_minutes,
    )
    recent_pairs = _build_paired_samples(
        [
            sample
            for sample in load_samples
            if recent_start <= _floor_to_bucket(sample.start_ts, bucket_minutes) < target_day_start
        ],
        historical_temperature_by_bucket,
        bucket_minutes,
    )

    forecast_values: list[float] = []
    full_day_profile: dict[int, float] = {}
    baseline_bucket_count = 0
    weather_adjusted_bucket_count = 0
    recent_residual_bucket_count = 0
    recent_only_bucket_count = 0
    missing_bucket_count = 0

    for target_period in target_period_starts:
        localized = target_period.astimezone(target_period.tzinfo)
        minute_of_day = localized.hour * 60 + localized.minute
        weekday = localized.weekday()
        baseline_candidates = _select_pairs(
            baseline_pairs,
            minute_of_day=minute_of_day,
            weekday=weekday,
        )
        if not baseline_candidates:
            baseline_candidates = _select_pairs(
                baseline_pairs,
                minute_of_day=minute_of_day,
            )

        future_temperature = future_temperature_by_bucket.get(
            _floor_to_bucket(target_period, bucket_minutes)
        )
        base_prediction = None
        if baseline_candidates:
            baseline_bucket_count += 1
            if future_temperature is not None and any(
                pair.temperature_c is not None for pair in baseline_candidates
            ):
                weather_adjusted_bucket_count += 1
            base_prediction = _predict_load(baseline_candidates, future_temperature)

        recent_candidates = _select_pairs(
            recent_pairs,
            minute_of_day=minute_of_day,
        )
        residuals: list[float] = []
        if recent_candidates and baseline_candidates:
            for pair in recent_candidates:
                expected_recent = _predict_load(baseline_candidates, pair.temperature_c)
                residuals.append(pair.load_kwh - expected_recent)
            if residuals:
                recent_residual_bucket_count += 1

        if base_prediction is None and recent_candidates:
            recent_only_bucket_count += 1
            value = mean(pair.load_kwh for pair in recent_candidates)
        elif base_prediction is not None:
            value = base_prediction + (mean(residuals) if residuals else 0.0)
        else:
            missing_bucket_count += 1
            value = 0.0

        value = round(max(0.0, value), 4)
        forecast_values.append(value)
        full_day_profile[minute_of_day] = value

    packed_profile = None
    if len(full_day_profile) == 96 and all(index * 15 in full_day_profile for index in range(96)):
        packed_profile = encode_quarter_hour_profile(
            [full_day_profile[index * 15] for index in range(96)]
        )

    return LoadForecastResult(
        load_forecast_by_period_kwh=forecast_values,
        packed_profile=packed_profile,
        baseline_bucket_count=baseline_bucket_count,
        weather_adjusted_bucket_count=weather_adjusted_bucket_count,
        recent_residual_bucket_count=recent_residual_bucket_count,
        recent_only_bucket_count=recent_only_bucket_count,
        missing_bucket_count=missing_bucket_count,
    )
