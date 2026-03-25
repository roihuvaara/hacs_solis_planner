from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .core import UsageBucket


@dataclass(frozen=True)
class UsageSample:
    start_ts: datetime
    kwh: float


def derive_rolling_usage_buckets(
    samples: list[UsageSample],
    bucket_minutes: int = 15,
) -> list[UsageBucket]:
    totals: dict[int, float] = {}
    counts: dict[int, int] = {}

    for sample in samples:
        localized = sample.start_ts.astimezone(sample.start_ts.tzinfo)
        minute_of_day = localized.hour * 60 + localized.minute
        bucket_minute = (minute_of_day // bucket_minutes) * bucket_minutes
        totals[bucket_minute] = totals.get(bucket_minute, 0.0) + sample.kwh
        counts[bucket_minute] = counts.get(bucket_minute, 0) + 1

    buckets = []
    for index in range((24 * 60) // bucket_minutes):
        bucket_minute = index * bucket_minutes
        average = totals.get(bucket_minute, 0.0) / max(counts.get(bucket_minute, 0), 1)
        buckets.append(
            UsageBucket(
                start_minute_of_day=bucket_minute,
                avg_kwh_per_15m=round(average, 4),
            )
        )
    return buckets


def encode_usage_buckets(buckets: list[UsageBucket]) -> str:
    return json.dumps(
        [
            {
                "start_minute_of_day": bucket.start_minute_of_day,
                "avg_kwh_per_15m": bucket.avg_kwh_per_15m,
            }
            for bucket in buckets
        ]
    )


def _expand_numeric_profile(values: list[float]) -> list[UsageBucket]:
    if len(values) == 24:
        expanded = [hourly_value / 4.0 for hourly_value in values for _ in range(4)]
    elif len(values) == 96:
        expanded = values
    else:
        raise ValueError("rolling_usage_7d must contain 24 hourly values or 96 quarter-hour values")

    return [
        UsageBucket(
            start_minute_of_day=index * 15,
            avg_kwh_per_15m=round(float(value), 4),
        )
        for index, value in enumerate(expanded)
    ]


def _load_usage_payload(payload: str | list[Any]) -> list[Any]:
    if isinstance(payload, list):
        return payload

    raw_payload = (payload or "").strip()
    if not raw_payload:
        return []
    if raw_payload.startswith("["):
        return json.loads(raw_payload)
    return [float(part) for part in raw_payload.split(",") if part]


def decode_usage_buckets(payload: str | list[Any]) -> list[UsageBucket]:
    raw_buckets = _load_usage_payload(payload)
    if raw_buckets and not isinstance(raw_buckets[0], dict):
        return _expand_numeric_profile([float(item) for item in raw_buckets])

    return [
        UsageBucket(
            start_minute_of_day=int(item["start_minute_of_day"]),
            avg_kwh_per_15m=float(item["avg_kwh_per_15m"]),
        )
        for item in raw_buckets
    ]
