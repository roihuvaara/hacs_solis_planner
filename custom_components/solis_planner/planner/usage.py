from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime

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


def decode_usage_buckets(payload: str) -> list[UsageBucket]:
    raw_buckets = json.loads(payload or "[]")
    return [
        UsageBucket(
            start_minute_of_day=int(item["start_minute_of_day"]),
            avg_kwh_per_15m=float(item["avg_kwh_per_15m"]),
        )
        for item in raw_buckets
    ]
