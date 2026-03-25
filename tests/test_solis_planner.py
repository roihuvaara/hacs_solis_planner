from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from custom_components.solis_planner.bridge import plan_schedule_payload
from custom_components.solis_planner.planner.core import (
    PeriodDecision,
    PeriodPrice,
    PlannerInputs,
    SolisSlot,
    compile_periods_to_solis_slots,
    plan_solis_schedule,
)
from custom_components.solis_planner.planner.ha_adapter import (
    planner_inputs_from_hass_state,
    planner_result_to_hass_payload,
    run_planner_from_hass_state,
)
from custom_components.solis_planner.planner.forecast import (
    LoadForecastResult,
    TemperatureSample,
    build_load_forecast_for_periods,
)
from custom_components.solis_planner.planner.usage import (
    UsageSample,
    decode_usage_buckets,
    derive_rolling_usage_buckets,
    encode_usage_buckets,
)


TZ = ZoneInfo("Europe/Helsinki")


def dt(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=TZ)


def build_price_horizon(
    start: datetime,
    values: list[float],
) -> list[PeriodPrice]:
    return [
        PeriodPrice(start_ts=start + timedelta(minutes=15 * index), price_cents_per_kwh=value)
        for index, value in enumerate(values)
    ]


def build_usage_profile(default_kwh: float = 0.15) -> str:
    buckets = [
        {"start_minute_of_day": index * 15, "avg_kwh_per_15m": default_kwh}
        for index in range(96)
    ]
    for index in range(24, 40):
        buckets[index]["avg_kwh_per_15m"] = 0.35
    return json.dumps(buckets)


def build_compact_hourly_usage_profile(default_kwh_per_hour: float = 0.6) -> str:
    values = [default_kwh_per_hour] * 24
    for index in range(6, 10):
        values[index] = 1.4
    return ",".join(f"{value:.3f}" for value in values)


class PlannerCoreTests(unittest.TestCase):
    def make_inputs(
        self,
        *,
        solar_forecast_tomorrow_kwh: float,
        solar_forecast_by_period_kwh: list[float] | None = None,
        load_forecast_by_period_kwh: list[float] | None = None,
        price_values: list[float] | None = None,
        battery_soc_pct: float = 22.0,
        now: datetime | None = None,
        current_charge_slots: list[SolisSlot] | None = None,
        current_discharge_slots: list[SolisSlot] | None = None,
    ) -> PlannerInputs:
        now = now or dt("2026-03-24T00:15:00")
        price_horizon = build_price_horizon(
            now.replace(hour=0, minute=0, second=0, microsecond=0),
            price_values
            or [
                6.0, 5.0, 4.0, 4.0, 5.0, 6.0, 8.0, 10.0,
                12.0, 14.0, 16.0, 20.0, 22.0, 24.0, 26.0, 28.0,
                30.0, 32.0, 34.0, 30.0, 26.0, 22.0, 18.0, 15.0,
                13.0, 12.0, 11.0, 10.0, 9.0, 8.0, 7.0, 6.0,
            ],
        )
        usage_buckets = decode_usage_buckets(build_usage_profile())
        return PlannerInputs(
            now=now,
            battery_soc_pct=battery_soc_pct,
            battery_capacity_kwh=12.0,
            usable_battery_kwh=10.0,
            reserve_soc_pct=18.0,
            max_charge_current_setting=40,
            solar_forecast_tomorrow_kwh=solar_forecast_tomorrow_kwh,
            solar_forecast_by_period_kwh=solar_forecast_by_period_kwh,
            load_forecast_by_period_kwh=load_forecast_by_period_kwh,
            price_horizon=price_horizon,
            rolling_usage_7d=usage_buckets,
            current_charge_slots=current_charge_slots or [],
            current_discharge_slots=current_discharge_slots or [],
        )

    def test_march_24_regression_keeps_single_charge_cycle_and_stable_hold(self) -> None:
        inputs = self.make_inputs(
            solar_forecast_tomorrow_kwh=2.0,
            solar_forecast_by_period_kwh=[
                0.0 if period.start_ts.hour < 6 else 0.03
                for period in build_price_horizon(dt("2026-03-24T00:00:00"), [0.0] * 32)
            ],
        )

        result = plan_solis_schedule(inputs)
        charge_periods = [decision for decision in result.period_plan if decision.strategy == "charge"]

        self.assertGreater(len(charge_periods), 0)
        self.assertEqual(
            1,
            sum(
                1
                for previous, current in zip(charge_periods, charge_periods[1:])
                if current.start_ts - previous.start_ts > timedelta(minutes=15)
            ) + 1,
        )

    def test_same_inputs_produce_same_future_plan(self) -> None:
        inputs = self.make_inputs(
            solar_forecast_tomorrow_kwh=4.0,
            solar_forecast_by_period_kwh=[0.04] * 32,
        )

        first = plan_solis_schedule(inputs)
        second = plan_solis_schedule(inputs)

        self.assertEqual(first.period_plan, second.period_plan)
        self.assertEqual(first.charge_slots, second.charge_slots)

    def test_explicit_load_forecast_takes_precedence_over_rolling_profile(self) -> None:
        now = dt("2026-03-24T05:45:00")
        inputs = self.make_inputs(
            solar_forecast_tomorrow_kwh=0.0,
            now=now,
            load_forecast_by_period_kwh=[
                *([0.0] * 24),
                0.6, 0.6, 0.6, 0.6,
                *([0.0] * 12),
            ],
            price_values=[
                *([8.0] * 24),
                30.0, 30.0, 30.0, 30.0,
                *([10.0] * 12),
            ],
        )

        result = plan_solis_schedule(inputs)

        self.assertAlmostEqual(2.4, result.expected_morning_load_kwh)

    def test_charge_slots_use_configured_max_charge_current(self) -> None:
        inputs = self.make_inputs(
            solar_forecast_tomorrow_kwh=0.0,
            solar_forecast_by_period_kwh=[0.0] * 32,
        )
        inputs = PlannerInputs(
            **{
                **inputs.__dict__,
                "max_charge_current_setting": 25,
            }
        )

        result = plan_solis_schedule(inputs)
        first_charge_slot = next(slot for slot in result.charge_slots if slot.enabled)

        self.assertEqual(25, first_charge_slot.current)


class LoadForecastTests(unittest.TestCase):
    def test_build_load_forecast_applies_weather_adjusted_baseline_and_recent_residual(self) -> None:
        target_periods = [dt("2026-03-30T06:00:00")]
        load_samples = [
            UsageSample(start_ts=dt("2026-03-09T06:00:00"), kwh=1.0),
            UsageSample(start_ts=dt("2026-03-16T06:00:00"), kwh=2.0),
            UsageSample(start_ts=dt("2026-03-23T06:00:00"), kwh=2.25),
        ]
        temperature_samples = [
            TemperatureSample(start_ts=dt("2026-03-09T06:00:00"), temperature_c=10.0),
            TemperatureSample(start_ts=dt("2026-03-16T06:00:00"), temperature_c=0.0),
            TemperatureSample(start_ts=dt("2026-03-23T06:00:00"), temperature_c=0.0),
        ]
        future_temperatures = [
            TemperatureSample(start_ts=dt("2026-03-30T06:00:00"), temperature_c=0.0),
        ]

        result = build_load_forecast_for_periods(
            target_period_starts=target_periods,
            load_samples=load_samples,
            historical_temperature_samples=temperature_samples,
            future_temperature_samples=future_temperatures,
            target_time=dt("2026-03-30T00:00:00"),
            baseline_days=14,
            recent_days=7,
            bucket_minutes=15,
        )

        self.assertIsInstance(result, LoadForecastResult)
        self.assertEqual([2.25], result.load_forecast_by_period_kwh)
        self.assertEqual(1, result.weather_adjusted_bucket_count)
        self.assertEqual(1, result.recent_residual_bucket_count)


class BridgeTests(unittest.TestCase):
    def test_bridge_plans_schedule_from_payload(self) -> None:
        payload = {
            "now": "2026-03-24T00:15:00+02:00",
            "battery_soc_pct": "22",
            "battery_capacity_kwh": "12",
            "usable_battery_kwh": "10",
            "reserve_soc_pct": "18",
            "max_charge_current_setting": "40",
            "solar_forecast_tomorrow_kwh": "3.5",
            "solar_forecast_by_period_kwh": json.dumps([0.0] * 32),
            "price_horizon": json.dumps(
                [
                    {"start_ts": "2026-03-24T00:00:00+02:00", "price_cents_per_kwh": 6.0},
                    {"start_ts": "2026-03-24T00:15:00+02:00", "price_cents_per_kwh": 5.0},
                    {"start_ts": "2026-03-24T00:30:00+02:00", "price_cents_per_kwh": 4.0},
                    {"start_ts": "2026-03-24T00:45:00+02:00", "price_cents_per_kwh": 4.0},
                    {"start_ts": "2026-03-24T01:00:00+02:00", "price_cents_per_kwh": 5.0},
                    {"start_ts": "2026-03-24T06:00:00+02:00", "price_cents_per_kwh": 28.0}
                ]
            ),
            "rolling_usage_7d": build_usage_profile(),
            "current_charge_slots": json.dumps([]),
            "current_discharge_slots": json.dumps([]),
        }

        result = plan_schedule_payload(payload)

        self.assertIn("charge_slots", result)
        self.assertIn("debug_summary", result)

    def test_bridge_accepts_compact_hourly_usage_profile(self) -> None:
        payload = {
            "now": "2026-03-24T00:15:00+02:00",
            "battery_soc_pct": "22",
            "battery_capacity_kwh": "12",
            "usable_battery_kwh": "10",
            "reserve_soc_pct": "18",
            "max_charge_current_setting": "40",
            "solar_forecast_tomorrow_kwh": "3.5",
            "solar_forecast_by_period_kwh": json.dumps([0.0] * 32),
            "price_horizon": json.dumps(
                [
                    {"start_ts": "2026-03-24T00:00:00+02:00", "price_cents_per_kwh": 6.0},
                    {"start_ts": "2026-03-24T00:15:00+02:00", "price_cents_per_kwh": 5.0},
                    {"start_ts": "2026-03-24T00:30:00+02:00", "price_cents_per_kwh": 4.0},
                    {"start_ts": "2026-03-24T00:45:00+02:00", "price_cents_per_kwh": 4.0},
                    {"start_ts": "2026-03-24T01:00:00+02:00", "price_cents_per_kwh": 5.0},
                    {"start_ts": "2026-03-24T06:00:00+02:00", "price_cents_per_kwh": 28.0}
                ]
            ),
            "rolling_usage_7d": build_compact_hourly_usage_profile(),
            "current_charge_slots": json.dumps([]),
            "current_discharge_slots": json.dumps([]),
        }

        result = planner_inputs_from_hass_state(payload)

        self.assertEqual(96, len(result.rolling_usage_7d))
        self.assertEqual(0.15, result.rolling_usage_7d[0].avg_kwh_per_15m)
        self.assertEqual(0.35, result.rolling_usage_7d[24].avg_kwh_per_15m)


if __name__ == "__main__":
    unittest.main()
