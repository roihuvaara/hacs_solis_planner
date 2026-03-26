from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from math import ceil, sqrt
from typing import Literal


DISABLED_SLOT = {"time": "00:00-00:00", "enabled": False, "current": 0, "soc": 19}
STATE_STEP_KWH = 0.1
BATTERY_NOMINAL_VOLTAGE = 50.0
PERIOD_HOURS = 0.25
ROUND_TRIP_EFFICIENCY = 0.9
CHARGE_EFFICIENCY = sqrt(ROUND_TRIP_EFFICIENCY)
DISCHARGE_EFFICIENCY = sqrt(ROUND_TRIP_EFFICIENCY)
SAFE_PLANNED_CHARGE_CURRENT_SETTING = 12


@dataclass(frozen=True)
class PeriodPrice:
    start_ts: datetime
    price_cents_per_kwh: float


@dataclass(frozen=True)
class UsageBucket:
    start_minute_of_day: int
    avg_kwh_per_15m: float


@dataclass(frozen=True)
class SolisSlot:
    time: str
    enabled: bool
    current: int
    soc: int


@dataclass(frozen=True)
class PlannerInputs:
    now: datetime
    battery_soc_pct: float
    battery_capacity_kwh: float
    usable_battery_kwh: float
    reserve_soc_pct: float
    max_charge_current_setting: int
    max_discharge_current_setting: int
    solar_forecast_tomorrow_kwh: float
    solar_forecast_by_period_kwh: list[float] | None
    load_forecast_by_period_kwh: list[float] | None
    price_horizon: list[PeriodPrice]
    rolling_usage_7d: list[UsageBucket]
    current_charge_slots: list[SolisSlot]
    current_discharge_slots: list[SolisSlot]


@dataclass(frozen=True)
class PeriodDecision:
    start_ts: datetime
    strategy: Literal["charge", "hold", "self_use"]
    target_soc_pct: int | None
    hold_soc_pct: int | None
    reason: str
    priority_score: float = 0.0


@dataclass(frozen=True)
class ForecastPeriod:
    start_ts: datetime
    price_cents_per_kwh: float
    load_forecast_kwh: float
    solar_forecast_kwh: float
    net_import_without_battery_kwh: float
    planned_action: Literal["charge", "hold", "self_use"]
    battery_start_kwh: float
    battery_end_kwh: float
    planned_grid_import_kwh: float
    planned_charge_kwh: float
    planned_discharge_kwh: float


@dataclass(frozen=True)
class PlannerResult:
    period_plan: list[PeriodDecision]
    charge_slots: list[SolisSlot]
    discharge_slots: list[SolisSlot]
    target_soc_pct: int
    hold_soc_pct: int
    morning_value_window_start: datetime | None
    morning_value_window_end: datetime | None
    expected_morning_load_kwh: float
    expected_morning_solar_kwh: float
    debug_status: str
    debug_summary: str
    forecast_periods: list[ForecastPeriod]
    forecast_total_grid_import_kwh: float
    end_battery_kwh: float
    end_battery_soc_pct: float
    total_planned_grid_charge_kwh: float


@dataclass(frozen=True)
class HorizonPeriod:
    start_ts: datetime
    price_cents_per_kwh: float
    load_forecast_kwh: float
    solar_forecast_kwh: float
    net_import_without_battery_kwh: float
    solar_surplus_kwh: float
    future_peak_price_cents: float


def floor_to_period(value: datetime) -> datetime:
    minute = (value.minute // 15) * 15
    return value.replace(minute=minute, second=0, microsecond=0)


def slot_time_range(slot: SolisSlot, reference: datetime) -> tuple[datetime, datetime]:
    start_raw, end_raw = slot.time.split("-")
    start_hour, start_minute = [int(part) for part in start_raw.split(":")]
    end_hour, end_minute = [int(part) for part in end_raw.split(":")]
    day = reference.date()
    start = datetime.combine(day, datetime.min.time(), tzinfo=reference.tzinfo).replace(
        hour=start_hour,
        minute=start_minute,
    )
    end = datetime.combine(day, datetime.min.time(), tzinfo=reference.tzinfo).replace(
        hour=end_hour,
        minute=end_minute,
    )
    if end <= start:
        end += timedelta(days=1)
    return start, end


def slot_overlaps_period(slot: SolisSlot, period_start: datetime) -> bool:
    if not slot.enabled:
        return False
    if slot.time == "00:00-00:00":
        return False
    slot_start, slot_end = slot_time_range(slot, period_start)
    period_end = period_start + timedelta(minutes=15)
    return slot_start < period_end and slot_end > period_start


def detect_live_strategy(inputs: PlannerInputs, current_period: datetime) -> tuple[str, int | None]:
    for slot in inputs.current_charge_slots:
        if slot_overlaps_period(slot, current_period):
            return "charge", slot.soc
    for slot in inputs.current_discharge_slots:
        if slot_overlaps_period(slot, current_period):
            return "hold", slot.soc
    return "self_use", None


def usage_lookup(rolling_usage_7d: list[UsageBucket]) -> dict[int, float]:
    return {
        bucket.start_minute_of_day: bucket.avg_kwh_per_15m
        for bucket in rolling_usage_7d
    }


def solar_forecast_series(inputs: PlannerInputs) -> list[float]:
    if inputs.solar_forecast_by_period_kwh:
        return list(inputs.solar_forecast_by_period_kwh)
    if not inputs.price_horizon:
        return []

    periods = len(inputs.price_horizon)
    total_weight = 0.0
    weights: list[float] = []
    for period in inputs.price_horizon:
        hour = period.start_ts.astimezone(inputs.now.tzinfo).hour
        if 6 <= hour < 18:
            distance = abs(12 - hour)
            weight = max(0.2, 1.0 - distance / 8.0)
        else:
            weight = 0.0
        weights.append(weight)
        total_weight += weight

    if total_weight == 0:
        return [0.0] * periods
    return [inputs.solar_forecast_tomorrow_kwh * weight / total_weight for weight in weights]


def clamp(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(maximum, value))


def clamp_float(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def reserve_floor_pct(inputs: PlannerInputs) -> int:
    return max(DISABLED_SLOT["soc"], ceil(inputs.reserve_soc_pct))


def max_buffer_kwh(inputs: PlannerInputs) -> float:
    reserve_pct = reserve_floor_pct(inputs)
    return max(0.0, inputs.usable_battery_kwh * (100 - reserve_pct) / 100.0)


def buffer_kwh_from_soc_pct(inputs: PlannerInputs, soc_pct: float) -> float:
    reserve_pct = reserve_floor_pct(inputs)
    return clamp_float(
        inputs.usable_battery_kwh * (soc_pct - reserve_pct) / 100.0,
        0.0,
        max_buffer_kwh(inputs),
    )


def soc_pct_from_buffer_kwh(inputs: PlannerInputs, buffer_kwh: float) -> float:
    reserve_pct = reserve_floor_pct(inputs)
    usable = max(inputs.usable_battery_kwh, 0.1)
    return clamp_float(
        reserve_pct + (buffer_kwh / usable) * 100.0,
        float(reserve_pct),
        100.0,
    )


def soc_target_pct_from_buffer_kwh(inputs: PlannerInputs, buffer_kwh: float) -> int:
    reserve_pct = reserve_floor_pct(inputs)
    usable = max(inputs.usable_battery_kwh, 0.1)
    return clamp(
        ceil(reserve_pct + (buffer_kwh / usable) * 100.0),
        reserve_pct,
        100,
    )


def battery_kwh_from_buffer_kwh(inputs: PlannerInputs, buffer_kwh: float) -> float:
    return round((soc_pct_from_buffer_kwh(inputs, buffer_kwh) / 100.0) * inputs.usable_battery_kwh, 3)


def current_setting_to_period_kwh(current_setting: int) -> float:
    return max(0.0, current_setting * BATTERY_NOMINAL_VOLTAGE * PERIOD_HOURS / 1000.0)


def planned_charge_current_setting(inputs: PlannerInputs) -> int:
    return max(1, min(inputs.max_charge_current_setting, SAFE_PLANNED_CHARGE_CURRENT_SETTING))


def build_horizon_periods(inputs: PlannerInputs) -> list[HorizonPeriod]:
    horizon = sorted(inputs.price_horizon, key=lambda period: period.start_ts)
    current_period = floor_to_period(inputs.now)
    future_periods = [period for period in horizon if period.start_ts >= current_period]
    if not future_periods:
        return []

    usage_by_bucket = usage_lookup(inputs.rolling_usage_7d)
    solar_by_period = solar_forecast_series(inputs)
    horizon_index = {period.start_ts: index for index, period in enumerate(horizon)}
    raw_periods: list[tuple[PeriodPrice, float, float, float, float]] = []

    for period in future_periods:
        index = horizon_index[period.start_ts]
        if inputs.load_forecast_by_period_kwh and index < len(inputs.load_forecast_by_period_kwh):
            load_kwh = max(0.0, float(inputs.load_forecast_by_period_kwh[index]))
        else:
            minute_of_day = period.start_ts.astimezone(inputs.now.tzinfo).hour * 60 + period.start_ts.minute
            load_kwh = max(0.0, usage_by_bucket.get(minute_of_day, 0.0))
        solar_kwh = max(0.0, float(solar_by_period[index])) if index < len(solar_by_period) else 0.0
        net_import = max(0.0, load_kwh - solar_kwh)
        solar_surplus = max(0.0, solar_kwh - load_kwh)
        raw_periods.append((period, load_kwh, solar_kwh, net_import, solar_surplus))

    future_peak_price = 0.0
    periods: list[HorizonPeriod] = []
    for period, load_kwh, solar_kwh, net_import, solar_surplus in reversed(raw_periods):
        if net_import > 0:
            future_peak_price = max(future_peak_price, period.price_cents_per_kwh)
        periods.append(
            HorizonPeriod(
                start_ts=period.start_ts,
                price_cents_per_kwh=period.price_cents_per_kwh,
                load_forecast_kwh=load_kwh,
                solar_forecast_kwh=solar_kwh,
                net_import_without_battery_kwh=net_import,
                solar_surplus_kwh=solar_surplus,
                future_peak_price_cents=future_peak_price,
            )
        )

    return list(reversed(periods))


def quantize_units(value_kwh: float, max_units: int) -> int:
    return clamp(int(round(value_kwh / STATE_STEP_KWH)), 0, max_units)


def tie_breaker(action: str, delivered_kwh: float = 0.0, charge_kwh: float = 0.0) -> int:
    if action == "self_use" and delivered_kwh > 0:
        return 0
    if action == "hold":
        return 1
    if action == "charge" and charge_kwh > 0:
        return 2
    return 3


def optimize_horizon(
    inputs: PlannerInputs,
    periods: list[HorizonPeriod],
) -> tuple[list[list[tuple[str, int]]], int]:
    capacity_kwh = max_buffer_kwh(inputs)
    max_units = max(0, quantize_units(capacity_kwh, int(round(capacity_kwh / STATE_STEP_KWH))))
    state_values = [round(index * STATE_STEP_KWH, 3) for index in range(max_units + 1)]
    current_units = quantize_units(buffer_kwh_from_soc_pct(inputs, inputs.battery_soc_pct), max_units)
    charge_limit_kwh = current_setting_to_period_kwh(inputs.max_charge_current_setting)
    charge_limit_kwh = current_setting_to_period_kwh(planned_charge_current_setting(inputs))
    discharge_limit_kwh = current_setting_to_period_kwh(inputs.max_discharge_current_setting)

    dp: list[list[float]] = [[0.0] * (max_units + 1) for _ in range(len(periods) + 1)]
    policy: list[list[tuple[str, int]]] = [[("hold", 0)] * (max_units + 1) for _ in range(len(periods))]

    for period_index in range(len(periods) - 1, -1, -1):
        period = periods[period_index]
        for state_units, state_kwh in enumerate(state_values):
            best_cost = float("inf")
            best_action = "hold"
            best_next_units = state_units

            hold_buffer_kwh = min(capacity_kwh, state_kwh + period.solar_surplus_kwh * CHARGE_EFFICIENCY)
            hold_next_units = quantize_units(hold_buffer_kwh, max_units)
            hold_cost = (
                period.net_import_without_battery_kwh * period.price_cents_per_kwh
                + dp[period_index + 1][hold_next_units]
            )
            best_cost = hold_cost
            best_action = "hold"
            best_next_units = hold_next_units

            discharge_from_battery_kwh = min(
                state_kwh,
                discharge_limit_kwh,
                period.net_import_without_battery_kwh / DISCHARGE_EFFICIENCY if period.net_import_without_battery_kwh > 0 else 0.0,
            )
            delivered_to_load_kwh = discharge_from_battery_kwh * DISCHARGE_EFFICIENCY
            self_use_buffer_kwh = min(
                capacity_kwh,
                state_kwh - discharge_from_battery_kwh + period.solar_surplus_kwh * CHARGE_EFFICIENCY,
            )
            self_use_next_units = quantize_units(self_use_buffer_kwh, max_units)
            self_use_cost = (
                max(0.0, period.net_import_without_battery_kwh - delivered_to_load_kwh) * period.price_cents_per_kwh
                + dp[period_index + 1][self_use_next_units]
            )
            if (
                self_use_cost < best_cost - 1e-9
                or (
                    abs(self_use_cost - best_cost) <= 1e-9
                    and tie_breaker("self_use", delivered_to_load_kwh) < tie_breaker(best_action)
                )
            ):
                best_cost = self_use_cost
                best_action = "self_use"
                best_next_units = self_use_next_units

            base_charge_buffer_kwh = min(capacity_kwh, state_kwh + period.solar_surplus_kwh * CHARGE_EFFICIENCY)
            max_grid_charge_kwh = min(
                charge_limit_kwh,
                max(0.0, (capacity_kwh - base_charge_buffer_kwh) / CHARGE_EFFICIENCY),
            )
            max_charge_buffer_kwh = min(capacity_kwh, base_charge_buffer_kwh + max_grid_charge_kwh * CHARGE_EFFICIENCY)
            min_charge_units = quantize_units(base_charge_buffer_kwh, max_units)
            max_charge_units = quantize_units(max_charge_buffer_kwh, max_units)

            for charge_next_units in range(min_charge_units, max_charge_units + 1):
                next_buffer_kwh = state_values[charge_next_units]
                grid_charge_kwh = max(0.0, (next_buffer_kwh - base_charge_buffer_kwh) / CHARGE_EFFICIENCY)
                if grid_charge_kwh <= 1e-9:
                    continue
                charge_cost = (
                    (period.net_import_without_battery_kwh + grid_charge_kwh) * period.price_cents_per_kwh
                    + dp[period_index + 1][charge_next_units]
                )
                if (
                    charge_cost < best_cost - 1e-9
                    or (
                        abs(charge_cost - best_cost) <= 1e-9
                        and tie_breaker("charge", charge_kwh=grid_charge_kwh) < tie_breaker(best_action)
                    )
                ):
                    best_cost = charge_cost
                    best_action = "charge"
                    best_next_units = charge_next_units

            dp[period_index][state_units] = best_cost
            policy[period_index][state_units] = (best_action, best_next_units)

    return policy, current_units


def simulate_period(
    *,
    inputs: PlannerInputs,
    period: HorizonPeriod,
    action: Literal["charge", "hold", "self_use"],
    state_units: int,
    desired_next_units: int,
    max_units: int,
) -> tuple[ForecastPeriod, int]:
    state_kwh = round(state_units * STATE_STEP_KWH, 3)
    capacity_kwh = max_buffer_kwh(inputs)
    charge_limit_kwh = current_setting_to_period_kwh(inputs.max_charge_current_setting)
    discharge_limit_kwh = current_setting_to_period_kwh(inputs.max_discharge_current_setting)
    passive_solar_gain_kwh = period.solar_surplus_kwh * CHARGE_EFFICIENCY

    planned_grid_import_kwh = period.net_import_without_battery_kwh
    planned_charge_kwh = 0.0
    planned_discharge_kwh = 0.0
    next_buffer_kwh = state_kwh

    if action == "self_use":
        discharge_from_battery_kwh = min(
            state_kwh,
            discharge_limit_kwh,
            period.net_import_without_battery_kwh / DISCHARGE_EFFICIENCY if period.net_import_without_battery_kwh > 0 else 0.0,
        )
        planned_discharge_kwh = discharge_from_battery_kwh * DISCHARGE_EFFICIENCY
        planned_grid_import_kwh = max(0.0, period.net_import_without_battery_kwh - planned_discharge_kwh)
        next_buffer_kwh = min(capacity_kwh, state_kwh - discharge_from_battery_kwh + passive_solar_gain_kwh)
    elif action == "charge":
        base_charge_buffer_kwh = min(capacity_kwh, state_kwh + passive_solar_gain_kwh)
        minimum_units = quantize_units(base_charge_buffer_kwh, max_units)
        maximum_units = quantize_units(
            min(
                capacity_kwh,
                base_charge_buffer_kwh + min(
                    charge_limit_kwh,
                    max(0.0, (capacity_kwh - base_charge_buffer_kwh) / CHARGE_EFFICIENCY),
                )
                * CHARGE_EFFICIENCY,
            ),
            max_units,
        )
        constrained_next_units = clamp(desired_next_units, minimum_units, maximum_units)
        next_buffer_kwh = round(constrained_next_units * STATE_STEP_KWH, 3)
        planned_charge_kwh = max(0.0, (next_buffer_kwh - base_charge_buffer_kwh) / CHARGE_EFFICIENCY)
        planned_grid_import_kwh = period.net_import_without_battery_kwh + planned_charge_kwh
    else:
        next_buffer_kwh = min(capacity_kwh, state_kwh + passive_solar_gain_kwh)

    next_units = quantize_units(next_buffer_kwh, max_units)
    forecast_period = ForecastPeriod(
        start_ts=period.start_ts,
        price_cents_per_kwh=round(period.price_cents_per_kwh, 4),
        load_forecast_kwh=round(period.load_forecast_kwh, 3),
        solar_forecast_kwh=round(period.solar_forecast_kwh, 3),
        net_import_without_battery_kwh=round(period.net_import_without_battery_kwh, 3),
        planned_action=action,
        battery_start_kwh=battery_kwh_from_buffer_kwh(inputs, state_kwh),
        battery_end_kwh=battery_kwh_from_buffer_kwh(inputs, next_buffer_kwh),
        planned_grid_import_kwh=round(planned_grid_import_kwh, 3),
        planned_charge_kwh=round(planned_charge_kwh, 3),
        planned_discharge_kwh=round(planned_discharge_kwh, 3),
    )
    return forecast_period, next_units


def build_forecast_from_policy(
    inputs: PlannerInputs,
    periods: list[HorizonPeriod],
    policy: list[list[tuple[str, int]]],
    current_units: int,
) -> list[ForecastPeriod]:
    if not periods:
        return []

    max_units = len(policy[0]) - 1 if policy else quantize_units(max_buffer_kwh(inputs), 1)
    current_period = floor_to_period(inputs.now)
    live_strategy, live_soc = detect_live_strategy(inputs, current_period)
    state_units = current_units
    forecast_periods: list[ForecastPeriod] = []

    for period_index, period in enumerate(periods):
        planned_action, desired_next_units = policy[period_index][state_units]
        if period_index == 0 and live_strategy in {"charge", "hold"} and live_strategy != planned_action:
            planned_action = live_strategy  # type: ignore[assignment]
            if live_strategy == "charge" and live_soc is not None:
                live_units = quantize_units(buffer_kwh_from_soc_pct(inputs, live_soc), max_units)
                desired_next_units = max(desired_next_units, live_units)
            else:
                desired_next_units = state_units

        forecast_period, state_units = simulate_period(
            inputs=inputs,
            period=period,
            action=planned_action,  # type: ignore[arg-type]
            state_units=state_units,
            desired_next_units=desired_next_units,
            max_units=max_units,
        )
        forecast_periods.append(forecast_period)

    return forecast_periods


def morning_summary_metrics(
    inputs: PlannerInputs,
    forecast_periods: list[ForecastPeriod],
) -> tuple[datetime | None, datetime | None, float, float]:
    morning_periods = [
        period
        for period in forecast_periods
        if 6 <= period.start_ts.astimezone(inputs.now.tzinfo).hour < 10
    ]
    if not morning_periods:
        return None, None, 0.0, 0.0

    discharge_periods = [
        period
        for period in morning_periods
        if period.planned_action == "self_use" and period.planned_discharge_kwh > 0
    ]
    start = discharge_periods[0].start_ts if discharge_periods else None
    end = discharge_periods[-1].start_ts + timedelta(minutes=15) if discharge_periods else None
    expected_load = round(sum(period.load_forecast_kwh for period in morning_periods), 3)
    expected_solar = round(sum(period.solar_forecast_kwh for period in morning_periods), 3)
    return start, end, expected_load, expected_solar


def decision_from_forecast(
    inputs: PlannerInputs,
    period: HorizonPeriod,
    forecast_period: ForecastPeriod,
) -> PeriodDecision:
    target_soc_pct: int | None = None
    hold_soc_pct: int | None = None
    priority_score = 0.0

    future_value_gap = max(0.0, period.future_peak_price_cents - period.price_cents_per_kwh)
    if forecast_period.planned_action == "charge":
        battery_end_buffer_kwh = buffer_kwh_from_soc_pct(
            inputs,
            (forecast_period.battery_end_kwh / max(inputs.usable_battery_kwh, 0.1)) * 100.0,
        )
        target_soc_pct = soc_target_pct_from_buffer_kwh(inputs, battery_end_buffer_kwh)
        priority_score = forecast_period.planned_charge_kwh * max(0.0, future_value_gap)
        reason = "grid-charge during a lower-value period to preserve battery for higher-value later periods"
    elif forecast_period.planned_action == "hold":
        battery_start_buffer_kwh = buffer_kwh_from_soc_pct(
            inputs,
            (forecast_period.battery_start_kwh / max(inputs.usable_battery_kwh, 0.1)) * 100.0,
        )
        hold_soc_pct = soc_target_pct_from_buffer_kwh(inputs, battery_start_buffer_kwh)
        priority_score = period.net_import_without_battery_kwh * future_value_gap
        reason = "hold battery because later periods have higher avoided-import value"
    else:
        priority_score = forecast_period.planned_discharge_kwh * period.price_cents_per_kwh
        if forecast_period.planned_discharge_kwh > 0:
            reason = "use battery in a higher-value import period"
        else:
            reason = "self-use with no protected future value in this period"

    return PeriodDecision(
        start_ts=period.start_ts,
        strategy=forecast_period.planned_action,
        target_soc_pct=target_soc_pct,
        hold_soc_pct=hold_soc_pct,
        reason=reason,
        priority_score=round(priority_score, 3),
    )


def plan_solis_schedule(inputs: PlannerInputs) -> PlannerResult:
    horizon_periods = build_horizon_periods(inputs)
    if not horizon_periods:
        reserve_pct = reserve_floor_pct(inputs)
        empty_charge_slots = [SolisSlot(**DISABLED_SLOT) for _ in range(6)]
        empty_discharge_slots = [SolisSlot(**DISABLED_SLOT) for _ in range(6)]
        return PlannerResult(
            period_plan=[],
            charge_slots=empty_charge_slots,
            discharge_slots=empty_discharge_slots,
            target_soc_pct=reserve_pct,
            hold_soc_pct=reserve_pct,
            morning_value_window_start=None,
            morning_value_window_end=None,
            expected_morning_load_kwh=0.0,
            expected_morning_solar_kwh=0.0,
            debug_status="ok",
            debug_summary="No future price periods available.",
            forecast_periods=[],
            forecast_total_grid_import_kwh=0.0,
            end_battery_kwh=battery_kwh_from_buffer_kwh(inputs, buffer_kwh_from_soc_pct(inputs, inputs.battery_soc_pct)),
            end_battery_soc_pct=round(inputs.battery_soc_pct, 1),
            total_planned_grid_charge_kwh=0.0,
        )

    policy, current_units = optimize_horizon(inputs, horizon_periods)
    forecast_periods = build_forecast_from_policy(inputs, horizon_periods, policy, current_units)
    period_plan = [
        decision_from_forecast(inputs, period, forecast_period)
        for period, forecast_period in zip(horizon_periods, forecast_periods)
    ]

    charge_slots, discharge_slots = compile_periods_to_solis_slots(
        now=inputs.now,
        period_plan=period_plan,
        current_charge_slots=inputs.current_charge_slots,
        current_discharge_slots=inputs.current_discharge_slots,
        max_charge_current_setting=planned_charge_current_setting(inputs),
        max_slots=6,
    )

    morning_start, morning_end, expected_load, expected_solar = morning_summary_metrics(inputs, forecast_periods)
    reserve_pct = reserve_floor_pct(inputs)
    target_soc = max(
        [decision.target_soc_pct for decision in period_plan if decision.target_soc_pct is not None] or [reserve_pct]
    )
    hold_soc = max(
        [decision.hold_soc_pct for decision in period_plan if decision.hold_soc_pct is not None] or [reserve_pct]
    )
    total_grid_import = round(sum(period.planned_grid_import_kwh for period in forecast_periods), 3)
    total_grid_charge = round(sum(period.planned_charge_kwh for period in forecast_periods), 3)
    end_battery_kwh = forecast_periods[-1].battery_end_kwh if forecast_periods else battery_kwh_from_buffer_kwh(
        inputs,
        buffer_kwh_from_soc_pct(inputs, inputs.battery_soc_pct),
    )
    end_battery_buffer = buffer_kwh_from_soc_pct(
        inputs,
        (end_battery_kwh / max(inputs.usable_battery_kwh, 0.1)) * 100.0,
    )
    end_battery_soc_pct = round(soc_pct_from_buffer_kwh(inputs, end_battery_buffer), 1)
    debug_summary = (
        f"Target {target_soc}% / hold {hold_soc}% across {len(forecast_periods)} periods; "
        f"grid charge {total_grid_charge:.2f} kWh, forecast import {total_grid_import:.2f} kWh, "
        f"end battery {end_battery_soc_pct:.1f}%."
    )

    return PlannerResult(
        period_plan=period_plan,
        charge_slots=charge_slots,
        discharge_slots=discharge_slots,
        target_soc_pct=target_soc,
        hold_soc_pct=hold_soc,
        morning_value_window_start=morning_start,
        morning_value_window_end=morning_end,
        expected_morning_load_kwh=expected_load,
        expected_morning_solar_kwh=expected_solar,
        debug_status="ok",
        debug_summary=debug_summary,
        forecast_periods=forecast_periods,
        forecast_total_grid_import_kwh=total_grid_import,
        end_battery_kwh=round(end_battery_kwh, 3),
        end_battery_soc_pct=end_battery_soc_pct,
        total_planned_grid_charge_kwh=total_grid_charge,
    )


def contiguous_windows(
    period_plan: list[PeriodDecision],
    strategy: Literal["charge", "hold"],
) -> list[tuple[datetime, datetime, list[PeriodDecision]]]:
    matching = [decision for decision in period_plan if decision.strategy == strategy]
    if not matching:
        return []

    windows: list[tuple[datetime, datetime, list[PeriodDecision]]] = []
    current_window = [matching[0]]
    for decision in matching[1:]:
        if decision.start_ts == current_window[-1].start_ts + timedelta(minutes=15):
            current_window.append(decision)
            continue
        start = current_window[0].start_ts
        end = current_window[-1].start_ts + timedelta(minutes=15)
        windows.append((start, end, current_window))
        current_window = [decision]

    start = current_window[0].start_ts
    end = current_window[-1].start_ts + timedelta(minutes=15)
    windows.append((start, end, current_window))
    return windows


def live_strategy_from_slots(
    now: datetime,
    current_charge_slots: list[SolisSlot],
    current_discharge_slots: list[SolisSlot],
) -> tuple[str, SolisSlot | None]:
    current_period = floor_to_period(now)
    for slot in current_charge_slots:
        if slot_overlaps_period(slot, current_period):
            return "charge", slot
    for slot in current_discharge_slots:
        if slot_overlaps_period(slot, current_period):
            return "hold", slot
    return "self_use", None


def slot_time(start: datetime, end: datetime) -> str:
    return f"{start:%H:%M}-{end:%H:%M}"


def prioritize_windows(
    windows: list[tuple[datetime, datetime, list[PeriodDecision]]],
    now: datetime,
    max_slots: int,
    live_strategy: str,
) -> list[tuple[datetime, datetime, list[PeriodDecision]]]:
    current_period = floor_to_period(now)

    def score(window: tuple[datetime, datetime, list[PeriodDecision]]) -> tuple[int, float, int]:
        start, end, decisions = window
        overlaps_current = int(start <= current_period < end)
        live_bonus = 1 if overlaps_current and decisions[0].strategy == live_strategy else 0
        protected_value = sum(decision.priority_score for decision in decisions)
        duration = int((end - start) / timedelta(minutes=15))
        return (live_bonus + overlaps_current, protected_value, duration)

    selected = sorted(windows, key=score, reverse=True)[:max_slots]
    return sorted(selected, key=lambda window: window[0])


def compile_windows_to_slots(
    windows: list[tuple[datetime, datetime, list[PeriodDecision]]],
    max_slots: int,
    *,
    is_charge: bool,
    default_current: int,
    live_slot: SolisSlot | None,
    now: datetime,
) -> list[SolisSlot]:
    slots: list[SolisSlot] = []
    current_period = floor_to_period(now)
    for start, end, decisions in windows:
        slot_start = start
        slot_end = end
        if live_slot and slot_overlaps_period(live_slot, current_period) and start <= current_period < end:
            slot_start, slot_end = slot_time_range(live_slot, now)
        soc = max(
            decision.target_soc_pct or decision.hold_soc_pct or DISABLED_SLOT["soc"]
            for decision in decisions
        )
        slots.append(
            SolisSlot(
                time=slot_time(slot_start, slot_end),
                enabled=True,
                current=default_current if is_charge else 0,
                soc=soc,
            )
        )

    while len(slots) < max_slots:
        slots.append(SolisSlot(**DISABLED_SLOT))
    return slots


def trim_windows_for_live_slot(
    windows: list[tuple[datetime, datetime, list[PeriodDecision]]],
    *,
    now: datetime,
    live_slot: SolisSlot | None,
    keep_current_window: bool,
) -> list[tuple[datetime, datetime, list[PeriodDecision]]]:
    if live_slot is None:
        return windows

    current_period = floor_to_period(now)
    live_start, live_end = slot_time_range(live_slot, now)
    trimmed_windows: list[tuple[datetime, datetime, list[PeriodDecision]]] = []
    current_window_kept = False

    for window in windows:
        start, end, decisions = window
        overlaps_live_slot = start < live_end and end > live_start
        contains_current = start <= current_period < end

        if not overlaps_live_slot:
            trimmed_windows.append(window)
            continue
        if keep_current_window and contains_current and not current_window_kept:
            trimmed_windows.append((start, end, decisions))
            current_window_kept = True

    return trimmed_windows


def compile_periods_to_solis_slots(
    now: datetime,
    period_plan: list[PeriodDecision],
    current_charge_slots: list[SolisSlot],
    current_discharge_slots: list[SolisSlot],
    max_charge_current_setting: int,
    max_slots: int = 6,
) -> tuple[list[SolisSlot], list[SolisSlot]]:
    current_period = floor_to_period(now)
    live_strategy, live_slot = live_strategy_from_slots(now, current_charge_slots, current_discharge_slots)
    normalized_plan: list[PeriodDecision] = []

    for decision in sorted(period_plan, key=lambda item: item.start_ts):
        if decision.start_ts == current_period and live_strategy != decision.strategy:
            normalized_plan.append(
                PeriodDecision(
                    start_ts=decision.start_ts,
                    strategy=live_strategy,  # type: ignore[arg-type]
                    target_soc_pct=live_slot.soc if live_strategy == "charge" and live_slot else decision.target_soc_pct,
                    hold_soc_pct=live_slot.soc if live_strategy == "hold" and live_slot else decision.hold_soc_pct,
                    reason=decision.reason,
                    priority_score=decision.priority_score,
                )
            )
            continue
        normalized_plan.append(decision)

    default_charge_current = next(
        (slot.current for slot in current_charge_slots if slot.enabled and slot.current > 0),
        max_charge_current_setting,
    )
    charge_windows = prioritize_windows(
        contiguous_windows(normalized_plan, "charge"),
        now,
        max_slots,
        live_strategy,
    )
    charge_windows = trim_windows_for_live_slot(
        charge_windows,
        now=now,
        live_slot=live_slot,
        keep_current_window=live_strategy == "charge",
    )
    hold_windows = prioritize_windows(
        contiguous_windows(normalized_plan, "hold"),
        now,
        max_slots,
        live_strategy,
    )
    hold_windows = trim_windows_for_live_slot(
        hold_windows,
        now=now,
        live_slot=live_slot,
        keep_current_window=live_strategy == "hold",
    )
    charge_slots = compile_windows_to_slots(
        charge_windows,
        max_slots,
        is_charge=True,
        default_current=default_charge_current,
        live_slot=live_slot if live_strategy == "charge" else None,
        now=now,
    )
    discharge_slots = compile_windows_to_slots(
        hold_windows,
        max_slots,
        is_charge=False,
        default_current=0,
        live_slot=live_slot if live_strategy == "hold" else None,
        now=now,
    )
    return charge_slots, discharge_slots
