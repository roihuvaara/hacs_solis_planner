from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from math import ceil
from statistics import mean
from typing import Literal


DISABLED_SLOT = {"time": "00:00-00:00", "enabled": False, "current": 0, "soc": 19}


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


def morning_periods(price_horizon: list[PeriodPrice], now: datetime) -> list[PeriodPrice]:
    current_period = floor_to_period(now)
    horizon = sorted(price_horizon, key=lambda period: period.start_ts)
    return [
        period
        for period in horizon
        if period.start_ts >= current_period
        and 6 <= period.start_ts.astimezone(now.tzinfo).hour < 10
    ]


def select_value_window(periods: list[PeriodPrice]) -> list[PeriodPrice]:
    if not periods:
        return []

    prices = [period.price_cents_per_kwh for period in periods]
    max_price = max(prices)
    avg_price = mean(prices)
    threshold = max(avg_price, max_price - max(1.0, (max_price - avg_price) * 0.35))
    peak_index = max(range(len(periods)), key=lambda index: periods[index].price_cents_per_kwh)
    start_index = peak_index
    end_index = peak_index

    while start_index > 0 and periods[start_index - 1].price_cents_per_kwh >= threshold:
        start_index -= 1
    while end_index + 1 < len(periods) and periods[end_index + 1].price_cents_per_kwh >= threshold:
        end_index += 1

    return periods[start_index : end_index + 1]


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


def expected_morning_energy(
    inputs: PlannerInputs,
    morning_window: list[PeriodPrice],
) -> tuple[float, float]:
    if not morning_window:
        return 0.0, 0.0

    usage_by_bucket = usage_lookup(inputs.rolling_usage_7d)
    solar_by_period = solar_forecast_series(inputs)
    horizon_index = {period.start_ts: index for index, period in enumerate(inputs.price_horizon)}
    total_load = 0.0
    total_solar = 0.0

    for period in morning_window:
        period_index = horizon_index.get(period.start_ts, 0)
        if inputs.load_forecast_by_period_kwh and period_index < len(inputs.load_forecast_by_period_kwh):
            total_load += inputs.load_forecast_by_period_kwh[period_index]
        else:
            minute_of_day = period.start_ts.astimezone(inputs.now.tzinfo).hour * 60 + period.start_ts.minute
            total_load += usage_by_bucket.get(minute_of_day, 0.0)
        if solar_by_period and period_index < len(solar_by_period):
            total_solar += solar_by_period[period_index]

    return total_load, total_solar


def clamp(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(maximum, value))


def derive_soc_targets(
    inputs: PlannerInputs,
    morning_window: list[PeriodPrice],
    expected_morning_load_kwh: float,
    expected_morning_solar_kwh: float,
) -> tuple[int, int]:
    reserve = max(DISABLED_SLOT["soc"], ceil(inputs.reserve_soc_pct))
    required_kwh = max(0.0, expected_morning_load_kwh - expected_morning_solar_kwh)
    hold_soc = clamp(
        ceil(inputs.reserve_soc_pct + (required_kwh / max(inputs.usable_battery_kwh, 0.1)) * 100),
        reserve,
        100,
    )

    if not morning_window:
        return reserve, reserve

    pre_morning = [
        period
        for period in inputs.price_horizon
        if period.start_ts < morning_window[0].start_ts and period.start_ts >= floor_to_period(inputs.now)
    ]
    if not pre_morning:
        return hold_soc, hold_soc

    cheapest = min(period.price_cents_per_kwh for period in pre_morning)
    morning_peak = max(period.price_cents_per_kwh for period in morning_window)
    spread = max(0.0, morning_peak - cheapest)
    solar_penalty = max(0.0, 5.0 - inputs.solar_forecast_tomorrow_kwh) * 2.5
    extra_margin = max(0.0, spread * 0.6 + solar_penalty - 6.0)
    target_soc = clamp(ceil(hold_soc + extra_margin), hold_soc, 100)
    return target_soc, hold_soc


def select_charge_cluster(pre_morning_periods: list[PeriodPrice]) -> list[PeriodPrice]:
    if not pre_morning_periods:
        return []

    min_price = min(period.price_cents_per_kwh for period in pre_morning_periods)
    prices = [period.price_cents_per_kwh for period in pre_morning_periods]
    spread = max(prices) - min(prices)
    threshold = min_price + max(1.0, spread * 0.15)
    cheapest_index = min(
        range(len(pre_morning_periods)),
        key=lambda index: pre_morning_periods[index].price_cents_per_kwh,
    )
    start_index = cheapest_index
    end_index = cheapest_index

    while start_index > 0 and pre_morning_periods[start_index - 1].price_cents_per_kwh <= threshold:
        start_index -= 1
    while end_index + 1 < len(pre_morning_periods) and pre_morning_periods[end_index + 1].price_cents_per_kwh <= threshold:
        end_index += 1

    return pre_morning_periods[start_index : end_index + 1]


def plan_solis_schedule(inputs: PlannerInputs) -> PlannerResult:
    horizon = sorted(inputs.price_horizon, key=lambda period: period.start_ts)
    current_period = floor_to_period(inputs.now)
    future_periods = [period for period in horizon if period.start_ts >= current_period]
    morning_candidates = morning_periods(horizon, inputs.now)
    morning_window = select_value_window(morning_candidates)
    expected_load, expected_solar = expected_morning_energy(inputs, morning_window)
    target_soc, hold_soc = derive_soc_targets(inputs, morning_window, expected_load, expected_solar)

    charge_cluster = select_charge_cluster(
        [period for period in future_periods if not morning_window or period.start_ts < morning_window[0].start_ts]
    )
    charge_starts = {period.start_ts for period in charge_cluster}
    hold_start = charge_cluster[-1].start_ts + timedelta(minutes=15) if charge_cluster else None
    hold_end = morning_window[-1].start_ts + timedelta(minutes=15) if morning_window else None

    live_strategy, live_soc = detect_live_strategy(inputs, current_period)
    period_plan: list[PeriodDecision] = []

    for period in future_periods:
        strategy = "self_use"
        reason = "default self-use outside value protection windows"
        decision_target = None
        decision_hold = None

        if period.start_ts in charge_starts:
            strategy = "charge"
            decision_target = target_soc
            reason = "cheap overnight charging to reach target before morning value window"
        elif hold_start and hold_end and hold_start <= period.start_ts < hold_end:
            strategy = "hold"
            decision_hold = hold_soc
            reason = "hold battery for morning value window"

        if period.start_ts == current_period and live_strategy != strategy:
            strategy = live_strategy
            if live_strategy == "charge":
                decision_target = live_soc or target_soc
                reason = "preserve current live charge period while recompiling future windows"
            elif live_strategy == "hold":
                decision_hold = live_soc or hold_soc
                reason = "preserve current live hold period while recompiling future windows"
            else:
                decision_target = None
                decision_hold = None
                reason = "preserve current live self-use period while recompiling future windows"

        period_plan.append(
            PeriodDecision(
                start_ts=period.start_ts,
                strategy=strategy,
                target_soc_pct=decision_target,
                hold_soc_pct=decision_hold,
                reason=reason,
            )
        )

    charge_slots, discharge_slots = compile_periods_to_solis_slots(
        now=inputs.now,
        period_plan=period_plan,
        current_charge_slots=inputs.current_charge_slots,
        current_discharge_slots=inputs.current_discharge_slots,
        max_charge_current_setting=inputs.max_charge_current_setting,
        max_slots=6,
    )

    morning_start = morning_window[0].start_ts if morning_window else None
    morning_end = morning_window[-1].start_ts + timedelta(minutes=15) if morning_window else None
    value_label = "morning peak cluster" if morning_window and len({period.price_cents_per_kwh for period in morning_window}) > 1 else "morning value window"
    debug_summary = (
        f"Target {target_soc}% / hold {hold_soc}% from expected load "
        f"{expected_load:.2f} kWh and solar {expected_solar:.2f} kWh in {value_label}."
    )

    return PlannerResult(
        period_plan=period_plan,
        charge_slots=charge_slots,
        discharge_slots=discharge_slots,
        target_soc_pct=target_soc,
        hold_soc_pct=hold_soc,
        morning_value_window_start=morning_start,
        morning_value_window_end=morning_end,
        expected_morning_load_kwh=round(expected_load, 3),
        expected_morning_solar_kwh=round(expected_solar, 3),
        debug_status="ok",
        debug_summary=debug_summary,
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

    def score(window: tuple[datetime, datetime, list[PeriodDecision]]) -> tuple[int, int, float]:
        start, end, decisions = window
        overlaps_current = int(start <= current_period < end)
        duration = int((end - start) / timedelta(minutes=15))
        value = max(
            decision.target_soc_pct or decision.hold_soc_pct or 0
            for decision in decisions
        )
        live_bonus = 1 if overlaps_current and decisions[0].strategy == live_strategy else 0
        return (live_bonus + overlaps_current, duration, float(value))

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
    hold_windows = prioritize_windows(
        contiguous_windows(normalized_plan, "hold"),
        now,
        max_slots,
        live_strategy,
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
