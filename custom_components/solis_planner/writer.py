from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any
import re


_SLOT_TIME_RE = re.compile(r"^(text)\.(.+)_slot1_(charge|discharge)_time$")
_SLOT_FIELD_TYPES = ("time", "current", "soc", "enabled")
_DEBUG_STATUS_ENTITY_ID = "input_text.solis_slot_planner_status"
_DEBUG_SUMMARY_ENTITY_ID = "input_text.solis_slot_planner_schedule"


@dataclass(frozen=True)
class SlotEntitySet:
    side: str
    slot: int
    time_entity_id: str
    current_entity_id: str
    soc_entity_id: str
    enabled_entity_id: str


def _normalize_slot_payload(
    slots: Sequence[Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    normalized = [
        {
            "time": "00:00-00:00",
            "enabled": False,
            "current": 0,
            "soc": 19,
        }
        for _ in range(6)
    ]
    if not slots:
        return normalized
    for index, item in enumerate(slots[:6]):
        normalized[index] = {
            "time": str(item.get("time", "00:00-00:00")),
            "enabled": bool(item.get("enabled", False)),
            "current": int(float(item.get("current", 0))),
            "soc": int(float(item.get("soc", 19))),
        }
    return normalized


def _coerce_number_state(value: Any) -> int:
    return int(round(float(value)))


def _read_slot_state(hass: Any, entities: SlotEntitySet) -> dict[str, Any]:
    time_state = hass.states.get(entities.time_entity_id)
    current_state = hass.states.get(entities.current_entity_id)
    soc_state = hass.states.get(entities.soc_entity_id)
    enabled_state = hass.states.get(entities.enabled_entity_id)
    return {
        "time": str(time_state.state) if time_state is not None else "00:00-00:00",
        "current": _coerce_number_state(current_state.state) if current_state is not None else 0,
        "soc": _coerce_number_state(soc_state.state) if soc_state is not None else 19,
        "enabled": (str(enabled_state.state).lower() == "on") if enabled_state is not None else False,
    }


def _discover_slot_entity_map(hass: Any) -> dict[str, list[SlotEntitySet]]:
    prefixes: set[str] = set()
    for entity_id in hass.states.async_entity_ids("text"):
        match = _SLOT_TIME_RE.match(entity_id)
        if match:
            prefixes.add(match.group(2))
    valid_prefixes: list[str] = []
    for prefix in sorted(prefixes):
        charge_time = f"text.{prefix}_slot1_charge_time"
        discharge_time = f"text.{prefix}_slot1_discharge_time"
        charge_switch = f"switch.{prefix}_slot1_charge"
        discharge_switch = f"switch.{prefix}_slot1_discharge"
        if (
            hass.states.get(charge_time) is not None
            and hass.states.get(discharge_time) is not None
            and hass.states.get(charge_switch) is not None
            and hass.states.get(discharge_switch) is not None
        ):
            valid_prefixes.append(prefix)
    if len(valid_prefixes) != 1:
        raise ValueError("Expected exactly one Solis inverter-control slot entity prefix")

    prefix = valid_prefixes[0]
    return {
        side: [
            SlotEntitySet(
                side=side,
                slot=slot,
                time_entity_id=f"text.{prefix}_slot{slot}_{side}_time",
                current_entity_id=f"number.{prefix}_slot{slot}_{side}_current",
                soc_entity_id=f"number.{prefix}_slot{slot}_{side}_soc",
                enabled_entity_id=f"switch.{prefix}_slot{slot}_{side}",
            )
            for slot in range(1, 7)
        ]
        for side in ("charge", "discharge")
    }


async def _write_slot(hass: Any, entities: SlotEntitySet, slot: Mapping[str, Any]) -> None:
    await hass.services.async_call(
        "text",
        "set_value",
        {
            "entity_id": entities.time_entity_id,
            "value": str(slot["time"]),
        },
        blocking=True,
    )
    await hass.services.async_call(
        "number",
        "set_value",
        {
            "entity_id": entities.current_entity_id,
            "value": int(slot["current"]),
        },
        blocking=True,
    )
    await hass.services.async_call(
        "number",
        "set_value",
        {
            "entity_id": entities.soc_entity_id,
            "value": int(slot["soc"]),
        },
        blocking=True,
    )
    await hass.services.async_call(
        "switch",
        "turn_on" if bool(slot["enabled"]) else "turn_off",
        {"entity_id": entities.enabled_entity_id},
        blocking=True,
    )


def _diff_slot(
    *,
    side: str,
    slot_index: int,
    expected: Mapping[str, Any],
    actual: Mapping[str, Any],
) -> list[dict[str, Any]]:
    mismatches: list[dict[str, Any]] = []
    for field in _SLOT_FIELD_TYPES:
        if expected[field] != actual[field]:
            mismatches.append(
                {
                    "side": side,
                    "slot": slot_index,
                    "field": field,
                    "expected": expected[field],
                    "actual": actual[field],
                }
            )
    return mismatches


async def apply_schedule_payload(
    hass: Any,
    *,
    charge_slots: Sequence[Mapping[str, Any]] | None,
    discharge_slots: Sequence[Mapping[str, Any]] | None,
    debug_status: str,
    debug_summary: str,
    verify: bool = True,
    write_strategy: str = "direct_entities",
) -> dict[str, Any]:
    if write_strategy != "direct_entities":
        raise ValueError("Only direct_entities write strategy is currently supported")

    entity_map = _discover_slot_entity_map(hass)
    normalized_charge = _normalize_slot_payload(charge_slots)
    normalized_discharge = _normalize_slot_payload(discharge_slots)
    trace: list[dict[str, Any]] = []

    for side, slots in (("charge", normalized_charge), ("discharge", normalized_discharge)):
        for entities, slot in zip(entity_map[side], slots, strict=True):
            trace.append(
                {
                    "step": "write_slot",
                    "side": side,
                    "slot": entities.slot,
                    "payload": dict(slot),
                }
            )
            await _write_slot(hass, entities, slot)

    await hass.services.async_call(
        "input_text",
        "set_value",
        {"entity_id": _DEBUG_STATUS_ENTITY_ID, "value": debug_status},
        blocking=True,
    )
    await hass.services.async_call(
        "input_text",
        "set_value",
        {"entity_id": _DEBUG_SUMMARY_ENTITY_ID, "value": debug_summary},
        blocking=True,
    )

    charge_readback = [_read_slot_state(hass, entities) for entities in entity_map["charge"]]
    discharge_readback = [_read_slot_state(hass, entities) for entities in entity_map["discharge"]]
    verification_errors: list[dict[str, Any]] = []
    if verify:
        for slot_index, (expected, actual) in enumerate(zip(normalized_charge, charge_readback, strict=True), start=1):
            verification_errors.extend(
                _diff_slot(side="charge", slot_index=slot_index, expected=expected, actual=actual)
            )
        for slot_index, (expected, actual) in enumerate(
            zip(normalized_discharge, discharge_readback, strict=True),
            start=1,
        ):
            verification_errors.extend(
                _diff_slot(side="discharge", slot_index=slot_index, expected=expected, actual=actual)
            )

    return {
        "charge_slots_written": normalized_charge,
        "discharge_slots_written": normalized_discharge,
        "charge_slots_readback": charge_readback,
        "discharge_slots_readback": discharge_readback,
        "verification_ok": not verification_errors,
        "verification_errors": verification_errors,
        "write_trace": trace,
        "debug_status": debug_status,
        "debug_summary": debug_summary,
        "write_strategy": write_strategy,
    }
