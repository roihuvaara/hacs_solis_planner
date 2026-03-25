from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from .planner.ha_adapter import (
    planner_inputs_from_hass_state,
    planner_result_to_hass_payload,
)
from .planner.core import plan_solis_schedule


def normalize_planner_state(planner_state: Mapping[str, Any] | str) -> dict[str, Any]:
    if isinstance(planner_state, str):
        return json.loads(planner_state)
    return dict(planner_state)


def plan_schedule_payload(planner_state: Mapping[str, Any] | str) -> dict[str, Any]:
    normalized_state = normalize_planner_state(planner_state)
    inputs = planner_inputs_from_hass_state(normalized_state)
    result = plan_solis_schedule(inputs)
    return planner_result_to_hass_payload(result)
