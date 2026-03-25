from __future__ import annotations

from collections.abc import Mapping
from typing import Any

try:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant, ServiceCall, ServiceResponse
except ModuleNotFoundError:  # pragma: no cover - local unit tests run without HA installed.
    ConfigEntry = Any  # type: ignore[misc,assignment]
    HomeAssistant = Any  # type: ignore[misc,assignment]
    ServiceCall = Any  # type: ignore[misc,assignment]
    ServiceResponse = dict[str, Any]  # type: ignore[misc,assignment]

from .bridge import plan_schedule_payload
from .const import DOMAIN, SERVICE_PLAN_SCHEDULE

type SolisPlannerConfigEntry = ConfigEntry


async def async_setup(hass: HomeAssistant, config: Mapping[str, Any]) -> bool:
    hass.data.setdefault(DOMAIN, {})
    await _async_register_services(hass)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: SolisPlannerConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = {}
    await _async_register_services(hass)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: SolisPlannerConfigEntry) -> bool:
    hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
    return True


async def _async_register_services(hass: HomeAssistant) -> None:
    if hass.services.has_service(DOMAIN, SERVICE_PLAN_SCHEDULE):
        return

    async def handle_plan_schedule(call: ServiceCall) -> ServiceResponse:
        planner_state = call.data.get("planner_state", {})
        if not isinstance(planner_state, Mapping):
            raise ValueError("planner_state must be a mapping")
        return plan_schedule_payload(dict(planner_state))

    hass.services.async_register(
        DOMAIN,
        SERVICE_PLAN_SCHEDULE,
        handle_plan_schedule,
        supports_response="only",
    )
