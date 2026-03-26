from __future__ import annotations

from typing import Any

try:
    from homeassistant.components.sensor import SensorEntity
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant, callback
    from homeassistant.helpers.dispatcher import async_dispatcher_connect
    from homeassistant.helpers.entity_platform import AddEntitiesCallback
except ModuleNotFoundError:  # pragma: no cover - imported only in HA runtime.
    SensorEntity = object  # type: ignore[misc,assignment]
    ConfigEntry = Any  # type: ignore[misc,assignment]
    HomeAssistant = Any  # type: ignore[misc,assignment]
    AddEntitiesCallback = Any  # type: ignore[misc,assignment]

    def callback(func: Any) -> Any:
        return func

    def async_dispatcher_connect(*args: Any, **kwargs: Any) -> Any:
        return None

from .const import DATA_LATEST_PLAN, DOMAIN, planner_update_signal


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    async_add_entities([SolisPlannerForecastSensor(hass, entry)])


class SolisPlannerForecastSensor(SensorEntity):
    _attr_has_entity_name = True
    _attr_name = "Forecast"
    _attr_native_unit_of_measurement = "%"
    _attr_icon = "mdi:battery-clock"

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self.hass = hass
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_forecast"

    @property
    def device_info(self) -> dict[str, Any]:
        return {
            "identifiers": {(DOMAIN, self._entry.entry_id)},
            "name": "Solis Planner",
        }

    @property
    def available(self) -> bool:
        return self._payload is not None

    @property
    def native_value(self) -> float | None:
        if self._payload is None:
            return None
        value = self._payload.get("end_battery_soc_pct")
        return float(value) if value is not None else None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        return dict(self._payload or {})

    async def async_added_to_hass(self) -> None:
        remove_listener = async_dispatcher_connect(
            self.hass,
            planner_update_signal(self._entry.entry_id),
            self._handle_update,
        )
        if remove_listener is not None:
            self.async_on_remove(remove_listener)

    @callback
    def _handle_update(self) -> None:
        self.async_write_ha_state()

    @property
    def _payload(self) -> dict[str, Any] | None:
        return self.hass.data.get(DOMAIN, {}).get(self._entry.entry_id, {}).get(DATA_LATEST_PLAN)
