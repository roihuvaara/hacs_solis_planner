DOMAIN = "solis_planner"
SERVICE_PLAN_SCHEDULE = "plan_schedule"
SERVICE_BUILD_LOAD_FORECAST = "build_load_forecast"
PLATFORMS = ["sensor"]
DATA_LATEST_PLAN = "latest_plan"
DEFAULT_SOLAR_ACTUAL_ENTITY_ID = "sensor.solis_ac_output_total_power"
DEFAULT_WEATHER_ENTITY_ID = "weather.forecast_koti"


def planner_update_signal(entry_id: str) -> str:
    return f"{DOMAIN}_planner_update_{entry_id}"
