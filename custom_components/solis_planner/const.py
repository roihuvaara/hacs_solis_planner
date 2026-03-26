DOMAIN = "solis_planner"
SERVICE_PLAN_SCHEDULE = "plan_schedule"
SERVICE_BUILD_LOAD_FORECAST = "build_load_forecast"
PLATFORMS = ["sensor"]
DATA_LATEST_PLAN = "latest_plan"


def planner_update_signal(entry_id: str) -> str:
    return f"{DOMAIN}_planner_update_{entry_id}"
