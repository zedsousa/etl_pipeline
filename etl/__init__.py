from dagster import Definitions

from .assets import aggregated_data
from .jobs import daily_etl_job, daily_etl_schedule
from .resources import APIResource, TargetDBResource

defs = Definitions(
    assets=[aggregated_data],
    resources={
        "api": APIResource(),
        "target_db": TargetDBResource(),
    },
    jobs=[daily_etl_job],
    schedules=[daily_etl_schedule],
)
