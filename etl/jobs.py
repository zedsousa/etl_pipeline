from dagster import AssetSelection, ScheduleDefinition, define_asset_job

daily_etl_job = define_asset_job(
    name="daily_etl_job",
    selection=AssetSelection.assets("aggregated_data"),
)

daily_etl_schedule = ScheduleDefinition(
    job=daily_etl_job,
    cron_schedule="0 3 * * *",
)
