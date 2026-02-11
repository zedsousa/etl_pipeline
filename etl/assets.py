from datetime import datetime

from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

from etl.resources import APIResource, TargetDBResource
from scripts.run_etl import run_etl

daily_partitions = DailyPartitionsDefinition(
    start_date="2026-01-01", end_date="2026-01-11"
)


@asset(partitions_def=daily_partitions)
def aggregated_data(
    context: AssetExecutionContext, api: APIResource, target_db: TargetDBResource
) -> None:
    """Asset que executa o ETL para uma data específica."""

    partition_date_str = context.partition_key
    date = datetime.strptime(partition_date_str, "%Y-%m-%d")

    context.log.info(f"Processando partição: {partition_date_str}")

    with api.get_client() as client:
        with target_db.get_session() as session:
            run_etl(date, client, session)

    context.log.info(f"Partição {partition_date_str} processada com sucesso.")
