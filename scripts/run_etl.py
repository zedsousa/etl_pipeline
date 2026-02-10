import logging
import sys
from datetime import datetime
from pathlib import Path

import httpx
import pandas as pd
from sqlalchemy.orm import Session

sys.path.append(str(Path(__file__).parent.parent))
from models.target import Data

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def extract_data(client: httpx.Client, date: datetime) -> list[dict]:
    """Extrai dados brutos da API para uma data específica."""
    start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = date.replace(hour=23, minute=59, second=59, microsecond=999999)

    response = client.get(
        "/data",
        params={
            "start_date": start_of_day.isoformat(),
            "end_date": end_of_day.isoformat(),
            "variables": ["wind_speed", "power"],
        },
    )

    response.raise_for_status()

    data: list[dict] = response.json()

    logger.info(f"{len(data)} registros extraídos com sucesso.")
    return data


def transform_data(data: list[dict]) -> pd.DataFrame:
    """Agrega dados em janelas de 10 minutos calculando estatísticas."""
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.set_index("timestamp", inplace=True)

    aggregated = df.resample("10min", label="left").agg(
        {
            "wind_speed": ["mean", "min", "max", "std"],
            "power": ["mean", "min", "max", "std"],
        }
    )

    aggregated.columns = ["_".join(col) for col in aggregated.columns]
    aggregated.reset_index(inplace=True)

    logger.info(f"{len(aggregated)} registros agregados com sucesso.")
    return aggregated


def load_data(df: pd.DataFrame, session: Session) -> None:
    """Carrega dados agregados no banco de dados alvo."""
    logger.info("Carregando dados no banco alvo...")

    signal_map = {
        "wind_speed_mean": 1,
        "wind_speed_min": 2,
        "wind_speed_max": 3,
        "wind_speed_std": 4,
        "power_mean": 5,
        "power_min": 6,
        "power_max": 7,
        "power_std": 8,
    }

    data_points = []

    for _, row in df.iterrows():
        for metric, signal_id in signal_map.items():
            if not pd.isna(row[metric]):
                data_points.append(
                    Data(
                        timestamp=row["timestamp"],
                        signal_id=signal_id,
                        value=float(row[metric]),
                    )
                )

    session.add_all(data_points)
    session.commit()

    logger.info(f"{len(data_points)} registros carregados com sucesso.")


def run_etl(date: datetime, api_client: httpx.Client, db_session: Session) -> None:
    """Executa o pipeline ETL completo para uma data específica."""
    logger.info(f"Iniciando ETL para {date.strftime('%Y-%m-%d')}")

    try:
        data = extract_data(api_client, date)
        aggregated = transform_data(data)
        load_data(aggregated, db_session)
    except Exception as e:
        logger.error(f"Erro durante o ETL: {str(e)}")
        raise e
