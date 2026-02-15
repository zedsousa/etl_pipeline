import logging
import sys
from datetime import datetime
from pathlib import Path

import httpx
import pandas as pd
from sqlalchemy import delete, insert
from sqlalchemy.orm import Session

sys.path.append(str(Path(__file__).parent.parent))
from models.target import Data, Signal

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


def load_data(df: pd.DataFrame, session: Session, target_date: datetime) -> None:
    """Carrega dados agregados no banco de dados alvo."""

    start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)

    stmt_delete = delete(Data).where(
        Data.timestamp >= start_of_day, Data.timestamp <= end_of_day
    )
    result = session.execute(stmt_delete)

    if result.rowcount > 0:
        logger.info(
            f"Limpeza de partição: {result.rowcount} registros antigos removidos."
        )

    logger.info("Carregando dados no banco alvo...")

    signal_names = [column for column in df.columns if column != "timestamp"]

    db_signals = session.query(Signal).all()
    signal_map = {signal.name: signal.id for signal in db_signals}

    missing_signals = [signal for signal in signal_names if signal not in signal_map]

    if missing_signals:
        new_signals = [Signal(name=signal) for signal in missing_signals]
        session.add_all(new_signals)
        session.commit()
        for signal in new_signals:
            session.refresh(signal)
        signal_map.update({signal.name: signal.id for signal in new_signals})

    data_points = []

    for _, row in df.iterrows():
        for metric in signal_names:
            signal_id = signal_map.get(metric)
            if not pd.isna(row[metric]):
                data_points.append(
                    {
                        "timestamp": row["timestamp"],
                        "signal_id": signal_id,
                        "value": float(row[metric]),
                    }
                )

    session.execute(insert(Data), data_points)
    session.commit()

    logger.info(f"{len(data_points)} registros carregados com sucesso.")


def run_etl(date: datetime, api_client: httpx.Client, db_session: Session) -> None:
    """Executa o pipeline ETL completo para uma data específica."""
    logger.info(f"Iniciando ETL para {date.strftime('%Y-%m-%d')}")

    try:
        data = extract_data(api_client, date)
        aggregated = transform_data(data)
        load_data(aggregated, db_session, date)
    except Exception as e:
        logger.error(f"Erro durante o ETL: {str(e)}")
        raise e
