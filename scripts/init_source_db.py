import os
import random
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_URL = os.getenv("SOURCE_DB_URL")


def generate_data() -> pd.DataFrame:
    end_date = datetime.now()
    start_date = end_date - timedelta(days=10)

    dates = pd.date_range(start=start_date, end=end_date, freq="1min")

    data = []
    for dt in dates:
        data.append(
            {
                "timestamp": dt,
                "wind_speed": round(random.uniform(0, 25), 2),
                "power": round(random.uniform(0, 1000), 2),
                "ambient_temperature": round(random.uniform(15, 35), 2),
            }
        )
    return pd.DataFrame(data)


def init_db() -> None:
    print("Gerando dados...")
    df = generate_data()

    engine = create_engine(DB_URL)

    print("Inserindo dados no banco...")
    df.to_sql("data", engine, if_exists="replace", index=False)

    print("Adicionando ")
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE data ADD PRIMARY KEY (timestamp);"))
        conn.commit()

    print("Dados inseridos no banco com sucesso.")


if __name__ == "__main__":
    init_db()
