import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models.target import Base, Signal

load_dotenv()

DB_URL = os.getenv("TARGET_DB_URL")


def init_db() -> None:
    engine = create_engine(DB_URL)

    print("Criando tabelas...")
    Base.metadata.create_all(engine)
    print("Tabelas criadas com sucesso.")

    signals = [
        Signal(name="wind_speed_mean"),
        Signal(name="wind_speed_min"),
        Signal(name="wind_speed_max"),
        Signal(name="wind_speed_std"),
        Signal(name="power_mean"),
        Signal(name="power_min"),
        Signal(name="power_max"),
        Signal(name="power_std"),
    ]

    Session = sessionmaker(bind=engine)
    session = Session()
    session.add_all(signals)
    session.commit()
    session.close()

    print("Dados inseridos no banco com sucesso.")


if __name__ == "__main__":
    init_db()
