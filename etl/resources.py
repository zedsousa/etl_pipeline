import os
from collections.abc import Generator
from contextlib import contextmanager

import httpx
from dagster import ConfigurableResource
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

load_dotenv()


class APIResource(ConfigurableResource):
    """Recurso para acessar API fonte."""

    api_url: str = os.getenv("API_URL", "")

    @contextmanager
    def get_client(self) -> Generator[httpx.Client, None, None]:
        client = httpx.Client(base_url=self.api_url)
        try:
            yield client
        finally:
            client.close()


class TargetDBResource(ConfigurableResource):
    """Recurso para acessar banco de dados alvo."""

    db_url: str = os.getenv("TARGET_DB_URL", "")

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        engine = create_engine(self.db_url)
        Session = sessionmaker(bind=engine)
        session = Session()
        try:
            yield session
        finally:
            session.close()
