from sqlalchemy import Column, DateTime, Float
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Data(Base):
    __tablename__ = "data"
    timestamp = Column(DateTime, primary_key=True)
    wind_speed = Column(Float)
    power = Column(Float)
    ambient_temperature = Column(Float)
