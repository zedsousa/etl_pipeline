from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Signal(Base):
    __tablename__ = "signal"
    id = Column(Integer, primary_key=True)
    name = Column(String)


class Data(Base):
    __tablename__ = "data"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    signal_id = Column(Integer, ForeignKey("signal.id"))
    value = Column(Float)
