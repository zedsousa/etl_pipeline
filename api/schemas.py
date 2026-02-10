from datetime import datetime

from pydantic import BaseModel


class DataResponse(BaseModel):
    timestamp: datetime
    wind_speed: float
    power: float
    ambient_temperature: float

    class Config:
        from_attributes = True
