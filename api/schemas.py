from datetime import datetime

from pydantic import BaseModel


class DataResponse(BaseModel):
    timestamp: datetime
    wind_speed: float | None = None
    power: float | None = None
    ambient_temperature: float | None = None

    class Config:
        from_attributes = True
