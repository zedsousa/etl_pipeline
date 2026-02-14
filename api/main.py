from datetime import datetime

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy.orm import Session

from api.database import get_session
from api.models.source import Data
from api.schemas import DataResponse

app = FastAPI()


VALID_COLUMNS = ["wind_speed", "power", "ambient_temperature"]


@app.get("/data", response_model=list[DataResponse])
def get_data(
    start_date: datetime,
    end_date: datetime,
    variables: list[str] = Query(default=VALID_COLUMNS),
    session: Session = Depends(get_session),
) -> list[DataResponse]:
    if not all(v in VALID_COLUMNS for v in variables):
        raise HTTPException(status_code=400, detail="Invalid variables")

    selected_columns = [Data.timestamp] + [getattr(Data, v) for v in variables]

    query = (
        session.query(*selected_columns)
        .filter(Data.timestamp >= start_date, Data.timestamp <= end_date)
        .order_by(Data.timestamp)
    )

    return query.all()  # type: ignore
