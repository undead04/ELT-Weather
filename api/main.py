from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List
import database, models, schemas, crud
from datetime import date

app = FastAPI(title="Weather & Air Quality API", version="1.0.0")

# Dependency
def get_db():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def read_root():
    return {"message": "Welcome to the Weather Data Warehouse API"}

@app.get("/weather/{city_id}",response_model=List[schemas.WeatherResponse])
def read_weather(
    city_id: int,
    from_date: date = Query(..., description="Start date (YYYY-MM-DD)"),
    to_date: date = Query(..., description="End date (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """
    Get weather data for a specific city within a date range.
    """
    weather = crud.get_list_weather(db, city_id=city_id, from_date=from_date, to_date=to_date)
    if weather is None:
        raise HTTPException(status_code=404, detail="Weather data not found for this city")
    return weather

@app.get("/air_quality/{city_id}", response_model=List[schemas.AirQualityResponse])
def read_list_air_quality(city_id: int,
    from_date: date = Query(..., description="Start date (YYYY-MM-DD)"),
    to_date: date = Query(..., description="End date (YYYY-MM-DD)"),
    db: Session = Depends(get_db)):
    """
    Get air quality data for a specific city within a date range.
    """
    air_quality = crud.get_list_air_quality(db, city_id=city_id, from_date=from_date, to_date=to_date)
    if air_quality is None:
        raise HTTPException(status_code=404, detail="Air quality data not found for this city")
    return air_quality

@app.get("/city", response_model=List[schemas.CityResponse])
def read_city(db: Session = Depends(get_db)):
    city = crud.get_all_city(db)
    if city is None:
        raise HTTPException(status_code=404, detail="City not found")
    return city

@app.get("/city/{id}", response_model=schemas.CityResponse)
def read_city_by_id(id: int, db: Session = Depends(get_db)):
    city = crud.get_city_by_id(db, city_id=id)
    if city is None:
        raise HTTPException(status_code=404, detail="City not found")
    return city
