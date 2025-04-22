from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Optional, Dict, Any
import os

app = FastAPI()

# Configuración MongoDB
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
DB_NAME = "sensor_db"
COLLECTION_NAME = "sensor_data"

# Conexión a MongoDB
client = AsyncIOMotorClient(MONGODB_URL)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Modelos comunes
class BaseSensorData(BaseModel):
    deviceId: str = Field(..., alias="deviceId")
    timestamp: int

# Modelos para los valores de cada mensaje
class EnvironmentalValues(BaseModel):
    solarRadiationMax: int
    solarRadiationMin: int
    solarRadiationAvg: int
    uvIndexMax: float
    uvIndexMin: float
    uvIndexAvg: float
    temperatureMax: float
    temperatureMin: float
    temperatureAvg: float
    humidityMax: float
    humidityMin: float
    humidityAvg: float

class WeatherValues(BaseModel):
    rainTicks: float
    windSpeedMax: int
    windSpeedMin: int
    windSpeedAvg: int
    windDirectionAvg: int

class AirQualityValues(BaseModel):
    massPM2_5Max: float
    massPM2_5Min: float
    massPM2_5Avg: float
    massPM10_0Max: float
    massPM10_0Min: float
    massPM10_0Avg: float
    noiseMax: float
    noiseMin: float
    noiseAvg: float

# Modelos principales
class EnvironmentalData(BaseSensorData):
    values: EnvironmentalValues

class WeatherData(BaseSensorData):
    values: WeatherValues

class AirQualityData(BaseSensorData):
    values: AirQualityValues

# Endpoints

@app.post("/environmental-data/")
async def receive_environmental_data(data: EnvironmentalData):
    try:
        document = data.dict()
        document["timestamp"] = datetime.utcfromtimestamp(document["timestamp"]/1000)
        await collection.insert_one(document)
        return {"status": "success", "deviceId": data.deviceId}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/weather-data/")
async def receive_weather_data(data: WeatherData):
    try:
        document = data.dict()
        document["timestamp"] = datetime.utcfromtimestamp(document["timestamp"]/1000)
        await collection.insert_one(document)
        return {"status": "success", "deviceId": data.deviceId}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/airquality-data/")
async def receive_airquality_data(data: AirQualityData):
    try:
        document = data.dict()
        document["timestamp"] = datetime.utcfromtimestamp(document["timestamp"]/1000)
        await collection.insert_one(document)
        return {"status": "success", "deviceId": data.deviceId}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)