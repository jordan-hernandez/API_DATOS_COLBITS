from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Optional, List, Dict, Any
import os

app = FastAPI(
    title="Sensor Data API",
    version="1.0.0",
    description="API para monitoreo de datos de sensores ambientales"
)

# Configuración MongoDB
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "sensor_db")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "sensor_data")

# Conexión a MongoDB
client = AsyncIOMotorClient(MONGODB_URL)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Modelos comunes
class BaseSensorData(BaseModel):
    deviceId: str
    timestamp: int

# Modelos para los valores de cada mensaje
# Agrega este modelo en la sección de Modelos para los valores
class TrafficValues(BaseModel):
    peaton: int
    bicicleta: int
    carro: int
    moto: int
    bus: int
    camion: int

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
class TrafficData(BaseSensorData):
    values: TrafficValues

class EnvironmentalData(BaseSensorData):
    values: EnvironmentalValues

class WeatherData(BaseSensorData):
    values: WeatherValues

class AirQualityData(BaseSensorData):
    values: AirQualityValues

# Helper functions
async def insert_sensor_data(data: BaseSensorData, data_type: str):
    """Función auxiliar para insertar datos en MongoDB"""
    document = data.dict()
    document.update({
        "timestamp": datetime.utcfromtimestamp(document["timestamp"]/1000),
        "data_type": data_type
    })
    await collection.insert_one(document)
    return {"status": "success", "deviceId": data.deviceId}

# Endpoints
@app.get("/", tags=["Root"])
async def root():
    """Endpoint raíz para verificar el estado del servicio"""
    return {
        "message": "API funcionando correctamente",
        "status": "ok",
        "version": app.version
    }
# Agrega este nuevo endpoint en la sección de Endpoints
@app.post("/traffic-data/", tags=["Tráfico"], status_code=status.HTTP_201_CREATED)
async def receive_traffic_data(data: TrafficData):
    """Endpoint para recibir datos de conteo de tráfico"""
    try:
        return await insert_sensor_data(data, "traffic")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
    
@app.post("/environmental-data/", tags=["Environmental"], status_code=status.HTTP_201_CREATED)
async def receive_environmental_data(data: EnvironmentalData):
    """Endpoint para recibir datos ambientales"""
    try:
        return await insert_sensor_data(data, "environmental")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.post("/weather-data/", tags=["Weather"], status_code=status.HTTP_201_CREATED)
async def receive_weather_data(data: WeatherData):
    """Endpoint para recibir datos meteorológicos"""
    try:
        return await insert_sensor_data(data, "weather")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.post("/airquality-data/", tags=["Air Quality"], status_code=status.HTTP_201_CREATED)
async def receive_airquality_data(data: AirQualityData):
    """Endpoint para recibir datos de calidad del aire"""
    try:
        return await insert_sensor_data(data, "air_quality")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.get("/data/{deviceId}", tags=["Consultas"], response_model=List[Dict[str, Any]])
async def get_device_data(deviceId: str, limit: int = 100):
    """Obtener los últimos registros de un dispositivo específico"""
    try:
        cursor = collection.find({"deviceId": deviceId}).sort("timestamp", -1).limit(limit)
        results = await cursor.to_list(length=limit)
        if not results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Dispositivo no encontrado"
            )
        
        # Conversión segura de timestamp
        epoch = datetime(1970, 1, 1)
        for item in results:
            item["_id"] = str(item["_id"])
            # Calcular timestamp en milisegundos correctamente
            dt = item["timestamp"]
            item["timestamp"] = int((dt - epoch).total_seconds() * 1000)
        
        return results
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

# Event handlers
@app.on_event("startup")
async def startup_db_client():
    """Verificar conexión con MongoDB al iniciar"""
    try:
        await client.admin.command('ping')
        print("✅ Conexión exitosa con MongoDB")
    except Exception as e:
        print(f"❌ Error de conexión con MongoDB: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_db_client():
    """Cerrar conexión con MongoDB al apagar"""
    client.close()
    print("🗄️ Conexión con MongoDB cerrada")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)