import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sensor-events")
PORT = int(os.getenv("INGESTION_PORT", "8000"))


class SensorEvent(BaseModel):
    sensor_id: str = Field(..., min_length=1)
    site: str = Field(..., min_length=1)
    temperature: float
    humidity: float
    event_time: Optional[datetime] = None


producer: Optional[AIOKafkaProducer] = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    await producer.start()
    try:
        yield
    finally:
        if producer is not None:
            await producer.stop()


app = FastAPI(title="Realtime Ingestion API", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/events")
async def ingest_event(event: SensorEvent) -> dict[str, str]:
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer not ready")

    payload = event.model_dump()
    payload["event_time"] = (
        event.event_time or datetime.now(timezone.utc)
    ).isoformat()

    try:
        await producer.send_and_wait(TOPIC, payload)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Kafka publish failed: {exc}") from exc

    return {"status": "accepted", "topic": TOPIC}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=PORT)
