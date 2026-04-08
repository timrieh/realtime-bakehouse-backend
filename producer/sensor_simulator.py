import os
import random
import time
from datetime import datetime, timezone

import requests


INGESTION_URL = os.getenv("INGESTION_URL", "http://localhost:8000/events")
INTERVAL_SECONDS = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1.0"))
SENSORS = ["sensor-01", "sensor-02", "sensor-03", "sensor-04"]
SITES = ["factory-a", "factory-b"]


def build_event() -> dict[str, object]:
    sensor_id = random.choice(SENSORS)
    temperature = round(random.uniform(15.0, 42.0), 2)
    humidity = round(random.uniform(20.0, 95.0), 2)

    if random.random() < 0.08:
        temperature = 999.0

    return {
        "sensor_id": sensor_id,
        "site": random.choice(SITES),
        "temperature": temperature,
        "humidity": humidity,
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    while True:
        payload = build_event()
        try:
            response = requests.post(INGESTION_URL, json=payload, timeout=5)
            print(f"{response.status_code} {payload}")
        except Exception as exc:
            print(f"producer error: {exc}")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
