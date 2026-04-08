import unittest
from datetime import datetime, timezone

from fastapi import HTTPException
from pydantic import ValidationError

from ingestion import app as ingestion_app


class DummyProducer:
    def __init__(self, should_fail: bool = False):
        self.should_fail = should_fail
        self.messages = []

    async def send_and_wait(self, topic, payload):
        if self.should_fail:
            raise RuntimeError("publish failed")
        self.messages.append((topic, payload))


class IngestionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.original_producer = ingestion_app.producer

    async def asyncTearDown(self):
        ingestion_app.producer = self.original_producer

    def test_sensor_event_requires_non_empty_sensor_id(self):
        with self.assertRaises(ValidationError):
            ingestion_app.SensorEvent(
                sensor_id="",
                site="factory-a",
                temperature=20.0,
                humidity=30.0,
            )

    async def test_ingest_event_publishes_payload(self):
        dummy = DummyProducer()
        ingestion_app.producer = dummy
        event = ingestion_app.SensorEvent(
            sensor_id="sensor-01",
            site="factory-a",
            temperature=21.5,
            humidity=40.0,
        )

        response = await ingestion_app.ingest_event(event)

        self.assertEqual(response, {"status": "accepted", "topic": ingestion_app.TOPIC})
        self.assertEqual(len(dummy.messages), 1)
        topic, payload = dummy.messages[0]
        self.assertEqual(topic, ingestion_app.TOPIC)
        self.assertEqual(payload["sensor_id"], "sensor-01")
        self.assertEqual(payload["site"], "factory-a")
        self.assertIsNotNone(datetime.fromisoformat(payload["event_time"]))

    async def test_ingest_event_keeps_explicit_event_time(self):
        dummy = DummyProducer()
        ingestion_app.producer = dummy
        event_time = datetime(2026, 4, 2, 13, 45, tzinfo=timezone.utc)
        event = ingestion_app.SensorEvent(
            sensor_id="sensor-02",
            site="factory-b",
            temperature=25.0,
            humidity=55.0,
            event_time=event_time,
        )

        await ingestion_app.ingest_event(event)

        _, payload = dummy.messages[0]
        self.assertEqual(payload["event_time"], event_time.isoformat())

    async def test_ingest_event_returns_502_on_kafka_failure(self):
        ingestion_app.producer = DummyProducer(should_fail=True)
        event = ingestion_app.SensorEvent(
            sensor_id="sensor-03",
            site="factory-a",
            temperature=22.0,
            humidity=50.0,
        )

        with self.assertRaises(HTTPException) as ctx:
            await ingestion_app.ingest_event(event)

        self.assertEqual(ctx.exception.status_code, 502)

