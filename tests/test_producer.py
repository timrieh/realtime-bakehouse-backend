import unittest
from unittest.mock import patch

from producer import sensor_simulator


class ProducerTests(unittest.TestCase):
    def test_build_event_returns_expected_structure(self):
        with patch.object(sensor_simulator.random, "choice", side_effect=["sensor-01", "factory-a"]):
            with patch.object(sensor_simulator.random, "uniform", side_effect=[21.5, 40.0]):
                with patch.object(sensor_simulator.random, "random", return_value=0.5):
                    event = sensor_simulator.build_event()

        self.assertEqual(event["sensor_id"], "sensor-01")
        self.assertEqual(event["site"], "factory-a")
        self.assertEqual(event["temperature"], 21.5)
        self.assertEqual(event["humidity"], 40.0)
        self.assertIn("event_time", event)

    def test_build_event_can_inject_invalid_temperature_for_dlq_path(self):
        with patch.object(sensor_simulator.random, "choice", side_effect=["sensor-02", "factory-b"]):
            with patch.object(sensor_simulator.random, "uniform", side_effect=[18.0, 45.0]):
                with patch.object(sensor_simulator.random, "random", return_value=0.01):
                    event = sensor_simulator.build_event()

        self.assertEqual(event["temperature"], 999.0)
        self.assertEqual(event["humidity"], 45.0)

