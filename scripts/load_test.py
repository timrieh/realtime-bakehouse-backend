import argparse
import json
import random
import statistics
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone


SENSORS = ["sensor-load-01", "sensor-load-02", "sensor-load-03", "sensor-load-04"]
SITES = ["factory-a", "factory-b"]


def build_event() -> bytes:
    payload = {
        "sensor_id": random.choice(SENSORS),
        "site": random.choice(SITES),
        "temperature": round(random.uniform(18.0, 39.0), 2),
        "humidity": round(random.uniform(25.0, 90.0), 2),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }
    return json.dumps(payload).encode("utf-8")


def send_event(url: str, timeout: float) -> tuple[bool, float, int]:
    body = build_event()
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    started_at = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            latency_ms = (time.perf_counter() - started_at) * 1000
            return True, latency_ms, response.status
    except urllib.error.HTTPError as exc:
        latency_ms = (time.perf_counter() - started_at) * 1000
        return False, latency_ms, exc.code
    except Exception:
        latency_ms = (time.perf_counter() - started_at) * 1000
        return False, latency_ms, 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple ingestion load test")
    parser.add_argument("--url", default="http://localhost:8000/events")
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args()

    started_at = time.perf_counter()
    latencies = []
    successes = 0
    failures = 0
    statuses: dict[int, int] = {}

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(send_event, args.url, args.timeout) for _ in range(args.requests)]
        for future in as_completed(futures):
            ok, latency_ms, status = future.result()
            latencies.append(latency_ms)
            statuses[status] = statuses.get(status, 0) + 1
            if ok:
                successes += 1
            else:
                failures += 1

    duration = time.perf_counter() - started_at
    throughput = args.requests / duration if duration else 0.0

    print(f"target={args.url}")
    print(f"requests={args.requests}")
    print(f"workers={args.workers}")
    print(f"duration_seconds={duration:.2f}")
    print(f"throughput_rps={throughput:.2f}")
    print(f"successes={successes}")
    print(f"failures={failures}")
    print(f"status_codes={statuses}")
    if latencies:
        print(f"latency_avg_ms={statistics.mean(latencies):.2f}")
        print(f"latency_p95_ms={sorted(latencies)[int(len(latencies) * 0.95) - 1]:.2f}")


if __name__ == "__main__":
    main()

