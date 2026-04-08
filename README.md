# Realtime Lakehouse Backend

Dieses Projekt liefert eine lokale, containerisierte Datenplattform fuer kontinuierliche Datenaufnahme, Stream Processing und Reporting-Bereitstellung.

## Zielbild

- Ingestion-Service nimmt Events per HTTP auf.
- Kafka puffert und entkoppelt Producer und Consumer.
- Spark Structured Streaming validiert, transformiert und aggregiert Events im Micro-Batch-Modell.
- Ungueltige Events werden in eine Dead Letter Queue (`sensor-events-dlq`) geschrieben.
- MinIO speichert den Iceberg-Warehouse-Layer.
- Ein Iceberg REST Catalog verwaltet die Tabellenmetadaten.
- Trino stellt die aggregierten Daten fuer Reporting und Ad-hoc-Abfragen bereit.

Die Architektur ist damit gleichzeitig echtzeitnah und kompatibel mit den Modulzielen `reliable`, `scalable` und `maintainable`.

## Projektstruktur

- `docker-compose.yml` lokaler Gesamtstack
- `ingestion/` FastAPI-Service fuer Event-Ingestion
- `producer/` simulierte Sensordatenquelle
- `processing/` Spark-Streaming-Job
- `scripts/` Initialisierung fuer MinIO und Kafka
- `tests/` automatisierte Unit-Tests
- `trino/etc/` Trino-Konfiguration

## Architektur in Kurzform

1. Sensordaten oder API-Events werden vom Ingestion-Service angenommen.
2. Der Ingestion-Service schreibt die Rohdaten nach Kafka in `sensor-events`.
3. Spark Structured Streaming liest den Stream im Micro-Batch-Modell.
4. Ungueltige Events landen mit Fehlergrund in `sensor-events-dlq`.
5. Gueltige Events werden validiert, dedupliziert und in `sensor_events_validated` gespeichert.
6. Daraus werden Gold-Tabellen fuer Reporting per `MERGE` aktualisiert.
7. Trino stellt die Tabellen fuer SQL-Abfragen bereit.

## Schnellstart

1. Beispielkonfiguration kopieren:

```bash
cp .env.example .env
```

2. Stack starten:

```bash
docker compose up -d --build
```

3. Logs beobachten:

```bash
docker compose logs -f --tail=150
```

4. Nach dem Start Daten pruefen:

- MinIO Console: `http://localhost:9001`
- Ingestion API: `http://localhost:8000/docs`
- Trino: `http://localhost:8085`

## Relevante Topics und Buckets

- Kafka Topic `sensor-events`
- Kafka Topic `sensor-events-dlq`
- MinIO Bucket `warehouse`

## Beispielabfragen in Trino

```sql
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.reporting;
SELECT * FROM iceberg.reporting.sensor_events_validated ORDER BY event_time DESC LIMIT 20;
SELECT * FROM iceberg.reporting.sensor_minute ORDER BY window_start DESC LIMIT 20;
SELECT * FROM iceberg.reporting.site_minute ORDER BY window_start DESC LIMIT 20;
SELECT * FROM iceberg.reporting.site_alert_minute ORDER BY window_start DESC LIMIT 20;
SELECT * FROM iceberg.reporting.sensor_latest_status ORDER BY processed_at DESC LIMIT 20;
```

## Fachliches Datenmodell

- `sensor_events_validated`: validierte und deduplizierte Silver-Datenbasis
- `sensor_minute`: 1-Minuten-Aggregation pro Sensor fuer operative Trendanalysen
- `site_minute`: 1-Minuten-Aggregation pro Standort fuer Kapazitaets- und Lastsicht
- `site_alert_minute`: Alarm-Sicht pro Standort und Minute mit `normal`, `warning`, `critical`
- `sensor_latest_status`: letzter bekannter Zustand je Sensor fuer ein Live-Dashboard

## Architekturentscheidungen

- Spark Structured Streaming nutzt Micro-Batches als pragmatischen Mittelweg zwischen klassischem Batch und Low-Latency-Streaming.
- Ein separater Iceberg REST Catalog entkoppelt Metadatenverwaltung von Compute und Query Layer.
- Kafka uebernimmt Backpressure, Replay und Entkopplung.
- DLQ bleibt in Kafka, damit fehlerhafte Events separat inspiziert und bei Bedarf reprocessiert werden koennen.
- Die Silver-Tabelle `sensor_events_validated` bildet die deduplizierte Reprocessing-Basis.
- Die Gold-Tabellen werden per `MERGE` auf fachlichen Schluesseln aktualisiert.

## Verifikation

Healthcheck:

```bash
curl http://localhost:8000/health
```

Automatisierte Tests:

```bash
python3 -m unittest discover -s tests -v
```

Einfacher Lasttest:

```bash
python3 scripts/load_test.py --url http://localhost:8000/events --requests 40 --workers 4
```

## Bekannte Grenzen

- Der Stack ist lokal und auf Demonstration sowie Abgabe ausgelegt, nicht auf produktiven Cluster-Betrieb.
- Replikation und horizontale Skalierung sind architektonisch vorgesehen, lokal aber bewusst reduziert.
