#!/bin/bash
set -euo pipefail

TOPICS=("${KAFKA_TOPIC}" "${KAFKA_DLQ_TOPIC}")

for topic in "${TOPICS[@]}"; do
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions 3 \
    --replication-factor 1
done

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
