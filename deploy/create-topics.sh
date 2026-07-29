#!/usr/bin/env bash
# Creates the Kafka topics for local/pilot testing on your Linux box.
# Assumes Kafka is already installed and running (native, no Docker).
#
# Set KAFKA_BIN to the directory holding kafka-topics.sh if it is not on PATH,
# e.g.  export KAFKA_BIN=/opt/kafka/bin
#
# For a SINGLE-NODE test broker, replication factor must be 1 (there is only one
# broker to hold a copy). Production uses RF=3 (plan section 4.1 / 5.2).
set -euo pipefail

BROKER="${BROKER:-localhost:9092}"
RF="${RF:-1}"
TOPICS_CMD="${KAFKA_BIN:+$KAFKA_BIN/}kafka-topics.sh"

create() {
  local name="$1" parts="$2" retention_ms="$3"
  echo "creating topic $name (partitions=$parts, rf=$RF, retention=${retention_ms}ms)"
  "$TOPICS_CMD" --bootstrap-server "$BROKER" --create --if-not-exists \
    --topic "$name" --partitions "$parts" --replication-factor "$RF" \
    --config "retention.ms=$retention_ms" \
    --config "compression.type=producer"
}

# name                partitions   retention
create gps.raw           12   $((7*24*3600*1000))   # 7 days
create gps.telemetry     12   $((3*24*3600*1000))   # 3 days
create gps.commands.out   6   $((1*24*3600*1000))   # 1 day
create gps.commands.ack   6   $((7*24*3600*1000))   # 7 days
create gps.dlq            3   $((30*24*3600*1000))  # 30 days

echo
echo "topics created. list them with:"
echo "  ${TOPICS_CMD} --bootstrap-server ${BROKER} --list"
