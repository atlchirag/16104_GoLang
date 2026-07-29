# Kafka concepts (for this project) + testing guide

## 1. The mental model

Kafka is a **durable pipe** between the listener and the database. Today the
.NET service parses a packet and immediately does 6–10 database round trips; if
Postgres is down the packet is **lost forever** (finding D1). Kafka turns "data
loss" into "data delay": once the listener gets a produce acknowledgement, the
record survives a database outage, a consumer crash, or a full restart.

```
Producer → Topic (split into ordered Partitions, replicated across Brokers) → Consumer groups (track Offsets)
```

## 2. The terms, mapped to our system

| Term | Meaning | Here |
|------|---------|------|
| **Broker** | One Kafka server process. A **cluster** is several. | 1 broker for testing; 3 in production. |
| **KRaft** | Kafka's built-in metadata mode using the Raft algorithm. **Replaces ZooKeeper** (gone in Kafka 4.x). | You install only Kafka. A single process runs as both **broker** and **controller** (the role that elects partition leaders). |
| **Topic** | A named append-only log of messages. | `gps.raw`, `gps.telemetry`, `gps.commands.out`, `gps.commands.ack`, `gps.dlq`. |
| **Partition** | A topic is split into N partitions; each is an ordered log. Unit of parallelism **and** ordering. | `gps.telemetry` = 12 partitions. Order is guaranteed only *within* a partition. |
| **Key** | Hashed to choose the partition. Same key → same partition → in order. | We key by **IMEI**: one device's packets stay ordered (needed for odometer, panic de-dup). |
| **Offset** | A message's position in a partition (0,1,2,…). A consumer records "processed up to X". | Enables **replay**: reset the offset backward to reprocess after a parser fix. |
| **Producer** | Writes to topics. | The **listener**. `acks=all`, idempotent, lz4. |
| **Consumer** | Reads from topics. | The **pgwriter**. Batches ~2000 and writes once. |
| **Consumer group** | Instances that *share* a topic's partitions (one partition per member). **Different groups each get their own copy** of every message. | `pg-writer` group writes to Postgres; later `alert-engine` and `mssql-mirror` groups read the same messages independently — that is the fan-out. |
| **Replication / RF / ISR** | Copies of a partition across brokers. RF=3 → 1 leader + 2 followers. ISR = replicas that are caught up. | `min.insync.replicas=2` + `acks=all` → survive losing 1 broker with zero loss. **Single-node test box: RF=1** (fine for dev, never prod). |
| **Consumer lag** | How far behind a consumer is. | Key health metric: if Postgres is down, lag grows then drains — instead of losing data. |

## 3. Why offsets are committed *after* the DB write

The consumer commits Kafka offsets only **after** the Postgres transaction
succeeds (`cmd/pgwriter`). If it crashes in between, Kafka redelivers the batch
and we write it again — harmless, because the unique index on
`(sys_service_id, gps_time)` + `ON CONFLICT DO NOTHING` makes the write
idempotent. Committing offsets first would silently lose the batch on a crash —
recreating D1 in a new place.

## 4. Starting Kafka (KRaft) on your Linux box, no Docker

If Kafka is already running as a service, skip this. Otherwise, for a quick
single-node test broker (Kafka 4.x):

```bash
cd /opt/kafka
# One-time: format the storage dir with a cluster id.
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c config/kraft/server.properties
# Start the broker (foreground; use a systemd unit for real deployments).
bin/kafka-server-start.sh config/kraft/server.properties
```

Then create topics: `export KAFKA_BIN=/opt/kafka/bin && bash deploy/create-topics.sh`.

## 5. Handy Kafka CLI for debugging

```bash
# List topics
$KAFKA_BIN/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Watch messages arrive on gps.telemetry (Ctrl-C to stop)
$KAFKA_BIN/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic gps.telemetry --from-beginning

# See consumer-group lag (how far behind pgwriter is)
$KAFKA_BIN/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group pg-writer

# Replay: move pg-writer back to the start of gps.telemetry, then restart it
$KAFKA_BIN/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group pg-writer --topic gps.telemetry --reset-offsets --to-earliest --execute
```

## 6. What "test" means at this stage

1. `go test ./...` on Windows — parser + framing correctness (no Kafka needed).
2. End-to-end on Linux — the 7 steps in the README. Success =
   simulator prints `ack=0xAA`, consumer logs `inserted`, one row appears, and a
   **second send does not create a duplicate** (idempotency proven).
3. Kill the consumer mid-run, restart it — no lost rows, no duplicates.
4. Stop Postgres, send packets, restart Postgres — messages sit in Kafka
   (lag grows) and drain in once the consumer can write again (this is the D1
   fix; fully realised once the listener disk-spool is added).
