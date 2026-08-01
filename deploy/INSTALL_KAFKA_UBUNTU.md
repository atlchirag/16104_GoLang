# Installing Kafka 4.3.1 (KRaft) on Ubuntu/Debian — single node, no Docker

Target: one Linux server running **listener + Kafka + pgwriter + Postgres** together.
Kafka 4.x has no ZooKeeper — one process is both broker and controller (KRaft).

> **Already done on `atlvm-6` (103.114.154.161) on 2026-07-31.**
> Cluster ID `0yFgWQF4S12MHyTRAOEyGA`. Keep this doc for rebuilds and for the
> second broker if the cluster ever grows.
>
> **Note for that host:** Postgres 18 listens on `192.168.23.135:5432`, *not*
> localhost, so `POSTGRES_DSN` must not use `localhost` there (Step 9).

Run everything as a sudo-capable user. `$` = your shell.

---

## Step 0 — Preflight

```bash
lsb_release -a          # distro + version
free -g                 # RAM: need >=8GB with Postgres on the same box
df -h /                 # disk, and see what else is mounted
nproc                   # cores
java -version 2>&1      # may not exist yet -- that is fine
```

**Capacity check before you go further.** Kafka keeps every message for its
retention window, on disk, whether or not pgwriter consumed it.
Rough sizing for our topics:

```
bytes/day  =  packets_per_device_per_day x device_count x ~300 bytes x 2
              (x2 because each packet lands in BOTH gps.raw and gps.telemetry)

disk needed ~ bytes/day x 7        (gps.raw is the 7-day topic)
```

Example: 5,000 devices x 2,880 packets/day (one per 30s) x 300B x 2 ≈ **8.6 GB/day**,
so ≈ **60 GB** for the 7-day window, plus headroom. Size `log.dirs` accordingly.
If that number is uncomfortable, cut `gps.raw` retention in `create-topics.sh`.

---

## Step 1 — Install Java 17+

Kafka 4.x brokers require **Java 17 or newer** (Java 8 and 11 are removed).

```bash
sudo apt update
sudo apt install -y openjdk-21-jdk-headless
java -version
```

On older Debian/Ubuntu without a 21 package, use `openjdk-17-jdk-headless`.
Note the actual path — you need it for the systemd unit:

```bash
readlink -f "$(which java)" | sed 's|/bin/java||'
# e.g. /usr/lib/jvm/java-21-openjdk-amd64
```

---

## Step 2 — Create a dedicated `kafka` user and directories

Never run a broker as root: a compromised or buggy broker should not be able to
touch the rest of the system, and its data dirs should not be world-writable.

```bash
sudo useradd -r -m -d /var/lib/kafka -s /usr/sbin/nologin kafka

sudo mkdir -p /var/lib/kafka/data   # message data (log.dirs)
sudo mkdir -p /var/log/kafka        # Kafka's own application logs
sudo mkdir -p /etc/kafka            # our config

sudo chown -R kafka:kafka /var/lib/kafka /var/log/kafka
sudo chmod 750 /var/lib/kafka/data
```

---

## Step 3 — Download and verify the release

Verifying matters: you are about to run this code as a service. A checksum
mismatch means a corrupt or tampered download — stop and re-fetch.

```bash
cd /tmp
KAFKA_VER=4.3.1
curl -LO "https://downloads.apache.org/kafka/${KAFKA_VER}/kafka_2.13-${KAFKA_VER}.tgz"
curl -LO "https://downloads.apache.org/kafka/${KAFKA_VER}/kafka_2.13-${KAFKA_VER}.tgz.sha512"

# Must print: kafka_2.13-4.3.1.tgz: OK
sha512sum -c "kafka_2.13-${KAFKA_VER}.tgz.sha512"
```

If the `.sha512` file is in the "BSD" format and `-c` complains, compare by eye:

```bash
sha512sum "kafka_2.13-${KAFKA_VER}.tgz"
cat "kafka_2.13-${KAFKA_VER}.tgz.sha512"
```

Extract to a versioned directory and symlink `/opt/kafka` at it. The symlink is
what makes upgrades safe: install 4.4.0 alongside, repoint the link, restart.

```bash
sudo tar -xzf "kafka_2.13-${KAFKA_VER}.tgz" -C /opt
sudo ln -sfn "/opt/kafka_2.13-${KAFKA_VER}" /opt/kafka
sudo chown -R kafka:kafka "/opt/kafka_2.13-${KAFKA_VER}"
/opt/kafka/bin/kafka-topics.sh --version
```

---

## Step 4 — Build the config

Start from the shipped config and append our overrides. Later keys win, so the
appended block takes effect.

```bash
sudo cp /opt/kafka/config/server.properties /etc/kafka/server.properties

# from your cloned repo:
sudo tee -a /etc/kafka/server.properties < deploy/kafka-server.properties

sudo chown kafka:kafka /etc/kafka/server.properties
sudo chmod 640 /etc/kafka/server.properties
```

Sanity-check the values that actually took effect (last occurrence wins):

```bash
for k in log.dirs listeners advertised.listeners num.partitions \
         auto.create.topics.enable offsets.topic.replication.factor; do
  printf '%-40s %s\n' "$k" "$(grep "^$k=" /etc/kafka/server.properties | tail -1)"
done
```

---

## Step 5 — Format the storage directory (one time only)

KRaft requires the data dir to be initialised with a cluster ID before first
start. **Only ever do this once.** Re-running it on a live cluster wipes the
metadata and orphans your data.

```bash
KAFKA_CLUSTER_ID="$(/opt/kafka/bin/kafka-storage.sh random-uuid)"
echo "$KAFKA_CLUSTER_ID"     # write this down

sudo -u kafka /opt/kafka/bin/kafka-storage.sh format \
    --standalone \
    -t "$KAFKA_CLUSTER_ID" \
    -c /etc/kafka/server.properties
```

`--standalone` registers this node as the sole controller voter — correct for a
single box, and it leaves the door open to add voters later.

Verify:

```bash
sudo cat /var/lib/kafka/data/meta.properties
```

---

## Step 6 — Install the systemd unit

```bash
sudo cp deploy/kafka.service /etc/systemd/system/kafka.service
# Edit JAVA_HOME if your path from Step 1 differs, and KAFKA_HEAP_OPTS to suit RAM.
sudo systemctl daemon-reload
sudo systemctl enable --now kafka
```

Check it came up:

```bash
systemctl status kafka --no-pager
sudo journalctl -u kafka -n 50 --no-pager
ss -ltnp | grep -E '9092|9093'      # expect 127.0.0.1:9092 and 127.0.0.1:9093
```

A broker that starts and immediately exits is nearly always: wrong `JAVA_HOME`,
`log.dirs` not writable by `kafka`, or a `--standalone` format that was skipped.
The journal says which.

---

## Step 7 — Create the topics

```bash
export KAFKA_BIN=/opt/kafka/bin
export BROKER=127.0.0.1:9092
export RF=1
bash deploy/create-topics.sh

$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BROKER --list
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BROKER --describe --topic gps.telemetry
```

---

## Step 8 — Smoke test the broker by itself

Prove Kafka works before blaming it for a Go bug. Two terminals:

```bash
# Terminal A -- consume
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 \
    --topic gps.dlq --from-beginning

# Terminal B -- produce, type a line, press Enter
/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 \
    --topic gps.dlq
```

The line should appear in Terminal A. Then restart Kafka and re-run the
consumer with `--from-beginning`: the message is still there. That is durability.

---

## Step 9 — Point the Go services at it

```bash
export KAFKA_BROKERS=127.0.0.1:9092
```

That is already the default in `cmd/listener` and `cmd/pgwriter`, and matches
`deploy/listener.env.example` / `deploy/pgwriter.env.example`.

Then follow the end-to-end steps in the README and `docs/KAFKA_AND_TESTING.md`.

---

## Step 10 — Operational must-dos

**Watch the disk.** On this box a full disk stops Kafka *and* Postgres *and* the
listener at once. Kafka does not gracefully degrade when `log.dirs` fills — it
shuts the log dir down.

```bash
df -h /var/lib/kafka/data
du -sh /var/lib/kafka/data/*
```

Set an alert at 75%.

**Rotate Kafka's own logs.** `/var/log/kafka/server.log` grows unbounded by
default (Kafka's log4j config keeps limited history, but check it):

```bash
ls -lh /var/log/kafka/
```

**Consumer lag is your health metric** — not CPU:

```bash
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 \
    --describe --group pg-writer
```

Steady lag near zero = healthy. Growing lag = pgwriter or Postgres is behind.

**Restarts are an outage.** One broker means every `systemctl restart kafka`
drops the pipeline for a few seconds. The listener must buffer (the planned
disk-spool) or those packets are lost at the front door.

---

## Known limits of this single-node setup

These are the accepted trade-offs of one server — worth stating plainly so they
are a decision, not a surprise:

| Risk | Consequence | Fix when it matters |
|------|-------------|---------------------|
| RF=1 | Disk loss = loss of anything Kafka accepted but pgwriter had not yet written | 3 brokers, RF=3, `min.insync.replicas=2` |
| Single broker | Every restart/upgrade is a pipeline outage | 3 brokers, rolling restart |
| Co-located with Postgres | They compete for page cache and IO; a Postgres burst slows Kafka fsyncs | Separate disks now, separate hosts later |
| PLAINTEXT, no auth | Anything that can reach 9092 has full read/write/delete | Fine while bound to 127.0.0.1; needs SASL/TLS the day a client moves off-box |

The loopback binding in `deploy/kafka-server.properties` is what keeps the
no-auth setup acceptable. If you ever change `listeners` to a routable IP,
add authentication in the same change.
