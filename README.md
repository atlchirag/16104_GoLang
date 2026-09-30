# GPS Platform — Go + Kafka + PostgreSQL

Migration of the `GPSTrackerListeners.AtlantaNew` .NET Windows service to a
decoupled Go pipeline, following the architecture in
`GPS_Platform_Go_Kafka_Architecture_and_Migration_Plan.docx`.

```
GPS device --TCP:16104--> [listener] --produce--> Kafka --consume--> [pgwriter] --> PostgreSQL
                          (parse, ack 0xAA,        (gps.raw,          (batch, idempotent)
                           publish; NO db)          gps.telemetry)
```

This repository currently implements the **thin end-to-end slice**: a real GPRMC
packet flows from device → listener → Kafka → consumer → a row in Postgres.
BLE sensors, WPT100, L400 fuel, `$LOC`, geodata and alerting are the next
increments (see **Status** below).

## Repository layout

| Path | What |
|------|------|
| `cmd/listener`   | TCP listener (Tier 1): frame, ack, parse, publish. No DB. |
| `cmd/pgwriter`   | Kafka→Postgres consumer (Tier 3): batch, idempotent writes. |
| `cmd/simulator`  | Fake GPS device for testing; replays captured packets. |
| `internal/protocol` | Pure packet parser + stream framing (fully unit-tested). |
| `internal/model` | The `TelemetryMessage` wire type shared by all binaries. |
| `internal/kafkax` | franz-go producer/consumer wrappers (durability config). |
| `internal/store` | pgx access, IMEI→service_id cache, idempotent insert, `schema.sql`. |
| `internal/idgen` | ULID generator for `message_id`. |
| `deploy/`        | Topic-creation script, env examples. |
| `docs/`          | Kafka concepts + full testing guide. |

The Go module path is `github.com/wiziot/gps-platform`. If you push to a
different repo, rename it: `go mod edit -module <your/path>` then
`gofmt -w` any imports (or just keep this path — it does not have to match).

## Develop on Windows (no Kafka needed)

The parser is pure Go, so you can build and test everything here before pushing:

```powershell
go build ./...
go test ./...        # protocol tests run against the real captured packet
go vet ./...
```

## Test on Linux (native Kafka, no Docker)

Your Linux box already has Kafka installed, so there is nothing to containerise.

```bash
git clone <your-repo> && cd gps-platform      # or: git pull

# 1. Make sure Kafka (KRaft) and PostgreSQL are running (see docs/KAFKA_AND_TESTING.md).

# 2. Create the topics (once).
export KAFKA_BIN=/opt/kafka/bin               # dir with kafka-topics.sh
bash deploy/create-topics.sh

# 3. Create the test database schema (once).
createdb atltracking 2>/dev/null || true
psql -d atltracking -f internal/store/schema.sql

# 4. Run the consumer (terminal A).
set -a; source deploy/pgwriter.env; set +a
go run ./cmd/pgwriter

# 5. Run the listener (terminal B).
set -a; source deploy/listener.env; set +a
go run ./cmd/listener

# 6. Send a packet (terminal C).
go run ./cmd/simulator                        # built-in real packet
#   or replay a real capture file:
# go run ./cmd/simulator -file /path/to/864180055638907.txt

# 7. Verify.
psql -d atltracking -c "SELECT sys_service_id, gps_time, gps_latitude, gps_longitude, tel_odometer FROM tbl_telemetry_27052026;"
```

Telemetry lands in one table per day, `tbl_telemetry_<ddmmyyyy>`, chosen from the
packet's own `gps_time` at +330. Those tables are created the night before by
the **daily-telemetry** service (`atlchirag/Daily_Telemetry`), which must be
installed and running for pgwriter to have anywhere to write. Send the
simulator with `-date keep` to hit the captured day, or let it retarget to today.

You should see the simulator print `ack=0xAA`, the consumer log `inserted`, and
one row in `tbl_telemetry_27052026`. Send it again — still one row (idempotency).

## Telemetry table naming: daily or monthly

This is the only port in the family that can write **monthly** tables, and it is
switchable without a rebuild:

```
TELEMETRY_TABLE_MODE=daily     tbl_telemetry_25092026   (default)
TELEMETRY_TABLE_MODE=monthly   tbl_telemetry_sep26
```

Anything unrecognised falls back to `daily`, never silently to monthly.
`pgwriter` logs the mode it started in, and warns loudly when monthly is active.

**Currently set to `monthly` on atlvm-6** (2026-09-25) at the data owner's
request, for a test. Every other port writes daily, unconditionally.

### The catch, and it has a date on it

`daily-telemetry` creates the next **day's** table at 23:55 IST. **Nothing
creates monthly tables** — they are leftovers from the SQL Server era. So in
monthly mode the target table must already exist, and must carry a unique index
over `(gps_time, sys_service_id)` or `ON CONFLICT` fails with 42P10 and every
row is diverted to the DLQ.

`tbl_telemetry_sep26` exists and its
`tbl_telemetry_sep26_sys_service_id_gps_time_idx` is UNIQUE over
`(sys_service_id, gps_time)` — Postgres infers `ON CONFLICT` by column set, not
order, so it matches. Verified with a real insert inside a rolled-back
transaction before the switch.

**`tbl_telemetry_oct26` does NOT exist.** Because the table name is derived from
`gps_time + 330 minutes`, writes start targeting it at **2026-09-30 18:30 UTC**.
From that moment, in monthly mode, every row fails with 42P01 and lands in
`gps.dlq` (30-day retention, so replayable). Before then, either create
`tbl_telemetry_oct26` with that unique index, or switch back:

```bash
sudo sed -i 's/^TELEMETRY_TABLE_MODE=.*/TELEMETRY_TABLE_MODE=daily/' /etc/gps/pgwriter.env
sudo systemctl restart gps-pgwriter
```

Rolling back needs no rebuild and no redeploy — it is an env flip and a restart.
The previous binary is kept as `/opt/gps/bin/pgwriter.daily.bak-<timestamp>`.

## Status

Done: TCP framing (fixes D5), GPRMC + I/O parsing, Kafka produce (`gps.raw` +
`gps.telemetry`), batched idempotent Postgres writes (fixes D6), IMEI→service_id
cache (fixes D2), graceful shutdown.

Next: BLE sensor parsing (`internal/protocol/ble`), WPT100, L400 fuel, `$LOC`
address + geodata, disk-spool fallback (D1 second layer), DLQ, golden-file
parity harness against the .NET output, and the alert-engine consumer (D4).

See `docs/KAFKA_AND_TESTING.md` for Kafka concepts and the deeper testing guide.
