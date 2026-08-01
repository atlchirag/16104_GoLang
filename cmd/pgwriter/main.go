// Command pgwriter is a Tier 3 consumer: it reads parsed telemetry from Kafka
// and writes it to PostgreSQL in batches. Offsets are committed only AFTER the
// database work succeeds (plan section 4.6.1), so a crash mid-batch causes
// redelivery — which is harmless because every write is idempotent.
//
// Config via environment:
//
//	KAFKA_BROKERS         default "localhost:9092"
//	CONSUMER_GROUP        default "pg-writer"
//	POSTGRES_DSN          default "postgres://postgres:root@localhost:5432/atltracking"
//	PGWRITER_UTC_OFFSET   minutes added to gps_time for table selection, default 330 (India)
//
// The defaults above are the LOCAL DEV values, so `go run ./cmd/pgwriter` works
// on a developer machine with no setup. They are deliberately NOT the
// production values -- credentials must not live in this repo, which is public.
//
// Two environments, switched by which env file you load:
//
//	PRODUCTION (atlvm-6)   deploy/pgwriter.env        Postgres role `newtrack`
//	                       on 192.168.23.135:5432. That host does NOT accept
//	                       "localhost" -- Postgres is not bound to loopback there.
//	LOCAL DEV              deploy/pgwriter.local.env  postgres@localhost.
//
// Both files are gitignored. To switch, either load the other file or comment
// or uncomment the POSTGRES_DSN block inside the one you use:
//
//	set -a; source deploy/pgwriter.env; set +a; ./pgwriter
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/wiziot/gps-platform/internal/kafkax"
	"github.com/wiziot/gps-platform/internal/model"
	"github.com/wiziot/gps-platform/internal/store"
)

const (
	maxBatchRecords = 2000
	maxBatchWait    = 200 * time.Millisecond
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
	group := env("CONSUMER_GROUP", "pg-writer")
	dsn := env("POSTGRES_DSN", "postgres://postgres:root@localhost:5432/atltracking")
	utcOffset := envInt("PGWRITER_UTC_OFFSET", 330)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, dsn, utcOffset)
	if err != nil {
		log.Error("postgres init failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	cl, err := kafkax.NewConsumer(brokers, group, kafkax.TopicTelemetry)
	if err != nil {
		log.Error("kafka consumer init failed", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	log.Info("pgwriter started", "brokers", brokers, "group", group, "topic", kafkax.TopicTelemetry)

	for ctx.Err() == nil {
		// Poll accumulates up to maxBatchRecords. We bound the wait so a quiet
		// topic still commits promptly.
		pollCtx, cancel := context.WithTimeout(ctx, maxBatchWait)
		fetches := cl.PollRecords(pollCtx, maxBatchRecords)
		cancel()

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == context.DeadlineExceeded || e.Err == context.Canceled {
					continue
				}
				log.Warn("poll error", "topic", e.Topic, "err", e.Err)
			}
		}

		records := fetches.Records()
		if len(records) == 0 {
			continue
		}

		if err := writeBatch(ctx, log, st, records); err != nil {
			// Do NOT commit offsets: the batch will be redelivered and retried.
			log.Error("batch write failed, will retry via redelivery", "count", len(records), "err", err)
			continue
		}

		// Database is durable now; commit Kafka offsets.
		if err := cl.CommitRecords(ctx, records...); err != nil {
			log.Error("offset commit failed", "err", err)
		}
	}

	log.Info("pgwriter stopped cleanly")
}

func writeBatch(ctx context.Context, log *slog.Logger, st *store.Store, records []*kgo.Record) error {
	for _, rec := range records {
		var msg model.TelemetryMessage
		if err := json.Unmarshal(rec.Value, &msg); err != nil {
			// A malformed message must not block the partition. In this thin
			// slice we log and skip; the next increment routes it to gps.dlq.
			log.Warn("decode failed, skipping (todo: DLQ)", "offset", rec.Offset, "err", err)
			continue
		}

		serviceID, err := st.ResolveServiceID(ctx, msg.IMEI)
		if err != nil {
			log.Warn("unknown imei, skipping", "imei", msg.IMEI, "err", err)
			continue
		}

		if err := st.InsertTelemetry(ctx, &msg, serviceID, rec.Value); err != nil {
			return err // fail the whole batch -> redelivery
		}
		log.Info("inserted", "imei", msg.IMEI, "service_id", serviceID, "gps_time", msg.GPSTime.Format(time.RFC3339))
	}
	return nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
