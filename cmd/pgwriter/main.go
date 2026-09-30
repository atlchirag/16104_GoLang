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
//	TELEMETRY_TABLE_MODE  "daily" (default) or "monthly". Monthly restores the
//	                      pre-August-2026 tbl_telemetry_<mmmyy> naming. NOTHING
//	                      creates monthly tables any more, so the target month's
//	                      table must already exist and must carry a unique index
//	                      over (gps_time, sys_service_id).
//
// The defaults above are the LOCAL DEV values, so `go run ./cmd/pgwriter` works
// on a developer machine with no setup.
//
// Two environments, switched by which env file you load:
//
//	PRODUCTION (atlvm-6)   deploy/pgwriter.env        Postgres role `newtrack`
//	                        That host does NOT accept
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
	"errors"
	"fmt"
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
	tableMode := env("TELEMETRY_TABLE_MODE", store.TableDaily)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, dsn, utcOffset, tableMode)
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

	// Records that can never be written go here rather than being retried
	// forever or dropped. Same broker set as the consumer.
	dlq, err := kafkax.NewProducer(brokers)
	if err != nil {
		log.Error("kafka dlq producer init failed", "err", err)
		os.Exit(1)
	}
	defer dlq.Close()

	if st.TableMode() == store.TableMonthly {
		log.Warn("TELEMETRY_TABLE_MODE=monthly: writing tbl_telemetry_<mmmyy>. " +
			"Nothing creates monthly tables automatically -- confirm next month's " +
			"table exists before the month rolls over, or rows will land in the DLQ as 42P01.")
	}
	log.Info("pgwriter started", "table_mode", st.TableMode(), "brokers", brokers, "group", group, "topic", kafkax.TopicTelemetry, "dlq", kafkax.TopicDLQ)

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

		// Transient failures are retried HERE, holding the batch. Returning and
		// polling again does NOT get these records back within one session --
		// see kafkax.RetryBatch.
		if err := kafkax.RetryBatch(ctx, log, len(records), func() error {
			return writeBatch(ctx, log, st, dlq, records)
		}); err != nil {
			continue // context cancelled; shutting down
		}

		// Database is durable now; commit Kafka offsets.
		if err := cl.CommitRecords(ctx, records...); err != nil {
			log.Error("offset commit failed", "err", err)
		}
	}

	log.Info("pgwriter stopped cleanly")
}

// writeBatch writes each record, separating the two kinds of failure.
//
// A TRANSIENT failure (database down, deadlock, timeout) returns an error: the
// batch is not committed and Kafka redelivers it, which is exactly what makes
// those conditions recover.
//
// A PERMANENT failure — a record that will fail identically no matter how often
// it is retried — is diverted to gps.dlq and skipped. Returning an error for
// those instead is what took the pipeline down: one device reporting a 2001
// timestamp aimed the insert at a daily table that did not exist, and the
// resulting infinite retry stalled the partition for hours, including for the
// correctly-dated packets queued behind it.
func writeBatch(ctx context.Context, log *slog.Logger, st *store.Store, dlq *kafkax.Producer, records []*kgo.Record) error {
	for _, rec := range records {
		var msg model.TelemetryMessage
		if err := json.Unmarshal(rec.Value, &msg); err != nil {
			if dlqErr := toDLQ(ctx, dlq, rec, "decode_failed", err); dlqErr != nil {
				return dlqErr
			}
			log.Warn("decode failed, routed to DLQ", "offset", rec.Offset, "err", err)
			continue
		}

		serviceID, err := st.ResolveServiceID(ctx, msg.IMEI)
		if errors.Is(err, store.ErrUnknownIMEI) {
			if dlqErr := toDLQ(ctx, dlq, rec, "unknown_imei", err); dlqErr != nil {
				return dlqErr
			}
			log.Warn("unknown imei, routed to DLQ", "imei", msg.IMEI, "offset", rec.Offset)
			continue
		}
		if err != nil {
			return err // transient lookup failure -> redelivery
		}

		if err := st.InsertTelemetry(ctx, &msg, serviceID, rec.Value); err != nil {
			condition, permanent := store.IsPermanent(err)
			if !permanent {
				return err // fail the whole batch -> redelivery
			}
			if dlqErr := toDLQ(ctx, dlq, rec, condition, err); dlqErr != nil {
				return dlqErr
			}
			log.Warn("permanent insert failure, routed to DLQ",
				"condition", condition, "imei", msg.IMEI, "offset", rec.Offset,
				"gps_time", msg.GPSTime.Format(time.RFC3339), "err", err)
			continue
		}
		log.Info("inserted", "imei", msg.IMEI, "service_id", serviceID, "gps_time", msg.GPSTime.Format(time.RFC3339))
	}
	return nil
}

// toDLQ copies a record to gps.dlq, preserving its key so a device's rejects
// stay ordered, and recording where it came from and why it was rejected.
//
// It publishes synchronously and propagates any failure to the caller, which
// aborts the batch. Skipping a record whose DLQ copy had not landed would
// destroy it: the source offset is committed moments later.
func toDLQ(ctx context.Context, dlq *kafkax.Producer, rec *kgo.Record, reason string, cause error) error {
	err := dlq.PublishSync(ctx, kafkax.TopicDLQ, string(rec.Key), rec.Value, map[string]string{
		"dlq_reason":       reason,
		"dlq_error":        cause.Error(),
		"dlq_source_topic": rec.Topic,
		"dlq_source_part":  strconv.FormatInt(int64(rec.Partition), 10),
		"dlq_source_off":   strconv.FormatInt(rec.Offset, 10),
		"dlq_at":           time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("dlq publish (offset %d, reason %s): %w", rec.Offset, reason, err)
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
