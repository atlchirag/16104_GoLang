package kafkax

import (
	"context"
	"log/slog"
	"time"
)

// RetryBatch runs a batch write until it succeeds, the context is cancelled, or
// the work itself gives up.
//
// # Why this exists
//
// The consumers say "do not commit the offsets and Kafka will redeliver". That
// is true only across a REBALANCE OR A RESTART. Within one session a franz-go
// client has already advanced its own position past the records PollRecords
// handed out, so simply returning an error and polling again does not get them
// back -- the next poll returns the NEXT records, and the failed ones are not
// seen again until the process restarts.
//
// That was demonstrated on 2026-09-18 during the 16802 install: a permission
// error on the day's table failed one batch, nothing retried it, and the packet
// only landed after `systemctl restart`. Nothing was lost -- the offsets were
// never committed, so the restart replayed it -- but the pipeline had silently
// stopped making progress on that partition in the meantime.
//
// So a transient failure is retried HERE, in process, holding the batch. The
// alternative is seeking the consumer back to the failed offsets, which is more
// machinery for the same effect and gets the ordering subtly wrong when a batch
// spans partitions.
//
// Permanent failures never reach this: the caller has already diverted those to
// the DLQ. What is left is "the database is down / deadlocked / out of
// connections", and the right response to all three is to keep trying.
func RetryBatch(ctx context.Context, log *slog.Logger, count int, do func() error) error {
	const (
		initialBackoff = 200 * time.Millisecond
		maxBackoff     = 30 * time.Second
	)

	backoff := initialBackoff
	for attempt := 1; ; attempt++ {
		err := do()
		if err == nil {
			if attempt > 1 {
				log.Info("batch succeeded after retrying", "attempts", attempt, "count", count)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Loud at first, then quiet: a database that is down for an hour should
		// not fill the journal, but the first few failures must be visible.
		if attempt <= 3 || attempt%20 == 0 {
			log.Error("batch write failed, holding the batch and retrying",
				"attempt", attempt, "count", count, "backoff", backoff, "err", err)
		}

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}
