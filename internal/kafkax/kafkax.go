// Package kafkax is a thin wrapper over the franz-go Kafka client (twmb/franz-go,
// chosen in appendix A of the plan: pure Go, no cgo, so it builds on Windows and
// Linux with no C toolchain). It centralises producer/consumer construction so
// the durability settings from plan section 4.5 live in exactly one place.
package kafkax

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Topic names (plan section 4.1).
const (
	TopicRaw       = "gps.raw"
	TopicTelemetry = "gps.telemetry"
	TopicDLQ       = "gps.dlq"
)

// Producer is used by the listener. Production is asynchronous: Publish returns
// as soon as the record is buffered locally, so a connection goroutine never
// blocks on a broker round trip (plan section 4.4.5).
type Producer struct {
	cl *kgo.Client
}

// NewProducer builds a durable producer:
//   - RequiredAcks(all): leader waits for all in-sync replicas before ack.
//   - Idempotent (franz-go default with acks=all): broker de-dupes retries.
//   - lz4 compression and a 20ms linger to batch effectively.
func NewProducer(brokers []string) (*Producer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.ProducerLinger(20*time.Millisecond),
		kgo.RecordDeliveryTimeout(2*time.Minute),
	)
	if err != nil {
		return nil, err
	}
	return &Producer{cl: cl}, nil
}

// Publish enqueues a record. onErr is called from a client goroutine if the
// record ultimately fails to deliver; that is where the disk-spool fallback
// (plan section 4.4.6) will hook in a later increment.
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte, onErr func(error)) {
	p.cl.Produce(ctx, &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	}, func(_ *kgo.Record, err error) {
		if err != nil && onErr != nil {
			onErr(err)
		}
	})
}

// PublishSync enqueues a record and blocks until the broker acknowledges it.
//
// The DLQ path uses this rather than Publish: a record is only dropped from the
// main pipeline once it is durably in gps.dlq, so the caller may commit the
// source offset immediately afterwards without risking silent loss.
func (p *Producer) PublishSync(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	rec := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	}
	for k, v := range headers {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	return p.cl.ProduceSync(ctx, rec).FirstErr()
}

// Flush blocks until all buffered records are delivered or ctx expires. Called
// during graceful shutdown.
func (p *Producer) Flush(ctx context.Context) error { return p.cl.Flush(ctx) }

// Close flushes and shuts the client down.
func (p *Producer) Close() { p.cl.Close() }

// NewConsumer builds a consumer-group client with auto-commit DISABLED. The
// consumer commits offsets manually, strictly after the database transaction
// commits (plan section 4.6.1), so a crash mid-batch redelivers rather than
// silently loses data.
func NewConsumer(brokers []string, group string, topics ...string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		// Start a brand-new group at the earliest offset so a fresh test run
		// drains everything already in the topic.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
}
