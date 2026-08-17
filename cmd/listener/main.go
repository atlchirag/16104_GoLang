// Command listener is Tier 1 of the architecture: it accepts device TCP
// connections, frames packets correctly, acknowledges each with 0xAA, parses
// them, and publishes to Kafka. It does NOT touch the database — that is the
// whole point of the redesign (plan section 3.2).
//
// Config via environment (fail fast if Kafka is unreachable):
//
//	LISTEN_ADDR      default ":16104"
//	KAFKA_BROKERS    default "localhost:9092"   (comma-separated)
//	INSTANCE_NAME    default hostname
package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/wiziot/gps-platform/internal/idgen"
	"github.com/wiziot/gps-platform/internal/kafkax"
	"github.com/wiziot/gps-platform/internal/model"
	"github.com/wiziot/gps-platform/internal/protocol"
)

const maxFrameBytes = 64 * 1024

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	addr := env("LISTEN_ADDR", ":16104")
	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
	instance := env("INSTANCE_NAME", hostname())

	producer, err := kafkax.NewProducer(brokers)
	if err != nil {
		log.Error("kafka producer init failed", "err", err)
		os.Exit(1)
	}
	defer producer.Close()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error("listen failed", "addr", addr, "err", err)
		os.Exit(1)
	}
	log.Info("listener started", "addr", addr, "brokers", brokers, "instance", instance)

	srv := &server{log: log, producer: producer, instance: instance, port: portOf(addr)}

	// Graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		log.Info("shutdown signal received, closing listener")
		_ = ln.Close()
	}()

	var wg sync.WaitGroup
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break // listener closed by shutdown
			}
			log.Warn("accept error", "err", err)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.handleConn(ctx, conn)
		}()
	}

	wg.Wait()
	flushCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := producer.Flush(flushCtx); err != nil {
		log.Warn("producer flush on shutdown", "err", err)
	}
	log.Info("listener stopped cleanly")
}

type server struct {
	log      *slog.Logger
	producer *kafkax.Producer
	instance string
	port     int
}

func (s *server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	remote := conn.RemoteAddr().String()
	s.log.Info("device connected", "remote", remote)

	scanner := protocol.NewFrameScanner(conn, maxFrameBytes)
	for scanner.Scan() {
		raw := string(scanner.Bytes())

		// 1) Acknowledge immediately, before any other work
		if _, err := conn.Write([]byte{0xAA}); err != nil {
			s.log.Warn("ack write failed", "remote", remote, "err", err)
			return
		}

		imei := protocol.PeekIMEI(raw)

		// 2) Publish the raw bytes first, so the original packet is retained for
		//    replay even if parsing below fails (plan section 3.4, "replay").
		s.producer.Publish(ctx, kafkax.TopicRaw, imei, []byte(raw), func(err error) {
			s.log.Error("produce raw failed", "imei", imei, "err", err)
		})

		// 3) Parse and publish the structured telemetry message.
		msg, err := protocol.ParseGPRMC(raw)
		if err != nil {
			// Non-GPRMC (command responses, heartbeats) and malformed packets
			// are expected traffic; log at debug and move on. A later increment
			// routes genuine parse failures to gps.dlq.
			s.log.Debug("skip non-telemetry frame", "imei", imei, "err", err)
			continue
		}
		msg.MessageID = idgen.NewULID()
		msg.Listener = model.ListenerMeta{Instance: s.instance, Port: s.port, RemoteAddr: remote}

		payload, err := json.Marshal(msg)
		if err != nil {
			s.log.Error("marshal telemetry failed", "imei", imei, "err", err)
			continue
		}
		s.producer.Publish(ctx, kafkax.TopicTelemetry, imei, payload, func(err error) {
			s.log.Error("produce telemetry failed", "imei", imei, "err", err)
		})
	}

	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		s.log.Warn("connection scan error", "remote", remote, "err", err)
	}
	s.log.Info("device disconnected", "remote", remote)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "listener"
	}
	return h
}

func portOf(addr string) int {
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		return 0
	}
	n := 0
	for _, c := range p {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}
