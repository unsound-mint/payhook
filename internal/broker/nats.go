// Package broker wraps NATS JetStream: stream setup and publishing.
package broker

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	Stream   = "PAYMENTS"
	Subjects = "payments.>"
)

type JetStream struct {
	nc *nats.Conn
	JS jetstream.JetStream
}

func Connect(ctx context.Context, url string) (*JetStream, error) {
	nc, err := nats.Connect(url, nats.Name("payhook"), nats.MaxReconnects(-1))
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	// Duplicates is the dedupe window: a message published twice with the same
	// Nats-Msg-Id within it is stored once. It covers the relay crashing after
	// publishing but before marking the outbox row.
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       Stream,
		Subjects:   []string{Subjects},
		Storage:    jetstream.FileStorage,
		Duplicates: 2 * time.Minute,
		MaxAge:     7 * 24 * time.Hour,
	}); err != nil {
		nc.Close()
		return nil, err
	}
	return &JetStream{nc: nc, JS: js}, nil
}

func (j *JetStream) Publish(ctx context.Context, subject, msgID string, data []byte) error {
	_, err := j.JS.Publish(ctx, subject, data, jetstream.WithMsgID(msgID))
	return err
}

// Close drains in-flight messages before closing the connection.
func (j *JetStream) Close() { _ = j.nc.Drain() }
