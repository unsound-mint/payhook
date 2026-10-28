// Package broker wraps NATS JetStream: stream setup and publishing.
package broker

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	Stream   = "PAYMENTS"
	Subjects = "payments.>"
)

type JetStream struct {
	nc     *nats.Conn
	closed chan struct{}
	JS     jetstream.JetStream
}

func Connect(ctx context.Context, url string) (*JetStream, error) {
	closed := make(chan struct{})
	var once sync.Once
	nc, err := nats.Connect(url, nats.Name("payhook"), nats.MaxReconnects(-1),
		nats.ClosedHandler(func(*nats.Conn) { once.Do(func() { close(closed) }) }))
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
	return &JetStream{nc: nc, closed: closed, JS: js}, nil
}

func (j *JetStream) Publish(ctx context.Context, subject, msgID string, data []byte) error {
	_, err := j.JS.Publish(ctx, subject, data, jetstream.WithMsgID(msgID))
	return err
}

// Close drains the connection (flushing pending publishes and acks) and waits
// for it to close. Drain itself returns immediately.
func (j *JetStream) Close() {
	if err := j.nc.Drain(); err != nil {
		j.nc.Close()
	}
	select {
	case <-j.closed:
	case <-time.After(5 * time.Second):
		j.nc.Close()
	}
}
