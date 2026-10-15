CREATE TABLE payments (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    merchant_id     text        NOT NULL,
    amount          bigint      NOT NULL CHECK (amount > 0),
    currency        char(3)     NOT NULL,
    status          text        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'succeeded', 'failed', 'canceled', 'refunded')),
    provider_ref    text,
    callback_url    text,
    idempotency_key text        NOT NULL,
    request_hash    text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    -- A merchant retrying POST /payments with the same key gets the same payment back.
    UNIQUE (merchant_id, idempotency_key)
);

-- Every provider webhook we accepted. The primary key is what makes
-- duplicate deliveries of the same event harmless.
CREATE TABLE webhook_events (
    provider    text        NOT NULL,
    event_id    text        NOT NULL,
    payment_id  uuid        REFERENCES payments (id),
    event_type  text        NOT NULL,
    payload     jsonb       NOT NULL,
    outcome     text        NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, event_id)
);

-- Transactional outbox: written in the same transaction as the status change,
-- published to NATS later by the relay.
CREATE TABLE outbox (
    id           bigserial PRIMARY KEY,
    aggregate_id uuid        NOT NULL,
    event_type   text        NOT NULL,
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- Consumer-side deduplication (the broker delivers at least once).
CREATE TABLE processed_messages (
    consumer     text        NOT NULL,
    message_id   text        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);

-- Messages the notifier gave up on after all retries.
CREATE TABLE dead_letters (
    id         bigserial PRIMARY KEY,
    consumer   text        NOT NULL,
    message_id text        NOT NULL,
    payload    jsonb       NOT NULL,
    last_error text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
