-- Merchant callback attempts per message. The notifier caps attempts on this
-- count, not on the broker's delivery count, so redeliveries caused by our own
-- failures (database down, shutdown) don't use up the merchant's retries.
-- Only messages with at least one failed callback get a row.
CREATE TABLE IF NOT EXISTS delivery_attempts (
    consumer   text        NOT NULL,
    message_id text        NOT NULL,
    attempts   integer     NOT NULL,
    last_error text        NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);
