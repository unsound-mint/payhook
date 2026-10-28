-- Outbox rows the relay can't turn into an event (payload of the wrong shape,
-- no event type) are set aside here instead of blocking every row behind them.
ALTER TABLE outbox
    ADD COLUMN IF NOT EXISTS failed_at  timestamptz,
    ADD COLUMN IF NOT EXISTS last_error text;
