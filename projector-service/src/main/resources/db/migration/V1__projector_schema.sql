CREATE TABLE IF NOT EXISTS account_balance_view (
    account_id    UUID   PRIMARY KEY,
    balance_minor BIGINT NOT NULL,
    last_event_id BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id     BIGINT      PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
