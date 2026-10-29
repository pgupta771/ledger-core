CREATE TABLE IF NOT EXISTS accounts (
    id             UUID        PRIMARY KEY,
    iban           TEXT        NULL UNIQUE,
    currency       CHAR(3)     NOT NULL,
    type           TEXT        NOT NULL,
    allow_negative BOOLEAN     NOT NULL DEFAULT FALSE,
    balance_minor  BIGINT      NOT NULL DEFAULT 0,
    version        BIGINT      NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_accounts_non_negative CHECK (allow_negative OR balance_minor >= 0)
);

CREATE TABLE IF NOT EXISTS transfers (
    id                   UUID        PRIMARY KEY,
    client_id            TEXT        NOT NULL,
    idempotency_key      TEXT        NOT NULL,
    status               TEXT        NOT NULL,
    currency             CHAR(3)     NOT NULL,
    amount_minor         BIGINT      NOT NULL CHECK (amount_minor > 0),
    from_account_id      UUID        NOT NULL REFERENCES accounts(id),
    to_account_id        UUID        NOT NULL REFERENCES accounts(id),
    reference            TEXT,
    reverses_transfer_id UUID        NULL UNIQUE REFERENCES transfers(id),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_transfers_client_idempotency UNIQUE (client_id, idempotency_key),
    CONSTRAINT chk_transfers_distinct_accounts CHECK (from_account_id <> to_account_id)
);

CREATE TABLE IF NOT EXISTS entries (
    id           BIGSERIAL   PRIMARY KEY,
    transfer_id  UUID        NOT NULL REFERENCES transfers(id),
    account_id   UUID        NOT NULL REFERENCES accounts(id),
    amount_minor BIGINT      NOT NULL CHECK (amount_minor <> 0),
    currency     CHAR(3)     NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entries_account_id_id ON entries (account_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_entries_transfer_id ON entries (transfer_id);

-- 1. Append-only trigger: reject UPDATE and DELETE on entries
CREATE OR REPLACE FUNCTION fn_prevent_entries_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Ledger entries are append-only: % on entries is prohibited', TG_OP
        USING ERRCODE = '55000';
END;
$$;

DROP TRIGGER IF EXISTS trg_entries_append_only ON entries;
CREATE TRIGGER trg_entries_append_only
BEFORE UPDATE OR DELETE ON entries
FOR EACH ROW
EXECUTE FUNCTION fn_prevent_entries_mutation();

-- 2. Currency match trigger: entry currency must equal its account currency and transfer currency
CREATE OR REPLACE FUNCTION fn_verify_entry_currency()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_account_currency  CHAR(3);
    v_transfer_currency CHAR(3);
BEGIN
    SELECT currency INTO v_account_currency FROM accounts WHERE id = NEW.account_id;
    IF v_account_currency IS NULL OR v_account_currency <> NEW.currency THEN
        RAISE EXCEPTION 'Entry currency (%) does not match account % currency (%)',
            NEW.currency, NEW.account_id, v_account_currency
            USING ERRCODE = '23514';
    END IF;

    SELECT currency INTO v_transfer_currency FROM transfers WHERE id = NEW.transfer_id;
    IF v_transfer_currency IS NULL OR v_transfer_currency <> NEW.currency THEN
        RAISE EXCEPTION 'Entry currency (%) does not match transfer % currency (%)',
            NEW.currency, NEW.transfer_id, v_transfer_currency
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_entries_currency_match ON entries;
CREATE TRIGGER trg_entries_currency_match
BEFORE INSERT ON entries
FOR EACH ROW
EXECUTE FUNCTION fn_verify_entry_currency();

-- 3. Deferred constraint triggers: verify >= 2 entries and SUM(amount_minor) = 0 at COMMIT
CREATE OR REPLACE FUNCTION check_transfer_balanced(p_transfer_id UUID)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_entry_count INT;
    v_sum_minor   BIGINT;
BEGIN
    SELECT COUNT(*), COALESCE(SUM(amount_minor), 0)
      INTO v_entry_count, v_sum_minor
      FROM entries
     WHERE transfer_id = p_transfer_id;

    IF v_entry_count < 2 THEN
        RAISE EXCEPTION 'Transfer % must have at least 2 entries, found %', p_transfer_id, v_entry_count
            USING ERRCODE = '23514';
    END IF;

    IF v_sum_minor <> 0 THEN
        RAISE EXCEPTION 'Transfer % entries do not sum to zero (sum = %)', p_transfer_id, v_sum_minor
            USING ERRCODE = '23514';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION fn_verify_entries_balanced()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM check_transfer_balanced(NEW.transfer_id);
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION fn_verify_transfer_has_entries()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM check_transfer_balanced(NEW.id);
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_entries_balanced_deferred ON entries;
CREATE CONSTRAINT TRIGGER trg_entries_balanced_deferred
AFTER INSERT ON entries
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION fn_verify_entries_balanced();

DROP TRIGGER IF EXISTS trg_transfers_has_entries_deferred ON transfers;
CREATE CONSTRAINT TRIGGER trg_transfers_has_entries_deferred
AFTER INSERT ON transfers
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION fn_verify_transfer_has_entries();

CREATE TABLE IF NOT EXISTS idempotency_keys (
    client_id     TEXT        NOT NULL,
    key           TEXT        NOT NULL,
    request_hash  CHAR(64)    NOT NULL,
    status        TEXT        NOT NULL,
    response_code INT,
    response_body JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (client_id, key)
);

CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL   PRIMARY KEY,
    aggregate_id UUID        NOT NULL,
    event_type   TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox (id ASC)
    WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS account_balance_view (
    account_id    UUID   PRIMARY KEY,
    balance_minor BIGINT NOT NULL,
    last_event_id BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id     BIGINT      PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
