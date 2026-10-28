# ADR-0003: Idempotency State Machine (`client_id`, `key`, `request_hash`)

## Context
Network timeouts and client retries can cause duplicate `POST /api/v1/transfers` requests. A payment ledger must guarantee that retrying a transfer with the same `Idempotency-Key` never debits an account twice, while catching accidental key reuse across different transfer parameters.

## Decision
We scope idempotency keys by `(client_id, key)` as the composite primary key of `idempotency_keys` and store a canonical `SHA-256` hash (`request_hash`) of `(fromAccountId, toAccountId, amountMinor, currency, reference, reversesTransferId)`. Every transfer transaction begins with `INSERT INTO idempotency_keys ... VALUES (..., 'IN_PROGRESS') ON CONFLICT (client_id, key) DO NOTHING`. If the row already exists:
1. A mismatched `request_hash` raises `IdempotencyPayloadMismatchException` (`HTTP 422`).
2. An `IN_PROGRESS` status raises `IdempotencyConflictException` (`HTTP 409` with `Retry-After: 1`).
3. A `COMPLETED` status with an identical `request_hash` returns the stored JSON response without re-running the transfer.

## Consequences
Concurrent duplicate requests serialize cleanly on the PostgreSQL unique index for `(client_id, key)`; the winning transaction executes the transfer and records the `COMPLETED` response atomically, and all concurrent retries receive the exact same transfer payload.
