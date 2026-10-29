# ADR-0005: Transactional Outbox (`FOR UPDATE SKIP LOCKED`) Over Dual Writes

## Context
Publishing a `TransferCompleted` message directly to Kafka during a PostgreSQL transfer runs the classic dual-write hazard: if the database commits but the Kafka send fails (or the JVM crashes), downstream balance projections silently lose money updates; if Kafka is sent before the database commits and the transaction rolls back, consumers observe phantom transfers.

## Decision
Every transfer transaction inserts a `TransferCompleted` row into the PostgreSQL `outbox` table atomically alongside `transfers`, `entries`, and `accounts`. A separate `OutboxPublisher` polls unpublished rows via `SELECT ... WHERE published_at IS NULL ORDER BY id ASC LIMIT ? FOR UPDATE SKIP LOCKED`, publishes synchronously to Kafka (`acks=all`, `enable.idempotence=true`) carrying the monotonic `outbox.id` as `eventId`, and sets `published_at`. In `projector-service`, each Kafka event is applied in a single transaction that first inserts `event_id` into `processed_events` (`ON CONFLICT DO NOTHING`) before updating `account_balance_view`.

## Consequences
No committed transfer can ever drop its Kafka event, multiple publisher instances can poll concurrently without blocking or double-claiming in-flight rows via `SKIP LOCKED`, and at-least-once duplicates created when a publisher crashes after Kafka ack but before setting `published_at` are deterministically ignored by `projector-service`.
