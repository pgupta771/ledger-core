package dev.ledger.service.idempotency;

import java.time.Instant;

/**
 * Stored idempotency record in PostgreSQL {@code idempotency_keys}.
 */
public record IdempotencyRecord(
        String clientId,
        String key,
        String requestHash,
        IdempotencyStatus status,
        Integer responseCode,
        String responseBodyJson,
        Instant createdAt) {}
