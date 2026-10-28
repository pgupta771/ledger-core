package dev.ledger.service.idempotency;

/**
 * Persistence states for an idempotency key record.
 */
public enum IdempotencyStatus {
    IN_PROGRESS,
    COMPLETED
}
