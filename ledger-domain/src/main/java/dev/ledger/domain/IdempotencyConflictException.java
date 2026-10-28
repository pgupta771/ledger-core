package dev.ledger.domain;

/**
 * Thrown when a request with the same (client_id, idempotency_key) is currently in flight (HTTP 409).
 */
public class IdempotencyConflictException extends RuntimeException {

    private final int retryAfterSeconds;

    public IdempotencyConflictException(String message, int retryAfterSeconds) {
        super(message);
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public int retryAfterSeconds() {
        return retryAfterSeconds;
    }
}
