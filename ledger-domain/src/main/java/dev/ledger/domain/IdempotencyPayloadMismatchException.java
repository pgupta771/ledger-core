package dev.ledger.domain;

/**
 * Thrown when an Idempotency-Key is reused with a different request payload hash (HTTP 422).
 */
public class IdempotencyPayloadMismatchException extends DomainValidationException {

    public IdempotencyPayloadMismatchException(String message) {
        super(message);
    }
}
