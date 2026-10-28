package dev.ledger.domain;

/**
 * Thrown when a domain invariant or value object validation fails.
 */
public class DomainValidationException extends RuntimeException {

    public DomainValidationException(String message) {
        super(message);
    }
}
