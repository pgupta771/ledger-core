package dev.ledger.domain;

/**
 * Thrown when an operation attempts to combine or transfer across mismatched ISO 4217 currencies.
 */
public class CurrencyMismatchException extends DomainValidationException {

    public CurrencyMismatchException(String message) {
        super(message);
    }
}
