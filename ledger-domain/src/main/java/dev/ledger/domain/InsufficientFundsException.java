package dev.ledger.domain;

import java.util.UUID;

/**
 * Thrown when a debit would bring a non-negative account below zero minor units.
 */
public class InsufficientFundsException extends DomainValidationException {

    private final UUID accountId;
    private final long availableMinor;
    private final long requestedMinor;

    public InsufficientFundsException(UUID accountId, long availableMinor, long requestedMinor) {
        super("Insufficient funds on account " + accountId
                + ": available=" + availableMinor + ", requested=" + requestedMinor);
        this.accountId = accountId;
        this.availableMinor = availableMinor;
        this.requestedMinor = requestedMinor;
    }

    public UUID accountId() {
        return accountId;
    }

    public long availableMinor() {
        return availableMinor;
    }

    public long requestedMinor() {
        return requestedMinor;
    }
}
