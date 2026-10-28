package dev.ledger.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Append-only double-entry ledger line item. Negative minor units represent a debit; positive represent a credit.
 */
public record Entry(
        Long id,
        UUID transferId,
        UUID accountId,
        long amountMinor,
        Currency currency,
        Instant createdAt) {

    public Entry {
        Objects.requireNonNull(transferId, "transferId must not be null");
        Objects.requireNonNull(accountId, "accountId must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        if (amountMinor == 0L) {
            throw new DomainValidationException("Entry amountMinor must be non-zero");
        }
    }
}
