package dev.ledger.service.transfer;

import dev.ledger.domain.Currency;
import dev.ledger.domain.DomainValidationException;
import java.util.Objects;
import java.util.UUID;

/**
 * Command payload for executing a single-currency balanced double-entry transfer.
 */
public record TransferCommand(
        String clientId,
        String idempotencyKey,
        UUID fromAccountId,
        UUID toAccountId,
        long amountMinor,
        Currency currency,
        String reference,
        UUID reversesTransferId) {

    public TransferCommand {
        Objects.requireNonNull(clientId, "clientId must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        Objects.requireNonNull(fromAccountId, "fromAccountId must not be null");
        Objects.requireNonNull(toAccountId, "toAccountId must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        if (clientId.isBlank()) {
            throw new DomainValidationException("clientId must not be blank");
        }
        if (idempotencyKey.isBlank()) {
            throw new DomainValidationException("idempotencyKey must not be blank");
        }
        if (fromAccountId.equals(toAccountId)) {
            throw new DomainValidationException("fromAccountId and toAccountId must be distinct");
        }
        if (amountMinor <= 0L) {
            throw new DomainValidationException("amountMinor must be strictly positive");
        }
    }
}
