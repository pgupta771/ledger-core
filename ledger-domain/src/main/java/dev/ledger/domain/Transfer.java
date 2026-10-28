package dev.ledger.domain;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Balanced double-entry transfer aggregate enforcing zero-sum and single-currency invariants.
 */
public record Transfer(
        UUID id,
        String clientId,
        String idempotencyKey,
        TransferStatus status,
        Currency currency,
        long amountMinor,
        UUID fromAccountId,
        UUID toAccountId,
        String reference,
        UUID reversesTransferId,
        List<Entry> entries,
        Instant createdAt) {

    public Transfer {
        Objects.requireNonNull(id, "Transfer id must not be null");
        Objects.requireNonNull(clientId, "clientId must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        Objects.requireNonNull(fromAccountId, "fromAccountId must not be null");
        Objects.requireNonNull(toAccountId, "toAccountId must not be null");
        Objects.requireNonNull(entries, "entries must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");

        if (clientId.isBlank()) {
            throw new DomainValidationException("clientId must not be blank");
        }
        if (idempotencyKey.isBlank()) {
            throw new DomainValidationException("idempotencyKey must not be blank");
        }
        if (fromAccountId.equals(toAccountId)) {
            throw new DomainValidationException("Cannot transfer between identical account " + fromAccountId);
        }
        if (amountMinor <= 0L) {
            throw new DomainValidationException("Transfer amountMinor must be positive, got: " + amountMinor);
        }
        if (entries.size() < LedgerInvariants.MIN_ENTRIES_PER_TRANSFER) {
            throw new DomainValidationException(
                    "Transfer requires at least " + LedgerInvariants.MIN_ENTRIES_PER_TRANSFER
                            + " entries, got " + entries.size());
        }

        long sum = 0L;
        for (Entry entry : entries) {
            if (!entry.currency().equals(currency)) {
                throw new CurrencyMismatchException(
                        "Entry currency " + entry.currency() + " does not match transfer currency " + currency);
            }
            if (!entry.transferId().equals(id)) {
                throw new DomainValidationException("Entry transferId mismatch");
            }
            sum = Math.addExact(sum, entry.amountMinor());
        }
        if (sum != 0L) {
            throw new DomainValidationException("Transfer entries do not sum to zero; sum=" + sum);
        }
        entries = List.copyOf(entries);
    }

    public static Transfer createBalanced(
            UUID id,
            String clientId,
            String idempotencyKey,
            Account fromAccount,
            Account toAccount,
            Money amount,
            String reference,
            UUID reversesTransferId,
            Instant now) {
        amount.requirePositive();
        if (!fromAccount.currency().equals(amount.currency()) || !toAccount.currency().equals(amount.currency())) {
            throw new CurrencyMismatchException("Cross-currency transfers are not supported in v1");
        }
        Entry debit = new Entry(
                null,
                id,
                fromAccount.id(),
                Math.negateExact(amount.amountMinor()),
                amount.currency(),
                now);
        Entry credit = new Entry(
                null,
                id,
                toAccount.id(),
                amount.amountMinor(),
                amount.currency(),
                now);
        return new Transfer(
                id,
                clientId,
                idempotencyKey,
                TransferStatus.COMPLETED,
                amount.currency(),
                amount.amountMinor(),
                fromAccount.id(),
                toAccount.id(),
                reference,
                reversesTransferId,
                List.of(debit, credit),
                now);
    }
}
