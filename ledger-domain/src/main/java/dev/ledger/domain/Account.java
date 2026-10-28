package dev.ledger.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable domain snapshot of a ledger account.
 */
public record Account(
        UUID id,
        String iban,
        Currency currency,
        AccountType type,
        boolean allowNegative,
        long balanceMinor,
        long version,
        Instant createdAt) {

    public Account {
        Objects.requireNonNull(id, "Account id must not be null");
        Objects.requireNonNull(currency, "Account currency must not be null");
        Objects.requireNonNull(type, "Account type must not be null");
        Objects.requireNonNull(createdAt, "Account createdAt must not be null");
        if (iban != null && !iban.isBlank()) {
            iban = Iban.of(iban).value();
        } else {
            iban = null;
        }
        if (type == AccountType.CUSTOMER && allowNegative) {
            throw new DomainValidationException("Customer accounts cannot have allowNegative=true");
        }
        if (!allowNegative && balanceMinor < 0L) {
            throw new InsufficientFundsException(id, balanceMinor, 0L);
        }
    }

    public Money balance() {
        return Money.ofMinor(balanceMinor, currency);
    }

    public Account applyEntry(long deltaMinor) {
        long updatedBalance = Math.addExact(this.balanceMinor, deltaMinor);
        if (!this.allowNegative && updatedBalance < 0L) {
            throw new InsufficientFundsException(this.id, this.balanceMinor, Math.negateExact(deltaMinor));
        }
        return new Account(
                id,
                iban,
                currency,
                type,
                allowNegative,
                updatedBalance,
                Math.addExact(version, 1L),
                createdAt);
    }
}
