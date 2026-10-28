package dev.ledger.domain;

import java.util.Objects;

/**
 * Exact monetary amount represented as a 64-bit signed integer in minor units plus an ISO 4217 currency.
 * Uses {@link Math#addExact} and {@link Math#subtractExact} to prevent silent integer overflow.
 */
public record Money(long amountMinor, Currency currency) implements Comparable<Money> {

    public Money {
        Objects.requireNonNull(currency, "Currency must not be null");
    }

    public static Money ofMinor(long amountMinor, Currency currency) {
        return new Money(amountMinor, currency);
    }

    public static Money ofMinor(long amountMinor, String currencyCode) {
        return new Money(amountMinor, Currency.of(currencyCode));
    }

    public static Money zero(Currency currency) {
        return new Money(0L, currency);
    }

    public Money add(Money other) {
        requireSameCurrency(other);
        return new Money(Math.addExact(this.amountMinor, other.amountMinor), this.currency);
    }

    public Money subtract(Money other) {
        requireSameCurrency(other);
        return new Money(Math.subtractExact(this.amountMinor, other.amountMinor), this.currency);
    }

    public Money negate() {
        return new Money(Math.negateExact(this.amountMinor), this.currency);
    }

    public boolean isPositive() {
        return this.amountMinor > 0L;
    }

    public boolean isNegative() {
        return this.amountMinor < 0L;
    }

    public boolean isZero() {
        return this.amountMinor == 0L;
    }

    public Money requirePositive() {
        if (this.amountMinor <= 0L) {
            throw new DomainValidationException("Amount must be strictly positive in minor units, got: " + amountMinor);
        }
        return this;
    }

    public void requireSameCurrency(Money other) {
        Objects.requireNonNull(other, "Comparison money must not be null");
        if (!this.currency.equals(other.currency)) {
            throw new CurrencyMismatchException(
                    "Currency mismatch: expected " + this.currency.code() + " but got " + other.currency.code());
        }
    }

    @Override
    public int compareTo(Money other) {
        requireSameCurrency(other);
        return Long.compare(this.amountMinor, other.amountMinor);
    }
}
