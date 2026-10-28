package dev.ledger.domain;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * ISO 4217 three-letter currency value object.
 */
public record Currency(String code, int minorUnitDigits) {

    private static final Pattern ISO_4217_PATTERN = Pattern.compile("^[A-Z]{3}$");

    public static final Currency EUR = Currency.of("EUR");
    public static final Currency USD = Currency.of("USD");
    public static final Currency GBP = Currency.of("GBP");

    public Currency {
        Objects.requireNonNull(code, "Currency code must not be null");
        if (!ISO_4217_PATTERN.matcher(code).matches()) {
            throw new DomainValidationException("Invalid ISO 4217 currency code: " + code);
        }
        try {
            java.util.Currency jdkCurrency = java.util.Currency.getInstance(code);
            int defaultDigits = Math.max(0, jdkCurrency.getDefaultFractionDigits());
            if (minorUnitDigits < 0) {
                minorUnitDigits = defaultDigits;
            }
        } catch (IllegalArgumentException ex) {
            throw new DomainValidationException("Unsupported ISO 4217 currency code: " + code);
        }
    }

    public static Currency of(String code) {
        if (code == null) {
            throw new DomainValidationException("Currency code must not be null");
        }
        String normalized = code.trim().toUpperCase();
        if (!ISO_4217_PATTERN.matcher(normalized).matches()) {
            throw new DomainValidationException("Invalid ISO 4217 currency code: " + code);
        }
        try {
            java.util.Currency jdkCurrency = java.util.Currency.getInstance(normalized);
            return new Currency(normalized, Math.max(0, jdkCurrency.getDefaultFractionDigits()));
        } catch (IllegalArgumentException ex) {
            throw new DomainValidationException("Unsupported ISO 4217 currency code: " + code);
        }
    }

    @Override
    public String toString() {
        return code;
    }
}
