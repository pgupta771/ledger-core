package dev.ledger.service.transfer;

import java.util.Locale;

/**
 * Supported concurrency control strategies for transfer execution (Section 7.2).
 */
public enum ConcurrencyMode {
    PESSIMISTIC,
    SERIALIZABLE;

    public static ConcurrencyMode fromProperty(String value) {
        if (value == null || value.isBlank()) {
            return PESSIMISTIC;
        }
        return ConcurrencyMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    }
}
