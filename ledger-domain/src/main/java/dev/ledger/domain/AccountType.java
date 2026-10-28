package dev.ledger.domain;

/**
 * Classification of ledger accounts. SYSTEM accounts (e.g., FUNDING) may permit negative balances.
 */
public enum AccountType {
    CUSTOMER,
    SYSTEM
}
