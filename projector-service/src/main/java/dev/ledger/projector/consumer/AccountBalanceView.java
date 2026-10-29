package dev.ledger.projector.consumer;

import java.util.UUID;

/**
 * Projected read-model balance row for an account.
 */
public record AccountBalanceView(UUID accountId, long balanceMinor, long lastEventId) {}
