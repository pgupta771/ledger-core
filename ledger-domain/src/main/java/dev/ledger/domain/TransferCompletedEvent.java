package dev.ledger.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * Immutable event contract emitted via the transactional outbox to topic {@code ledger.transfers.v1}.
 */
public record TransferCompletedEvent(
        long eventId,
        UUID transferId,
        UUID fromAccountId,
        UUID toAccountId,
        long amountMinor,
        String currency,
        Instant createdAt) {}
