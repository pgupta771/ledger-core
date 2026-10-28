package dev.ledger.service.transfer;

import dev.ledger.domain.Transfer;

/**
 * Wrapper around a {@link Transfer} indicating whether the result was freshly executed or replayed
 * from the idempotency store.
 */
public record TransferExecutionResult(Transfer transfer, boolean idempotentReplay) {}
