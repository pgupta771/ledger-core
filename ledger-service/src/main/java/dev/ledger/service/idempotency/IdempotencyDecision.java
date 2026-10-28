package dev.ledger.service.idempotency;

/**
 * Java 21 sealed hierarchy representing the state-machine outcome for an existing idempotency row.
 */
public sealed interface IdempotencyDecision {

    record ReplayStoredResponse(int responseCode, String responseBodyJson) implements IdempotencyDecision {}

    record RejectInFlight(int retryAfterSeconds) implements IdempotencyDecision {}

    record RejectPayloadMismatch(String expectedHash, String actualHash) implements IdempotencyDecision {}

    static IdempotencyDecision evaluate(IdempotencyRecord existing, String incomingRequestHash) {
        if (!existing.requestHash().equals(incomingRequestHash)) {
            return new RejectPayloadMismatch(existing.requestHash(), incomingRequestHash);
        }
        return switch (existing.status()) {
            case IN_PROGRESS -> new RejectInFlight(1);
            case COMPLETED -> new ReplayStoredResponse(
                    existing.responseCode() != null ? existing.responseCode() : 201,
                    existing.responseBodyJson());
        };
    }
}
