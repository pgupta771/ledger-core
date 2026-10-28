package dev.ledger.service;

import static org.assertj.core.api.Assertions.assertThat;

import dev.ledger.domain.Currency;
import dev.ledger.service.idempotency.IdempotencyDecision;
import dev.ledger.service.idempotency.IdempotencyRecord;
import dev.ledger.service.idempotency.IdempotencyStatus;
import dev.ledger.service.idempotency.RequestHasher;
import dev.ledger.service.transfer.TransferCommand;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class IdempotencyStateMachineTest {

    @Test
    void evaluatesCompletedInFlightAndPayloadMismatchTransitions() {
        UUID a1 = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID a2 = UUID.fromString("00000000-0000-0000-0000-000000000002");

        TransferCommand cmd1 = new TransferCommand("c1", "k1", a1, a2, 1000L, Currency.EUR, "ref-1", null);
        TransferCommand cmdDiffAmount = new TransferCommand("c1", "k1", a1, a2, 2000L, Currency.EUR, "ref-1", null);

        String hash1 = RequestHasher.sha256(cmd1);
        String hash2 = RequestHasher.sha256(cmdDiffAmount);
        assertThat(hash1).hasSize(64).isNotEqualTo(hash2);

        IdempotencyRecord completedRecord = new IdempotencyRecord(
                "c1", "k1", hash1, IdempotencyStatus.COMPLETED, 201, "{\"id\":\"ok\"}", Instant.now());
        assertThat(IdempotencyDecision.evaluate(completedRecord, hash1))
                .isEqualTo(new IdempotencyDecision.ReplayStoredResponse(201, "{\"id\":\"ok\"}"));

        assertThat(IdempotencyDecision.evaluate(completedRecord, hash2))
                .isInstanceOf(IdempotencyDecision.RejectPayloadMismatch.class);

        IdempotencyRecord inFlightRecord = new IdempotencyRecord(
                "c1", "k1", hash1, IdempotencyStatus.IN_PROGRESS, null, null, Instant.now());
        assertThat(IdempotencyDecision.evaluate(inFlightRecord, hash1))
                .isEqualTo(new IdempotencyDecision.RejectInFlight(1));

        assertThat(IdempotencyDecision.evaluate(inFlightRecord, hash2))
                .isInstanceOf(IdempotencyDecision.RejectPayloadMismatch.class);
    }
}
