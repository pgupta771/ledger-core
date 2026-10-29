package dev.ledger.service.outbox;

/**
 * Fault-injection seam allowing integration tests to simulate crashes after Kafka broker ack
 * and before PostgreSQL commits {@code published_at}.
 */
@FunctionalInterface
public interface OutboxFaultHook {

    OutboxFaultHook NO_OP = outboxId -> {};

    void afterKafkaAckBeforeMarkPublished(long outboxId);
}
