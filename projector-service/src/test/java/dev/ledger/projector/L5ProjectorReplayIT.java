package dev.ledger.projector;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.ledger.domain.TransferCompletedEvent;
import dev.ledger.projector.consumer.AccountBalanceView;
import dev.ledger.projector.consumer.BalanceProjectorService;
import dev.ledger.projector.consumer.TransferEventsKafkaConsumer;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(properties = {
    "ledger.projector.topic=ledger.transfers.replay.it"
})
@Testcontainers
class L5ProjectorReplayIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private BalanceProjectorService projectorService;

    @Autowired
    private TransferEventsKafkaConsumer kafkaConsumer;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void deduplicatesOutboxRedeliveryAndReconstructsIdenticalViewOnOffsetZeroReplay() throws Exception {
        // Wait for consumer partition assignment
        long assignDeadline = System.currentTimeMillis() + 15_000L;
        while (kafkaConsumer.assignedPartitions().isEmpty() && System.currentTimeMillis() < assignDeadline) {
            Thread.sleep(100L);
        }

        UUID funding = UUID.randomUUID();
        UUID alice = UUID.randomUUID();
        UUID bob = UUID.randomUUID();
        Instant now = Instant.now();

        TransferCompletedEvent e1 = new TransferCompletedEvent(
                101L, UUID.randomUUID(), funding, alice, 50_000L, "EUR", now);
        TransferCompletedEvent e2 = new TransferCompletedEvent(
                102L, UUID.randomUUID(), alice, bob, 12_500L, "EUR", now);
        TransferCompletedEvent e3 = new TransferCompletedEvent(
                103L, UUID.randomUUID(), bob, alice, 2_500L, "EUR", now);

        // Publish e1 TWICE (simulating outbox crash before published_at) + e2 + e3
        publish(e1);
        publish(e1);
        publish(e2);
        publish(e3);

        awaitProjectedBalance(alice, 40_000L);
        awaitProjectedBalance(bob, 10_000L);
        awaitProjectedBalance(funding, -50_000L);

        assertThat(meterRegistry.counter("projector_events_total", "outcome", "duplicate").count())
                .isGreaterThanOrEqualTo(1.0);

        Map<UUID, AccountBalanceView> liveSnapshot = projectorService.snapshotAllBalances();
        assertThat(liveSnapshot.get(alice).balanceMinor()).isEqualTo(40_000L);
        assertThat(liveSnapshot.get(bob).balanceMinor()).isEqualTo(10_000L);
        assertThat(liveSnapshot.get(funding).balanceMinor()).isEqualTo(-50_000L);

        // Wipe the entire read model & processed_events and replay from Kafka topic offset 0!
        kafkaConsumer.replayAllFromBeginning();

        awaitProjectedBalance(alice, 40_000L);
        awaitProjectedBalance(bob, 10_000L);
        awaitProjectedBalance(funding, -50_000L);

        Map<UUID, AccountBalanceView> replayedSnapshot = projectorService.snapshotAllBalances();
        assertThat(replayedSnapshot).isEqualTo(liveSnapshot);
    }

    private void publish(TransferCompletedEvent event) throws Exception {
        kafkaTemplate.send(
                "ledger.transfers.replay.it",
                event.transferId().toString(),
                objectMapper.writeValueAsString(event)).get(10, TimeUnit.SECONDS);
    }

    private void awaitProjectedBalance(UUID accountId, long expectedBalance) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000L;
        while (System.currentTimeMillis() < deadline) {
            AccountBalanceView view = projectorService.snapshotAllBalances().get(accountId);
            if (view != null && view.balanceMinor() == expectedBalance) {
                return;
            }
            Thread.sleep(100L);
        }
        assertThat(projectorService.snapshotAllBalances().get(accountId))
                .isNotNull()
                .extracting(AccountBalanceView::balanceMinor)
                .isEqualTo(expectedBalance);
    }
}
