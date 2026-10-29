package dev.ledger.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.ledger.domain.Account;
import dev.ledger.domain.AccountType;
import dev.ledger.domain.Currency;
import dev.ledger.domain.TransferCompletedEvent;
import dev.ledger.service.outbox.OutboxFaultHook;
import dev.ledger.service.outbox.OutboxPublisher;
import dev.ledger.service.repository.AccountRepository;
import dev.ledger.service.transfer.TransferCommand;
import dev.ledger.service.transfer.TransferCoordinator;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(properties = {
    "ledger.outbox.scheduling-enabled=false",
    "ledger.outbox.topic=ledger.transfers.l5.test"
})
@Testcontainers
class L5OutboxCrashIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private TransferCoordinator transferCoordinator;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void crashBeforeMarkPublishedRedeliversOnRestartAndSupportsConcurrentSkipLockedPollers() throws Exception {
        Account funding = accountRepository.createAccount(
                UUID.randomUUID(), null, Currency.EUR, AccountType.SYSTEM, true, 0L);
        Account alice = accountRepository.createAccount(
                UUID.randomUUID(), null, Currency.EUR, AccountType.CUSTOMER, false, 0L);
        Account bob = accountRepository.createAccount(
                UUID.randomUUID(), null, Currency.EUR, AccountType.CUSTOMER, false, 0L);

        // 1. Commit a funding transfer -> creates unpublished outbox row
        var firstTransfer = transferCoordinator.execute(new TransferCommand(
                "l5-client", "l5-k1", funding.id(), alice.id(), 10_000L, Currency.EUR, "seed", null)).transfer();

        assertThat(outboxPublisher.countUnpublished()).isEqualTo(1.0);

        // 2. Simulate publisher crash right AFTER Kafka broker ack and BEFORE setting published_at
        AtomicBoolean crashedOnce = new AtomicBoolean(false);
        outboxPublisher.setFaultHook(outboxId -> {
            if (crashedOnce.compareAndSet(false, true)) {
                throw new RuntimeException("Simulated kill -9 after Kafka ack before published_at update");
            }
        });

        assertThatThrownBy(() -> outboxPublisher.publishNextBatch())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Simulated kill -9");

        // Row must still be unpublished in PostgreSQL because the transaction rolled back!
        assertThat(outboxPublisher.countUnpublished()).isEqualTo(1.0);

        // 3. Clear fault hook ("restart") and add 9 more transfers, then poll concurrently from 4 pollers
        outboxPublisher.setFaultHook(OutboxFaultHook.NO_OP);
        for (int i = 2; i <= 10; i++) {
            transferCoordinator.execute(new TransferCommand(
                    "l5-client", "l5-k" + i, alice.id(), bob.id(), 500L, Currency.EUR, "pay-" + i, null));
        }

        try (ExecutorService pollers = Executors.newFixedThreadPool(4)) {
            List<Callable<Integer>> tasks = new ArrayList<>();
            for (int p = 0; p < 4; p++) {
                tasks.add(() -> outboxPublisher.publishNextBatch());
            }
            for (Future<Integer> f : pollers.invokeAll(tasks)) {
                f.get();
            }
        }

        assertThat(outboxPublisher.countUnpublished()).isEqualTo(0.0);

        // 4. Consume Kafka topic and verify all 10 unique transfer IDs arrived (plus the 1 crash duplicate)
        List<TransferCompletedEvent> consumed = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "l5-verifier-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of("ledger.transfers.l5.test"));
            long deadline = System.currentTimeMillis() + 15_000L;
            while (System.currentTimeMillis() < deadline && consumed.size() < 11) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> rec : records) {
                    consumed.add(objectMapper.readValue(rec.value(), TransferCompletedEvent.class));
                }
            }
        }

        assertThat(consumed.size()).isGreaterThanOrEqualTo(11);
        Set<UUID> distinctTransfers = consumed.stream()
                .map(TransferCompletedEvent::transferId)
                .collect(Collectors.toSet());
        assertThat(distinctTransfers).hasSize(10).contains(firstTransfer.id());
    }
}
