package dev.ledger.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.ledger.domain.Account;
import dev.ledger.domain.AccountType;
import dev.ledger.domain.Currency;
import dev.ledger.domain.IdempotencyConflictException;
import dev.ledger.domain.IdempotencyPayloadMismatchException;
import dev.ledger.domain.Transfer;
import dev.ledger.service.idempotency.RequestHasher;
import dev.ledger.service.repository.AccountRepository;
import dev.ledger.service.transfer.IdempotentTransferService;
import dev.ledger.service.transfer.TransferCommand;
import dev.ledger.service.transfer.TransferExecutionResult;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@Testcontainers
class L3IdempotencyStormIT {

    private static final int STORM_THREADS = 50;
    private static final int CONSECUTIVE_ROUNDS = 20;

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
    private IdempotentTransferService idempotentTransferService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void fiftyConcurrentRequestsWithSameKeyProduceExactlyOneTransferAcrossTwentyConsecutiveRuns() throws Exception {
        Account sender = accountRepository.createAccount(
                UUID.randomUUID(), null, Currency.EUR, AccountType.CUSTOMER, false, 1_000_000L);
        Account receiver = accountRepository.createAccount(
                UUID.randomUUID(), null, Currency.EUR, AccountType.CUSTOMER, false, 0L);

        long transferAmount = 1_000L;

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int round = 1; round <= CONSECUTIVE_ROUNDS; round++) {
                String idempotencyKey = "storm-key-round-" + round;
                TransferCommand command = new TransferCommand(
                        "storm-client",
                        idempotencyKey,
                        sender.id(),
                        receiver.id(),
                        transferAmount,
                        Currency.EUR,
                        "storm-round-" + round,
                        null);

                CountDownLatch startGate = new CountDownLatch(1);
                List<Callable<TransferExecutionResult>> tasks = new ArrayList<>(STORM_THREADS);
                for (int t = 0; t < STORM_THREADS; t++) {
                    tasks.add(() -> {
                        startGate.await();
                        return idempotentTransferService.executePessimistic(command);
                    });
                }

                List<Future<TransferExecutionResult>> futures = new ArrayList<>(STORM_THREADS);
                for (Callable<TransferExecutionResult> task : tasks) {
                    futures.add(pool.submit(task));
                }
                startGate.countDown();

                List<Transfer> responses = new ArrayList<>(STORM_THREADS);
                int freshCount = 0;
                int replayCount = 0;
                for (Future<TransferExecutionResult> f : futures) {
                    TransferExecutionResult res = f.get();
                    responses.add(res.transfer());
                    if (res.idempotentReplay()) {
                        replayCount++;
                    } else {
                        freshCount++;
                    }
                }

                assertThat(freshCount).isEqualTo(1);
                assertThat(replayCount).isEqualTo(STORM_THREADS - 1);

                Set<UUID> distinctTransferIds = responses.stream()
                        .map(Transfer::id)
                        .collect(Collectors.toSet());
                assertThat(distinctTransferIds).hasSize(1);

                Integer dbTransfersForKey = jdbcTemplate.queryForObject(
                        "SELECT COUNT(*) FROM transfers WHERE client_id = 'storm-client' AND idempotency_key = ?",
                        Integer.class,
                        idempotencyKey);
                assertThat(dbTransfersForKey).isEqualTo(1);
            }
        }

        Account finalSender = accountRepository.findById(sender.id()).orElseThrow();
        Account finalReceiver = accountRepository.findById(receiver.id()).orElseThrow();
        assertThat(finalSender.balanceMinor()).isEqualTo(1_000_000L - (CONSECUTIVE_ROUNDS * transferAmount));
        assertThat(finalReceiver.balanceMinor()).isEqualTo(CONSECUTIVE_ROUNDS * transferAmount);
    }

    @Test
    void rejectsSameKeyWithDifferentPayload422AndInFlightKey409() {
        Account a1 = accountRepository.createAccount(
                UUID.randomUUID(), null, Currency.EUR, AccountType.CUSTOMER, false, 50_000L);
        Account a2 = accountRepository.createAccount(
                UUID.randomUUID(), null, Currency.EUR, AccountType.CUSTOMER, false, 0L);

        TransferCommand original = new TransferCommand(
                "client-422", "key-422", a1.id(), a2.id(), 1_000L, Currency.EUR, "first", null);
        idempotentTransferService.executePessimistic(original);

        TransferCommand modifiedAmount = new TransferCommand(
                "client-422", "key-422", a1.id(), a2.id(), 2_500L, Currency.EUR, "first", null);
        assertThatThrownBy(() -> idempotentTransferService.executePessimistic(modifiedAmount))
                .isInstanceOf(IdempotencyPayloadMismatchException.class);

        // Simulate a committed IN_PROGRESS key from an external worker
        TransferCommand inFlightCmd = new TransferCommand(
                "client-409", "key-409", a1.id(), a2.id(), 500L, Currency.EUR, "inflight", null);
        jdbcTemplate.update(
                """
                INSERT INTO idempotency_keys (client_id, key, request_hash, status, created_at)
                VALUES (?, ?, ?, 'IN_PROGRESS', ?)
                """,
                inFlightCmd.clientId(),
                inFlightCmd.idempotencyKey(),
                RequestHasher.sha256(inFlightCmd),
                Timestamp.from(Instant.now()));

        assertThatThrownBy(() -> idempotentTransferService.executePessimistic(inFlightCmd))
                .isInstanceOf(IdempotencyConflictException.class);
    }
}
