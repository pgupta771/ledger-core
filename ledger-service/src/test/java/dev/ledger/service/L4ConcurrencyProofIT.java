package dev.ledger.service;

import static org.assertj.core.api.Assertions.assertThat;

import dev.ledger.domain.Account;
import dev.ledger.domain.AccountType;
import dev.ledger.domain.Currency;
import dev.ledger.domain.InsufficientFundsException;
import dev.ledger.service.repository.AccountRepository;
import dev.ledger.service.transfer.ConcurrencyMode;
import dev.ledger.service.transfer.TransferCommand;
import dev.ledger.service.transfer.TransferCoordinator;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
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

/**
 * Milestone L4 Concurrency Proof:
 * 100 threads, 10 accounts, 20,000 random transfers under both PESSIMISTIC and SERIALIZABLE strategies,
 * plus 10 consecutive contention rounds verifying zero invariant violations.
 */
@SpringBootTest
@Testcontainers
class L4ConcurrencyProofIT {

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
    private JdbcTemplate jdbcTemplate;

    @Test
    void hundredThreadsTenAccountsTwentyThousandTransfersConserveMoneyUnderBothStrategies() throws Exception {
        runConcurrentWorkload(ConcurrencyMode.PESSIMISTIC, 100, 10, 20_000, "l4-20k-pess");
        runConcurrentWorkload(ConcurrencyMode.SERIALIZABLE, 100, 10, 20_000, "l4-20k-ser");
    }

    @Test
    void tenConsecutiveRoundsHaveZeroInvariantViolationsAcrossBothStrategies() throws Exception {
        for (int round = 1; round <= 10; round++) {
            runConcurrentWorkload(ConcurrencyMode.PESSIMISTIC, 100, 10, 500, "l4-r" + round + "-pess");
            runConcurrentWorkload(ConcurrencyMode.SERIALIZABLE, 100, 10, 500, "l4-r" + round + "-ser");
        }
    }

    private void runConcurrentWorkload(
            ConcurrencyMode mode,
            int threadCount,
            int accountCount,
            int totalTransfers,
            String runPrefix) throws Exception {
        List<UUID> accountIds = new ArrayList<>(accountCount);
        // 1 SYSTEM funding account + (accountCount - 1) CUSTOMER accounts starting with 50,000 minor units
        accountIds.add(accountRepository.createAccount(
                UUID.randomUUID(), null, Currency.EUR, AccountType.SYSTEM, true, 0L).id());
        long customerInitialBalance = 50_000L;
        for (int i = 1; i < accountCount; i++) {
            accountIds.add(accountRepository.createAccount(
                    UUID.randomUUID(), null, Currency.EUR, AccountType.CUSTOMER, false, customerInitialBalance).id());
        }
        long expectedTotalMinor = (accountCount - 1L) * customerInitialBalance;

        AtomicInteger sequence = new AtomicInteger(0);
        AtomicInteger unexpectedErrors = new AtomicInteger(0);
        List<Throwable> capturedErrors = java.util.Collections.synchronizedList(new ArrayList<>());

        long startNanos = System.nanoTime();
        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            List<Callable<Void>> workers = new ArrayList<>(threadCount);
            for (int t = 0; t < threadCount; t++) {
                workers.add(() -> {
                    ThreadLocalRandom rng = ThreadLocalRandom.current();
                    while (true) {
                        int idx = sequence.incrementAndGet();
                        if (idx > totalTransfers) {
                            break;
                        }
                        int fromIdx = rng.nextInt(accountCount);
                        int toIdx = rng.nextInt(accountCount - 1);
                        if (toIdx >= fromIdx) {
                            toIdx++;
                        }
                        long amount = rng.nextLong(100L, 2_501L);
                        TransferCommand cmd = new TransferCommand(
                                runPrefix,
                                "idem-" + idx,
                                accountIds.get(fromIdx),
                                accountIds.get(toIdx),
                                amount,
                                Currency.EUR,
                                mode.name(),
                                null);
                        try {
                            transferCoordinator.execute(cmd, mode);
                        } catch (InsufficientFundsException ignored) {
                            // Valid business rejection when a customer account lacks funds
                        } catch (Throwable ex) {
                            unexpectedErrors.incrementAndGet();
                            if (capturedErrors.size() < 5) {
                                capturedErrors.add(ex);
                            }
                        }
                    }
                    return null;
                });
            }
            List<Future<Void>> futures = executor.invokeAll(workers);
            for (Future<Void> f : futures) {
                f.get();
            }
        }
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);

        assertThat(unexpectedErrors.get())
                .withFailMessage("Expected 0 deadlock/serialization errors surfaced to callers in %s, got %s: %s",
                        mode, unexpectedErrors.get(), capturedErrors)
                .isZero();
        assertThat(elapsed).isLessThan(Duration.ofSeconds(120));

        List<Account> finalAccounts = accountIds.stream()
                .map(id -> accountRepository.findById(id).orElseThrow())
                .toList();

        long actualTotalMinor = finalAccounts.stream().mapToLong(Account::balanceMinor).sum();
        assertThat(actualTotalMinor).isEqualTo(expectedTotalMinor);

        // Customer accounts (indices 1..N-1) must never have negative balances
        for (int i = 1; i < finalAccounts.size(); i++) {
            assertThat(finalAccounts.get(i).balanceMinor()).isGreaterThanOrEqualTo(0L);
        }

        // Sum of all ledger entries for these accounts must equal the net balance delta
        Long globalEntriesSum = jdbcTemplate.queryForObject(
                "SELECT COALESCE(SUM(amount_minor), 0) FROM entries WHERE transfer_id IN "
                        + "(SELECT id FROM transfers WHERE client_id = ?)",
                Long.class,
                runPrefix);
        assertThat(globalEntriesSum).isZero();
    }
}
