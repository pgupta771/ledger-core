package dev.ledger.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.ledger.domain.Account;
import dev.ledger.domain.AccountType;
import dev.ledger.domain.Currency;
import dev.ledger.domain.CurrencyMismatchException;
import dev.ledger.domain.Entry;
import dev.ledger.domain.Iban;
import dev.ledger.domain.InsufficientFundsException;
import dev.ledger.domain.Transfer;
import dev.ledger.service.repository.AccountRepository;
import dev.ledger.service.transfer.TransferCommand;
import dev.ledger.service.transfer.TransferEngine;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
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
class L2TransferEngineIT {

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
    private TransferEngine transferEngine;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void executesHappyPathTransferAndPersistsEntriesBalancesAndOutbox() {
        Account alice = accountRepository.createAccount(
                UUID.randomUUID(),
                Iban.generateSynthetic("NL", 101L).value(),
                Currency.EUR,
                AccountType.CUSTOMER,
                false,
                25_000L);
        Account bob = accountRepository.createAccount(
                UUID.randomUUID(),
                Iban.generateSynthetic("DE", 102L).value(),
                Currency.EUR,
                AccountType.CUSTOMER,
                false,
                5_000L);

        Transfer transfer = transferEngine.executePessimistic(new TransferCommand(
                "client-l2",
                "idem-happy-1",
                alice.id(),
                bob.id(),
                7_500L,
                Currency.EUR,
                "Rent split",
                null));

        assertThat(transfer.entries()).hasSize(2);
        assertThat(transfer.entries().stream().mapToLong(Entry::amountMinor).sum()).isZero();

        Account updatedAlice = accountRepository.findById(alice.id()).orElseThrow();
        Account updatedBob = accountRepository.findById(bob.id()).orElseThrow();
        assertThat(updatedAlice.balanceMinor()).isEqualTo(17_500L);
        assertThat(updatedBob.balanceMinor()).isEqualTo(12_500L);

        Integer outboxCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox WHERE aggregate_id = ? AND event_type = 'TransferCompleted'",
                Integer.class,
                transfer.id());
        assertThat(outboxCount).isEqualTo(1);
    }

    @Test
    void rollsBackCleanlyOnInsufficientFundsAndCurrencyMismatch() {
        Account alice = accountRepository.createAccount(
                UUID.randomUUID(), null, Currency.EUR, AccountType.CUSTOMER, false, 1_000L);
        Account bobEur = accountRepository.createAccount(
                UUID.randomUUID(), null, Currency.EUR, AccountType.CUSTOMER, false, 500L);
        Account charlieUsd = accountRepository.createAccount(
                UUID.randomUUID(), null, Currency.USD, AccountType.CUSTOMER, false, 500L);

        assertThatThrownBy(() -> transferEngine.executePessimistic(new TransferCommand(
                "client-l2", "idem-overdraft", alice.id(), bobEur.id(), 1_500L, Currency.EUR, "Too much", null)))
                .isInstanceOf(InsufficientFundsException.class);

        assertThatThrownBy(() -> transferEngine.executePessimistic(new TransferCommand(
                "client-l2", "idem-fx", alice.id(), charlieUsd.id(), 500L, Currency.EUR, "Cross currency", null)))
                .isInstanceOf(CurrencyMismatchException.class);

        assertThat(accountRepository.findById(alice.id()).orElseThrow().balanceMinor()).isEqualTo(1_000L);
        assertThat(accountRepository.findById(bobEur.id()).orElseThrow().balanceMinor()).isEqualTo(500L);
    }

    @Test
    void preservesTotalMoneyAndInvariantsAcrossRandomDatabaseTransferSequence() {
        List<UUID> accountIds = new ArrayList<>();
        UUID fundingId = accountRepository.createAccount(
                UUID.randomUUID(), null, Currency.EUR, AccountType.SYSTEM, true, 0L).id();
        accountIds.add(fundingId);
        for (int i = 1; i < 6; i++) {
            accountIds.add(accountRepository.createAccount(
                    UUID.randomUUID(), null, Currency.EUR, AccountType.CUSTOMER, false, 10_000L).id());
        }

        Random rng = new Random(42L);
        for (int i = 0; i < 60; i++) {
            int fromIdx = rng.nextInt(accountIds.size());
            int toIdx = rng.nextInt(accountIds.size());
            if (fromIdx == toIdx) {
                continue;
            }
            long amount = 100L + rng.nextInt(4_000);
            try {
                transferEngine.executePessimistic(new TransferCommand(
                        "client-seq",
                        "idem-seq-" + i,
                        accountIds.get(fromIdx),
                        accountIds.get(toIdx),
                        amount,
                        Currency.EUR,
                        "step-" + i,
                        null));
            } catch (InsufficientFundsException ignored) {
                // Expected when customer balance is lower than random transfer amount
            }
        }

        long totalBalance = accountIds.stream()
                .mapToLong(id -> accountRepository.findById(id).orElseThrow().balanceMinor())
                .sum();
        assertThat(totalBalance).isEqualTo(50_000L);
    }
}
