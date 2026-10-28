package dev.ledger.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Verifies that PostgreSQL enforces all double-entry ledger invariants at the database layer.
 */
@SpringBootTest
@Testcontainers
class L1DatabaseConstraintsIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Test
    void rejectsUpdateAndDeleteOnAppendOnlyEntries() {
        UUID fundingId = insertAccount("EUR", "SYSTEM", true, 0L);
        UUID customerId = insertAccount("EUR", "CUSTOMER", false, 0L);
        UUID transferId = insertBalancedTransfer(fundingId, customerId, "EUR", 5_000L, "idem-append-1", null);

        assertThatThrownBy(() -> jdbcTemplate.update(
                "UPDATE entries SET amount_minor = 9999 WHERE transfer_id = ?", transferId))
                .isInstanceOf(UncategorizedSQLException.class)
                .hasMessageContaining("append-only");

        assertThatThrownBy(() -> jdbcTemplate.update(
                "DELETE FROM entries WHERE transfer_id = ?", transferId))
                .isInstanceOf(UncategorizedSQLException.class)
                .hasMessageContaining("append-only");
    }

    @Test
    void deferredConstraintTriggerRejectsUnbalancedTransferAtCommit() {
        UUID a1 = insertAccount("EUR", "SYSTEM", true, 0L);
        UUID a2 = insertAccount("EUR", "CUSTOMER", false, 0L);
        UUID transferId = UUID.randomUUID();

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            jdbcTemplate.update(
                    """
                    INSERT INTO transfers (id, client_id, idempotency_key, status, currency, amount_minor, from_account_id, to_account_id)
                    VALUES (?, 'client-test', ?, 'COMPLETED', 'EUR', 1000, ?, ?)
                    """,
                    transferId, "idem-unbalanced-" + transferId, a1, a2);
            jdbcTemplate.update(
                    "INSERT INTO entries (transfer_id, account_id, amount_minor, currency) VALUES (?, ?, -1000, 'EUR')",
                    transferId, a1);
            jdbcTemplate.update(
                    "INSERT INTO entries (transfer_id, account_id, amount_minor, currency) VALUES (?, ?, 900, 'EUR')",
                    transferId, a2);
        })).satisfies(ex -> assertThat(ex.getMessage()).contains("do not sum to zero"));
    }

    @Test
    void rejectsNegativeCustomerBalanceButAllowsNegativeSystemAccountBalance() {
        UUID customerId = insertAccount("EUR", "CUSTOMER", false, 500L);
        UUID systemId = insertAccount("EUR", "SYSTEM", true, 0L);

        assertThatThrownBy(() -> jdbcTemplate.update(
                "UPDATE accounts SET balance_minor = -1 WHERE id = ?", customerId))
                .isInstanceOf(DataIntegrityViolationException.class);

        jdbcTemplate.update("UPDATE accounts SET balance_minor = -50000 WHERE id = ?", systemId);
        Long sysBalance = jdbcTemplate.queryForObject(
                "SELECT balance_minor FROM accounts WHERE id = ?", Long.class, systemId);
        assertThat(sysBalance).isEqualTo(-50_000L);
    }

    @Test
    void rejectsEntryCurrencyMismatchAndDuplicateReversal() {
        UUID eurAcc = insertAccount("EUR", "SYSTEM", true, 0L);
        UUID usdAcc = insertAccount("USD", "CUSTOMER", false, 0L);
        UUID eurAcc2 = insertAccount("EUR", "CUSTOMER", false, 0L);

        UUID originalTransferId = insertBalancedTransfer(eurAcc, eurAcc2, "EUR", 1_000L, "idem-orig-1", null);
        insertBalancedTransfer(eurAcc2, eurAcc, "EUR", 1_000L, "idem-rev-1", originalTransferId);

        // Second reversal of the same transfer must fail UNIQUE(reverses_transfer_id)
        assertThatThrownBy(() -> insertBalancedTransfer(
                eurAcc2, eurAcc, "EUR", 1_000L, "idem-rev-2", originalTransferId))
                .isInstanceOf(DataIntegrityViolationException.class);

        // Entry with EUR on a USD account must fail trg_entries_currency_match
        UUID badTransferId = UUID.randomUUID();
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            jdbcTemplate.update(
                    """
                    INSERT INTO transfers (id, client_id, idempotency_key, status, currency, amount_minor, from_account_id, to_account_id)
                    VALUES (?, 'client-test', ?, 'COMPLETED', 'EUR', 500, ?, ?)
                    """,
                    badTransferId, "idem-curr-" + badTransferId, eurAcc, usdAcc);
            jdbcTemplate.update(
                    "INSERT INTO entries (transfer_id, account_id, amount_minor, currency) VALUES (?, ?, -500, 'EUR')",
                    badTransferId, eurAcc);
            jdbcTemplate.update(
                    "INSERT INTO entries (transfer_id, account_id, amount_minor, currency) VALUES (?, ?, 500, 'EUR')",
                    badTransferId, usdAcc);
        })).satisfies(ex -> assertThat(ex.getMessage()).contains("currency"));
    }

    private UUID insertAccount(String currency, String type, boolean allowNegative, long balanceMinor) {
        UUID id = UUID.randomUUID();
        jdbcTemplate.update(
                """
                INSERT INTO accounts (id, currency, type, allow_negative, balance_minor, version)
                VALUES (?, ?, ?, ?, ?, 0)
                """,
                id, currency, type, allowNegative, balanceMinor);
        return id;
    }

    private UUID insertBalancedTransfer(
            UUID fromId, UUID toId, String currency, long amountMinor, String idempotencyKey, UUID reversesId) {
        UUID transferId = UUID.randomUUID();
        transactionTemplate.executeWithoutResult(status -> {
            jdbcTemplate.update(
                    """
                    INSERT INTO transfers (id, client_id, idempotency_key, status, currency, amount_minor, from_account_id, to_account_id, reverses_transfer_id)
                    VALUES (?, 'client-test', ?, 'COMPLETED', ?, ?, ?, ?, ?)
                    """,
                    transferId, idempotencyKey + "-" + transferId, currency, amountMinor, fromId, toId, reversesId);
            jdbcTemplate.update(
                    "INSERT INTO entries (transfer_id, account_id, amount_minor, currency) VALUES (?, ?, ?, ?)",
                    transferId, fromId, -amountMinor, currency);
            jdbcTemplate.update(
                    "INSERT INTO entries (transfer_id, account_id, amount_minor, currency) VALUES (?, ?, ?, ?)",
                    transferId, toId, amountMinor, currency);
        });
        return transferId;
    }
}
