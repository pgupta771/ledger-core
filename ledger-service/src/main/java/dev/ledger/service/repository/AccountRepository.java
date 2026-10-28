package dev.ledger.service.repository;

import dev.ledger.domain.Account;
import dev.ledger.domain.AccountType;
import dev.ledger.domain.Currency;
import dev.ledger.domain.Iban;
import dev.ledger.domain.ResourceNotFoundException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/**
 * Explicit JDBC repository for {@link Account} persistence and deterministic row locking.
 */
@Repository
public class AccountRepository {

    private static final RowMapper<Account> ACCOUNT_ROW_MAPPER = AccountRepository::mapAccount;

    private final JdbcTemplate jdbcTemplate;

    public AccountRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Account createAccount(
            UUID id,
            String iban,
            Currency currency,
            AccountType type,
            boolean allowNegative,
            long initialBalanceMinor) {
        String normalizedIban = (iban != null && !iban.isBlank()) ? Iban.of(iban).value() : null;
        Instant now = Instant.now();
        Account account = new Account(
                id, normalizedIban, currency, type, allowNegative, initialBalanceMinor, 0L, now);
        jdbcTemplate.update(
                """
                INSERT INTO accounts (id, iban, currency, type, allow_negative, balance_minor, version, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                account.id(),
                account.iban(),
                account.currency().code(),
                account.type().name(),
                account.allowNegative(),
                account.balanceMinor(),
                account.version(),
                Timestamp.from(account.createdAt()));
        return account;
    }

    public Optional<Account> findById(UUID id) {
        List<Account> rows = jdbcTemplate.query(
                """
                SELECT id, iban, currency, type, allow_negative, balance_minor, version, created_at
                FROM accounts
                WHERE id = ?
                """,
                ACCOUNT_ROW_MAPPER,
                id);
        return rows.stream().findFirst();
    }

    /**
     * Loads two accounts in deterministic ascending UUID order, optionally acquiring pessimistic
     * {@code SELECT ... FOR UPDATE} row locks one by one to guarantee deadlock freedom.
     */
    public Map<UUID, Account> loadPairInCanonicalOrder(UUID firstId, UUID secondId, boolean lockForUpdate) {
        UUID lowId = firstId.compareTo(secondId) <= 0 ? firstId : secondId;
        UUID highId = lowId.equals(firstId) ? secondId : firstId;

        Map<UUID, Account> result = new LinkedHashMap<>(2);
        result.put(lowId, loadSingle(lowId, lockForUpdate));
        if (!highId.equals(lowId)) {
            result.put(highId, loadSingle(highId, lockForUpdate));
        }
        return result;
    }

    private Account loadSingle(UUID id, boolean lockForUpdate) {
        String sql = lockForUpdate
                ? """
                  SELECT id, iban, currency, type, allow_negative, balance_minor, version, created_at
                  FROM accounts
                  WHERE id = ?
                  FOR UPDATE
                  """
                : """
                  SELECT id, iban, currency, type, allow_negative, balance_minor, version, created_at
                  FROM accounts
                  WHERE id = ?
                  """;
        List<Account> rows = jdbcTemplate.query(sql, ACCOUNT_ROW_MAPPER, id);
        if (rows.isEmpty()) {
            throw new ResourceNotFoundException("Account not found: " + id);
        }
        return rows.getFirst();
    }

    public void updateBalance(Account updated) {
        int rows = jdbcTemplate.update(
                """
                UPDATE accounts
                SET balance_minor = ?, version = ?
                WHERE id = ?
                """,
                updated.balanceMinor(),
                updated.version(),
                updated.id());
        if (rows != 1) {
            throw new ResourceNotFoundException("Failed to update balance for account: " + updated.id());
        }
    }

    private static Account mapAccount(ResultSet rs, int rowNum) throws SQLException {
        return new Account(
                rs.getObject("id", UUID.class),
                rs.getString("iban"),
                Currency.of(rs.getString("currency").trim()),
                AccountType.valueOf(rs.getString("type")),
                rs.getBoolean("allow_negative"),
                rs.getLong("balance_minor"),
                rs.getLong("version"),
                rs.getTimestamp("created_at").toInstant());
    }
}
