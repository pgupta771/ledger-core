package dev.ledger.service.transfer;

import dev.ledger.domain.Currency;
import dev.ledger.domain.DomainValidationException;
import dev.ledger.domain.Entry;
import dev.ledger.domain.ResourceNotFoundException;
import dev.ledger.service.repository.AccountRepository;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * Serves keyset-paginated account statements over the composite {@code (account_id, id DESC)} index.
 */
@Service
public class StatementService {

    private static final int MAX_PAGE_LIMIT = 1_000;

    private final JdbcTemplate jdbcTemplate;
    private final AccountRepository accountRepository;

    public StatementService(JdbcTemplate jdbcTemplate, AccountRepository accountRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.accountRepository = accountRepository;
    }

    public StatementPage getStatement(UUID accountId, Long cursor, int limit) {
        if (limit < 1 || limit > MAX_PAGE_LIMIT) {
            throw new DomainValidationException("Statement limit must be between 1 and " + MAX_PAGE_LIMIT);
        }
        if (accountRepository.findById(accountId).isEmpty()) {
            throw new ResourceNotFoundException("Account not found: " + accountId);
        }

        int fetchLimit = limit + 1;
        List<Entry> rows;
        if (cursor == null) {
            rows = jdbcTemplate.query(
                    """
                    SELECT id, transfer_id, account_id, amount_minor, currency, created_at
                    FROM entries
                    WHERE account_id = ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    StatementService::mapEntry,
                    accountId,
                    fetchLimit);
        } else {
            rows = jdbcTemplate.query(
                    """
                    SELECT id, transfer_id, account_id, amount_minor, currency, created_at
                    FROM entries
                    WHERE account_id = ? AND id < ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    StatementService::mapEntry,
                    accountId,
                    cursor,
                    fetchLimit);
        }

        boolean hasMore = rows.size() > limit;
        List<Entry> pageItems = hasMore ? rows.subList(0, limit) : rows;
        Long nextCursor = hasMore ? pageItems.getLast().id() : null;
        return new StatementPage(accountId, List.copyOf(pageItems), nextCursor, hasMore);
    }

    private static Entry mapEntry(ResultSet rs, int rowNum) throws SQLException {
        return new Entry(
                rs.getLong("id"),
                rs.getObject("transfer_id", UUID.class),
                rs.getObject("account_id", UUID.class),
                rs.getLong("amount_minor"),
                Currency.of(rs.getString("currency").trim()),
                rs.getTimestamp("created_at").toInstant());
    }
}
