package dev.ledger.service.transfer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.ledger.domain.Account;
import dev.ledger.domain.Currency;
import dev.ledger.domain.Entry;
import dev.ledger.domain.Money;
import dev.ledger.domain.Transfer;
import dev.ledger.domain.TransferStatus;
import dev.ledger.service.repository.AccountRepository;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Core double-entry transfer execution engine backed by explicit SQL and deterministic lock ordering.
 */
@Service
public class TransferEngine {

    private final JdbcTemplate jdbcTemplate;
    private final AccountRepository accountRepository;
    private final ObjectMapper objectMapper;

    public TransferEngine(
            JdbcTemplate jdbcTemplate,
            AccountRepository accountRepository,
            ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.accountRepository = accountRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Executes a balanced double-entry transfer using pessimistic {@code SELECT ... FOR UPDATE}
     * locks acquired in canonical ascending account UUID order.
     */
    @Transactional
    public Transfer executePessimistic(TransferCommand command) {
        return executeInCurrentTransaction(command, true);
    }

    /**
     * Executes the core transfer steps inside an active transaction, optionally acquiring
     * pessimistic row locks (true for pessimistic mode; false for SERIALIZABLE mode).
     */
    public Transfer executeInCurrentTransaction(TransferCommand command, boolean lockForUpdate) {
        Map<UUID, Account> accounts = accountRepository.loadPairInCanonicalOrder(
                command.fromAccountId(), command.toAccountId(), lockForUpdate);

        Account fromAccount = accounts.get(command.fromAccountId());
        Account toAccount = accounts.get(command.toAccountId());

        Money amount = Money.ofMinor(command.amountMinor(), command.currency()).requirePositive();
        Instant now = Instant.now();
        UUID transferId = UUID.randomUUID();

        Transfer draft = Transfer.createBalanced(
                transferId,
                command.clientId(),
                command.idempotencyKey(),
                fromAccount,
                toAccount,
                amount,
                command.reference(),
                command.reversesTransferId(),
                now);

        Account updatedFrom = fromAccount.applyEntry(Math.negateExact(command.amountMinor()));
        Account updatedTo = toAccount.applyEntry(command.amountMinor());

        jdbcTemplate.update(
                """
                INSERT INTO transfers (
                    id, client_id, idempotency_key, status, currency, amount_minor,
                    from_account_id, to_account_id, reference, reverses_transfer_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                draft.id(),
                draft.clientId(),
                draft.idempotencyKey(),
                draft.status().name(),
                draft.currency().code(),
                draft.amountMinor(),
                draft.fromAccountId(),
                draft.toAccountId(),
                draft.reference(),
                draft.reversesTransferId(),
                Timestamp.from(draft.createdAt()));

        List<Entry> persistedEntries = new ArrayList<>(draft.entries().size());
        for (Entry entry : draft.entries()) {
            long entryId = insertEntry(entry);
            persistedEntries.add(new Entry(
                    entryId,
                    entry.transferId(),
                    entry.accountId(),
                    entry.amountMinor(),
                    entry.currency(),
                    entry.createdAt()));
        }

        accountRepository.updateBalance(updatedFrom);
        accountRepository.updateBalance(updatedTo);

        Transfer completed = new Transfer(
                draft.id(),
                draft.clientId(),
                draft.idempotencyKey(),
                draft.status(),
                draft.currency(),
                draft.amountMinor(),
                draft.fromAccountId(),
                draft.toAccountId(),
                draft.reference(),
                draft.reversesTransferId(),
                persistedEntries,
                draft.createdAt());

        insertOutboxEvent(completed, updatedFrom, updatedTo);
        return completed;
    }

    public Optional<Transfer> findById(UUID transferId) {
        List<Map<String, Object>> transferRows = jdbcTemplate.queryForList(
                """
                SELECT id, client_id, idempotency_key, status, currency, amount_minor,
                       from_account_id, to_account_id, reference, reverses_transfer_id, created_at
                FROM transfers
                WHERE id = ?
                """,
                transferId);
        if (transferRows.isEmpty()) {
            return Optional.empty();
        }
        Map<String, Object> row = transferRows.getFirst();
        List<Entry> entries = jdbcTemplate.query(
                """
                SELECT id, transfer_id, account_id, amount_minor, currency, created_at
                FROM entries
                WHERE transfer_id = ?
                ORDER BY id ASC
                """,
                (rs, rn) -> new Entry(
                        rs.getLong("id"),
                        rs.getObject("transfer_id", UUID.class),
                        rs.getObject("account_id", UUID.class),
                        rs.getLong("amount_minor"),
                        Currency.of(rs.getString("currency").trim()),
                        rs.getTimestamp("created_at").toInstant()),
                transferId);

        return Optional.of(new Transfer(
                (UUID) row.get("id"),
                (String) row.get("client_id"),
                (String) row.get("idempotency_key"),
                TransferStatus.valueOf((String) row.get("status")),
                Currency.of(((String) row.get("currency")).trim()),
                ((Number) row.get("amount_minor")).longValue(),
                (UUID) row.get("from_account_id"),
                (UUID) row.get("to_account_id"),
                (String) row.get("reference"),
                (UUID) row.get("reverses_transfer_id"),
                entries,
                ((Timestamp) row.get("created_at")).toInstant()));
    }

    private long insertEntry(Entry entry) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement(
                    """
                    INSERT INTO entries (transfer_id, account_id, amount_minor, currency, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    new String[] {"id"});
            ps.setObject(1, entry.transferId());
            ps.setObject(2, entry.accountId());
            ps.setLong(3, entry.amountMinor());
            ps.setString(4, entry.currency().code());
            ps.setTimestamp(5, Timestamp.from(entry.createdAt()));
            return ps;
        }, keyHolder);
        return keyHolder.getKey().longValue();
    }

    private void insertOutboxEvent(Transfer transfer, Account updatedFrom, Account updatedTo) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("transferId", transfer.id().toString());
        payload.put("fromAccountId", transfer.fromAccountId().toString());
        payload.put("toAccountId", transfer.toAccountId().toString());
        payload.put("amountMinor", transfer.amountMinor());
        payload.put("currency", transfer.currency().code());
        payload.put("fromBalanceMinor", updatedFrom.balanceMinor());
        payload.put("toBalanceMinor", updatedTo.balanceMinor());
        payload.put("createdAt", transfer.createdAt().toString());

        try {
            String json = objectMapper.writeValueAsString(payload);
            jdbcTemplate.update(
                    """
                    INSERT INTO outbox (aggregate_id, event_type, payload, created_at)
                    VALUES (?, 'TransferCompleted', ?::jsonb, ?)
                    """,
                    transfer.id(),
                    json,
                    Timestamp.from(transfer.createdAt()));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize TransferCompleted outbox payload", ex);
        }
    }
}
