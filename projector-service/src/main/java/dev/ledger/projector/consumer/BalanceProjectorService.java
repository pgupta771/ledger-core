package dev.ledger.projector.consumer;

import dev.ledger.domain.TransferCompletedEvent;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Applies {@link TransferCompletedEvent} messages idempotently into {@code account_balance_view}
 * guarded by {@code processed_events} inside a single PostgreSQL transaction (Section 7.4).
 */
@Service
public class BalanceProjectorService {

    private final JdbcTemplate jdbcTemplate;
    private final MeterRegistry meterRegistry;

    public BalanceProjectorService(JdbcTemplate jdbcTemplate, MeterRegistry meterRegistry) {
        this.jdbcTemplate = jdbcTemplate;
        this.meterRegistry = meterRegistry;
    }

    @Transactional
    public boolean applyEvent(TransferCompletedEvent event) {
        int inserted = jdbcTemplate.update(
                """
                INSERT INTO processed_events (event_id)
                VALUES (?)
                ON CONFLICT (event_id) DO NOTHING
                """,
                event.eventId());

        if (inserted == 0) {
            meterRegistry.counter("projector_events_total", "outcome", "duplicate").increment();
            return false;
        }

        // Apply in deterministic ascending account UUID order to avoid deadlocks under concurrency
        if (event.fromAccountId().compareTo(event.toAccountId()) <= 0) {
            upsertDelta(event.fromAccountId(), Math.negateExact(event.amountMinor()), event.eventId());
            upsertDelta(event.toAccountId(), event.amountMinor(), event.eventId());
        } else {
            upsertDelta(event.toAccountId(), event.amountMinor(), event.eventId());
            upsertDelta(event.fromAccountId(), Math.negateExact(event.amountMinor()), event.eventId());
        }

        meterRegistry.counter("projector_events_total", "outcome", "applied").increment();
        return true;
    }

    public Map<UUID, AccountBalanceView> snapshotAllBalances() {
        List<AccountBalanceView> rows = jdbcTemplate.query(
                "SELECT account_id, balance_minor, last_event_id FROM account_balance_view ORDER BY account_id ASC",
                (rs, rn) -> new AccountBalanceView(
                        rs.getObject("account_id", UUID.class),
                        rs.getLong("balance_minor"),
                        rs.getLong("last_event_id")));
        return rows.stream().collect(Collectors.toMap(AccountBalanceView::accountId, v -> v));
    }

    @Transactional
    public void wipeProjectionForReplay() {
        jdbcTemplate.update("DELETE FROM account_balance_view");
        jdbcTemplate.update("DELETE FROM processed_events");
    }

    private void upsertDelta(UUID accountId, long deltaMinor, long eventId) {
        jdbcTemplate.update(
                """
                INSERT INTO account_balance_view (account_id, balance_minor, last_event_id)
                VALUES (?, ?, ?)
                ON CONFLICT (account_id) DO UPDATE
                SET balance_minor = account_balance_view.balance_minor + EXCLUDED.balance_minor,
                    last_event_id = GREATEST(account_balance_view.last_event_id, EXCLUDED.last_event_id)
                """,
                accountId,
                deltaMinor,
                eventId);
    }
}
