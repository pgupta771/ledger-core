package dev.ledger.service.transfer;

import dev.ledger.domain.Entry;
import java.util.List;
import java.util.UUID;

/**
 * Keyset-paginated statement response ordered by descending {@code entries.id}.
 */
public record StatementPage(
        UUID accountId,
        List<Entry> items,
        Long nextCursor,
        boolean hasMore) {}
