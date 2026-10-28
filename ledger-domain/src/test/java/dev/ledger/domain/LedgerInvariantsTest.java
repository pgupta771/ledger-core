package dev.ledger.domain;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LedgerInvariantsTest {

    @Test
    void requiresAtLeastTwoEntriesPerTransfer() {
        assertThat(LedgerInvariants.MIN_ENTRIES_PER_TRANSFER).isEqualTo(2);
    }
}
