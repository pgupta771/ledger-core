package dev.ledger.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TransferDomainTest {

    @Test
    void createsBalancedTransferAndAppliesEntriesToAccounts() {
        Instant now = Instant.now();
        Account alice = new Account(
                UUID.randomUUID(),
                Iban.generateSynthetic("NL", 1L).value(),
                Currency.EUR,
                AccountType.CUSTOMER,
                false,
                10_000L,
                0L,
                now);
        Account bob = new Account(
                UUID.randomUUID(),
                Iban.generateSynthetic("NL", 2L).value(),
                Currency.EUR,
                AccountType.CUSTOMER,
                false,
                2_000L,
                0L,
                now);

        Transfer transfer = Transfer.createBalanced(
                UUID.randomUUID(),
                "client-1",
                "idem-1",
                alice,
                bob,
                Money.ofMinor(3_500L, Currency.EUR),
                "Invoice 101",
                null,
                now);

        assertThat(transfer.entries()).hasSize(2);
        assertThat(transfer.entries().stream().mapToLong(Entry::amountMinor).sum()).isZero();

        Account updatedAlice = alice.applyEntry(-3_500L);
        Account updatedBob = bob.applyEntry(3_500L);

        assertThat(updatedAlice.balanceMinor()).isEqualTo(6_500L);
        assertThat(updatedBob.balanceMinor()).isEqualTo(5_500L);
    }

    @Test
    void rejectsUnbalancedEntriesNegativeCustomerBalancesAndCurrencyMismatch() {
        Instant now = Instant.now();
        UUID transferId = UUID.randomUUID();
        UUID a1 = UUID.randomUUID();
        UUID a2 = UUID.randomUUID();

        Entry debit = new Entry(1L, transferId, a1, -500L, Currency.EUR, now);
        Entry unbalancedCredit = new Entry(2L, transferId, a2, 400L, Currency.EUR, now);

        assertThatThrownBy(() -> new Transfer(
                transferId,
                "client-1",
                "idem-1",
                TransferStatus.COMPLETED,
                Currency.EUR,
                500L,
                a1,
                a2,
                "bad",
                null,
                List.of(debit, unbalancedCredit),
                now))
                .isInstanceOf(DomainValidationException.class)
                .hasMessageContaining("do not sum to zero");

        Account customer = new Account(a1, null, Currency.EUR, AccountType.CUSTOMER, false, 100L, 0L, now);
        assertThatThrownBy(() -> customer.applyEntry(-200L))
                .isInstanceOf(InsufficientFundsException.class);

        Account systemFunding = new Account(a2, null, Currency.EUR, AccountType.SYSTEM, true, 0L, 0L, now);
        assertThat(systemFunding.applyEntry(-50_000L).balanceMinor()).isEqualTo(-50_000L);
    }
}
