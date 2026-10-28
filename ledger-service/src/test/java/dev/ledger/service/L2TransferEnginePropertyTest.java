package dev.ledger.service;

import static org.assertj.core.api.Assertions.assertThat;

import dev.ledger.domain.Account;
import dev.ledger.domain.AccountType;
import dev.ledger.domain.Currency;
import dev.ledger.domain.Entry;
import dev.ledger.domain.InsufficientFundsException;
import dev.ledger.domain.Money;
import dev.ledger.domain.Transfer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/**
 * Property-based tests (jqwik) verifying that any sequence of transfers preserves total money,
 * never produces a negative customer balance, and always produces zero-sum entry pairs.
 */
class L2TransferEnginePropertyTest {

    record TransferAction(int fromIdx, int toIdx, long amountMinor) {}

    @Provide
    Arbitrary<List<TransferAction>> transferSequences() {
        Arbitrary<Integer> accountIdx = Arbitraries.integers().between(0, 9);
        Arbitrary<Long> amount = Arbitraries.longs().between(1L, 15_000L);
        Arbitrary<TransferAction> step = Combinators.combine(accountIdx, accountIdx, amount)
                .as(TransferAction::new)
                .filter(a -> a.fromIdx() != a.toIdx());
        return step.list().ofMinSize(10).ofMaxSize(80);
    }

    @Property(tries = 100)
    void randomSequenceOfTransfersPreservesTotalMoneyAndNonNegativeCustomerBalances(
            @ForAll("transferSequences") List<TransferAction> actions) {
        Instant now = Instant.now();
        List<Account> accounts = new ArrayList<>(10);
        // Account 0 is a SYSTEM FUNDING account (allowNegative = true); 1..9 are CUSTOMER accounts
        accounts.add(new Account(UUID.randomUUID(), null, Currency.EUR, AccountType.SYSTEM, true, 0L, 0L, now));
        for (int i = 1; i < 10; i++) {
            accounts.add(new Account(UUID.randomUUID(), null, Currency.EUR, AccountType.CUSTOMER, false, 10_000L, 0L, now));
        }

        long initialTotalMinor = accounts.stream().mapToLong(Account::balanceMinor).sum();

        int seq = 0;
        for (TransferAction action : actions) {
            Account from = accounts.get(action.fromIdx());
            Account to = accounts.get(action.toIdx());
            try {
                Transfer transfer = Transfer.createBalanced(
                        UUID.randomUUID(),
                        "prop-client",
                        "idem-" + (++seq),
                        from,
                        to,
                        Money.ofMinor(action.amountMinor(), Currency.EUR),
                        "jqwik",
                        null,
                        now);

                Account updatedFrom = from.applyEntry(-action.amountMinor());
                Account updatedTo = to.applyEntry(action.amountMinor());
                accounts.set(action.fromIdx(), updatedFrom);
                accounts.set(action.toIdx(), updatedTo);

                long entrySum = transfer.entries().stream().mapToLong(Entry::amountMinor).sum();
                assertThat(entrySum).isZero();
            } catch (InsufficientFundsException ignored) {
                // Rejected overdraft leaves balances untouched
            }
        }

        long finalTotalMinor = accounts.stream().mapToLong(Account::balanceMinor).sum();
        assertThat(finalTotalMinor).isEqualTo(initialTotalMinor);

        for (int i = 1; i < 10; i++) {
            assertThat(accounts.get(i).balanceMinor()).isGreaterThanOrEqualTo(0L);
        }
    }
}
