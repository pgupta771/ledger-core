package dev.ledger.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DomainEdgeCasesTest {

    @Test
    void coversDomainValidationBranchesAndExceptionAccessors() {
        UUID id = UUID.randomUUID();
        InsufficientFundsException ife = new InsufficientFundsException(id, 100L, 250L);
        assertThat(ife.accountId()).isEqualTo(id);
        assertThat(ife.availableMinor()).isEqualTo(100L);
        assertThat(ife.requestedMinor()).isEqualTo(250L);

        IdempotencyConflictException ice = new IdempotencyConflictException("busy", 2);
        assertThat(ice.retryAfterSeconds()).isEqualTo(2);

        assertThat(new IdempotencyPayloadMismatchException("mismatch")).hasMessage("mismatch");
        assertThat(new ResourceNotFoundException("missing")).hasMessage("missing");
        assertThat(new DuplicateReversalException(id)).hasMessageContaining(id.toString());

        assertThatThrownBy(() -> Currency.of(null)).isInstanceOf(DomainValidationException.class);
        assertThat(new Currency("EUR", -1).minorUnitDigits()).isEqualTo(2);
        assertThat(Currency.GBP.toString()).isEqualTo("GBP");

        for (String c : List.of("BE", "ES", "IT")) {
            Iban gen = Iban.generateSynthetic(c, 77L);
            assertThat(gen.toString()).startsWith(c);
        }
        assertThatThrownBy(() -> Iban.generateSynthetic("ZZ", 1L))
                .isInstanceOf(DomainValidationException.class);

        Instant now = Instant.now();
        assertThatThrownBy(() -> new Account(id, null, Currency.EUR, AccountType.CUSTOMER, true, 0L, 0L, now))
                .isInstanceOf(DomainValidationException.class);
        assertThatThrownBy(() -> new Account(id, null, Currency.EUR, AccountType.CUSTOMER, false, -1L, 0L, now))
                .isInstanceOf(InsufficientFundsException.class);

        Account blankIbanAcc = new Account(id, "   ", Currency.EUR, AccountType.CUSTOMER, false, 50L, 0L, now);
        assertThat(blankIbanAcc.iban()).isNull();
        assertThat(blankIbanAcc.balance()).isEqualTo(Money.ofMinor(50L, Currency.EUR));

        UUID tid = UUID.randomUUID();
        assertThatThrownBy(() -> new Entry(1L, tid, id, 0L, Currency.EUR, now))
                .isInstanceOf(DomainValidationException.class);

        UUID id2 = UUID.randomUUID();
        Entry e1 = new Entry(1L, tid, id, -10L, Currency.EUR, now);
        Entry e2 = new Entry(2L, tid, id2, 10L, Currency.EUR, now);

        assertThatThrownBy(() -> new Transfer(
                tid, " ", "k", TransferStatus.COMPLETED, Currency.EUR, 10L, id, id2, null, null, List.of(e1, e2), now))
                .isInstanceOf(DomainValidationException.class);
        assertThatThrownBy(() -> new Transfer(
                tid, "c", " ", TransferStatus.COMPLETED, Currency.EUR, 10L, id, id2, null, null, List.of(e1, e2), now))
                .isInstanceOf(DomainValidationException.class);
        assertThatThrownBy(() -> new Transfer(
                tid, "c", "k", TransferStatus.COMPLETED, Currency.EUR, 10L, id, id, null, null, List.of(e1, e2), now))
                .isInstanceOf(DomainValidationException.class);
        assertThatThrownBy(() -> new Transfer(
                tid, "c", "k", TransferStatus.COMPLETED, Currency.EUR, 0L, id, id2, null, null, List.of(e1, e2), now))
                .isInstanceOf(DomainValidationException.class);
        assertThatThrownBy(() -> new Transfer(
                tid, "c", "k", TransferStatus.COMPLETED, Currency.EUR, 10L, id, id2, null, null, List.of(e1), now))
                .isInstanceOf(DomainValidationException.class);
    }
}
