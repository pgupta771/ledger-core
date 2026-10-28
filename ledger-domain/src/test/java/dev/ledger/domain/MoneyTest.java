package dev.ledger.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;
import org.junit.jupiter.api.Test;

class MoneyTest {

    @Test
    void addsAndSubtractsExactMinorUnits() {
        Money a = Money.ofMinor(10_50L, "EUR");
        Money b = Money.ofMinor(2_75L, "EUR");

        assertThat(a.add(b)).isEqualTo(Money.ofMinor(13_25L, "EUR"));
        assertThat(a.subtract(b)).isEqualTo(Money.ofMinor(7_75L, "EUR"));
        assertThat(a.negate()).isEqualTo(Money.ofMinor(-10_50L, "EUR"));
        assertThat(a.isPositive()).isTrue();
        assertThat(a.negate().isNegative()).isTrue();
        assertThat(Money.zero(Currency.EUR).isZero()).isTrue();
        assertThat(a.compareTo(b)).isGreaterThan(0);
    }

    @Test
    void throwsArithmeticExceptionOnLongOverflow() {
        Money max = Money.ofMinor(Long.MAX_VALUE, Currency.EUR);
        Money one = Money.ofMinor(1L, Currency.EUR);
        Money min = Money.ofMinor(Long.MIN_VALUE, Currency.EUR);

        assertThatThrownBy(() -> max.add(one)).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> min.subtract(one)).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(min::negate).isInstanceOf(ArithmeticException.class);
    }

    @Test
    void rejectsCurrencyMismatchAndInvalidCodes() {
        Money eur = Money.ofMinor(100L, "EUR");
        Money usd = Money.ofMinor(100L, "USD");

        assertThatThrownBy(() -> eur.add(usd)).isInstanceOf(CurrencyMismatchException.class);
        assertThatThrownBy(() -> eur.subtract(usd)).isInstanceOf(CurrencyMismatchException.class);
        assertThatThrownBy(() -> Currency.of("INVALID")).isInstanceOf(DomainValidationException.class);
        assertThatThrownBy(() -> Currency.of("ZZZ")).isInstanceOf(DomainValidationException.class);
        assertThatThrownBy(() -> Money.zero(Currency.EUR).requirePositive())
                .isInstanceOf(DomainValidationException.class);
    }

    @Property
    void moneyAdditionAndNegationAlwaysSumToZero(
            @ForAll @LongRange(min = -1_000_000_000_000L, max = 1_000_000_000_000L) long amountMinor) {
        Money m = Money.ofMinor(amountMinor, Currency.EUR);
        assertThat(m.add(m.negate()).isZero()).isTrue();
    }
}
