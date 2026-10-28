package dev.ledger.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class IbanTest {

    @Test
    void acceptsValidSyntheticIbansAcrossAllRequiredCountries() {
        for (String country : List.of("NL", "DE", "IE", "GB", "FR")) {
            Iban iban = Iban.generateSynthetic(country, 123456L);
            assertThat(iban.countryCode()).isEqualTo(country);
            assertThat(Iban.of("  " + iban.value().toLowerCase() + " ").value()).isEqualTo(iban.value());
        }
    }

    @Test
    void rejectsWrongCheckDigitsWrongLengthAndUnsupportedCountries() {
        Iban validNl = Iban.generateSynthetic("NL", 42L);
        String corruptedChecksum = "NL00" + validNl.value().substring(4);

        assertThatThrownBy(() -> Iban.of(corruptedChecksum))
                .isInstanceOf(DomainValidationException.class)
                .hasMessageContaining("mod-97");

        // Wrong length for NL (19 chars instead of 18)
        assertThatThrownBy(() -> Iban.of(validNl.value() + "0"))
                .isInstanceOf(DomainValidationException.class)
                .hasMessageContaining("length");

        // Unsupported country code
        assertThatThrownBy(() -> Iban.of("US64SVBKUS6S3300958879"))
                .isInstanceOf(DomainValidationException.class)
                .hasMessageContaining("Unsupported IBAN country");

        // Invalid format
        assertThatThrownBy(() -> Iban.of("NOT-AN-IBAN"))
                .isInstanceOf(DomainValidationException.class);
    }
}
