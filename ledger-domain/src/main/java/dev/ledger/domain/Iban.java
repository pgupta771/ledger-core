package dev.ledger.domain;

import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Validated International Bank Account Number (ISO 13616) using per-country length rules and mod-97 verification.
 */
public record Iban(String value) {

    private static final Pattern FORMAT_PATTERN = Pattern.compile("^[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}$");

    private static final Map<String, Integer> COUNTRY_LENGTHS = Map.of(
            "NL", 18,
            "DE", 22,
            "IE", 22,
            "GB", 22,
            "FR", 27,
            "BE", 16,
            "ES", 24,
            "IT", 27
    );

    public Iban {
        Objects.requireNonNull(value, "IBAN must not be null");
        String normalized = value.replaceAll("\\s+", "").toUpperCase();
        if (!FORMAT_PATTERN.matcher(normalized).matches()) {
            throw new DomainValidationException("Invalid IBAN format: " + value);
        }
        String country = normalized.substring(0, 2);
        Integer expectedLength = COUNTRY_LENGTHS.get(country);
        if (expectedLength == null) {
            throw new DomainValidationException("Unsupported IBAN country code: " + country);
        }
        if (normalized.length() != expectedLength) {
            throw new DomainValidationException(
                    "Invalid IBAN length for country " + country + ": expected " + expectedLength
                            + " characters, got " + normalized.length());
        }
        if (computeMod97(normalized) != 1) {
            throw new DomainValidationException("Invalid IBAN mod-97 checksum for: " + normalized);
        }
        value = normalized;
    }

    public static Iban of(String raw) {
        return new Iban(raw);
    }

    public String countryCode() {
        return value.substring(0, 2);
    }

    /**
     * Generates a valid synthetic test IBAN for tests and benchmarks (never a real personal account).
     */
    public static Iban generateSynthetic(String countryCode, long sequence) {
        String country = Objects.requireNonNull(countryCode).trim().toUpperCase();
        Integer totalLen = COUNTRY_LENGTHS.get(country);
        if (totalLen == null) {
            throw new DomainValidationException("Unsupported synthetic IBAN country: " + country);
        }
        int bbanLen = totalLen - 4;
        String prefix = switch (country) {
            case "NL" -> "TEST";
            case "IE", "GB" -> "TEST";
            default -> "";
        };
        int digitsLen = bbanLen - prefix.length();
        String digits = String.format("%0" + digitsLen + "d", Math.abs(sequence) % (long) Math.pow(10, Math.min(digitsLen, 15)));
        if (digits.length() < digitsLen) {
            digits = "0".repeat(digitsLen - digits.length()) + digits;
        }
        String bban = prefix + digits;
        int mod = computeMod97(country + "00" + bban);
        int checkDigits = 98 - mod;
        String checkStr = String.format("%02d", checkDigits);
        return new Iban(country + checkStr + bban);
    }

    static int computeMod97(String normalizedIban) {
        String rearranged = normalizedIban.substring(4) + normalizedIban.substring(0, 4);
        long remainder = 0L;
        for (int i = 0; i < rearranged.length(); i++) {
            char ch = rearranged.charAt(i);
            if (ch >= '0' && ch <= '9') {
                remainder = (remainder * 10L + (ch - '0')) % 97L;
            } else if (ch >= 'A' && ch <= 'Z') {
                int val = ch - 'A' + 10;
                remainder = (remainder * 100L + val) % 97L;
            } else {
                throw new DomainValidationException("Illegal character in IBAN: " + ch);
            }
        }
        return (int) remainder;
    }

    @Override
    public String toString() {
        return value;
    }
}
