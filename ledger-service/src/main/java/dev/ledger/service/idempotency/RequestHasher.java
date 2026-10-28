package dev.ledger.service.idempotency;

import dev.ledger.service.transfer.TransferCommand;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Computes canonical SHA-256 request hashes for transfer idempotency verification.
 */
public final class RequestHasher {

    private RequestHasher() {}

    public static String sha256(TransferCommand command) {
        String canonical = String.join(
                "|",
                command.fromAccountId().toString(),
                command.toAccountId().toString(),
                Long.toString(command.amountMinor()),
                command.currency().code(),
                command.reference() != null ? command.reference() : "",
                command.reversesTransferId() != null ? command.reversesTransferId().toString() : "");
        return sha256Hex(canonical);
    }

    public static String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(bytes);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 algorithm not available", ex);
        }
    }
}
