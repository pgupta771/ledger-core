package dev.ledger.service.transfer;

import dev.ledger.domain.DuplicateReversalException;
import dev.ledger.domain.ResourceNotFoundException;
import dev.ledger.domain.Transfer;
import dev.ledger.domain.TransferStatus;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Creates compensating double-entry reversal transfers, ensuring each transfer is reversed at most once.
 */
@Service
public class ReversalService {

    private final JdbcTemplate jdbcTemplate;
    private final TransferEngine transferEngine;
    private final IdempotentTransferService idempotentTransferService;

    public ReversalService(
            JdbcTemplate jdbcTemplate,
            TransferEngine transferEngine,
            IdempotentTransferService idempotentTransferService) {
        this.jdbcTemplate = jdbcTemplate;
        this.transferEngine = transferEngine;
        this.idempotentTransferService = idempotentTransferService;
    }

    @Transactional
    public TransferExecutionResult reverseTransfer(String clientId, UUID originalTransferId, String idempotencyKey) {
        List<Map<String, Object>> lockedRows = jdbcTemplate.queryForList(
                "SELECT id, status FROM transfers WHERE id = ? FOR UPDATE", originalTransferId);
        if (lockedRows.isEmpty()) {
            throw new ResourceNotFoundException("Transfer not found: " + originalTransferId);
        }

        Transfer original = transferEngine.findById(originalTransferId)
                .orElseThrow(() -> new ResourceNotFoundException("Transfer not found: " + originalTransferId));

        TransferCommand reversalCmd = new TransferCommand(
                clientId,
                idempotencyKey,
                original.toAccountId(),
                original.fromAccountId(),
                original.amountMinor(),
                original.currency(),
                "Reversal of transfer " + original.id(),
                original.id());

        List<Map<String, Object>> existingRev = jdbcTemplate.queryForList(
                "SELECT client_id, idempotency_key FROM transfers WHERE reverses_transfer_id = ?",
                originalTransferId);
        if (!existingRev.isEmpty()) {
            Map<String, Object> rev = existingRev.getFirst();
            boolean sameIdempotencyCall = clientId.equals(rev.get("client_id"))
                    && idempotencyKey.equals(rev.get("idempotency_key"));
            if (!sameIdempotencyCall) {
                throw new DuplicateReversalException(originalTransferId);
            }
            return idempotentTransferService.executeInCurrentTransaction(reversalCmd, true);
        }

        if (original.status() == TransferStatus.REVERSED) {
            throw new DuplicateReversalException(originalTransferId);
        }

        try {
            TransferExecutionResult result = idempotentTransferService.executeInCurrentTransaction(reversalCmd, true);
            jdbcTemplate.update(
                    "UPDATE transfers SET status = 'REVERSED' WHERE id = ?", originalTransferId);
            return result;
        } catch (DuplicateKeyException ex) {
            throw new DuplicateReversalException(originalTransferId);
        }
    }
}
