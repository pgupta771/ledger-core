package dev.ledger.service.transfer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.ledger.domain.IdempotencyConflictException;
import dev.ledger.domain.IdempotencyPayloadMismatchException;
import dev.ledger.domain.Transfer;
import dev.ledger.service.idempotency.IdempotencyDecision;
import dev.ledger.service.idempotency.IdempotencyRecord;
import dev.ledger.service.idempotency.IdempotencyStatus;
import dev.ledger.service.idempotency.RequestHasher;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Coordinates idempotent transfer execution within a single PostgreSQL transaction (Section 7.1).
 */
@Service
public class IdempotentTransferService {

    private final JdbcTemplate jdbcTemplate;
    private final TransferEngine transferEngine;
    private final ObjectMapper objectMapper;
    private final Counter idempotentReplayCounter;

    public IdempotentTransferService(
            JdbcTemplate jdbcTemplate,
            TransferEngine transferEngine,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {
        this.jdbcTemplate = jdbcTemplate;
        this.transferEngine = transferEngine;
        this.objectMapper = objectMapper;
        this.idempotentReplayCounter = Counter.builder("ledger_idempotent_replays_total")
                .description("Total number of idempotent transfer replays served from stored responses")
                .register(meterRegistry);
    }

    @Transactional
    public TransferExecutionResult executePessimistic(TransferCommand command) {
        return executeInCurrentTransaction(command, true);
    }

    public TransferExecutionResult executeInCurrentTransaction(TransferCommand command, boolean lockForUpdate) {
        String requestHash = RequestHasher.sha256(command);

        for (int attempt = 0; attempt < 3; attempt++) {
            int inserted = jdbcTemplate.update(
                    """
                    INSERT INTO idempotency_keys (client_id, key, request_hash, status, created_at)
                    VALUES (?, ?, ?, 'IN_PROGRESS', ?)
                    ON CONFLICT (client_id, key) DO NOTHING
                    """,
                    command.clientId(),
                    command.idempotencyKey(),
                    requestHash,
                    Timestamp.from(Instant.now()));

            if (inserted == 1) {
                Transfer created = transferEngine.executeInCurrentTransaction(command, lockForUpdate);
                String responseJson = serializeTransfer(created);
                jdbcTemplate.update(
                        """
                        UPDATE idempotency_keys
                        SET status = 'COMPLETED',
                            response_code = 201,
                            response_body = ?::jsonb
                        WHERE client_id = ? AND key = ?
                        """,
                        responseJson,
                        command.clientId(),
                        command.idempotencyKey());
                return new TransferExecutionResult(created, false);
            }

            Optional<IdempotencyRecord> existingOpt = findRecord(command.clientId(), command.idempotencyKey());
            if (existingOpt.isEmpty()) {
                // Concurrent owner transaction rolled back; retry insert
                continue;
            }

            IdempotencyDecision decision = IdempotencyDecision.evaluate(existingOpt.get(), requestHash);
            return switch (decision) {
                case IdempotencyDecision.ReplayStoredResponse replay -> {
                    idempotentReplayCounter.increment();
                    yield new TransferExecutionResult(deserializeTransfer(replay.responseBodyJson()), true);
                }
                case IdempotencyDecision.RejectInFlight inFlight ->
                        throw new IdempotencyConflictException(
                                "Transfer with Idempotency-Key '" + command.idempotencyKey() + "' is currently in flight",
                                inFlight.retryAfterSeconds());
                case IdempotencyDecision.RejectPayloadMismatch mismatch ->
                        throw new IdempotencyPayloadMismatchException(
                                "Idempotency-Key '" + command.idempotencyKey()
                                        + "' was already used with a different request payload");
            };
        }

        throw new IdempotencyConflictException(
                "Could not acquire idempotency slot for key: " + command.idempotencyKey(), 1);
    }

    public Optional<IdempotencyRecord> findRecord(String clientId, String key) {
        List<IdempotencyRecord> rows = jdbcTemplate.query(
                """
                SELECT client_id, key, request_hash, status, response_code, response_body::text AS response_body_json, created_at
                FROM idempotency_keys
                WHERE client_id = ? AND key = ?
                """,
                (rs, rn) -> new IdempotencyRecord(
                        rs.getString("client_id"),
                        rs.getString("key"),
                        rs.getString("request_hash").trim(),
                        IdempotencyStatus.valueOf(rs.getString("status")),
                        (Integer) rs.getObject("response_code"),
                        rs.getString("response_body_json"),
                        rs.getTimestamp("created_at").toInstant()),
                clientId,
                key);
        return rows.stream().findFirst();
    }

    private String serializeTransfer(Transfer transfer) {
        try {
            return objectMapper.writeValueAsString(transfer);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize Transfer for idempotency storage", ex);
        }
    }

    private Transfer deserializeTransfer(String json) {
        try {
            return objectMapper.readValue(json, Transfer.class);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to deserialize stored Transfer from idempotency_keys", ex);
        }
    }
}
