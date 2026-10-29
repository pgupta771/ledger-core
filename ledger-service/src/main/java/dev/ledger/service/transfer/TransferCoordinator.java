package dev.ledger.service.transfer;

import dev.ledger.domain.DomainValidationException;
import dev.ledger.domain.IdempotencyConflictException;
import dev.ledger.domain.InsufficientFundsException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Executes transfers using the configured concurrency strategy ({@code pessimistic} or {@code serializable}),
 * enforcing bounded jittered retries on PostgreSQL SQLSTATE {@code 40001} and {@code 40P01} and recording metrics.
 */
@Service
public class TransferCoordinator {

    private static final Logger log = LoggerFactory.getLogger(TransferCoordinator.class);

    private final IdempotentTransferService idempotentTransferService;
    private final TransactionTemplate pessimisticTx;
    private final TransactionTemplate serializableTx;
    private final ConcurrencyMode defaultMode;
    private final int maxRetries;
    private final MeterRegistry meterRegistry;
    private final Counter lockRetriesCounter;
    private final Timer transferDurationTimer;

    public TransferCoordinator(
            IdempotentTransferService idempotentTransferService,
            PlatformTransactionManager transactionManager,
            MeterRegistry meterRegistry,
            @Value("${ledger.concurrency:pessimistic}") String configuredMode,
            @Value("${ledger.serializable.max-retries:80}") int maxRetries) {
        this.idempotentTransferService = idempotentTransferService;
        this.defaultMode = ConcurrencyMode.fromProperty(configuredMode);
        this.maxRetries = maxRetries;
        this.meterRegistry = meterRegistry;

        this.pessimisticTx = new TransactionTemplate(transactionManager);
        this.pessimisticTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.pessimisticTx.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);

        this.serializableTx = new TransactionTemplate(transactionManager);
        this.serializableTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.serializableTx.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE);

        this.lockRetriesCounter = Counter.builder("ledger_lock_retries_total")
                .description("Total transaction retries caused by SQLSTATE 40001 or 40P01")
                .register(meterRegistry);
        this.transferDurationTimer = Timer.builder("ledger_transfer_duration_seconds")
                .description("End-to-end transfer execution duration")
                .register(meterRegistry);
    }

    public ConcurrencyMode defaultMode() {
        return defaultMode;
    }

    public TransferExecutionResult execute(TransferCommand command) {
        return execute(command, defaultMode);
    }

    public TransferExecutionResult execute(TransferCommand command, ConcurrencyMode mode) {
        Timer.Sample sample = Timer.start(meterRegistry);
        String outcome = "error";
        try {
            TransferExecutionResult result = switch (mode) {
                case PESSIMISTIC -> executePessimistic(command);
                case SERIALIZABLE -> executeSerializableWithRetry(command);
            };
            outcome = result.idempotentReplay() ? "replayed" : "completed";
            return result;
        } catch (InsufficientFundsException ex) {
            outcome = "insufficient_funds";
            throw ex;
        } catch (DomainValidationException | IdempotencyConflictException ex) {
            outcome = "rejected";
            throw ex;
        } finally {
            sample.stop(transferDurationTimer);
            meterRegistry.counter("ledger_transfers_total", "outcome", outcome).increment();
        }
    }

    private TransferExecutionResult executePessimistic(TransferCommand command) {
        return pessimisticTx.execute(
                status -> idempotentTransferService.executeInCurrentTransaction(command, true));
    }

    private TransferExecutionResult executeSerializableWithRetry(TransferCommand command) {
        RuntimeException lastException = null;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return serializableTx.execute(
                        status -> idempotentTransferService.executeInCurrentTransaction(command, false));
            } catch (RuntimeException ex) {
                if (!isRetryableSerializationOrDeadlock(ex) || attempt == maxRetries) {
                    throw ex;
                }
                lastException = ex;
                lockRetriesCounter.increment();
                sleepWithBoundedJitter(attempt);
            }
        }
        throw lastException;
    }

    static boolean isRetryableSerializationOrDeadlock(Throwable error) {
        Throwable cursor = error;
        while (cursor != null) {
            if (cursor instanceof SQLException sqlEx) {
                String state = sqlEx.getSQLState();
                if ("40001".equals(state) || "40P01".equals(state)) {
                    return true;
                }
            }
            cursor = cursor.getCause();
        }
        return false;
    }

    private static void sleepWithBoundedJitter(int attempt) {
        long capMs = Math.min(25L, 1L << Math.min(attempt, 5));
        long sleepMs = ThreadLocalRandom.current().nextLong(1L, capMs + 1L);
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during SERIALIZABLE transaction retry backoff", ie);
        }
    }
}
