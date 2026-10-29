package dev.ledger.service.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.ledger.domain.TransferCompletedEvent;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Transactional outbox relay using {@code FOR UPDATE SKIP LOCKED} for safe multi-instance polling.
 */
@Service
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;
    private final int batchSize;
    private final boolean schedulingEnabled;
    private final Timer publishLagTimer;
    private final AtomicReference<OutboxFaultHook> faultHook = new AtomicReference<>(OutboxFaultHook.NO_OP);

    record OutboxRow(long id, UUID aggregateId, String payloadJson, Instant createdAt) {}

    public OutboxPublisher(
            JdbcTemplate jdbcTemplate,
            TransactionTemplate transactionTemplate,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            @Value("${ledger.outbox.topic:ledger.transfers.v1}") String topic,
            @Value("${ledger.outbox.batch-size:100}") int batchSize,
            @Value("${ledger.outbox.scheduling-enabled:true}") boolean schedulingEnabled) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
        this.batchSize = batchSize;
        this.schedulingEnabled = schedulingEnabled;

        Gauge.builder("ledger_outbox_unpublished", this, OutboxPublisher::countUnpublished)
                .description("Current count of unpublished rows in the outbox table")
                .register(meterRegistry);
        this.publishLagTimer = Timer.builder("ledger_outbox_publish_lag_seconds")
                .description("Lag between outbox row creation and Kafka broker acknowledgement")
                .register(meterRegistry);
    }

    public void setFaultHook(OutboxFaultHook hook) {
        this.faultHook.set(hook != null ? hook : OutboxFaultHook.NO_OP);
    }

    public double countUnpublished() {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL", Long.class);
        return count != null ? count.doubleValue() : 0.0;
    }

    @Scheduled(fixedDelayString = "${ledger.outbox.poll-interval-ms:200}")
    public void scheduledPoll() {
        if (!schedulingEnabled) {
            return;
        }
        try {
            publishNextBatch();
        } catch (Exception ex) {
            log.warn("Outbox scheduled poll failed; will retry on next tick: {}", ex.getMessage());
        }
    }

    /**
     * Claims up to {@code batchSize} unpublished outbox rows using {@code FOR UPDATE SKIP LOCKED},
     * publishes them to Kafka ({@code acks=all}), and marks {@code published_at}.
     */
    public int publishNextBatch() {
        Integer published = transactionTemplate.execute(status -> {
            List<OutboxRow> rows = jdbcTemplate.query(
                    """
                    SELECT id, aggregate_id, payload::text AS payload_json, created_at
                    FROM outbox
                    WHERE published_at IS NULL
                    ORDER BY id ASC
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
                    """,
                    (rs, rn) -> new OutboxRow(
                            rs.getLong("id"),
                            rs.getObject("aggregate_id", UUID.class),
                            rs.getString("payload_json"),
                            rs.getTimestamp("created_at").toInstant()),
                    batchSize);

            if (rows.isEmpty()) {
                return 0;
            }

            List<Object[]> updateArgs = new ArrayList<>(rows.size());
            for (OutboxRow row : rows) {
                try {
                    String eventJson = toWireEventJson(row);
                    kafkaTemplate.send(topic, row.aggregateId().toString(), eventJson)
                            .get(15, TimeUnit.SECONDS);

                    faultHook.get().afterKafkaAckBeforeMarkPublished(row.id());

                    Instant now = Instant.now();
                    publishLagTimer.record(Duration.between(row.createdAt(), now));
                    updateArgs.add(new Object[] {Timestamp.from(now), row.id()});
                } catch (RuntimeException rex) {
                    throw rex;
                } catch (Exception ex) {
                    throw new IllegalStateException("Failed publishing outbox row " + row.id(), ex);
                }
            }

            jdbcTemplate.batchUpdate("UPDATE outbox SET published_at = ? WHERE id = ?", updateArgs);
            return rows.size();
        });
        return published != null ? published : 0;
    }

    private String toWireEventJson(OutboxRow row) throws Exception {
        JsonNode node = objectMapper.readTree(row.payloadJson());
        TransferCompletedEvent event = new TransferCompletedEvent(
                row.id(),
                UUID.fromString(node.get("transferId").asText()),
                UUID.fromString(node.get("fromAccountId").asText()),
                UUID.fromString(node.get("toAccountId").asText()),
                node.get("amountMinor").asLong(),
                node.get("currency").asText(),
                Instant.parse(node.get("createdAt").asText()));
        return objectMapper.writeValueAsString(event);
    }
}
