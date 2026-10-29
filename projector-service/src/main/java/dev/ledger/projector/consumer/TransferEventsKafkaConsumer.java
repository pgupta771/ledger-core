package dev.ledger.projector.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.ledger.domain.TransferCompletedEvent;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for {@code ledger.transfers.v1} supporting full-topic replay from offset 0.
 */
@Component
public class TransferEventsKafkaConsumer implements ConsumerSeekAware {

    private final BalanceProjectorService projectorService;
    private final ObjectMapper objectMapper;
    private final AtomicReference<ConsumerSeekCallback> seekCallbackRef = new AtomicReference<>();
    private final Set<TopicPartition> assignedPartitions = ConcurrentHashMap.newKeySet();

    public TransferEventsKafkaConsumer(BalanceProjectorService projectorService, ObjectMapper objectMapper) {
        this.projectorService = projectorService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        this.seekCallbackRef.set(callback);
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        this.seekCallbackRef.set(callback);
        this.assignedPartitions.addAll(assignments.keySet());
    }

    @Override
    public void onPartitionsRevoked(java.util.Collection<TopicPartition> partitions) {
        this.assignedPartitions.removeAll(partitions);
    }

    @KafkaListener(
            id = "projector-listener",
            topics = "${ledger.projector.topic:ledger.transfers.v1}",
            groupId = "${spring.kafka.consumer.group-id:ledger-balance-projector}")
    public void onMessage(ConsumerRecord<String, String> record) throws Exception {
        TransferCompletedEvent event = objectMapper.readValue(record.value(), TransferCompletedEvent.class);
        projectorService.applyEvent(event);
    }

    public Set<TopicPartition> assignedPartitions() {
        return Set.copyOf(assignedPartitions);
    }

    /**
     * Wipes the read model and seeks all assigned partitions back to offset 0 to rebuild deterministically.
     */
    public void replayAllFromBeginning() {
        ConsumerSeekCallback callback = seekCallbackRef.get();
        if (callback == null || assignedPartitions.isEmpty()) {
            throw new IllegalStateException("Consumer partitions are not yet assigned for replay");
        }
        projectorService.wipeProjectionForReplay();
        callback.seekToBeginning(assignedPartitions);
    }
}
