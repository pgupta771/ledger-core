package dev.ledger.projector.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.ledger.domain.TransferCompletedEvent;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for {@code ledger.transfers.v1} supporting thread-safe full-topic replay from offset 0.
 */
@Component
public class TransferEventsKafkaConsumer extends AbstractConsumerSeekAware {

    private final BalanceProjectorService projectorService;
    private final ObjectMapper objectMapper;
    private final Set<TopicPartition> assignedPartitions = ConcurrentHashMap.newKeySet();
    private final ReentrantLock projectionLock = new ReentrantLock();

    public TransferEventsKafkaConsumer(BalanceProjectorService projectorService, ObjectMapper objectMapper) {
        this.projectorService = projectorService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        super.onPartitionsAssigned(assignments, callback);
        this.assignedPartitions.addAll(assignments.keySet());
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        super.onPartitionsRevoked(partitions);
        this.assignedPartitions.removeAll(partitions);
    }

    @KafkaListener(
            id = "projector-listener",
            topics = "${ledger.projector.topic:ledger.transfers.v1}",
            groupId = "${spring.kafka.consumer.group-id:ledger-balance-projector}")
    public void onMessage(ConsumerRecord<String, String> record) throws Exception {
        TransferCompletedEvent event = objectMapper.readValue(record.value(), TransferCompletedEvent.class);
        projectionLock.lock();
        try {
            projectorService.applyEvent(event);
        } finally {
            projectionLock.unlock();
        }
    }

    public Set<TopicPartition> assignedPartitions() {
        return Set.copyOf(assignedPartitions);
    }

    /**
     * Wipes the read model and queues a thread-safe offset-0 seek across all assigned partitions.
     */
    public void replayAllFromBeginning() {
        if (assignedPartitions.isEmpty()) {
            throw new IllegalStateException("Consumer partitions are not yet assigned for replay");
        }
        projectionLock.lock();
        try {
            projectorService.wipeProjectionForReplay();
            seekToBeginning();
        } finally {
            projectionLock.unlock();
        }
    }
}
