package dev.ledger.service;

import static org.assertj.core.api.Assertions.assertThat;

import dev.ledger.domain.Account;
import dev.ledger.domain.AccountType;
import dev.ledger.domain.Currency;
import dev.ledger.service.outbox.OutboxPublisher;
import dev.ledger.service.repository.AccountRepository;
import dev.ledger.service.transfer.TransferCommand;
import dev.ledger.service.transfer.TransferCoordinator;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"ledger.outbox.scheduling-enabled=false"})
@Testcontainers
class L7ObservabilityIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private TransferCoordinator transferCoordinator;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void exposesAllRequiredPrometheusLedgerMetrics() {
        Account funding = accountRepository.createAccount(
                UUID.randomUUID(), null, Currency.EUR, AccountType.SYSTEM, true, 0L);
        Account alice = accountRepository.createAccount(
                UUID.randomUUID(), null, Currency.EUR, AccountType.CUSTOMER, false, 0L);

        TransferCommand cmd = new TransferCommand(
                "metrics-client", "metrics-key-1", funding.id(), alice.id(), 2_500L, Currency.EUR, "obs", null);
        transferCoordinator.execute(cmd);
        transferCoordinator.execute(cmd); // idempotent replay
        outboxPublisher.publishNextBatch();

        ResponseEntity<String> prom = restTemplate.getForEntity("/actuator/prometheus", String.class);
        assertThat(prom.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(prom.getBody()).contains(
                "ledger_transfers_total",
                "ledger_transfer_duration_seconds",
                "ledger_idempotent_replays_total",
                "ledger_lock_retries_total",
                "ledger_outbox_unpublished",
                "ledger_outbox_publish_lag_seconds");
    }
}
