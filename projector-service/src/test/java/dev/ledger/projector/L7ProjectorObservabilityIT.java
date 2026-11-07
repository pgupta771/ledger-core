package dev.ledger.projector;

import static org.assertj.core.api.Assertions.assertThat;

import dev.ledger.domain.TransferCompletedEvent;
import dev.ledger.projector.consumer.AccountBalanceView;
import dev.ledger.projector.consumer.BalanceProjectorService;
import java.time.Instant;
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

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class L7ProjectorObservabilityIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Autowired
    private BalanceProjectorService projectorService;

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void servesProjectedBalancesAndExposesProjectorPrometheusMetrics() {
        UUID funding = UUID.randomUUID();
        UUID alice = UUID.randomUUID();
        TransferCompletedEvent event = new TransferCompletedEvent(
                9_001L, UUID.randomUUID(), funding, alice, 15_000L, "EUR", Instant.now());

        projectorService.applyEvent(event);
        projectorService.applyEvent(event); // duplicate

        ResponseEntity<AccountBalanceView> ok =
                restTemplate.getForEntity("/api/v1/balances/" + alice, AccountBalanceView.class);
        assertThat(ok.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(ok.getBody().balanceMinor()).isEqualTo(15_000L);

        ResponseEntity<String> missing =
                restTemplate.getForEntity("/api/v1/balances/" + UUID.randomUUID(), String.class);
        assertThat(missing.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

        ResponseEntity<String> prom = restTemplate.getForEntity("/actuator/prometheus", String.class);
        assertThat(prom.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(prom.getBody()).contains("projector_events_total");
    }
}
