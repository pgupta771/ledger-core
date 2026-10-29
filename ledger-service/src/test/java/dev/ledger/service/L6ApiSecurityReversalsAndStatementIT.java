package dev.ledger.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.jwk.source.ImmutableSecret;
import dev.ledger.domain.Account;
import dev.ledger.domain.AccountType;
import dev.ledger.domain.Entry;
import dev.ledger.domain.Iban;
import dev.ledger.domain.Transfer;
import dev.ledger.service.api.LedgerController.CreateAccountRequest;
import dev.ledger.service.api.LedgerController.CreateTransferRequest;
import dev.ledger.service.transfer.StatementPage;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"ledger.outbox.scheduling-enabled=false"})
@Testcontainers
class L6ApiSecurityReversalsAndStatementIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Value("${ledger.security.jwt-secret}")
    private String jwtSecret;

    @Test
    void enforcesJwtScopesServesOpenApiAndReturnsRfc9457Errors() {
        // 1. OpenAPI contract publicly accessible
        ResponseEntity<String> openApi = restTemplate.getForEntity("/v3/api-docs", String.class);
        assertThat(openApi.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(openApi.getBody()).contains("/api/v1/transfers", "/api/v1/accounts/{id}/statement");

        // 2. Unauthenticated -> 401
        ResponseEntity<String> unauth = restTemplate.postForEntity(
                "/api/v1/accounts", new CreateAccountRequest("EUR", null, AccountType.CUSTOMER, 0L), String.class);
        assertThat(unauth.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);

        // 3. Read-only token -> 403 Forbidden on POST
        String readOnlyToken = mintJwt("reader-1", "ledger:read");
        ResponseEntity<ProblemDetail> forbidden = restTemplate.exchange(
                "/api/v1/accounts",
                HttpMethod.POST,
                jsonEntity(readOnlyToken, null, new CreateAccountRequest("EUR", null, AccountType.CUSTOMER, 0L)),
                ProblemDetail.class);
        assertThat(forbidden.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);

        // 4. Invalid IBAN -> 400 RFC 9457 ProblemDetail with no stack trace
        String rwToken = mintJwt("client-l6", "ledger:read ledger:write");
        ResponseEntity<ProblemDetail> badIban = restTemplate.exchange(
                "/api/v1/accounts",
                HttpMethod.POST,
                jsonEntity(rwToken, null, new CreateAccountRequest("EUR", "NL00TEST0000000001", AccountType.CUSTOMER, 0L)),
                ProblemDetail.class);
        assertThat(badIban.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(badIban.getBody()).isNotNull();
        assertThat(badIban.getBody().getDetail()).contains("mod-97").doesNotContain("Exception");

        // 5. Missing resource -> 404 Not Found; missing header & invalid limit -> 400 Bad Request
        ResponseEntity<ProblemDetail> notFound = restTemplate.exchange(
                "/api/v1/accounts/" + UUID.randomUUID(),
                HttpMethod.GET,
                jsonEntity(readOnlyToken, null, null),
                ProblemDetail.class);
        assertThat(notFound.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

        ResponseEntity<ProblemDetail> badLimit = restTemplate.exchange(
                "/api/v1/accounts/" + UUID.randomUUID() + "/statement?limit=0",
                HttpMethod.GET,
                jsonEntity(readOnlyToken, null, null),
                ProblemDetail.class);
        assertThat(badLimit.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        Account a1 = restTemplate.exchange(
                "/api/v1/accounts",
                HttpMethod.POST,
                jsonEntity(rwToken, null, new CreateAccountRequest("EUR", null, AccountType.CUSTOMER, 0L)),
                Account.class).getBody();
        Account a2 = restTemplate.exchange(
                "/api/v1/accounts",
                HttpMethod.POST,
                jsonEntity(rwToken, null, new CreateAccountRequest("EUR", null, AccountType.CUSTOMER, 0L)),
                Account.class).getBody();

        ResponseEntity<ProblemDetail> missingHeader = restTemplate.exchange(
                "/api/v1/transfers",
                HttpMethod.POST,
                jsonEntity(rwToken, null, new CreateTransferRequest(a1.id(), a2.id(), 100L, "EUR", "x")),
                ProblemDetail.class);
        assertThat(missingHeader.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        ResponseEntity<ProblemDetail> insufficient = restTemplate.exchange(
                "/api/v1/transfers",
                HttpMethod.POST,
                jsonEntity(rwToken, "insuff-1", new CreateTransferRequest(a1.id(), a2.id(), 100L, "EUR", "x")),
                ProblemDetail.class);
        assertThat(insufficient.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
    }

    @Test
    void executesTransfersIdempotentReplaysAndAtMostOnceReversalsOverHttp() {
        String token = mintJwt("client-rev", "ledger:read ledger:write");

        Account funding = restTemplate.exchange(
                "/api/v1/accounts",
                HttpMethod.POST,
                jsonEntity(token, null, new CreateAccountRequest("EUR", null, AccountType.SYSTEM, 0L)),
                Account.class).getBody();

        Account alice = restTemplate.exchange(
                "/api/v1/accounts",
                HttpMethod.POST,
                jsonEntity(token, null, new CreateAccountRequest(
                        "EUR", Iban.generateSynthetic("NL", 901L).value(), AccountType.CUSTOMER, 0L)),
                Account.class).getBody();

        Account bob = restTemplate.exchange(
                "/api/v1/accounts",
                HttpMethod.POST,
                jsonEntity(token, null, new CreateAccountRequest(
                        "EUR", Iban.generateSynthetic("DE", 902L).value(), AccountType.CUSTOMER, 0L)),
                Account.class).getBody();

        // Fund Alice with 20,000 minor units
        restTemplate.exchange(
                "/api/v1/transfers",
                HttpMethod.POST,
                jsonEntity(token, "fund-alice-1", new CreateTransferRequest(
                        funding.id(), alice.id(), 20_000L, "EUR", "Initial funding")),
                Transfer.class);

        // Alice -> Bob 7,500 minor units (201 Created)
        CreateTransferRequest payReq = new CreateTransferRequest(alice.id(), bob.id(), 7_500L, "EUR", "Order 55");
        ResponseEntity<Transfer> created = restTemplate.exchange(
                "/api/v1/transfers", HttpMethod.POST, jsonEntity(token, "pay-55", payReq), Transfer.class);
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        // GET /api/v1/transfers/{id} -> 200 OK
        ResponseEntity<Transfer> fetched = restTemplate.exchange(
                "/api/v1/transfers/" + created.getBody().id(),
                HttpMethod.GET,
                jsonEntity(token, null, null),
                Transfer.class);
        assertThat(fetched.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Same key + same payload -> 200 OK replay
        ResponseEntity<Transfer> replayed = restTemplate.exchange(
                "/api/v1/transfers", HttpMethod.POST, jsonEntity(token, "pay-55", payReq), Transfer.class);
        assertThat(replayed.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(replayed.getBody().id()).isEqualTo(created.getBody().id());

        // Same key + different amount -> 422 Unprocessable Entity
        CreateTransferRequest tampered = new CreateTransferRequest(alice.id(), bob.id(), 9_999L, "EUR", "Order 55");
        ResponseEntity<ProblemDetail> mismatch = restTemplate.exchange(
                "/api/v1/transfers", HttpMethod.POST, jsonEntity(token, "pay-55", tampered), ProblemDetail.class);
        assertThat(mismatch.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);

        // First reversal -> 201 Created, balances restored (Alice=20,000, Bob=0)
        ResponseEntity<Transfer> rev1 = restTemplate.exchange(
                "/api/v1/transfers/" + created.getBody().id() + "/reversals",
                HttpMethod.POST,
                jsonEntity(token, "rev-key-1", null),
                Transfer.class);
        assertThat(rev1.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(rev1.getBody().reversesTransferId()).isEqualTo(created.getBody().id());

        // Idempotent replay of the exact same reversal key -> 200 OK
        ResponseEntity<Transfer> rev1Replay = restTemplate.exchange(
                "/api/v1/transfers/" + created.getBody().id() + "/reversals",
                HttpMethod.POST,
                jsonEntity(token, "rev-key-1", null),
                Transfer.class);
        assertThat(rev1Replay.getStatusCode()).isEqualTo(HttpStatus.OK);

        Account aliceAfterRev = restTemplate.exchange(
                "/api/v1/accounts/" + alice.id(), HttpMethod.GET, jsonEntity(token, null, null), Account.class).getBody();
        Account bobAfterRev = restTemplate.exchange(
                "/api/v1/accounts/" + bob.id(), HttpMethod.GET, jsonEntity(token, null, null), Account.class).getBody();
        assertThat(aliceAfterRev.balanceMinor()).isEqualTo(20_000L);
        assertThat(bobAfterRev.balanceMinor()).isZero();

        // Second reversal with a new key -> 409 Conflict
        ResponseEntity<ProblemDetail> rev2 = restTemplate.exchange(
                "/api/v1/transfers/" + created.getBody().id() + "/reversals",
                HttpMethod.POST,
                jsonEntity(token, "rev-key-2", null),
                ProblemDetail.class);
        assertThat(rev2.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(rev2.getBody().getDetail()).contains("already been reversed");
    }

    @Test
    void keysetPaginatesTenThousandStatementEntriesWithoutGapsOrDuplicates() {
        String token = mintJwt("client-stmt", "ledger:read ledger:write");

        Account funding = restTemplate.exchange(
                "/api/v1/accounts",
                HttpMethod.POST,
                jsonEntity(token, null, new CreateAccountRequest("EUR", null, AccountType.SYSTEM, 0L)),
                Account.class).getBody();

        Account target = restTemplate.exchange(
                "/api/v1/accounts",
                HttpMethod.POST,
                jsonEntity(token, null, new CreateAccountRequest("EUR", null, AccountType.CUSTOMER, 0L)),
                Account.class).getBody();

        // Batch-insert 10,000 balanced transfers touching target (= 10,000 entries on target account)
        int totalEntries = 10_000;
        transactionTemplate.executeWithoutResult(status -> {
            Timestamp now = Timestamp.from(Instant.now());
            List<Object[]> transferBatch = new ArrayList<>(totalEntries);
            List<Object[]> entryBatch = new ArrayList<>(totalEntries * 2);
            for (int i = 1; i <= totalEntries; i++) {
                UUID tid = UUID.randomUUID();
                transferBatch.add(new Object[] {
                    tid, "stmt-seed", "stmt-k-" + i, "COMPLETED", "EUR", 10L, funding.id(), target.id(), now
                });
                entryBatch.add(new Object[] {tid, funding.id(), -10L, "EUR", now});
                entryBatch.add(new Object[] {tid, target.id(), 10L, "EUR", now});
            }
            jdbcTemplate.batchUpdate(
                    """
                    INSERT INTO transfers (id, client_id, idempotency_key, status, currency, amount_minor, from_account_id, to_account_id, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    transferBatch);
            jdbcTemplate.batchUpdate(
                    """
                    INSERT INTO entries (transfer_id, account_id, amount_minor, currency, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    entryBatch);
            jdbcTemplate.update(
                    "UPDATE accounts SET balance_minor = ? WHERE id = ?", totalEntries * 10L, target.id());
            jdbcTemplate.update(
                    "UPDATE accounts SET balance_minor = ? WHERE id = ?", -(totalEntries * 10L), funding.id());
        });

        // Walk all 20 pages (limit=500) over HTTP using keyset cursor
        Set<Long> seenIds = new HashSet<>(totalEntries);
        long previousId = Long.MAX_VALUE;
        long totalSum = 0L;
        Long cursor = null;
        int pages = 0;

        while (true) {
            String url = "/api/v1/accounts/" + target.id() + "/statement?limit=500"
                    + (cursor != null ? "&cursor=" + cursor : "");
            ResponseEntity<StatementPage> resp = restTemplate.exchange(
                    url, HttpMethod.GET, jsonEntity(token, null, null), StatementPage.class);
            assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
            StatementPage page = resp.getBody();
            assertThat(page).isNotNull();
            pages++;

            for (Entry entry : page.items()) {
                assertThat(entry.id()).isLessThan(previousId);
                previousId = entry.id();
                assertThat(seenIds.add(entry.id())).isTrue();
                totalSum += entry.amountMinor();
            }

            if (!page.hasMore()) {
                break;
            }
            cursor = page.nextCursor();
        }

        assertThat(pages).isEqualTo(20);
        assertThat(seenIds).hasSize(10_000);
        assertThat(totalSum).isEqualTo(100_000L);
    }

    private String mintJwt(String subject, String scope) {
        SecretKey key = new SecretKeySpec(jwtSecret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
        NimbusJwtEncoder encoder = new NimbusJwtEncoder(new ImmutableSecret<>(key));
        Instant now = Instant.now();
        JwsHeader header = JwsHeader.with(MacAlgorithm.HS256).build();
        JwtClaimsSet claims = JwtClaimsSet.builder()
                .subject(subject)
                .issuedAt(now)
                .expiresAt(now.plusSeconds(3600))
                .claim("scope", scope)
                .build();
        return encoder.encode(JwtEncoderParameters.from(header, claims)).getTokenValue();
    }

    private static <T> HttpEntity<T> jsonEntity(String bearerToken, String idempotencyKey, T body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (bearerToken != null) {
            headers.setBearerAuth(bearerToken);
        }
        if (idempotencyKey != null) {
            headers.set("Idempotency-Key", idempotencyKey);
        }
        return new HttpEntity<>(body, headers);
    }
}
