# Double-Entry Payments Ledger (`ledger-core`)

> **Disclaimer:** This repository is a technical demonstration system operating exclusively on synthetic test data. It is not a licensed payment institution or production banking rail.

Java 21 / Spring Boot 3.3 double-entry ledger service backed by PostgreSQL 16 and Apache Kafka with database-enforced zero-sum and append-only invariants, idempotent transfer APIs, interchangeable concurrency strategies (pessimistic lock ordering vs. `SERIALIZABLE` retries), a transactional outbox, and a deduplicating balance projector.

## Milestone Status
- [x] **L0:** Multi-module Maven skeleton, Flyway schema, Docker Compose stack, Testcontainers smoke ITs.
- [ ] **L1:** Domain model (`Money`, `Currency`, `Iban`, `Account`, `Transfer`, `Entry`) and PostgreSQL constraint/trigger verification.
- [ ] **L2:** Pessimistic transfer engine and `jqwik` property-based tests.
- [ ] **L3:** `Idempotency-Key` state machine and 50-thread idempotency storm test.
- [ ] **L4:** `SERIALIZABLE` strategy, 100-thread / 20,000-transfer concurrency proof, and contention benchmarks.
- [ ] **L5:** Transactional outbox (`FOR UPDATE SKIP LOCKED`), Kafka publisher, and deduplicating projector service.
- [ ] **L6:** REST API, OAuth2/JWT scopes, reversals, keyset-paginated statements, and RFC 9457 error contracts.
- [ ] **L7:** Prometheus metrics, ADRs, benchmark scripts, and final README.
