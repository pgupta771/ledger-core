# ADR-0006: Explicit JDBC and SQL Over JPA/Hibernate on the Transfer Hot Path

## Context
The transfer execution path requires deterministic lock acquisition order (`SELECT ... FOR UPDATE` sorted by ascending account UUID), explicit transaction isolation control (`READ COMMITTED` pessimistic vs. `SERIALIZABLE`), and exact inspection of PostgreSQL `SQLSTATE` codes (`40001`, `40P01`, `23505`, `23514`).

## Decision
We implement all account locking, transfer/entry insertion, idempotency state transitions, and outbox writes using Spring `JdbcTemplate` with explicit parameterized SQL rather than JPA/Hibernate entities.

## Consequences
Lock acquisition order is 100% explicit (two sequential `SELECT ... WHERE id = ? FOR UPDATE` calls in ascending `UUID` order, avoiding index-scan lock reordering), there is zero first-level persistence-context staleness when retrying `SERIALIZABLE` transactions, and batch/concurrent throughput has minimal ORM overhead.
