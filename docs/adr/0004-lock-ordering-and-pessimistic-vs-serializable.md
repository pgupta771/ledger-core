# ADR-0004: Canonical Lock Ordering (`SELECT FOR UPDATE`) vs. `SERIALIZABLE` Retries

## Context
Concurrent transfers touching overlapping accounts risk lost updates, negative balances (write skew), and AB-BA deadlocks (e.g., concurrent `A -> B` and `B -> A` transfers).

## Decision
We implement two runtime-selectable concurrency strategies (`ledger.concurrency=pessimistic|serializable`):
1. **Pessimistic (`READ COMMITTED` + Canonical `SELECT ... FOR UPDATE`):** Acquires row locks sequentially in ascending account `UUID` order (`lowId` then `highId`). Because lock acquisition order forms a strict total order across all accounts, AB-BA deadlocks cannot occur.
2. **Serializable (`ISOLATION_SERIALIZABLE` + Bounded Jittered Retry):** Executes without explicit `FOR UPDATE` locks and retries inside a fresh `REQUIRES_NEW` transaction on PostgreSQL `SQLSTATE 40001` (`serialization_failure`) or `40P01` (`deadlock_detected`).

## Measured Results
*Environment: Intel(R) Core(TM) i3-1005G1 CPU @ 1.20GHz (4 vCPUs, 4.2Gi RAM, WSL2 Ubuntu, openjdk version 21.0.12.1 2026-08-18), Testcontainers PostgreSQL 16, 32 concurrent threads, 2,000 transfers per scenario.*

| Workload Profile | Strategy | Throughput | Elapsed Time | DB Retries (`40001`/`40P01`) |
| :--- | :--- | ---: | ---: | ---: |
| **Uniform (50 accounts)** | `pessimistic` (`SELECT FOR UPDATE`) | **283 tx/s** | 7061 ms | **0** |
| **Uniform (50 accounts)** | `serializable` (SSI + retry) | **266 tx/s** | 7514 ms | **1185** |
| **Hot-Account (1 hub + 19 spokes)** | `pessimistic` (`SELECT FOR UPDATE`) | **211 tx/s** | 9462 ms | **0** |
| **Hot-Account (1 hub + 19 spokes)** | `serializable` (SSI + retry) | **29 tx/s** | 67351 ms | **16800** |

## Consequences
Under uniform contention both strategies achieve comparable correctness, but under hot-account contention (e.g., merchant settlement or funding accounts), pessimistic row locking in canonical `UUID` order avoids wasted abort-and-retry round trips (`0` retries) and delivers substantially higher sustained throughput, making `pessimistic` our default production setting.
