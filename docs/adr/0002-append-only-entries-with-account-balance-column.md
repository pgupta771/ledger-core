# ADR-0002: Append-Only Entries with Materialized Account Balance and Deferred Triggers

## Context
A double-entry ledger requires an immutable audit trail of every debit and credit while simultaneously enforcing non-negative customer balances under high concurrency without scanning historical entries on every transfer.

## Decision
We store immutable journal lines in `entries` protected by a `BEFORE UPDATE OR DELETE` trigger (`trg_entries_append_only`) and a `DEFERRABLE INITIALLY DEFERRED` constraint trigger (`trg_entries_balanced_deferred`) that verifies `COUNT(*) >= 2` and `SUM(amount_minor) = 0` per `transfer_id` at transaction commit. Simultaneously, we maintain the current balance in `accounts.balance_minor` guarded by `CHECK (allow_negative OR balance_minor >= 0)` and updated within the same ACID transaction as the `entries` insert.

## Consequences
Point-in-time balance checks and overdraft enforcement (`SELECT ... FOR UPDATE` on `accounts`) execute in $O(1)$ index lookups rather than $O(N)$ aggregate scans over `entries`, while the database independently guarantees entry immutability and zero-sum transfer balance at commit time.
