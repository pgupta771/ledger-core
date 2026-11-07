#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[load-test] Running 50-thread Idempotency Storm (20 rounds) + 100-thread 20,000-transfer Concurrency Proof..."
mvn test-compile failsafe:integration-test failsafe:verify \
  -Dit.test=L3IdempotencyStormIT,L4ConcurrencyProofIT \
  -pl ledger-service
echo "[load-test] All concurrency and idempotency invariants passed!"
