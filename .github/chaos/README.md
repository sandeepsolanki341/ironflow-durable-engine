# IronFlow nightly chaos suite

Proves failure-proof correctness *by construction*: flood an ephemeral cluster with 10,000
multi-step transactional workflows, hard-kill random workers every 30 seconds while inducing
network partitions, let everything settle, then assert three invariants in SQL. A green
`chaos: passing` badge means the last nightly run survived all of it with zero correctness loss.

## Files

| File | Role |
|------|------|
| `../workflows/nightly-chaos.yml` | The scheduled GitHub Actions job that orchestrates everything. Runs at 00:00 UTC nightly. |
| `verify-invariants.sql` | The proof. One query, one verdict row. Checks the three invariants against the real schema. |
| `verify-and-report.sh` | Runs the SQL, parses the verdict, writes the badge + summary JSON, sets the exit code. |
| `chaos-monkey.sh` | Hard-kills (`docker kill --signal=SIGKILL`) a random engine replica every 30s and partitions another. |
| `chaos-load.js` | k6 generator that floods exactly 10,000 workflows. |
| `../../docker-compose.chaos.yml` | Override that scales the engine to 3 killable replicas. |

## The three invariants

1. **Zero lost state transitions.** Per execution, `sequence_number` is gap-free `1..max` and
   `next_sequence = max + 1`. A gap is a transition the engine reserved a slot for but never
   committed — precisely what a `kill -9` mid-commit would produce in a broken engine.

2. **Zero duplicated activity side effects.** Each scheduled activity task yields at most one
   `ACTIVITY_COMPLETED` (correlated by the `taskId` in the event payload), and each saga
   compensation completes at most once (by `registrationSeq`). A duplicate is the
   at-least-once-becomes-at-least-twice failure: a worker dies after the side effect but before
   the ack, and recovery double-runs it. The version-fence CAS is designed to prevent this; the
   SQL verifies the fence held under chaos.

3. **100% completion or clean saga rollback.** After settling, no execution is left in a live
   state (`RUNNING`, `COMPENSATING`, `DIVERGENT`), and every `FAILED_COMPENSATED` execution ran
   all of its registered compensations. A stranded live execution is a workflow the chaos lost —
   the exact failure durable execution exists to make impossible.

## Why this is a real proof and not a smoke test

The verdict is a single boolean derived from set-based SQL over the entire event history, not a
sampled check. The badge and the CI status cannot disagree: `verify-and-report.sh` exits
non-zero on any violation, which fails the job, which turns the badge red. There is no path where
a violated invariant leaves a green badge.

## Running it locally

```bash
docker compose -f docker-compose.yml -f docker-compose.chaos.yml up -d --build --scale engine=3
# flood + chaos (in two terminals, or background the load):
k6 run -e TOTAL_WORKFLOWS=10000 .github/chaos/chaos-load.js &
ENGINE_SERVICE=engine ENGINE_REPLICAS=3 \
  COMPOSE_CMD="docker compose -f docker-compose.yml -f docker-compose.chaos.yml" \
  bash .github/chaos/chaos-monkey.sh
# after settling:
OUT_DIR=. bash .github/chaos/verify-and-report.sh && echo PASS || echo FAIL
```
