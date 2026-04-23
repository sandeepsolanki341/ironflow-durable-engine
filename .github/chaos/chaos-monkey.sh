#!/usr/bin/env bash
# =============================================================================================
# Chaos Monkey. Every INTERVAL seconds, hard-kills (kill -9 equivalent: `docker kill`) one random
# engine replica and briefly severs another's network, then lets it heal. Runs for DURATION, then
# ensures every replica is back up so the system can settle before verification.
#
# WHY docker kill AND NOT docker stop
#   docker stop sends SIGTERM and waits - a graceful shutdown, which is NOT what proves crash
#   safety. docker kill sends SIGKILL immediately: the process dies mid-transaction with no
#   chance to flush, ack, or clean up. That is the failure durable execution must survive, so it
#   is the only kind worth injecting here.
#
# WHY THIS IS SAFE TO DO REPEATEDLY
#   The engine's correctness rests on Postgres, not on any worker's memory. A killed worker loses
#   nothing durable: its leases expire, the reaper reclaims them, another worker replays from
#   history. The whole point of the exercise is that killing workers at random should be a
#   non-event for correctness - only for latency.
# =============================================================================================
set -uo pipefail

INTERVAL="${CHAOS_INTERVAL:-30}"
DURATION="${CHAOS_DURATION:-300}"
ENGINE_SERVICE="${ENGINE_SERVICE:-engine}"
COMPOSE="${COMPOSE_CMD:-docker compose}"

echo "[chaos] starting: interval=${INTERVAL}s duration=${DURATION}s target=${ENGINE_SERVICE}"
END=$(( $(date +%s) + DURATION ))

engine_ids() {
  $COMPOSE ps -q "$ENGINE_SERVICE"
}

while [ "$(date +%s)" -lt "$END" ]; do
  mapfile -t IDS < <(engine_ids)
  COUNT=${#IDS[@]}
  if [ "$COUNT" -eq 0 ]; then
    echo "[chaos] no engine containers found; waiting"
    sleep "$INTERVAL"; continue
  fi

  # --- Hard kill a random replica ---
  VICTIM="${IDS[$((RANDOM % COUNT))]}"
  echo "[chaos] $(date -u +%T) kill -9 -> ${VICTIM:0:12} (of $COUNT replicas)"
  docker kill --signal=SIGKILL "$VICTIM" >/dev/null 2>&1 || echo "[chaos] kill failed (already down?)"

  # --- Network dropout on a DIFFERENT replica, if one exists ---
  if [ "$COUNT" -gt 1 ]; then
    # pick a second, distinct victim
    while :; do
      NETVICTIM="${IDS[$((RANDOM % COUNT))]}"
      [ "$NETVICTIM" != "$VICTIM" ] && break
    done
    echo "[chaos] $(date -u +%T) network drop -> ${NETVICTIM:0:12} for 5s"
    # Disconnect from the compose network, then reconnect after a short outage. This simulates a
    # partition: the worker is alive but cannot reach Postgres, so its leases lapse and its
    # in-flight commits fail - a different failure surface than a clean kill.
    NET="$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$NETVICTIM" 2>/dev/null | head -1)"
    if [ -n "$NET" ]; then
      docker network disconnect "$NET" "$NETVICTIM" >/dev/null 2>&1 || true
      sleep 5
      docker network connect "$NET" "$NETVICTIM" >/dev/null 2>&1 || true
    fi
  fi

  # --- Bring the killed replica back so the herd doesn't dwindle to zero ---
  echo "[chaos] restarting downed replicas"
  $COMPOSE up -d --no-recreate --scale "${ENGINE_SERVICE}=${ENGINE_REPLICAS:-3}" "$ENGINE_SERVICE" >/dev/null 2>&1 || true

  sleep "$INTERVAL"
done

# Heal: guarantee full replica count is up before we stop injecting faults, so the system has
# healthy workers to drain the backlog before verification runs.
echo "[chaos] duration elapsed; restoring full cluster"
$COMPOSE up -d --scale "${ENGINE_SERVICE}=${ENGINE_REPLICAS:-3}" "$ENGINE_SERVICE" >/dev/null 2>&1 || true
echo "[chaos] done"
