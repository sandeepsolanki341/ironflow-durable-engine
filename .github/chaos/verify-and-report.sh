#!/usr/bin/env bash
# =============================================================================================
# Runs the invariant verification SQL, parses the one-row verdict, emits:
#   - chaos-badge.json      : shields.io endpoint format, for the README badge
#   - chaos-summary.json    : full metrics, for the CI job summary and artifacts
# Exit code is 0 iff every invariant held. The GitHub Actions job fails on non-zero, which is
# what turns the badge red - the badge and the build status can never disagree.
# =============================================================================================
set -euo pipefail

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-ironflow}"
PGDATABASE="${PGDATABASE:-ironflow}"
export PGPASSWORD="${PGPASSWORD:-ironflow}"
SQL_FILE="${SQL_FILE:-$(dirname "$0")/verify-invariants.sql}"
OUT_DIR="${OUT_DIR:-.}"

# Run the verification, tab-separated, tuples only, so parsing is trivial and locale-proof.
ROW="$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
        -At -F $'\t' -f "$SQL_FILE" | tail -n 1)"

IFS=$'\t' read -r total lost dup_side dup_comp stranded incomplete completed rolled passing <<< "$ROW"

echo "Verification row: total=$total lost=$lost dup_side=$dup_side dup_comp=$dup_comp stranded=$stranded incomplete=$incomplete completed=$completed rolled_back=$rolled passing=$passing"

# shields.io endpoint schema. schemaVersion 1 is required; color + message drive the badge.
if [ "$passing" = "t" ]; then
  COLOR="brightgreen"; MESSAGE="passing"; EXIT=0
else
  COLOR="red"; MESSAGE="failing"; EXIT=1
fi

cat > "$OUT_DIR/chaos-badge.json" <<JSON
{
  "schemaVersion": 1,
  "label": "chaos",
  "message": "$MESSAGE",
  "color": "$COLOR"
}
JSON

# Rich summary for artifacts / job summary.
cat > "$OUT_DIR/chaos-summary.json" <<JSON
{
  "passing": $( [ "$passing" = "t" ] && echo true || echo false ),
  "generatedAt": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "totals": {
    "executions": ${total:-0},
    "completed": ${completed:-0},
    "rolledBack": ${rolled:-0}
  },
  "violations": {
    "lostTransitions": ${lost:-0},
    "duplicatedSideEffects": ${dup_side:-0},
    "duplicatedCompensations": ${dup_comp:-0},
    "strandedExecutions": ${stranded:-0},
    "incompleteRollbacks": ${incomplete:-0}
  }
}
JSON

echo "Wrote $OUT_DIR/chaos-badge.json and $OUT_DIR/chaos-summary.json"
exit $EXIT
