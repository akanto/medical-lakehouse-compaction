#!/usr/bin/env bash
# Run the given rate rows sequentially, each a fresh JVM, halting on failure.
# Reusable restart helper: after diagnosing a failed row, relaunch the
# remaining rows with e.g.  flock -n results/chain.lock bash scripts/run_rows.sh 2 1
# (Unlike chain_rows.sh it has NO rate-5 handoff gate, so it is safe to start
# at any point once the previous supervisor has exited.)
set -uo pipefail
cd "$HOME/hybrid-cloud-medical-imaging" || exit 3
LOG=results/chain_supervisor.log
STATUS=results/chain_status
ts(){ date -u +%Y-%m-%dT%H:%M:%SZ; }
say(){ echo "[$(ts)] $*" | tee -a "$LOG"; }

[ "$#" -ge 1 ] || { echo "usage: run_rows.sh RATE [RATE...]" >&2; exit 64; }
say "run_rows start (pid $$); rows: $*"
for RATE in "$@"; do
  PROF="conf/profiles/experiment_rate${RATE}.yaml"
  sed "s/^rate_gbit:.*/rate_gbit: [${RATE}]/" conf/profiles/experiment.yaml > "$PROF"
  grep -q "^rate_gbit: \[${RATE}\]$" "$PROF" || { say "HALT: bad profile $PROF"; echo "FAILED:profile-${RATE}" > "$STATUS"; exit 2; }
  echo "RUNNING:rate${RATE}" > "$STATUS"
  say "launching rate ${RATE}"
  RSTART=$(date +%s)
  .venv/bin/python scripts/run_experiment.py --profile "$PROF" --output-dir results > "results/experiment_${RATE}.log" 2>&1
  RC=$?
  say "rate ${RATE} rc=${RC} after $(( ($(date +%s)-RSTART)/60 )) min"
  [ "$RC" -ne 0 ] && { say "HALT: rate ${RATE} rc=${RC}"; echo "FAILED:rate${RATE}-rc${RC}" > "$STATUS"; exit "$RC"; }
  OUT=$(find results -maxdepth 1 -name 'benchmark_*.json' -newermt "@$RSTART" 2>/dev/null | head -1)
  [ -z "$OUT" ] && { say "HALT: rate ${RATE} no output"; echo "FAILED:rate${RATE}-no-output" > "$STATUS"; exit 4; }
  say "rate ${RATE} final: $OUT"
done
say "ALLROWS DONE (rows: $*)"; echo DONE > "$STATUS"
