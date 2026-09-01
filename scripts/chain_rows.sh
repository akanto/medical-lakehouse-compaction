#!/usr/bin/env bash
# Server-side rate-row chain for the 200-series WAN grid.
#
# Drives the remaining fresh-JVM rate rows (2, then 1) back-to-back after the
# currently in-flight rate-5 row finishes, so the fleet never idles at a row
# boundary even if the operator's laptop session dies. Each row is a fresh
# python -> JVM process (native memory is reset on exit; see the kernel-OOM
# history in docs/benchmark-runbook.md and conf/profiles/experiment.yaml).
#
# Failure handling: apply_tsg_level() in run_experiment.py uses check=True, so a
# hard shaping/SSH failure raises and the row exits non-zero. On any non-zero
# row (or a missing final JSON) the chain HALTS — it does NOT advance to the
# next row — and records the reason in results/chain_status. That is the
# "halt on network-config failure" contract: we never burn grid hours behind a
# broken gateway. All planned cells of a row that starts always run to
# completion (no autonomous truncation of a healthy row).
#
# Correctness lives here on the server. Local monitoring is best-effort
# notification only.
set -uo pipefail
cd "$HOME/hybrid-cloud-medical-imaging" || exit 3

LOG=results/chain_supervisor.log
STATUS=results/chain_status
ts()  { date -u +%Y-%m-%dT%H:%M:%SZ; }
say() { echo "[$(ts)] $*" | tee -a "$LOG"; }

START=$(date +%s)
echo RUNNING > "$STATUS"
say "supervisor start (pid $$); remaining rows: 2 1"

# 1) Wait for the in-flight rate-5 row to exit.
say "waiting for in-flight run_experiment.py (rate-5) to exit"
while pgrep -f "scripts/run_experiment.py" >/dev/null 2>&1; do sleep 30; done
say "no run_experiment.py running; letting the JVM release native memory"
sleep 20

# 2) Boundary success gate: rate-5 must have produced a fresh final JSON.
NEWJSON=$(find results -maxdepth 1 -name 'benchmark_*.json' -newermt "@$START" 2>/dev/null | head -1)
if [ -z "$NEWJSON" ]; then
  say "CHAIN-HALT: no new results/benchmark_*.json since start -> rate-5 did not complete cleanly"
  echo "FAILED:rate5-no-output" > "$STATUS"
  exit 1
fi
say "rate-5 final present: $NEWJSON"

# 3) Rows 2 then 1 — fresh JVM each; halt the chain on any failure.
for RATE in 2 1; do
  PROF="conf/profiles/experiment_rate${RATE}.yaml"
  sed "s/^rate_gbit:.*/rate_gbit: [${RATE}]/" conf/profiles/experiment.yaml > "$PROF"
  if ! grep -q "^rate_gbit: \[${RATE}\]$" "$PROF"; then
    say "CHAIN-HALT: failed to write per-row profile $PROF"
    echo "FAILED:profile-${RATE}" > "$STATUS"; exit 2
  fi
  echo "RUNNING:rate${RATE}" > "$STATUS"
  say "launching rate ${RATE} (profile $PROF)"
  RSTART=$(date +%s)
  .venv/bin/python scripts/run_experiment.py --profile "$PROF" \
      --output-dir results > "results/experiment_${RATE}.log" 2>&1
  RC=$?
  say "rate ${RATE} exited rc=${RC} after $(( ($(date +%s)-RSTART)/60 )) min"
  if [ "$RC" -ne 0 ]; then
    say "CHAIN-HALT: rate ${RATE} failed rc=${RC}; NOT continuing to further rows"
    echo "FAILED:rate${RATE}-rc${RC}" > "$STATUS"; exit "$RC"
  fi
  OUT=$(find results -maxdepth 1 -name 'benchmark_*.json' -newermt "@$RSTART" 2>/dev/null | head -1)
  if [ -z "$OUT" ]; then
    say "CHAIN-HALT: rate ${RATE} rc=0 but no new final JSON; treating as failure"
    echo "FAILED:rate${RATE}-no-output" > "$STATUS"; exit 4
  fi
  say "rate ${RATE} final present: $OUT"
done

say "ALLROWS DONE (rows 2 and 1 complete)"
echo "DONE" > "$STATUS"
