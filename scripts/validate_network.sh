#!/usr/bin/env bash
# Network path validation — run FROM the Spark instance against MinIO.
#
# Usage: ./validate_network.sh <minio_private_ip> [results_dir]
#
# Records reviewer-defensible evidence for the current shaping level:
# - ping RTT (nominal vs observed; netem delay is additive on the substrate)
# - iperf3 throughput with 4 parallel streams (AWS caps single flows at
#   ~5 Gb/s outside cluster placement groups; S3A is multi-connection, so
#   parallel streams reflect what the workloads can actually get)
# - tracepath showing the TSG hop (proof traffic transits the gateway)
# - endpoint TCP buffer sysctls (left at OS defaults, but recorded)
# Requires `iperf3 -s` running on the MinIO host (make minio-deploy does this).

set -euo pipefail

MINIO_IP="${1:?Usage: $0 <minio_private_ip> [results_dir]}"
RESULTS_DIR="${2:-results}"

mkdir -p "$RESULTS_DIR"
TS=$(date +%Y%m%d_%H%M%S)
LOG="$RESULTS_DIR/network_validation_${TS}.log"

{
  echo "=== Network validation $TS ==="
  echo "--- ping $MINIO_IP (20 probes) ---"
  ping -c 20 -q "$MINIO_IP"

  echo "--- tracepath $MINIO_IP (TSG hop proof) ---"
  tracepath -n -m 5 "$MINIO_IP" || true

  echo "--- iperf3 -> $MINIO_IP (10 s, 4 parallel streams) ---"
  iperf3 -c "$MINIO_IP" -t 10 -P 4 --json

  echo "--- TCP buffer sysctls (OS defaults, recorded for the paper) ---"
  sysctl net.ipv4.tcp_rmem net.ipv4.tcp_wmem net.core.rmem_max net.core.wmem_max
} 2>&1 | tee "$LOG"

echo
echo "Saved to $LOG"
