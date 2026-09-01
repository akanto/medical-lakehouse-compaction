#!/usr/bin/env bash
# Per-node baseline bandwidth cap — run on the Spark and MinIO instances.
#
# Usage: sudo ./setup_node_cap.sh [rate_gbit]   (default 10)
#
# Pins each endpoint to its baseline (non-burst) network rate, neutralizing
# the EC2 burst-credit mechanism (c5n.2xlarge: 10 Gb/s baseline, 25 Gb/s
# burst) so the path ceiling is stable instead of a decaying credit window.
# Same HTB class shape as the TSG, without the netem leaf.

set -euo pipefail

RATE_GBIT="${1:-10}"
IFACE=$(ip route show default | awk '{print $5; exit}')

modprobe sch_htb 2>/dev/null || true
tc qdisc del dev "$IFACE" root 2>/dev/null || true
tc qdisc add dev "$IFACE" root handle 1: htb default 11
tc class add dev "$IFACE" parent 1: classid 1:11 htb \
  rate "${RATE_GBIT}Gbit" ceil "${RATE_GBIT}Gbit" burst 1mb cburst 500kb mtu 9001

echo "Node cap on $IFACE: ${RATE_GBIT}Gbit"
tc class show dev "$IFACE"
