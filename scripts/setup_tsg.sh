#!/usr/bin/env bash
# Traffic Shaping Gateway — HTB bandwidth cap + netem delay, run ON the TSG
# instance (the dedicated gateway that forwards all Spark<->MinIO traffic).
#
# Usage:   sudo ./setup_tsg.sh --rate <gbit|none> --rtt <ms>
# Examples:
#   sudo ./setup_tsg.sh --rate 1 --rtt 25      # 1 Gb/s cap, 25 ms nominal RTT
#   sudo ./setup_tsg.sh --rate none --rtt 0    # clear all shaping
#
# Design notes (methodology from the IEEE Access TSG work):
# - HTB handles the rate cap, netem handles delay ONLY. netem's own rate
#   limiting proved unstable in prior experiments — never use it.
# - RTT/2: the TSG has one ENI and every packet of BOTH directions egresses it
#   exactly once, so an egress delay D adds 2*D to the round trip. We take the
#   nominal RTT and apply RTT/2 (e.g. --rtt 25 -> netem delay 12.5ms).
#   --rtt 0 applies no netem delay (substrate baseline; measure it with ping).
# - The HTB class caps the SUM of both directions through the ENI. Acceptable:
#   benchmark traffic is heavily MinIO->Spark asymmetric, so the cap
#   effectively binds the data direction.
# - netem limit 100000: delayed packets sit in netem's queue; the default
#   limit (1000) drop-tails part of the TCP window at every tier.
# - mtu 9001 on the HTB class: explicit jumbo-frame accounting (intra-VPC MTU
#   is 9001 and stays that way — AWS Direct Connect supports jumbo frames, so
#   this is WAN-realistic; offloads also stay enabled).

set -euo pipefail

RATE=""
RTT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --rate) RATE="$2"; shift 2 ;;
    --rtt)  RTT="$2";  shift 2 ;;
    *) echo "Unknown argument: $1" >&2
       echo "Usage: sudo $0 --rate <gbit|none> --rtt <ms>" >&2
       exit 1 ;;
  esac
done

[ -n "$RATE" ] && [ -n "$RTT" ] || {
  echo "Usage: sudo $0 --rate <gbit|none> --rtt <ms>" >&2
  exit 1
}

# Single ENI on a Nitro instance (typically ens5); take it from the default route.
IFACE=$(ip route show default | awk '{print $5; exit}')

modprobe sch_htb 2>/dev/null || true
tc qdisc del dev "$IFACE" root 2>/dev/null || true

if [ "$RATE" = "none" ] && [ "$RTT" = "0" ]; then
  echo "Cleared all shaping on $IFACE"
  tc qdisc show dev "$IFACE"
  exit 0
fi

# "none" -> effectively uncapped placeholder class well above the path
# baseline; the per-node 10 Gbit caps on Spark/MinIO then govern the ceiling.
if [ "$RATE" = "none" ]; then
  RATE_SPEC="25Gbit"
else
  RATE_SPEC="${RATE}Gbit"
fi

HALF_RTT=$(awk "BEGIN {print $RTT / 2}")

tc qdisc add dev "$IFACE" root handle 1: htb default 11
tc class add dev "$IFACE" parent 1: classid 1:11 htb \
  rate "$RATE_SPEC" ceil "$RATE_SPEC" burst 1mb cburst 500kb mtu 9001

if [ "$RTT" != "0" ]; then
  tc qdisc add dev "$IFACE" parent 1:11 handle 10: netem \
    delay "${HALF_RTT}ms" limit 100000
fi

echo "TSG shaping on $IFACE: rate=$RATE_SPEC delay=${HALF_RTT}ms (nominal RTT ${RTT}ms)"
tc qdisc show dev "$IFACE"
tc class show dev "$IFACE"
