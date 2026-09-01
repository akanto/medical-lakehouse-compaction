# Benchmark runbook

The full campaign, step by step: provision, load, sweep, collect, destroy.
Every step states the command, what correct output looks like, and the
condition that must hold before continuing. A step whose check fails is a stop
— later steps assume the earlier ones held.

Budget roughly 11 hours of grid time plus about an hour of setup. The testbed
runs three `c5n.2xlarge` instances for the whole of it.

## Step 0 — Prerequisites

```bash
aws sts get-caller-identity     # must print an account, not an error
terraform version               # >= 1.5
```

Check that the key pair named in `KEY_NAME` exists in `AWS_REGION` and that its
fingerprint matches the private key you hold. A mismatch surfaces only after the
instances are already running.

## Step 1 — Configure

```bash
cp .env.example .env
curl -s https://checkip.amazonaws.com     # add this address to ALLOWED_SSH_CIDRS
```

List every address you operate from, VPN and non-VPN both. Toggling a VPN
part-way through the grid otherwise locks you out of a running campaign.

## Step 2 — Provision

```bash
make tf-init
make tf-apply
make configure
```

`tf-apply` prints the three public addresses. `configure` writes
`conf/profiles/experiment.local.yaml` from the terraform outputs; every later
step reads that file, so re-run it after any `tf-apply`.

## Step 3 — Set up the nodes

First boot runs cloud-init, which takes a few minutes. The setup targets wait
for SSH themselves.

```bash
make remote-sync remote-setup     # repo + venv on the Spark host
make tsg-setup                    # must print "TSG ready (ip_forward=1)"
make tsg-authorize                # must print "spark-to-tsg SSH OK"
make node-caps                    # pin Spark and MinIO to their 10 Gb/s baseline
make minio-deploy
```

`remote-setup` must end with a `java -version` line. `node-caps` neutralizes EC2
burst credits, without which the path ceiling drifts over an eleven-hour run.

## Step 4 — Validate the network path

Run this before downloading anything: it is cheap, and it is the only check that
proves traffic actually crosses the shaping gateway.

```bash
make network-validate
```

Shaping is still clear here, so expect:

- ping average between 0.2 and 0.8 ms — the substrate baseline through the
  gateway. Record it; every later measured RTT is nominal plus this value.
- tracepath showing **two** hops: the gateway, then MinIO reached, `pmtu 9001`.
- iperf3 around 9–10 Gb/s, above every shaped tier.

If ping fails, or tracepath does not show the gateway hop, stop. Traffic is
bypassing the gateway and nothing measured afterwards means anything.

## Step 5 — Fetch the dataset

```bash
make remote-download        # detached; 200 series, ~18.7 GB
make remote-download-log    # follow; Ctrl-C leaves the download running
```

The log names its source: TCIA, or the S3 cache when `DATASET_BACKUP_BUCKET` is
set. TCIA takes 30–60 minutes and is gated on completeness — all 200 series
present, at least 50 slices each — because the downloader can report success
with series missing after a transient timeout. It retries three times, resuming.
Done when the log prints that the fetch is complete and verified.

## Step 6 — Build the four layouts

Both steps write tens of gigabytes through the gateway, so shaping must be
clear. It already is if Step 4's spot-check was cleaned up.

```bash
make remote-ingest                        # L1: one file per slice
make remote-pipeline-log LOG=ingest.log
make remote-optimize                      # L2, L3, L4 from L1
make remote-pipeline-log LOG=optimize.log
```

`optimize` must end with `Done.` and report `UID manifest bounds: truncate(64)`.
That setting rides table creation: changing it means rebuilding the tables and
re-running the whole campaign, and a build must never mix the two.

Optionally capture the exact table shapes, which is the input to the layout
table:

```bash
ssh ubuntu@$(terraform -chdir=terraform output -raw spark_public_ip) \
  "cd ~/medical-lakehouse-compaction && .venv/bin/python scripts/collect_table_stats.py \
   --profile conf/profiles/experiment.local.yaml --output-dir results"
```

## Step 7 — Run the grid

```bash
make remote-experiment        # detached; applies each cell on the gateway itself
make remote-experiment-log    # follow
make status                   # resource state, running processes, last checkpoint
```

The run is three launches, one per bandwidth row, each in a fresh JVM: a single
JVM has never survived 200 series. `scripts/run_rows.sh 5 2 1` chains them.

Order is rate `5, 2, 1` outer, latency `0, 2, 5, 10, 25` inner — 15 cells of 84
executions each (4 layouts × 3 repetitions × (5 W1 series + W2 + W3)). Ascending
latency puts the cheap points first and the 25 ms anchor last, so an early stop
costs the anchor rather than the result.

**Check the first few cells.** Each prints `Measured RTT: <x> ms (nominal <n>
ms)`, and `x` must be `n` plus the substrate RTT from Step 4. A measured RTT of
`None`, or one far off that sum, means the gateway SSH or the shaping failed.
Stop the run; the numbers are not usable.

Checkpoints land after every cell, so completed cells survive an interruption.

## Step 8 — Collect and check

```bash
make remote-collect         # rsync into results/aws/
python3 scripts/validate_cell.py results/aws/<the collected benchmark json>
```

Before destroying anything, confirm on the collected file:

- 15 network levels, 1,260 entries, exactly 84 per cell
- every `wall_clock_s` and `bytes_on_wire` above zero
- 5 W1 series, each with 3 repetitions per layout per cell
- each cell's measured RTT equal to nominal plus the substrate RTT, within
  about 0.6 ms

If the three rate rows were merged, do it with `scripts/merge_rate_rows.py`,
which checks that the protocol was identical across launches. A launch that
stopped early yields fewer than 15 levels, and the counts above should be
adjusted to what actually completed rather than the run being reported as whole.

**Do not continue until this passes.** After teardown the data on the nodes is
gone.

## Step 9 — Tear down

```bash
make tf-destroy
```

Mandatory, and the largest cost item if skipped. Confirm nothing lingers:

```bash
aws ec2 describe-instances --region "$AWS_REGION" \
  --filters "Name=tag:Name,Values=medical-imaging-*" \
            "Name=instance-state-name,Values=running,stopped,pending" \
  --query 'Reservations[].Instances[].InstanceId' --output text
aws ec2 describe-volumes --region "$AWS_REGION" \
  --filters "Name=tag:Name,Values=medical-imaging-*" \
  --query 'Volumes[].VolumeId' --output text
```

Both must print nothing. An optional dataset cache bucket is created outside
terraform and survives destroy by design.

## Failure modes worth knowing

**Measured RTT is `None` or far from nominal.** The Spark host could not reach
the gateway over SSH, or the shaping command failed. `make tsg-authorize` must
have printed `spark-to-tsg SSH OK`. Nothing measured after this point is valid.

**tracepath shows one hop.** Traffic is not crossing the gateway. The two
subnets and their route tables are what force it through; a single subnet cannot
be shaped, because route tables are only consulted when traffic leaves a subnet.

**The driver is killed part-way through a long row.** Native memory grows on top
of the heap over many hours. The profile sets `driver_memory: 12g` with one JVM
per rate row for exactly this reason; raising the heap without splitting the
rows makes it worse, not better.

**SSH starts timing out mid-campaign.** Your public address changed. Add it to
`ALLOWED_SSH_CIDRS` and re-run `make tf-apply`; this does not disturb the
running grid.

**Throughput drifts across cells.** The node caps were not applied, so EC2 burst
credits are in play. Re-run `make node-caps` and restart the affected row.
