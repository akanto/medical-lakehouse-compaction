# Compaction as a Storage Layout Decision for 3D Medical Imaging in Network-Constrained Hybrid Clouds

Reproduction package for the paper of the same name.

The study measures how four storage layouts of the same 3D medical imaging
dataset behave when the object store sits on the far side of a constrained
network. It builds each layout as an Apache Iceberg table, then sweeps three
bandwidth limits against five injected round-trip times and records execution
time, object-store round-trips and read amplification for three workloads.

The repository has two independent halves.

| | What it does | What it needs |
|---|---|---|
| **Results** | Redraws every table and figure the paper reports, from the campaign committed here | Python and matplotlib |
| **Experiment** | Re-runs the whole measurement campaign from scratch | An AWS account and about 11 hours |

Most visitors want the first.

## Results

```bash
make install    # matplotlib, nothing else
make            # tables and figures
```

`make` prints Tables II and III and writes the three figures to `figures/`.
Individual steps are `make tables` and `make figures`.

Table I of the paper is a set of definitions rather than a measurement, and is
not generated here.

### The committed campaign

`results/campaign-2026-08-30/` is the paper's dataset: 1,260 measurements across
15 network configurations, 84 per cell. Its `README.md` records how the campaign
was run, how it was verified, what changed against the previous protocol, and
where the run deviated from the runbook. The three per-rate JSONs the merged
file was built from are committed beside it, so `scripts/merge_rate_rows.py` can
be checked rather than trusted.

## Experiment

Re-running the campaign provisions three `c5n.2xlarge` instances — Spark, MinIO,
and a traffic-shaping gateway between them — fetches 200 CT series from
[TCIA](https://www.cancerimagingarchive.net/collection/lidc-idri/), builds the
four layouts, and sweeps the grid. Budget roughly 11 hours of wall time.

```bash
cp .env.example .env       # key pair, your SSH addresses, region
make install-experiment
make tf-init tf-apply
make configure             # writes experiment.local.yaml from terraform outputs
make remote-sync remote-setup tsg-setup tsg-authorize node-caps minio-deploy
make remote-download remote-ingest remote-optimize
make network-validate remote-experiment
make remote-collect
make tf-destroy
```

`make status` reports what is running at any point. `docs/benchmark-runbook.md`
is the operator's guide, including the failure modes worth knowing before
spending the hours.

No address, bucket name or key is stored in this repository. Terraform reads its
inputs from `.env`, and the profiles carry no addresses at all: `make configure`
generates `conf/profiles/experiment.local.yaml` from `terraform output`, and
git-ignores it, so a live topology cannot reach a commit.

The dataset is fetched from TCIA by default. Setting `DATASET_BACKUP_BUCKET`
in `.env` additionally caches the tree in S3 — the first run uploads it, later
runs restore in minutes — and is the only thing that creates an IAM role.

### Locally, without AWS

The harness also runs against MinIO in Docker, on ten series, in about ten
minutes. This is the fastest way to see the pipeline work, and what the unit
tests exercise.

```bash
make install-experiment
make download            # 10 series from TCIA
make smoke-test          # ingest + build all four layouts
make benchmark evaluate  # run the three workloads and print them
make test
```

## Layouts and workloads

The four layouts, all built from the same 200 series:

| | Layout | Produced by |
|---|---|---|
| L1 | Raw ingested table, one file per slice | ingestion, no compaction |
| L2 | Size-based compaction to 256 MB files | generic bin-packing |
| L3 | Series-clustered by sort order | domain-aware compaction |
| L4 | Series-clustered by partition spec | partition-spec compaction |

The three workloads: **W1** 3D reconstruction of one series; **W2** loading a
patient-level training shard of 50 series; **W3** a metadata cohort scan across
the full table, projecting no pixel data.

## Layout of this repository

```
evaluation/     the RESULTS lane: two table scripts and the figure script
results/        the committed campaign, and its provenance README
medical_lakehouse_compaction/
                ingestion, the three compaction strategies, the three
                workloads, and the metrics collector
scripts/        the EXPERIMENT lane: fetch, ingest, optimize, sweep, merge,
                and the network and per-cell validation gates
conf/profiles/  run profiles, each documenting why its values are what they are
terraform/      the three-instance testbed
tests/          unit tests for the harness
docs/           the campaign runbook
```

## Dataset

LIDC-IDRI, from The Cancer Imaging Archive, used under CC BY 3.0. It is not
redistributed here; `make download` and `make remote-download` fetch it from
TCIA. Only the measurements taken over it are committed.

## Citing

The paper is not yet published. This section will carry the citation once it is.

## License

Apache 2.0 — see [LICENSE](LICENSE).
