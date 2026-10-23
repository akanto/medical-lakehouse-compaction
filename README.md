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
| **Experiment** | Re-runs the whole measurement campaign from scratch | An AWS account and about 10 hours |

Most visitors want the first.

## Results

```bash
make install    # matplotlib, nothing else
make            # Tables II, III and V, and the three figures
```

Each table and figure of the paper that carries a measurement has its own
target:

| Paper | Command |
|---|---|
| Table II, measured properties of the four tables | `make tables` |
| Table III, structural cost per layout | `make tables` |
| Table IV, stability of the pass totals | `make cell-stats` |
| Table V, break-even count of each consolidated layout | `make break-even` |
| Figures 2–4, execution time against round-trip time | `make figures` |

`make tables` also lists the Parquet row groups per data file of each layout.
`make figures` writes the PNGs to `figures/`.

`make cell-stats` prints the dispersion behind the reported medians: the total
execution time of each of the three passes, which is the paper's reproducibility
table, and then the median, mean and standard deviation of every one of the 180
cells, three workloads by four layouts by fifteen network configurations.

`make break-even` sets the measured build time of L2, L3 and L4 against the
time each read saves over L1, and prints the break-even count of each layout in
each network configuration, the grid of the paper's Table V. A dash marks a
configuration in which the layout is not faster than L1. Running the script
with `--cells` also lists the saving behind every count.

### The committed campaign

`results/campaign-2026-10-21/` is the paper's dataset: 1,260 measurements across
15 network configurations, 84 per cell. Its `README.md` records how the campaign
was run, how it was verified, what changed against the previous protocol, and
where the run deviated from the runbook. The three per-rate JSONs the merged
file was built from are committed beside it, so `scripts/merge_rate_rows.py` can
be checked rather than trusted, together with the table shapes and the build
times.

## Experiment

Re-running the campaign provisions three `c5n.2xlarge` instances — Spark, the
object store, and a traffic-shaping gateway between them — fetches 200 CT series from
[TCIA](https://www.cancerimagingarchive.net/collection/lidc-idri/), builds the
four layouts, and sweeps the grid. Budget roughly 10 hours of wall time.

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

The object store is SILO `RELEASE.2026-09-16T00-00-00Z`, a community-maintained
fork of MinIO, pinned by digest in `docker-compose.yml`. SILO reads the same
`MINIO_*` settings, which is why the container, the Makefile targets and the
terraform outputs keep the name `minio`.

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

The harness also runs against the object store in Docker, on ten series, in
about ten minutes. This is the fastest way to see the pipeline work, and what the unit
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
evaluation/     the RESULTS lane: two table scripts, the dispersion listing,
                the break-even listing, and the figure script
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

The paper is accepted at the 2026 IEEE 18th International Scientific
Conference on Informatics (Informatics 2026), Poprad, Slovakia, and will appear
in its proceedings:

> A. Kanto, M. Kozlovszky, and A. C. Marosi, "Compaction as a Storage Layout
> Decision for 3D Medical Imaging in Network-Constrained Hybrid Clouds," in
> *Proc. 2026 IEEE 18th International Scientific Conference on Informatics*,
> Poprad, Slovakia, 2026.

Pages and DOI will be added once the proceedings are published.

## License

Apache 2.0 — see [LICENSE](LICENSE).
