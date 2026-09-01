#!/usr/bin/env bash
# Fetch the LIDC-IDRI subset onto this host from TCIA, optionally caching it
# in S3 so later runs restore in minutes instead of hours.
# Runs ON the Spark host from the repo root (invoked by `make remote-download`).
#
# The cache is OFF unless DATASET_BACKUP_BUCKET is set (see .env.example). With
# it unset this is a plain TCIA download and needs no AWS credentials at all.
# When set, S3 auth comes from the instance profile (terraform:
# aws_iam_role.spark, itself only created when backup_bucket is non-empty).
#
#   usage: fetch_dataset.sh <n_series> <output_dir>
#
# S3 path present (series_<N>/MANIFEST.json exists):
#   in-region sync down + verify against the manifest — fast, and frozen:
#   the bucket copy is the authoritative dataset definition (TCIA's series
#   ordering is not guaranteed stable across downloads).
# S3 path absent:
#   TCIA download with a completeness gate (the downloader can print "Done"
#   with series missing after transient timeouts), up to 3 resumable
#   attempts, then upload + write MANIFEST.json LAST as completeness token.
set -euo pipefail

N_SERIES="${1:?usage: fetch_dataset.sh <n_series> <output_dir>}"
OUTPUT_DIR="${2:?usage: fetch_dataset.sh <n_series> <output_dir>}"

BUCKET="${DATASET_BACKUP_BUCKET:-}"
PREFIX="series_${N_SERIES}"
# Manifest lives NEXT TO the dicom dir, never inside it — ingestion walks
# --dicom-dir and must only ever see DICOM files there.
MANIFEST="$(dirname "$OUTPUT_DIR")/MANIFEST_${PREFIX}.json"
PY=.venv/bin/python

mkdir -p "$OUTPUT_DIR"

if [ -n "$BUCKET" ] && aws s3api head-object --bucket "$BUCKET" --key "$PREFIX/MANIFEST.json" >/dev/null 2>&1; then
    echo "=== Source: s3://$BUCKET/$PREFIX/ (frozen backup) ==="
    aws s3 cp "s3://$BUCKET/$PREFIX/MANIFEST.json" "$MANIFEST" --only-show-errors
    aws s3 sync "s3://$BUCKET/$PREFIX/dicom/" "$OUTPUT_DIR" --only-show-errors
    "$PY" scripts/make_manifest.py verify --dicom-dir "$OUTPUT_DIR" --manifest "$MANIFEST"
    echo "=== Restore from S3 complete and verified ==="
    exit 0
fi

if [ -n "$BUCKET" ]; then
    echo "=== Source: TCIA (public) — no cache at s3://$BUCKET/$PREFIX/ yet ==="
else
    echo "=== Source: TCIA (public) — no dataset cache configured ==="
fi
attempt=1
until "$PY" scripts/download_dataset.py --n-series "$N_SERIES" --output "$OUTPUT_DIR" \
      && "$PY" scripts/make_manifest.py generate \
             --dicom-dir "$OUTPUT_DIR" --expect-series "$N_SERIES" \
             --source tcia --output "$MANIFEST"; do
    if [ "$attempt" -ge 3 ]; then
        echo "TCIA download still incomplete after 3 attempts — giving up." >&2
        exit 1
    fi
    attempt=$((attempt + 1))
    echo "--- Download incomplete (transient TCIA failure?) — retry $attempt/3 (resumable) ---"
    sleep 30
done

if [ -z "$BUCKET" ]; then
    echo "=== TCIA download complete and verified (no cache configured) ==="
    exit 0
fi

echo "=== Uploading to s3://$BUCKET/$PREFIX/dicom/ ==="
aws s3 sync "$OUTPUT_DIR" "s3://$BUCKET/$PREFIX/dicom/" --only-show-errors

# Cross-check the remote object count/bytes against the manifest before
# planting the completeness token.
summary=$(aws s3 ls --recursive --summarize "s3://$BUCKET/$PREFIX/dicom/" | tail -2)
remote_files=$(echo "$summary" | awk '/Total Objects:/ {print $3}')
remote_bytes=$(echo "$summary" | awk '/Total Size:/ {print $3}')
local_files=$("$PY" -c "import json;print(json.load(open('$MANIFEST'))['total_files'])")
local_bytes=$("$PY" -c "import json;print(json.load(open('$MANIFEST'))['total_bytes'])")
if [ "$remote_files" != "$local_files" ] || [ "$remote_bytes" != "$local_bytes" ]; then
    echo "UPLOAD MISMATCH: remote $remote_files files/$remote_bytes B vs manifest $local_files files/$local_bytes B" >&2
    echo "MANIFEST.json NOT uploaded — rerun to resync." >&2
    exit 1
fi

aws s3 cp "$MANIFEST" "s3://$BUCKET/$PREFIX/MANIFEST.json" --only-show-errors
echo "=== Backup complete: s3://$BUCKET/$PREFIX/ ($local_files files, $local_bytes bytes) ==="
