#!/usr/bin/env bash
# Create the persistent LIDC-IDRI backup bucket. Run ONCE from the operator's
# laptop with normal AWS credentials — deliberately NOT part of terraform/, so
# `make tf-destroy` can never take the dataset backup down with the testbed.
#
# Idempotent: safe to re-run; every step converges to the same end state.
#
# Entirely OPTIONAL: without it the dataset is fetched from TCIA every run.
# Set DATASET_BACKUP_BUCKET (and optionally EXTRA_TAGS) in .env first.
#
# Layout (written by scripts/fetch_dataset.sh on the Spark host):
#   s3://$BUCKET/series_<N>/dicom/...        raw DICOM tree
#   s3://$BUCKET/series_<N>/MANIFEST.json    completeness token (written last)
set -euo pipefail

BUCKET="${DATASET_BACKUP_BUCKET:?set DATASET_BACKUP_BUCKET in .env first}"
# Same region as the testbed: free in-region transfer.
REGION="${AWS_REGION:-eu-central-1}"

if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
    echo "Bucket s3://$BUCKET already exists — converging config."
else
    # us-east-1 is the only region where LocationConstraint must be omitted;
    # everywhere else it is mandatory.
    aws s3api create-bucket \
        --bucket "$BUCKET" \
        --region "$REGION" \
        --create-bucket-configuration "LocationConstraint=$REGION"
    echo "Created s3://$BUCKET in $REGION."
fi

aws s3api put-public-access-block \
    --bucket "$BUCKET" \
    --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

# The same EXTRA_TAGS the EC2 resources carry, converted from the JSON object
# in .env to the TagSet syntax the S3 API wants. Skipped when EXTRA_TAGS is
# empty, which is the default.
TAGSET=$(python3 -c '
import json, os, sys
tags = json.loads(os.environ.get("EXTRA_TAGS") or "{}")
if not tags:
    sys.exit(1)
print("TagSet=[" + ",".join(f"{{Key={k},Value={v}}}" for k, v in tags.items()) + "]")
') && aws s3api put-bucket-tagging --bucket "$BUCKET" --tagging "$TAGSET" \
  || echo "No EXTRA_TAGS set — bucket left untagged."

echo "OK: s3://$BUCKET is private, tagged, in $REGION."
aws s3api get-bucket-location --bucket "$BUCKET"
