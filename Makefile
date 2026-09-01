# Compaction as a Storage Layout Decision for 3D Medical Imaging in
# Network-Constrained Hybrid Clouds — reproduction package.
#
# Two independent lanes:
#
#   RESULTS   Redraw the paper's tables and figures from the committed campaign
#             JSON. Needs matplotlib and nothing else — no AWS, no Spark, no
#             dataset. This is the default target.
#
#   EXPERIMENT  Re-run the measurement campaign end to end: provision the AWS
#             testbed, fetch the dataset, build the four layouts, sweep the
#             network grid, collect, destroy. Needs an AWS account and about
#             eleven hours.
#
# A third, unadvertised path runs the pipeline against MinIO in Docker on ten
# series, so the harness can be exercised without touching AWS: see PLAYGROUND.

SHELL := /bin/bash
.DEFAULT_GOAL := all

# Connection and account settings. Copy .env.example to .env and fill it in;
# only the EXPERIMENT lane reads any of it.
ifneq (,$(wildcard .env))
include .env
export
endif

VENV    ?= .venv

# Prefer the project venv when `make install` has created one, so the
# documented `make install && make` works without activating anything first.
PYTHON  ?= $(shell [ -x $(VENV)/bin/python ] && echo $(VENV)/bin/python || echo python3)
PACKAGE := medical_lakehouse_compaction

# --- Committed campaign: the paper's dataset ---------------------------------
CAMPAIGN     := results/campaign-2026-08-30
MERGED       := $(CAMPAIGN)/benchmark_campaign_20260830_merged.json
SHAPES       := $(CAMPAIGN)/table_shapes_raw_20260829_190144.json
FIGURES_DIR  := figures

# --- Terraform inputs, mapped from the plain names in .env -------------------
# The TF_VAR_ prefix lives here and nowhere else, so .env stays readable.
export TF_VAR_aws_region        := $(AWS_REGION)
export TF_VAR_key_name          := $(KEY_NAME)
export TF_VAR_allowed_ssh_cidrs := $(ALLOWED_SSH_CIDRS)
export TF_VAR_extra_tags        := $(EXTRA_TAGS)
export TF_VAR_backup_bucket     := $(DATASET_BACKUP_BUCKET)

TF_DIR := terraform
AWS_REGION ?= eu-central-1

# Profiles. The committed profile carries no addresses; `make configure`
# generates the .local.yaml sibling from the live terraform outputs.
EXPERIMENT_PROFILE ?= conf/profiles/experiment.local.yaml
PROFILE            ?= conf/profiles/dev.yaml
BENCHMARK_PROFILE  ?= conf/profiles/local.yaml
RESULTS_DIR        ?= results/local
N_SERIES           ?= 10
DICOM_DIR          ?= data/dicom
REMOTE_N_SERIES    ?= 200

REMOTE_USER := ubuntu
REMOTE_DIR  := medical-lakehouse-compaction
SSH_OPTS    := -o StrictHostKeyChecking=no -o ConnectTimeout=10
RSYNC_OPTS  := -avz --exclude-from=.rsync-exclude

# Lazily evaluated: local targets never invoke terraform.
SPARK_HOST      = $(shell terraform -chdir=$(TF_DIR) output -raw spark_public_ip 2>/dev/null)
MINIO_HOST      = $(shell terraform -chdir=$(TF_DIR) output -raw minio_public_ip 2>/dev/null)
MINIO_PRIVATE_IP = $(shell terraform -chdir=$(TF_DIR) output -raw minio_private_ip 2>/dev/null)
TSG_HOST        = $(shell terraform -chdir=$(TF_DIR) output -raw tsg_public_ip 2>/dev/null)
TSG_PRIVATE_IP  = $(shell terraform -chdir=$(TF_DIR) output -raw tsg_private_ip 2>/dev/null)

.PHONY: all help tables figures clean \
        install install-experiment test \
        minio-up minio-down smoke-test benchmark evaluate download \
        tf-init tf-plan tf-apply tf-destroy tf-output configure status \
        remote-sync remote-setup tsg-setup tsg-authorize node-caps minio-deploy \
        remote-download remote-download-log remote-ingest remote-optimize \
        remote-pipeline-log network-validate remote-experiment \
        remote-experiment-log remote-collect backup-bucket

# ── RESULTS ───────────────────────────────────────────────────────────────────

## Redraw every table and figure the paper reports.
all: tables figures

## Tables II and III, printed.
tables:
	@$(PYTHON) evaluation/make_shapes_table.py $(SHAPES)
	@echo
	@$(PYTHON) evaluation/make_structural_table.py $(MERGED)

## Figures 1-3 into figures/ (not committed: IEEE copyright once accepted).
figures:
	@mkdir -p $(FIGURES_DIR)
	@$(PYTHON) evaluation/make_figures.py

# ── SETUP ─────────────────────────────────────────────────────────────────────

## Install the RESULTS lane only: matplotlib, no Spark.
install:
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -r requirements.txt

## Install the EXPERIMENT lane too: Spark, pydicom, boto3, TCIA client.
install-experiment:
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -r requirements-experiment.txt

test:
	$(PYTHON) -m pytest tests/ -v

# ── PLAYGROUND (Docker MinIO, no AWS) ─────────────────────────────────────────

minio-up:
	@if ! docker inspect minio --format='{{.State.Health.Status}}' 2>/dev/null | grep -q healthy; then \
		docker rm -f minio 2>/dev/null || true; \
		docker compose up -d; \
	fi
	@echo "Waiting for MinIO to be ready..."
	@until docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; do sleep 1; done
	@docker exec minio mc mb local/dicom-lakehouse --ignore-existing
	@echo "MinIO ready at http://localhost:9000"

minio-down:
	docker compose down

download:
	$(PYTHON) scripts/download_dataset.py --n-series $(N_SERIES) --output $(DICOM_DIR)

## Ingest and compact ten series against Docker MinIO — the whole harness, ~10 min.
smoke-test: minio-up
	@echo "Clearing previous warehouse data for reproducible run..."
	@docker exec minio mc rm --recursive --force local/dicom-lakehouse/warehouse >/dev/null 2>&1 || true
	$(PYTHON) scripts/run_ingestion.py --profile $(PROFILE) --dicom-dir $(DICOM_DIR)
	$(PYTHON) scripts/run_optimization.py --profile $(PROFILE)
	@echo "Smoke test passed — four Iceberg tables written to MinIO"

benchmark: minio-up
	$(PYTHON) scripts/run_benchmark.py --profile $(BENCHMARK_PROFILE) --output-dir $(RESULTS_DIR)

evaluate:
	$(PYTHON) scripts/evaluate.py --results-dir $(RESULTS_DIR)

# ── EXPERIMENT: testbed lifecycle ─────────────────────────────────────────────

tf-init:
	terraform -chdir=$(TF_DIR) init

## Show what tf-apply would create, without creating it.
tf-plan:
	terraform -chdir=$(TF_DIR) plan

tf-apply:
	terraform -chdir=$(TF_DIR) apply -auto-approve
	@$(MAKE) tf-output

tf-destroy:
	terraform -chdir=$(TF_DIR) destroy -auto-approve

tf-output:
	@echo "spark: $(SPARK_HOST)  minio: $(MINIO_HOST)  tsg: $(TSG_HOST)"

## Write conf/profiles/experiment.local.yaml from the live terraform outputs.
configure:
	$(PYTHON) scripts/configure_profile.py --profile conf/profiles/experiment.yaml

## Optional: create the S3 dataset cache. Needs DATASET_BACKUP_BUCKET in .env.
backup-bucket:
	bash scripts/create_backup_bucket.sh

# Read-only health check: live AWS resources, then what the pipeline is doing
# on the Spark host. Safe at any time, and degrades when nothing is provisioned.
status:
	@echo "=== EC2 instances (medical-imaging-*, $(AWS_REGION)) ==="
	@aws ec2 describe-instances --region $(AWS_REGION) \
	  --filters "Name=tag:Name,Values=medical-imaging-spark,medical-imaging-minio,medical-imaging-tsg" \
	            "Name=instance-state-name,Values=pending,running,stopping,stopped" \
	  --query 'Reservations[].Instances[].{name:Tags[?Key==`Name`]|[0].Value,state:State.Name,public_ip:PublicIpAddress}' \
	  --output table 2>/dev/null | grep -v '^$$' || echo "  (none)"
	@echo "=== EBS volumes (medical-imaging-*) ==="
	@aws ec2 describe-volumes --region $(AWS_REGION) \
	  --filters "Name=tag:Name,Values=medical-imaging-*" \
	  --query 'Volumes[].{id:VolumeId,state:State,gb:Size}' --output table 2>/dev/null | grep -v '^$$' || echo "  (none)"
	@if [ -z "$(SPARK_HOST)" ]; then \
	  echo "=== Remote activity: no terraform outputs — testbed not provisioned ==="; \
	else \
	  echo "=== Remote activity (spark $(SPARK_HOST)) ==="; \
	  ssh $(SSH_OPTS) $(REMOTE_USER)@$(SPARK_HOST) ' \
	    found=0; \
	    for p in fetch_[d]ataset.sh run_[i]ngestion.py run_[o]ptimization.py run_[e]xperiment.py collect_[t]able_stats.py; do \
	      out=$$(pgrep -fa "$$p" | head -1); \
	      [ -n "$$out" ] && { echo "RUNNING: $$out"; found=1; }; done; \
	    [ $$found -eq 0 ] && echo "no pipeline process running"; \
	    cd ~/$(REMOTE_DIR) 2>/dev/null || exit 0; \
	    echo "--- experiment progress ---"; \
	    test -f results/experiment.log \
	      && grep -E "=== Cell:|Checkpoint:|Results saved" results/experiment.log | tail -3 \
	      || echo "(no experiment log)"' \
	  || echo "spark host unreachable (SSH failed)"; \
	fi

# ── EXPERIMENT: provisioning (once after tf-apply, in this order) ─────────────

remote-sync:
	@test -n "$(SPARK_HOST)" || (echo "No spark_public_ip — run make tf-apply first" && exit 1)
	ssh $(SSH_OPTS) $(REMOTE_USER)@$(SPARK_HOST) "mkdir -p ~/$(REMOTE_DIR)"
	rsync $(RSYNC_OPTS) ./ $(REMOTE_USER)@$(SPARK_HOST):~/$(REMOTE_DIR)/

remote-setup:
	@test -n "$(SPARK_HOST)" || (echo "No spark_public_ip — run make tf-apply first" && exit 1)
	@echo "Waiting for SSH on $(SPARK_HOST)..."
	@until ssh $(SSH_OPTS) $(REMOTE_USER)@$(SPARK_HOST) exit 2>/dev/null; do sleep 5; done
	ssh $(SSH_OPTS) $(REMOTE_USER)@$(SPARK_HOST) "cd ~/$(REMOTE_DIR) && \
		python3 -m venv .venv && \
		.venv/bin/pip install --upgrade pip && \
		.venv/bin/pip install -r requirements-experiment.txt && \
		java -version"

tsg-setup:
	@test -n "$(TSG_HOST)" || (echo "No tsg_public_ip — run make tf-apply first" && exit 1)
	@echo "Waiting for SSH on $(TSG_HOST)..."
	@until ssh $(SSH_OPTS) $(REMOTE_USER)@$(TSG_HOST) exit 2>/dev/null; do sleep 5; done
	scp $(SSH_OPTS) scripts/setup_tsg.sh $(REMOTE_USER)@$(TSG_HOST):~/
	ssh $(SSH_OPTS) $(REMOTE_USER)@$(TSG_HOST) "sysctl net.ipv4.ip_forward | grep -q '= 1' \
		&& echo 'TSG ready (ip_forward=1)' || (echo 'ip_forward is OFF' && exit 1)"

# run_experiment.py applies each grid cell by SSHing spark -> TSG on the private
# address, which needs its own key: the operator's key never leaves the laptop.
tsg-authorize:
	@test -n "$(SPARK_HOST)" || (echo "No spark_public_ip — run make tf-apply first" && exit 1)
	ssh $(SSH_OPTS) $(REMOTE_USER)@$(SPARK_HOST) "test -f ~/.ssh/id_ed25519 || ssh-keygen -t ed25519 -N '' -f ~/.ssh/id_ed25519 -q"
	ssh $(SSH_OPTS) $(REMOTE_USER)@$(SPARK_HOST) "cat ~/.ssh/id_ed25519.pub" | \
		ssh $(SSH_OPTS) $(REMOTE_USER)@$(TSG_HOST) "cat >> ~/.ssh/authorized_keys && sort -u -o ~/.ssh/authorized_keys ~/.ssh/authorized_keys"
	ssh $(SSH_OPTS) $(REMOTE_USER)@$(SPARK_HOST) "ssh -o StrictHostKeyChecking=no $(REMOTE_USER)@$(TSG_PRIVATE_IP) 'echo spark-to-tsg SSH OK'"

# Pin Spark and MinIO to their 10 Gb/s baseline, neutralizing EC2 burst credits
# so the path ceiling is stable across an eleven-hour run.
node-caps:
	@test -n "$(SPARK_HOST)" || (echo "No spark_public_ip — run make tf-apply first" && exit 1)
	scp $(SSH_OPTS) scripts/setup_node_cap.sh $(REMOTE_USER)@$(SPARK_HOST):~/
	scp $(SSH_OPTS) scripts/setup_node_cap.sh $(REMOTE_USER)@$(MINIO_HOST):~/
	ssh $(SSH_OPTS) $(REMOTE_USER)@$(SPARK_HOST) "sudo bash ~/setup_node_cap.sh 10"
	ssh $(SSH_OPTS) $(REMOTE_USER)@$(MINIO_HOST) "sudo bash ~/setup_node_cap.sh 10"

minio-deploy:
	@test -n "$(MINIO_HOST)" || (echo "No minio_public_ip — run make tf-apply first" && exit 1)
	@echo "Waiting for SSH on $(MINIO_HOST)..."
	@until ssh $(SSH_OPTS) $(REMOTE_USER)@$(MINIO_HOST) exit 2>/dev/null; do sleep 5; done
	scp $(SSH_OPTS) docker-compose.yml $(REMOTE_USER)@$(MINIO_HOST):~/
	ssh $(SSH_OPTS) $(REMOTE_USER)@$(MINIO_HOST) "sudo docker compose up -d && \
		(pgrep iperf3 >/dev/null || iperf3 -s -D)"
	@echo "MinIO up at http://$(MINIO_PRIVATE_IP):9000 (private, via TSG)"

# ── EXPERIMENT: data pipeline (on the Spark host, detached where long) ────────

# TCIA download by default. Set DATASET_BACKUP_BUCKET in .env to cache the tree
# in S3, after which later runs restore in minutes rather than hours.
remote-download:
	ssh $(SSH_OPTS) $(REMOTE_USER)@$(SPARK_HOST) "cd ~/$(REMOTE_DIR) && \
		DATASET_BACKUP_BUCKET='$(DATASET_BACKUP_BUCKET)' \
		nohup bash scripts/fetch_dataset.sh $(REMOTE_N_SERIES) data/dicom \
		> download.log 2>&1 < /dev/null &"
	@echo "Fetch started (REMOTE_N_SERIES=$(REMOTE_N_SERIES)) — follow with make remote-download-log"

remote-download-log:
	ssh $(SSH_OPTS) $(REMOTE_USER)@$(SPARK_HOST) "tail -n 50 -f ~/$(REMOTE_DIR)/download.log"

# Run ingest and optimize with shaping CLEARED — they write ~28 GB through the
# TSG:  ssh to the TSG, then  sudo bash ~/setup_tsg.sh --rate none --rtt 0
remote-ingest:
	ssh $(SSH_OPTS) $(REMOTE_USER)@$(SPARK_HOST) "cd ~/$(REMOTE_DIR) && \
		nohup .venv/bin/python scripts/run_ingestion.py --profile $(EXPERIMENT_PROFILE) --dicom-dir data/dicom \
		> ingest.log 2>&1 < /dev/null &"
	@echo "Ingestion started — follow with make remote-pipeline-log LOG=ingest.log"

remote-optimize:
	ssh $(SSH_OPTS) $(REMOTE_USER)@$(SPARK_HOST) "cd ~/$(REMOTE_DIR) && \
		nohup .venv/bin/python scripts/run_optimization.py --profile $(EXPERIMENT_PROFILE) \
		> optimize.log 2>&1 < /dev/null &"
	@echo "Optimization started — follow with make remote-pipeline-log LOG=optimize.log"

LOG ?= ingest.log
remote-pipeline-log:
	ssh $(SSH_OPTS) $(REMOTE_USER)@$(SPARK_HOST) "tail -n 50 -f ~/$(REMOTE_DIR)/$(LOG)"

# ── EXPERIMENT: the campaign ─────────────────────────────────────────────────

network-validate:
	scp $(SSH_OPTS) scripts/validate_network.sh $(REMOTE_USER)@$(SPARK_HOST):~/
	ssh $(SSH_OPTS) $(REMOTE_USER)@$(SPARK_HOST) "cd ~/$(REMOTE_DIR) && \
		bash ~/validate_network.sh $(MINIO_PRIVATE_IP) results"

remote-experiment:
	ssh $(SSH_OPTS) $(REMOTE_USER)@$(SPARK_HOST) "cd ~/$(REMOTE_DIR) && mkdir -p results && \
		nohup .venv/bin/python scripts/run_experiment.py --profile $(EXPERIMENT_PROFILE) --output-dir results \
		> results/experiment.log 2>&1 < /dev/null &"
	@echo "Experiment started — follow with make remote-experiment-log"

remote-experiment-log:
	ssh $(SSH_OPTS) $(REMOTE_USER)@$(SPARK_HOST) "tail -n 50 -f ~/$(REMOTE_DIR)/results/experiment.log"

remote-collect:
	mkdir -p results/aws
	rsync -avz $(REMOTE_USER)@$(SPARK_HOST):~/$(REMOTE_DIR)/results/ results/aws/
	@echo "Results collected into results/aws/"

# ── Housekeeping ─────────────────────────────────────────────────────────────

clean:
	rm -rf $(FIGURES_DIR) .pytest_cache .ruff_cache
	find . -type d -name "__pycache__" -print0 | xargs -0 rm -rf
	rm -rf build dist *.egg-info

help:
	@echo "RESULTS — redraw the paper from the committed campaign (no AWS needed)"
	@echo "  make                  tables and figures"
	@echo "  make tables           Tables II and III, printed"
	@echo "  make figures          Figures 1-3 into figures/"
	@echo "  make install          install this lane (matplotlib only)"
	@echo
	@echo "PLAYGROUND — exercise the harness locally against Docker MinIO"
	@echo "  make install-experiment   install Spark, pydicom, boto3, TCIA client"
	@echo "  make download             fetch N_SERIES=$(N_SERIES) series from TCIA"
	@echo "  make smoke-test           ingest + compact all four layouts"
	@echo "  make benchmark evaluate   run the three workloads and print them"
	@echo "  make test                 unit tests"
	@echo
	@echo "EXPERIMENT — re-run the campaign on AWS (~11 h, needs .env)"
	@echo "  make tf-init tf-apply     provision the three-instance testbed"
	@echo "  make configure            write experiment.local.yaml from terraform"
	@echo "  make remote-sync remote-setup tsg-setup tsg-authorize node-caps minio-deploy"
	@echo "  make remote-download remote-ingest remote-optimize"
	@echo "  make network-validate remote-experiment remote-collect"
	@echo "  make status               what is running right now"
	@echo "  make tf-destroy           tear it all down"
