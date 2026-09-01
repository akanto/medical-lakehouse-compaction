terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  # Stamped on every resource (instances, VPC, subnets, SG, volumes).
  # Empty by default. Some organizations run automation that stops or deletes
  # long-running instances, and a multi-hour benchmark grid looks idle to one;
  # if yours needs an exemption tag, set EXTRA_TAGS in .env.
  default_tags {
    tags = var.extra_tags
  }
}

# Everything is pinned to one AZ: cross-AZ traffic costs $0.01/GB each way and
# adds ~0.3-1 ms of uncontrolled substrate RTT — both would contaminate the sweep.
data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  az = data.aws_availability_zones.available.names[0]
}

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  tags                 = { Name = "medical-imaging-vpc" }
}

# Spark and MinIO live in SEPARATE subnets: AWS route tables are only consulted
# when traffic leaves a subnet, so intra-subnet traffic could never be forced
# through the Traffic Shaping Gateway. Each side's route table points the
# opposite subnet's CIDR at the TSG's ENI (the standard AWS middlebox pattern).
resource "aws_subnet" "spark" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = local.az
  map_public_ip_on_launch = true
  tags                    = { Name = "medical-imaging-spark" }
}

resource "aws_subnet" "minio" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = local.az
  map_public_ip_on_launch = true
  tags                    = { Name = "medical-imaging-minio" }
}

resource "aws_subnet" "tsg" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.3.0/24"
  availability_zone       = local.az
  map_public_ip_on_launch = true
  tags                    = { Name = "medical-imaging-tsg" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
}

resource "aws_route_table" "spark" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "medical-imaging-rt-spark" }
}

resource "aws_route_table" "minio" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "medical-imaging-rt-minio" }
}

resource "aws_route_table" "tsg" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "medical-imaging-rt-tsg" }
}

resource "aws_route" "spark_igw" {
  route_table_id         = aws_route_table.spark.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.main.id
}

resource "aws_route" "minio_igw" {
  route_table_id         = aws_route_table.minio.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.main.id
}

resource "aws_route" "tsg_igw" {
  route_table_id         = aws_route_table.tsg.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.main.id
}

# More-specific-than-local routes targeting the TSG ENI force all inter-site
# traffic through the gateway, symmetrically in both directions.
resource "aws_route" "spark_to_minio_via_tsg" {
  route_table_id         = aws_route_table.spark.id
  destination_cidr_block = aws_subnet.minio.cidr_block
  network_interface_id   = aws_instance.tsg.primary_network_interface_id
}

resource "aws_route" "minio_to_spark_via_tsg" {
  route_table_id         = aws_route_table.minio.id
  destination_cidr_block = aws_subnet.spark.cidr_block
  network_interface_id   = aws_instance.tsg.primary_network_interface_id
}

resource "aws_route_table_association" "spark" {
  subnet_id      = aws_subnet.spark.id
  route_table_id = aws_route_table.spark.id
}

resource "aws_route_table_association" "minio" {
  subnet_id      = aws_subnet.minio.id
  route_table_id = aws_route_table.minio.id
}

resource "aws_route_table_association" "tsg" {
  subnet_id      = aws_subnet.tsg.id
  route_table_id = aws_route_table.tsg.id
}

resource "aws_security_group" "internal" {
  name   = "medical-imaging-internal"
  vpc_id = aws_vpc.main.id

  ingress {
    from_port = 0
    to_port   = 65535
    protocol  = "tcp"
    self      = true
  }
  # UDP for tracepath probes (validate_network.sh's TSG-hop proof)
  ingress {
    from_port = 0
    to_port   = 65535
    protocol  = "udp"
    self      = true
  }
  # ICMP for measured-RTT verification pings (nominal vs observed, logged per cell)
  ingress {
    from_port = -1
    to_port   = -1
    protocol  = "icmp"
    self      = true
  }
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.allowed_ssh_cidrs
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Spark-host identity for the OPTIONAL dataset cache bucket (created OUTSIDE
# terraform by scripts/create_backup_bucket.sh so tf-destroy can never delete
# the cache). Scope: read/write objects, no delete — it is an append-only
# archive. All three resources are skipped when backup_bucket is "", which is
# the default: the dataset is then always fetched from TCIA.
resource "aws_iam_role" "spark" {
  count = var.backup_bucket == "" ? 0 : 1
  name  = "medical-imaging-spark"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "spark_dataset_backup" {
  count = var.backup_bucket == "" ? 0 : 1
  name  = "dataset-backup-bucket"
  role  = aws_iam_role.spark[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = "arn:aws:s3:::${var.backup_bucket}"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "arn:aws:s3:::${var.backup_bucket}/*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "spark" {
  count = var.backup_bucket == "" ? 0 : 1
  name  = "medical-imaging-spark"
  role  = aws_iam_role.spark[0].name
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]
  filter {
    name = "name"
    # 24.04 (noble): ships Python 3.12 — the project requires >=3.11, which
    # rules out 22.04 (Python 3.10).
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
}

resource "aws_instance" "spark" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type_spark
  subnet_id              = aws_subnet.spark.id
  availability_zone      = local.az
  vpc_security_group_ids = [aws_security_group.internal.id]
  key_name               = var.key_name
  iam_instance_profile   = one(aws_iam_instance_profile.spark[*].name)

  root_block_device {
    volume_size           = 100
    volume_type           = "gp3"
    delete_on_termination = true
    tags                  = merge({ Name = "medical-imaging-spark-root" }, var.extra_tags)
  }

  # Minimal bootstrap; real provisioning is `make remote-setup` (venv +
  # `pip install -r requirements-experiment.txt` from the rsynced repo).
  # DEBIAN_FRONTEND: iperf3 on 24.04 asks a debconf question and hangs
  # cloud-init forever without it.
  user_data = <<-EOF
    #!/bin/bash
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y python3-venv python3-pip openjdk-17-jre-headless iperf3 iproute2 unzip
    # AWS CLI v2 for the dataset backup bucket. NOT via apt: Ubuntu 24.04 has
    # no awscli package (snap-only), and a missing package fails the whole
    # apt-get line above with it.
    curl -sL https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -o /tmp/awscliv2.zip
    unzip -q /tmp/awscliv2.zip -d /tmp && /tmp/aws/install
  EOF

  # Without this, user_data edits are applied in-place via stop/start and the
  # once-per-instance cloud-init script never re-runs (silently stale nodes).
  user_data_replace_on_change = true

  tags = { Name = "medical-imaging-spark" }
}

resource "aws_instance" "minio" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type_minio
  subnet_id              = aws_subnet.minio.id
  availability_zone      = local.az
  vpc_security_group_ids = [aws_security_group.internal.id]
  key_name               = var.key_name

  # No separate EBS volume: MinIO holds ~28 GB of Iceberg tables; gp3 max
  # provisioned throughput (1000 MiB/s ~ 8.4 Gb/s) clears the top 5 Gb/s
  # network tier so the disk is never the binding constraint, and the stock
  # docker-compose named volume on the root disk works unchanged.
  root_block_device {
    volume_size           = var.minio_root_volume_gb
    volume_type           = "gp3"
    throughput            = 1000
    iops                  = 10000
    delete_on_termination = true
    tags                  = merge({ Name = "medical-imaging-minio-root" }, var.extra_tags)
  }

  user_data = <<-EOF
    #!/bin/bash
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y docker.io docker-compose-v2 iproute2 iperf3
    systemctl enable --now docker
  EOF

  user_data_replace_on_change = true

  tags = { Name = "medical-imaging-minio" }
}

# Traffic Shaping Gateway: forwards all Spark<->MinIO traffic (HTB rate cap +
# netem delay applied by scripts/setup_tsg.sh). c5n.2xlarge baseline network is
# 10 Gb/s — the 5 Gb/s top tier crosses the single ENI twice (in + out), so the
# gateway must sustain 10 Gb/s aggregate without burst credits.
resource "aws_instance" "tsg" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type_tsg
  subnet_id              = aws_subnet.tsg.id
  availability_zone      = local.az
  vpc_security_group_ids = [aws_security_group.internal.id]
  key_name               = var.key_name

  # Mandatory for a middlebox: AWS drops forwarded packets whose src/dst is
  # not the instance unless this check is disabled.
  source_dest_check = false

  root_block_device {
    volume_size           = 20
    volume_type           = "gp3"
    delete_on_termination = true
    tags                  = merge({ Name = "medical-imaging-tsg-root" }, var.extra_tags)
  }

  user_data = <<-EOF
    #!/bin/bash
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y iproute2 iperf3
    sysctl -w net.ipv4.ip_forward=1
    echo "net.ipv4.ip_forward=1" > /etc/sysctl.d/99-tsg.conf
  EOF

  user_data_replace_on_change = true

  tags = { Name = "medical-imaging-tsg" }
}
