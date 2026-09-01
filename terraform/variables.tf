variable "aws_region" {
  default = "eu-central-1"
}

# All three instances are c5n.2xlarge: baseline (non-burst) network is 10 Gb/s,
# so the 5 Gb/s top shaped tier is always below every node's sustained capacity
# and the TSG can forward 2x5 Gb/s through its single ENI without burst credits.
variable "instance_type_spark" {
  default = "c5n.2xlarge"
}

variable "instance_type_minio" {
  default = "c5n.2xlarge"
}

variable "instance_type_tsg" {
  default = "c5n.2xlarge"
}

variable "minio_root_volume_gb" {
  default = 150
}

variable "extra_tags" {
  description = <<-EOT
    Additional tags applied to every resource and to each root volume. Empty by
    default. Some organizations run automation that stops or deletes
    long-running instances, and a multi-hour benchmark grid looks idle to one;
    if yours needs an exemption tag, set EXTRA_TAGS in .env.
  EOT
  type        = map(string)
  default     = {}
}

variable "key_name" {
  description = "EC2 key pair name for SSH access"
  type        = string
}

variable "backup_bucket" {
  description = <<-EOT
    OPTIONAL S3 bucket caching the fetched DICOM tree, created by
    scripts/create_backup_bucket.sh (NOT by terraform, so tf-destroy can never
    delete it). Leave empty — the default — to always fetch from TCIA; the IAM
    role, policy and instance profile are then not created at all.
  EOT
  type        = string
  default     = ""
}

variable "allowed_ssh_cidrs" {
  description = "Operator IPs for SSH access (e.g. with and without VPN)"
  type        = list(string)
}
