output "spark_public_ip" {
  value = aws_instance.spark.public_ip
}

output "spark_private_ip" {
  value = aws_instance.spark.private_ip
}

output "minio_public_ip" {
  value = aws_instance.minio.public_ip
}

output "minio_private_ip" {
  value = aws_instance.minio.private_ip
}

output "tsg_public_ip" {
  value = aws_instance.tsg.public_ip
}

output "tsg_private_ip" {
  value = aws_instance.tsg.private_ip
}

# tsg_host is the PRIVATE IP: run_experiment.py SSHes to the TSG from the
# Spark host, and the security group only allows SSH from allowed_ssh_cidrs —
# so the public IP is unreachable from inside the VPC. The private IP rides
# the VPC local route (never the shaped path).
output "experiment_profile_snippet" {
  value = <<-EOT
    endpoint: "http://${aws_instance.minio.private_ip}:9000"
    minio_ping_host: "${aws_instance.minio.private_ip}"
    tsg_host: "${aws_instance.tsg.private_ip}"
    tsg_ssh_user: "ubuntu"
  EOT
}
