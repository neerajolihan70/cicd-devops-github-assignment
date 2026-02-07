output "s3_bucket" {
  value = aws_s3_bucket.raw_bucket.bucket
}

output "redshift_role_arn" {
  value = aws_iam_role.redshift_role.arn
}

output "redshift_workgroup" {
  value = aws_redshiftserverless_workgroup.wg.workgroup_name
}
