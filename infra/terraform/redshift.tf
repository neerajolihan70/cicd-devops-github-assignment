resource "aws_redshiftserverless_namespace" "ns" {
  namespace_name = "${var.project}-ns"
  db_name        = "analytics"
  iam_roles      = [aws_iam_role.redshift_role.arn]
}

resource "aws_redshiftserverless_workgroup" "wg" {
  workgroup_name = "${var.project}-wg"
  namespace_name = aws_redshiftserverless_namespace.ns.namespace_name
  base_capacity  = 8
  publicly_accessible = true
}
