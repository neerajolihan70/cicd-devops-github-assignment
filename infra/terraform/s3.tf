resource "aws_s3_bucket" "raw_bucket" {
  bucket = "${var.project}-raw-${random_id.suffix.hex}"

  force_destroy = true
}

resource "random_id" "suffix" {
  byte_length = 4
}
