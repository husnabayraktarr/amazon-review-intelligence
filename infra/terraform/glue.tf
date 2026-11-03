# Batch processing: bronze -> silver -> SCD2 -> gold
resource "aws_glue_job" "this" {
  for_each = local.glue_jobs

  name              = each.key
  role_arn          = "arn:aws:iam::${local.account_id}:role/GlueReviewRole"
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = each.value.workers

  command {
    name            = "glueetl"
    script_location = "${local.scripts_uri}/${each.value.script}"
    python_version  = "3"
  }

  default_arguments = {
    "--BUCKET"           = local.bucket
    "--datalake-formats" = "iceberg"
  }
}
