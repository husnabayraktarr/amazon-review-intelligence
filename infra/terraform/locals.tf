locals {
  account_id  = "021882900412"
  region      = "us-east-1"
  bucket      = "husna-review-intel-2026"
  scripts_uri = "s3://husna-review-intel-2026/scripts"

  glue_jobs = {
    "reviews-bronze-to-silver"  = { script = "reviews_bronze_to_silver.py", workers = 4 }
    "products-bronze-to-silver" = { script = "products_bronze_to_silver.py", workers = 2 }
    "build-dim-product-scd2"    = { script = "build_dim_product_scd2.py", workers = 2 }
    "build-gold-star-schema"    = { script = "build_gold_star_schema.py", workers = 4 }
  }

  state_machine_arn = "arn:aws:states:us-east-1:021882900412:stateMachine:review-batch-pipeline"

  alarms = {
    "review-pipeline-failed" = {
      namespace  = "AWS/States", metric = "ExecutionsFailed", statistic = "Sum",
      period     = 300, evaluation_periods = 1, threshold = 1,
      dimensions = tomap({ StateMachineArn = "arn:aws:states:us-east-1:021882900412:stateMachine:review-batch-pipeline" })
    }
    "review-alerts-lambda-errors" = {
      namespace  = "AWS/Lambda", metric = "Errors", statistic = "Sum",
      period     = 300, evaluation_periods = 1, threshold = 1,
      dimensions = tomap({ FunctionName = "review-quality-alerts" })
    }
    "review-stream-lag" = {
      namespace  = "AWS/Lambda", metric = "IteratorAge", statistic = "Maximum",
      period     = 60, evaluation_periods = 3, threshold = 60000,
      dimensions = tomap({ FunctionName = "review-quality-alerts" })
    }
    "review-invalid-events-spike" = {
      namespace  = "AWS/SQS", metric = "ApproximateNumberOfMessagesVisible", statistic = "Maximum",
      period     = 300, evaluation_periods = 1, threshold = 10,
      dimensions = tomap({ QueueName = "review-events-invalid" })
    }
  }
}
