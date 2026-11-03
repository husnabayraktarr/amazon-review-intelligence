# Orchestration: the pipeline state machine and its weekly schedule
resource "aws_sfn_state_machine" "pipeline" {
  name       = "review-batch-pipeline"
  role_arn   = "arn:aws:iam::${local.account_id}:role/ReviewPipelineStepFunctionsRole"
  definition = file("${path.module}/../state_machine.json")
  type       = "STANDARD"
}

resource "aws_scheduler_schedule" "weekly" {
  name                         = "review-pipeline-weekly"
  group_name                   = "default"
  schedule_expression          = "cron(0 6 ? * MON *)"
  schedule_expression_timezone = "America/New_York"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.pipeline.arn
    role_arn = "arn:aws:iam::${local.account_id}:role/ReviewPipelineSchedulerRole"
    input    = "{}"
  }
}
