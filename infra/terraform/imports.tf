# Adopt resources that were created by hand: import, don't recreate
import {
  for_each = local.glue_jobs
  to       = aws_glue_job.this[each.key]
  id       = each.key
}

import {
  to = aws_sfn_state_machine.pipeline
  id = local.state_machine_arn
}

import {
  to = aws_scheduler_schedule.weekly
  id = "default/review-pipeline-weekly"
}

import {
  to = aws_sns_topic.alerts
  id = "arn:aws:sns:us-east-1:021882900412:review-quality-alerts"
}

import {
  to = aws_dynamodb_table.live_stats
  id = "review-live-stats"
}

import {
  for_each = local.alarms
  to       = aws_cloudwatch_metric_alarm.this[each.key]
  id       = each.key
}
