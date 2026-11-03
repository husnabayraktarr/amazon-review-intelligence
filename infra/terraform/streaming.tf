# Real-time layer: alert topic and the live-stats table used by the Lambda
resource "aws_sns_topic" "alerts" {
  name = "review-quality-alerts"
}

resource "aws_dynamodb_table" "live_stats" {
  name         = "review-live-stats"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}
