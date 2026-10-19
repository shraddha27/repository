output "state_machine_arn" {
  value       = aws_sfn_state_machine.catalogue.arn
  description = "Step Functions workflow ARN."
}

output "catalogue_bucket" {
  value       = aws_s3_bucket.catalogue.bucket
  description = "Private S3 bucket containing 90-day catalogue exports."
}

output "run_state_table" {
  value       = aws_dynamodb_table.runs.name
  description = "DynamoDB table storing run, batch, and per-SKU checkpoints."
}

output "alarm_topic_arn" {
  value       = aws_sns_topic.alarms.arn
  description = "Subscribe operators to this SNS topic to receive workflow alarms."
}
