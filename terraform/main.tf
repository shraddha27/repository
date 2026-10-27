terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  skip_credentials_validation = var.localstack_endpoint != null
  skip_metadata_api_check     = var.localstack_endpoint != null
  skip_requesting_account_id  = var.localstack_endpoint != null
  skip_region_validation      = var.localstack_endpoint != null
  s3_use_path_style           = var.localstack_endpoint != null

  dynamic "endpoints" {
    for_each = var.localstack_endpoint == null ? [] : [var.localstack_endpoint]
    content {
      applicationautoscaling = endpoints.value
      cloudwatch             = endpoints.value
      dynamodb               = endpoints.value
      ec2                    = endpoints.value
      ecr                    = endpoints.value
      ecs                    = endpoints.value
      events                 = endpoints.value
      iam                    = endpoints.value
      logs                   = endpoints.value
      s3                     = endpoints.value
      secretsmanager         = endpoints.value
      sqs                    = endpoints.value
      sfn                    = endpoints.value
      sns                    = endpoints.value
      sts                    = endpoints.value
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  is_localstack            = var.localstack_endpoint != null
  product_api_secret_arn   = local.is_localstack ? aws_secretsmanager_secret.local_product_api_key[0].arn : var.product_api_key_secret_arn
  warehouse_api_secret_arn = local.is_localstack ? aws_secretsmanager_secret.local_warehouse_api_key[0].arn : var.warehouse_api_key_secret_arn
  task_subnet_ids          = local.is_localstack ? [aws_subnet.localstack[0].id] : var.subnet_ids
  task_security_group_ids  = local.is_localstack ? [aws_security_group.localstack[0].id] : var.security_group_ids
  task_queue_url           = local.is_localstack ? "${trimsuffix(var.task_aws_endpoint_url, "/")}/${data.aws_caller_identity.current.account_id}/${aws_sqs_queue.batches.name}" : aws_sqs_queue.batches.url
}

resource "aws_vpc" "localstack" {
  count      = local.is_localstack ? 1 : 0
  cidr_block = "10.20.0.0/16"
  tags       = { Name = "${var.name_prefix}-localstack" }
}

resource "aws_subnet" "localstack" {
  count                   = local.is_localstack ? 1 : 0
  vpc_id                  = aws_vpc.localstack[0].id
  cidr_block              = "10.20.1.0/24"
  map_public_ip_on_launch = true
  tags                    = { Name = "${var.name_prefix}-localstack" }
}

resource "aws_security_group" "localstack" {
  count       = local.is_localstack ? 1 : 0
  name        = "${var.name_prefix}-localstack"
  description = "Local-only security group for the catalogue ECS task."
  vpc_id      = aws_vpc.localstack[0].id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_secretsmanager_secret" "local_product_api_key" {
  count = local.is_localstack ? 1 : 0
  name  = "${var.name_prefix}-local-product-api-key"
}

resource "aws_secretsmanager_secret_version" "local_product_api_key" {
  count         = local.is_localstack ? 1 : 0
  secret_id     = aws_secretsmanager_secret.local_product_api_key[0].id
  secret_string = var.localstack_product_api_key
}

resource "aws_secretsmanager_secret" "local_warehouse_api_key" {
  count = local.is_localstack ? 1 : 0
  name  = "${var.name_prefix}-local-warehouse-api-key"
}

resource "aws_secretsmanager_secret_version" "local_warehouse_api_key" {
  count         = local.is_localstack ? 1 : 0
  secret_id     = aws_secretsmanager_secret.local_warehouse_api_key[0].id
  secret_string = var.localstack_warehouse_api_key
}

resource "aws_s3_bucket" "catalogue" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_public_access_block" "catalogue" {
  bucket                  = aws_s3_bucket.catalogue.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "catalogue" {
  bucket = aws_s3_bucket.catalogue.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "catalogue" {
  bucket = aws_s3_bucket.catalogue.id
  rule {
    id     = "expire-catalogue-exports"
    status = "Enabled"
    filter {
      prefix = "catalogue/"
    }
    expiration {
      days = 90
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_dynamodb_table" "runs" {
  name                        = "${var.name_prefix}-catalogue-runs"
  billing_mode                = "PAY_PER_REQUEST"
  hash_key                    = "pk"
  range_key                   = "sk"
  deletion_protection_enabled = !local.is_localstack

  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }
}

resource "aws_sqs_queue" "dead_letter" {
  name                        = "${var.name_prefix}-wms-batches-dlq.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  message_retention_seconds   = 1209600
}

resource "aws_sqs_queue" "batches" {
  name                        = "${var.name_prefix}-wms-batches.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  deduplication_scope         = "messageGroup"
  fifo_throughput_limit       = "perMessageGroupId"
  visibility_timeout_seconds  = 300
  message_retention_seconds   = 345600
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead_letter.arn
    maxReceiveCount     = 5
  })
}

resource "aws_cloudwatch_log_group" "task" {
  name              = "/ecs/${var.name_prefix}-catalogue-sync"
  retention_in_days = 90
}

resource "aws_cloudwatch_log_group" "worker" {
  name              = "/ecs/${var.name_prefix}-catalogue-worker"
  retention_in_days = 90
}

resource "aws_iam_role" "task_execution" {
  name = "${var.name_prefix}-catalogue-task-execution"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "task_execution_managed" {
  role       = aws_iam_role.task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "task_execution_secrets" {
  name = "${var.name_prefix}-catalogue-secret-read"
  role = aws_iam_role.task_execution.id

  lifecycle {
    precondition {
      condition = local.is_localstack || (
        length(trimspace(var.product_api_key_secret_arn)) > 0 &&
        length(trimspace(var.warehouse_api_key_secret_arn)) > 0
      )
      error_message = "Set both secret ARNs for AWS deployments."
    }
  }

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = [local.product_api_secret_arn, local.warehouse_api_secret_arn]
    }]
  })
}

resource "aws_iam_role" "task" {
  name = "${var.name_prefix}-catalogue-task"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "task_access" {
  name = "${var.name_prefix}-catalogue-task-access"
  role = aws_iam_role.task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
        Resource = [
          "${aws_s3_bucket.catalogue.arn}/catalogue/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:BatchWriteItem",
          "dynamodb:DeleteItem",
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:Query",
          "dynamodb:TransactWriteItems",
          "dynamodb:UpdateItem"
        ]
        Resource = aws_dynamodb_table.runs.arn
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:ChangeMessageVisibility",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl",
          "sqs:ReceiveMessage",
          "sqs:SendMessage"
        ]
        Resource = [
          aws_sqs_queue.batches.arn,
          aws_sqs_queue.dead_letter.arn
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["ecs:UpdateTaskProtection"]
        Resource = "*"
        Condition = {
          ArnEquals = {
            "ecs:cluster" = aws_ecs_cluster.catalogue.arn
          }
        }
      }
    ]
  })
}

resource "aws_ecs_cluster" "catalogue" {
  name = "${var.name_prefix}-catalogue"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_ecs_task_definition" "catalogue" {
  family                   = "${var.name_prefix}-catalogue-sync"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

  lifecycle {
    precondition {
      condition = local.is_localstack || (
        length(var.subnet_ids) > 0 && length(var.security_group_ids) > 0
      )
      error_message = "Set private subnet_ids and security_group_ids for AWS deployments."
    }
  }

  container_definitions = jsonencode([{
    name      = "catalogue-sync"
    image     = var.container_image
    essential = true
    command   = ["python", "-m", "app.main"]
    environment = concat([
      { name = "S3_BUCKET", value = aws_s3_bucket.catalogue.bucket },
      { name = "DYNAMODB_TABLE", value = aws_dynamodb_table.runs.name },
      { name = "PRODUCT_API_URL", value = var.product_api_url },
      { name = "WAREHOUSE_API_URL", value = var.warehouse_api_url },
      { name = "PAGE_SIZE", value = "500" },
      { name = "BATCH_SIZE", value = "100" },
      { name = "SQS_QUEUE_URL", value = local.task_queue_url },
      { name = "RUN_COMPLETION_TIMEOUT_SECONDS", value = "14400" }
      ], local.is_localstack ? [
      { name = "AWS_ENDPOINT_URL", value = var.task_aws_endpoint_url },
      { name = "AWS_ACCESS_KEY_ID", value = "test" },
      { name = "AWS_SECRET_ACCESS_KEY", value = "test" },
      { name = "AWS_DEFAULT_REGION", value = var.aws_region }
    ] : [])
    secrets = [
      { name = "PRODUCT_API_KEY", valueFrom = local.product_api_secret_arn },
      { name = "WAREHOUSE_API_KEY", valueFrom = local.warehouse_api_secret_arn }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.task.name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "sync"
      }
    }
  }])
}

resource "aws_ecs_task_definition" "worker" {
  family                   = "${var.name_prefix}-catalogue-worker"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([{
    name      = "catalogue-worker"
    image     = var.container_image
    essential = true
    command   = ["python", "-m", "app.worker"]
    environment = concat([
      { name = "DYNAMODB_TABLE", value = aws_dynamodb_table.runs.name },
      { name = "SQS_QUEUE_URL", value = local.task_queue_url },
      { name = "WAREHOUSE_API_URL", value = var.warehouse_api_url },
      { name = "BATCH_SIZE", value = "100" },
      { name = "WORKER_POLL_SECONDS", value = "20" },
      { name = "WORKER_VISIBILITY_TIMEOUT", value = "300" },
      { name = "ECS_CLUSTER_NAME", value = aws_ecs_cluster.catalogue.name }
      ], local.is_localstack ? [
      { name = "AWS_ENDPOINT_URL", value = var.task_aws_endpoint_url },
      { name = "AWS_ACCESS_KEY_ID", value = "test" },
      { name = "AWS_SECRET_ACCESS_KEY", value = "test" },
      { name = "AWS_DEFAULT_REGION", value = var.aws_region }
    ] : [])
    secrets = [
      { name = "WAREHOUSE_API_KEY", valueFrom = local.warehouse_api_secret_arn }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.worker.name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "worker"
      }
    }
  }])
}

resource "aws_ecs_service" "workers" {
  name            = "${var.name_prefix}-catalogue-workers"
  cluster         = aws_ecs_cluster.catalogue.id
  task_definition = aws_ecs_task_definition.worker.arn
  desired_count   = var.worker_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = local.task_subnet_ids
    security_groups  = local.task_security_group_ids
    assign_public_ip = false
  }

  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 200

  lifecycle {
    ignore_changes = [desired_count]
  }
}

resource "aws_appautoscaling_target" "workers" {
  min_capacity       = var.worker_desired_count
  max_capacity       = var.worker_max_capacity
  resource_id        = "service/${aws_ecs_cluster.catalogue.name}/${aws_ecs_service.workers.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"

  depends_on = [aws_ecs_service.workers]

  lifecycle {
    precondition {
      condition     = var.worker_desired_count <= var.worker_max_capacity
      error_message = "worker_max_capacity must be at least worker_desired_count."
    }
  }
}

resource "aws_appautoscaling_policy" "workers_queue_backlog" {
  name               = "${var.name_prefix}-workers-queue-backlog"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.workers.resource_id
  scalable_dimension = aws_appautoscaling_target.workers.scalable_dimension
  service_namespace  = aws_appautoscaling_target.workers.service_namespace

  target_tracking_scaling_policy_configuration {
    target_value       = var.worker_target_backlog_per_task
    scale_out_cooldown = 60
    scale_in_cooldown  = 300

    customized_metric_specification {
      metrics {
        label = "Visible SQS worker batches"
        id    = "m1"

        metric_stat {
          metric {
            metric_name = "ApproximateNumberOfMessagesVisible"
            namespace   = "AWS/SQS"

            dimensions {
              name  = "QueueName"
              value = aws_sqs_queue.batches.name
            }
          }

          stat = "Sum"
        }

        return_data = false
      }

      metrics {
        label = "Running ECS worker tasks"
        id    = "m2"

        metric_stat {
          metric {
            metric_name = "RunningTaskCount"
            namespace   = "ECS/ContainerInsights"

            dimensions {
              name  = "ClusterName"
              value = aws_ecs_cluster.catalogue.name
            }

            dimensions {
              name  = "ServiceName"
              value = aws_ecs_service.workers.name
            }
          }

          stat = "Average"
        }

        return_data = false
      }

      metrics {
        label       = "Visible batches per running worker"
        id          = "e1"
        expression  = "m1 / m2"
        return_data = true
      }
    }
  }
}

resource "aws_cloudwatch_dashboard" "workers" {
  dashboard_name = "${var.name_prefix}-worker-throughput"
  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title  = "WMS request rate and errors"
          view   = "timeSeries"
          region = var.aws_region
          period = 60
          metrics = [
            [{ expression = "m1/60", label = "Requests/second", id = "e1" }],
            [{ expression = "m2/60", label = "Errors/second", id = "e2" }],
            [
              "ProductCatalogue/Workers",
              "WmsRequestAttempts",
              "ServiceName",
              "catalogue-worker",
              "Operation",
              "Batch",
              { stat = "Sum", period = 60, id = "m1", visible = false }
            ],
            [
              "ProductCatalogue/Workers",
              "WmsRequestErrors",
              "ServiceName",
              "catalogue-worker",
              "Operation",
              "Batch",
              { stat = "Sum", period = 60, id = "m2", visible = false }
            ]
          ]
          yAxis = {
            left = { min = 0 }
          }
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title  = "WMS request latency"
          view   = "timeSeries"
          region = var.aws_region
          period = 60
          metrics = [
            [
              "ProductCatalogue/Workers",
              "WmsRequestLatencyMs",
              "ServiceName",
              "catalogue-worker",
              "Operation",
              "Batch",
              { stat = "Average", period = 60, id = "m1" }
            ],
            [
              "ProductCatalogue/Workers",
              "WmsRequestLatencyMs",
              "ServiceName",
              "catalogue-worker",
              "Operation",
              "Batch",
              { stat = "p95", period = 60, id = "m2" }
            ]
          ]
          yAxis = {
            left = { min = 0, label = "Milliseconds" }
          }
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "WMS SQS queue backlog"
          view   = "timeSeries"
          region = var.aws_region
          period = 60
          metrics = [
            [
              "AWS/SQS",
              "ApproximateNumberOfMessagesVisible",
              "QueueName",
              aws_sqs_queue.batches.name,
              { stat = "Maximum", id = "m1" }
            ],
            [
              "AWS/SQS",
              "ApproximateNumberOfMessagesNotVisible",
              "QueueName",
              aws_sqs_queue.batches.name,
              { stat = "Maximum", id = "m2" }
            ],
            [
              "AWS/SQS",
              "ApproximateAgeOfOldestMessage",
              "QueueName",
              aws_sqs_queue.batches.name,
              { stat = "Maximum", id = "m3", yAxis = "right" }
            ]
          ]
          yAxis = {
            left  = { min = 0, label = "Messages" }
            right = { min = 0, label = "Seconds" }
          }
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "Running workers and visible batches per worker"
          view   = "timeSeries"
          region = var.aws_region
          period = 60
          metrics = [
            [
              "ECS/ContainerInsights",
              "RunningTaskCount",
              "ClusterName",
              aws_ecs_cluster.catalogue.name,
              "ServiceName",
              aws_ecs_service.workers.name,
              { stat = "Average", id = "m1", yAxis = "left" }
            ],
            [
              "AWS/SQS",
              "ApproximateNumberOfMessagesVisible",
              "QueueName",
              aws_sqs_queue.batches.name,
              { stat = "Sum", id = "m2", visible = false }
            ],
            [
              { expression = "m2/m1", label = "Visible batches per running worker", id = "e1", yAxis = "right" }
            ]
          ]
          yAxis = {
            left  = { min = 0, label = "Running tasks" }
            right = { min = 0, label = "Batches per worker" }
          }
        }
      }
    ]
  })
}

resource "aws_sfn_state_machine" "catalogue" {
  name     = "${var.name_prefix}-catalogue-sync"
  role_arn = aws_iam_role.workflow.arn
  type     = "STANDARD"

  definition = jsonencode({
    Comment = "Run the daily product catalogue integration as one observable ECS task."
    StartAt = "RunCatalogueSync"
    States = {
      RunCatalogueSync = {
        Type     = "Task"
        Resource = "arn:aws:states:::ecs:runTask.sync"
        Parameters = {
          Cluster        = aws_ecs_cluster.catalogue.arn
          TaskDefinition = aws_ecs_task_definition.catalogue.arn
          LaunchType     = "FARGATE"
          NetworkConfiguration = {
            AwsvpcConfiguration = {
              Subnets        = local.task_subnet_ids
              SecurityGroups = local.task_security_group_ids
              AssignPublicIp = "DISABLED"
            }
          }
          Overrides = {
            ContainerOverrides = [{
              Name = "catalogue-sync"
              Environment = [{
                Name      = "RUN_ID"
                "Value.$" = "$$.Execution.Name"
              }]
            }]
          }
        }
        End = true
      }
    }
  })
}

resource "aws_iam_role" "workflow" {
  name = "${var.name_prefix}-catalogue-workflow"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "workflow_run_task" {
  name = "${var.name_prefix}-catalogue-workflow-run-task"
  role = aws_iam_role.workflow.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ecs:RunTask"]
        Resource = aws_ecs_task_definition.catalogue.arn
        Condition = {
          ArnEquals = {
            "ecs:cluster" = aws_ecs_cluster.catalogue.arn
          }
        }
      },
      {
        Effect   = "Allow"
        Action   = ["ecs:DescribeTasks", "ecs:StopTask"]
        Resource = "*"
        Condition = {
          ArnEquals = {
            "ecs:cluster" = aws_ecs_cluster.catalogue.arn
          }
        }
      },
      {
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = [aws_iam_role.task.arn, aws_iam_role.task_execution.arn]
        Condition = {
          StringLike = {
            "iam:PassedToService" = "ecs-tasks.amazonaws.com"
          }
        }
      },
      {
        Effect = "Allow"
        Action = [
          "events:DescribeRule",
          "events:PutRule",
          "events:PutTargets"
        ]
        Resource = "arn:aws:events:${var.aws_region}:${data.aws_caller_identity.current.account_id}:rule/StepFunctionsGetEventsForECSTaskRule"
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "daily" {
  name                = "${var.name_prefix}-catalogue-daily"
  description         = "Start the product catalogue sync once per day in UTC."
  schedule_expression = var.schedule_expression
}

resource "aws_iam_role" "schedule" {
  name = "${var.name_prefix}-catalogue-schedule"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "schedule_start_workflow" {
  name = "${var.name_prefix}-catalogue-start-workflow"
  role = aws_iam_role.schedule.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = aws_sfn_state_machine.catalogue.arn
    }]
  })
}

resource "aws_cloudwatch_event_target" "daily" {
  rule     = aws_cloudwatch_event_rule.daily.name
  arn      = aws_sfn_state_machine.catalogue.arn
  role_arn = aws_iam_role.schedule.arn
}

resource "aws_sns_topic" "alarms" {
  name = "${var.name_prefix}-catalogue-alarms"
}

resource "aws_cloudwatch_metric_alarm" "workflow_failed" {
  alarm_name          = "${var.name_prefix}-catalogue-execution-failed"
  alarm_description   = "At least one scheduled catalogue workflow failed or timed out."
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    StateMachineArn = aws_sfn_state_machine.catalogue.arn
  }
  alarm_actions = [aws_sns_topic.alarms.arn]
}

resource "aws_cloudwatch_metric_alarm" "workflow_timed_out" {
  alarm_name          = "${var.name_prefix}-catalogue-execution-timed-out"
  alarm_description   = "At least one scheduled catalogue workflow timed out."
  namespace           = "AWS/States"
  metric_name         = "ExecutionsTimedOut"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    StateMachineArn = aws_sfn_state_machine.catalogue.arn
  }
  alarm_actions = [aws_sns_topic.alarms.arn]
}

resource "aws_cloudwatch_metric_alarm" "workflow_slow" {
  alarm_name          = "${var.name_prefix}-catalogue-execution-over-4h"
  alarm_description   = "A completed catalogue workflow exceeded the four-hour target."
  namespace           = "AWS/States"
  metric_name         = "ExecutionTime"
  statistic           = "Maximum"
  unit                = "Milliseconds"
  period              = 300
  evaluation_periods  = 1
  threshold           = 14400000
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    StateMachineArn = aws_sfn_state_machine.catalogue.arn
  }
  alarm_actions = [aws_sns_topic.alarms.arn]
}

resource "aws_cloudwatch_metric_alarm" "worker_dead_letter" {
  alarm_name          = "${var.name_prefix}-catalogue-worker-dead-letter"
  alarm_description   = "One or more catalogue batches exceeded SQS worker retries."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    QueueName = aws_sqs_queue.dead_letter.name
  }
  alarm_actions = [aws_sns_topic.alarms.arn]
}
