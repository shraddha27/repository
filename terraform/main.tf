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
      cloudwatch     = endpoints.value
      dynamodb       = endpoints.value
      ec2            = endpoints.value
      ecr            = endpoints.value
      ecs            = endpoints.value
      events         = endpoints.value
      iam            = endpoints.value
      logs           = endpoints.value
      s3             = endpoints.value
      secretsmanager = endpoints.value
      sfn            = endpoints.value
      sns            = endpoints.value
      sts            = endpoints.value
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

resource "aws_cloudwatch_log_group" "task" {
  name              = "/ecs/${var.name_prefix}-catalogue-sync"
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
      }
    ]
  })
}

resource "aws_ecs_cluster" "catalogue" {
  name = "${var.name_prefix}-catalogue"
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
      { name = "BATCH_SIZE", value = "100" }
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
  alarm_name          = "${var.name_prefix}-catalogue-execution-over-30m"
  alarm_description   = "A completed catalogue workflow exceeded the 30-minute target."
  namespace           = "AWS/States"
  metric_name         = "ExecutionTime"
  statistic           = "Maximum"
  unit                = "Milliseconds"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1800000
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    StateMachineArn = aws_sfn_state_machine.catalogue.arn
  }
  alarm_actions = [aws_sns_topic.alarms.arn]
}
