variable "aws_region" {
  type        = string
  description = "AWS region for the integration resources."
}

variable "name_prefix" {
  type        = string
  description = "Short environment prefix used in resource names."
  default     = "product-catalogue"
}

variable "bucket_name" {
  type        = string
  description = "Globally unique S3 bucket name for retained catalogue exports."
}

variable "container_image" {
  type        = string
  description = "Immutable ECR image URI containing this repository's application."
}

variable "product_api_url" {
  type        = string
  description = "Reachable base URL for the external PIM API."
}

variable "warehouse_api_url" {
  type        = string
  description = "Reachable base URL for the external WMS API."
}

variable "subnet_ids" {
  type        = list(string)
  description = "Private subnet IDs with NAT egress to the PIM and WMS APIs."
  default     = []
}

variable "security_group_ids" {
  type        = list(string)
  description = "Task security groups allowing required HTTPS egress."
  default     = []
}

variable "product_api_key_secret_arn" {
  type        = string
  description = "Secrets Manager ARN containing the PIM API key."
  default     = ""
}

variable "warehouse_api_key_secret_arn" {
  type        = string
  description = "Secrets Manager ARN containing the WMS API key."
  default     = ""
}

variable "localstack_endpoint" {
  type        = string
  description = "Optional LocalStack endpoint. Set only for local development."
  default     = null
  nullable    = true
}

variable "task_aws_endpoint_url" {
  type        = string
  description = "LocalStack endpoint reachable from the ECS task container."
  default     = "http://host.docker.internal:4566"
}

variable "localstack_product_api_key" {
  type        = string
  description = "Mock PIM API key stored in LocalStack Secrets Manager."
  default     = "challenge-product-key"
}

variable "localstack_warehouse_api_key" {
  type        = string
  description = "Mock WMS API key stored in LocalStack Secrets Manager."
  default     = "challenge-warehouse-key"
}

variable "schedule_expression" {
  type        = string
  description = "EventBridge schedule expression (UTC)."
  default     = "cron(0 2 * * ? *)"
}
