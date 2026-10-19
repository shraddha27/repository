# Product Catalogue Integration

This repository contains the existing implementation of a product catalogue integration.

The implementation currently works for the existing small-scale environment.

You have been asked to take ownership of the solution and prepare it for the next stage of the platform.

See the assessment document supplied separately for the business context, requirements and deliverables.

## Running locally

Prerequisites: Docker Desktop, Python 3.11+, Terraform 1.5+.

Start the mock external systems:

```bash
docker compose up --build
```

Product API: http://localhost:8001 (Swagger: /docs)
Warehouse API: http://localhost:8002 (Swagger: /docs)

Install Python dependencies:

```bash
python -m venv .venv
# Windows: .venv\\Scripts\\activate
# Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
pytest
python -m app.main
```

The default local run stores exports under `runtime/s3/` and run checkpoints in
`runtime/state.sqlite3`. Each run receives a unique ID. To resume a run after a
restart, provide its ID with `python -m app.main --run-id <run-id>`. A run with
an ambiguous WMS outcome is deliberately blocked and must be reconciled before
it can be retried.

### Using local AWS services with LocalStack

With the `localstack` Compose profile, LocalStack provides the AWS services
used by the Terraform deployment, including S3, DynamoDB, ECS, Step Functions,
EventBridge, IAM, Secrets Manager, CloudWatch, CloudWatch Logs, and SNS. ECS
uses Docker Desktop's Docker socket to run the task container locally. The
normal Compose setup for the mock APIs does not need the LocalStack profile.
The current LocalStack image requires an auth token for this service set.
Obtain one from your LocalStack account and enter it into the PowerShell
session without committing it to the repository:

```powershell
$secureToken = Read-Host "LocalStack auth token" -AsSecureString
$env:LOCALSTACK_AUTH_TOKEN = [System.Net.NetworkCredential]::new("", $secureToken).Password
```

Then start the mocks and LocalStack from the repository root:
  
```powershell
docker compose --profile localstack up --build -d
```

Build the application image that the local ECS task will run:

```powershell
docker build -t product-catalogue-integration:local .
```

Configure Terraform to target LocalStack and apply the same infrastructure
defined for AWS. Terraform creates the local S3 bucket, DynamoDB table,
Secrets Manager values, ECS resources, Step Functions state machine,
EventBridge schedule, CloudWatch resources, and SNS topic. The VPC and security
group created in this mode are local-development-only:

```powershell
$env:AWS_ACCESS_KEY_ID = "test"
$env:AWS_SECRET_ACCESS_KEY = "test"
$env:AWS_DEFAULT_REGION = "us-east-1"
terraform -chdir=terraform init
terraform -chdir=terraform apply -auto-approve `
  -var="aws_region=us-east-1" `
  -var="bucket_name=product-catalogue-local" `
  -var="container_image=product-catalogue-integration:local" `
  -var="product_api_url=http://host.docker.internal:8001" `
  -var="warehouse_api_url=http://host.docker.internal:8002" `
  -var="localstack_endpoint=http://localhost:4566" `
  -var="task_aws_endpoint_url=http://host.docker.internal:4566"
```

To run the integration directly from the host against LocalStack S3 and
DynamoDB (without ECS), set the application environment and run it:

```powershell
$env:S3_BUCKET = "product-catalogue-local"
$env:DYNAMODB_TABLE = "product-catalogue-catalogue-runs"
$env:AWS_ENDPOINT_URL = "http://localhost:4566"
$env:AWS_ACCESS_KEY_ID = "test"
$env:AWS_SECRET_ACCESS_KEY = "test"
$env:AWS_DEFAULT_REGION = "us-east-1"
.\.venv\Scripts\python.exe -m app.main --run-id localstack-test-001
```

To exercise the workflow immediately, start a Step Functions execution using
LocalStack's bundled `awslocal` CLI:

```powershell
docker compose exec localstack awslocal stepfunctions start-execution `
  --state-machine-arn arn:aws:states:us-east-1:000000000000:stateMachine:product-catalogue-catalogue-sync
```

The workflow runs the ECS task and passes its execution name to the app as the
run ID. Use the execution ARN from the command's response to inspect it:

```powershell
docker compose exec localstack awslocal stepfunctions describe-execution `
  --execution-arn <execution-arn>
```

LocalStack health is available at `http://localhost:4566/_localstack/health`.
`docker compose down` stops the services while preserving the named LocalStack
data volume. To intentionally remove only that volume, stop Compose and run
`docker volume rm repository_localstack-data` (replace `repository` with the
Compose project name if it differs).

For real AWS mode, set `S3_BUCKET` and `DYNAMODB_TABLE` and do not set
`AWS_ENDPOINT_URL` or Terraform's `localstack_endpoint`; the process
role/credentials must allow access to those resources. `PRODUCT_API_KEY` and
`WAREHOUSE_API_KEY` are read from the environment, so inject them securely
(Terraform configures ECS Secrets Manager references). `PRODUCT_API_URL`,
`WAREHOUSE_API_URL`, and `EXPORT_DIR` can also be overridden.

Terraform in `terraform/` defines the scheduled Step Functions workflow, Fargate
task, private S3 export bucket with 90-day expiry, DynamoDB checkpoints, and
CloudWatch alarms. It requires an immutable container image, private subnet and
security group IDs with NAT egress, external PIM/WMS URLs, and Secrets Manager ARNs; see
`terraform/variables.tf`. Run `terraform init`, `terraform validate`, and
`terraform plan` from that directory after supplying those environment-specific
values. Build the application image, publish it to ECR, and pass its immutable
image URI as `container_image`:

```bash
docker build -t product-catalogue-integration:local .
```

The catalogue export is written incrementally to a temporary CSV and published
only after all product pages are retrieved. PIM calls are limited to 10 requests
per second; WMS calls are limited to 20 requests per second and sent in batches
of at most 100. Both APIs' 429 responses honor `Retry-After`; the PIM client also
retries transient 5xx and transport failures. WMS 5xx/transport failures are
treated as ambiguous and are not retried automatically because the WMS may have
accepted the batch before the response was lost. For the supplied loopback WMS
mock only, HTTP 500 is retried because its simulated rate-limit error raises
before processing the batch.

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the architecture and trade-offs and
[UNFINISHED.md](./UNFINISHED.md) for known gaps, validation needs, and risks.

## Important

This is an intentionally imperfect existing solution. You are not expected to preserve the current architecture. You may refactor, replace or add components where justified.

The mock external APIs are part of the test environment. Do not modify their behaviour.
