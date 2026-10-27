import os


def _bounded_positive_int(name: str, default: int, maximum: int) -> int:
    value = os.getenv(name, str(default))
    try:
        parsed = int(value)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error

    if not 1 <= parsed <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")
    return parsed


PRODUCT_API_URL = os.getenv("PRODUCT_API_URL", "http://localhost:8001")
WAREHOUSE_API_URL = os.getenv("WAREHOUSE_API_URL", "http://localhost:8002")
PRODUCT_API_KEY = os.getenv("PRODUCT_API_KEY", "challenge-product-key")
WAREHOUSE_API_KEY = os.getenv("WAREHOUSE_API_KEY", "challenge-warehouse-key")
EXPORT_DIR = os.getenv("EXPORT_DIR", "./runtime/exports")
STATE_DB = os.getenv("STATE_DB", "./runtime/state.sqlite3")
S3_BUCKET = os.getenv("S3_BUCKET")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
RUN_ID = os.getenv("RUN_ID")
PAGE_SIZE = _bounded_positive_int("PAGE_SIZE", 500, 500)
BATCH_SIZE = _bounded_positive_int("BATCH_SIZE", 100, 100)
WMS_WORKERS = _bounded_positive_int("WMS_WORKERS", 4, 32)
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
WORKER_POLL_SECONDS = _bounded_positive_int("WORKER_POLL_SECONDS", 20, 20)
WORKER_VISIBILITY_TIMEOUT = _bounded_positive_int(
    "WORKER_VISIBILITY_TIMEOUT", 300, 43200
)
RUN_COMPLETION_TIMEOUT_SECONDS = _bounded_positive_int(
    "RUN_COMPLETION_TIMEOUT_SECONDS", 14400, 86400
)
