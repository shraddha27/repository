import os


def _positive_int(name: str, default: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")
    return value


PRODUCT_API_URL = os.getenv("PRODUCT_API_URL", "http://localhost:8001")
WAREHOUSE_API_URL = os.getenv("WAREHOUSE_API_URL", "http://localhost:8002")
PRODUCT_API_KEY = os.getenv("PRODUCT_API_KEY", "challenge-product-key")
WAREHOUSE_API_KEY = os.getenv("WAREHOUSE_API_KEY", "challenge-warehouse-key")
EXPORT_DIR = os.getenv("EXPORT_DIR", "./runtime/exports")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://catalogue:catalogue@localhost:5432/catalogue_async",
)
S3_BUCKET = os.getenv("S3_BUCKET")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
RUN_ID = os.getenv("RUN_ID")
PAGE_SIZE = _positive_int("PAGE_SIZE", 500, 500)
BATCH_SIZE = _positive_int("BATCH_SIZE", 100, 100)
WMS_CONCURRENCY = _positive_int("WMS_CONCURRENCY", 16, 256)
RUN_COMPLETION_TIMEOUT_SECONDS = _positive_int(
    "RUN_COMPLETION_TIMEOUT_SECONDS", 1800, 86400
)
PRODUCT_REQUESTS_PER_SECOND = 10
WAREHOUSE_REQUESTS_PER_SECOND = 20
MAX_RETRIES = 5
