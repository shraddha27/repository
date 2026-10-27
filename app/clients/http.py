import json
import logging
import sys
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from threading import Lock
from typing import Any, Callable, Protocol

import httpx
from botocore.exceptions import ClientError

from app.aws import client as aws_client

TRANSIENT_STATUSES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
logger = logging.getLogger(__name__)
RequestObserver = Callable[[float, int | None], None]


class RequestRateLimiter(Protocol):
    def wait(self) -> None: ...


class RateLimiter:
    def __init__(self, requests_per_second: float) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self._interval = 1 / requests_per_second
        self._next_request = 0.0
        self._lock = Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = self._next_request - now
            if delay > 0:
                time.sleep(delay)
            self._next_request = max(now, self._next_request) + self._interval


class DynamoDBRateLimiter:
    def __init__(self, table_name: str, requests_per_second: int) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self.client = aws_client("dynamodb")
        self.table_name = table_name
        self._interval_ms = max(1, round(1000 / requests_per_second))

    def wait(self) -> None:
        while True:
            now_ms = time.time_ns() // 1_000_000
            try:
                self.client.update_item(
                    TableName=self.table_name,
                    Key={
                        "pk": {"S": "RATE#WMS"},
                        "sk": {"S": "GLOBAL"},
                    },
                    UpdateExpression="SET next_allowed_at = :next",
                    ConditionExpression=(
                        "attribute_not_exists(next_allowed_at) OR "
                        "next_allowed_at <= :now"
                    ),
                    ExpressionAttributeValues={
                        ":now": {"N": str(now_ms)},
                        ":next": {"N": str(now_ms + self._interval_ms)},
                    },
                )
                return
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") != (
                    "ConditionalCheckFailedException"
                ):
                    raise
                time.sleep(self._interval_ms / 1000)


def _retry_delay(headers: Mapping[str, str], retry_number: int) -> float:
    retry_after = headers.get("Retry-After")
    if retry_after is not None:
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(retry_after)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
            except (TypeError, ValueError, OverflowError):
                pass
    return min(2 ** (retry_number - 1), 30)


def emit_wms_attempt_metric(latency_seconds: float, status_code: int | None) -> None:
    timestamp_ms = time.time_ns() // 1_000_000
    event = {
        "_aws": {
            "Timestamp": timestamp_ms,
            "CloudWatchMetrics": [
                {
                    "Namespace": "ProductCatalogue/Workers",
                    "Dimensions": [["ServiceName", "Operation"]],
                    "Metrics": [
                        {"Name": "WmsRequestAttempts", "Unit": "Count"},
                        {"Name": "WmsRequestErrors", "Unit": "Count"},
                        {"Name": "WmsRequestLatencyMs", "Unit": "Milliseconds"},
                    ],
                }
            ],
        },
        "ServiceName": "catalogue-worker",
        "Operation": "Batch",
        "WmsRequestAttempts": 1,
        "WmsRequestErrors": int(status_code is None or status_code >= 400),
        "WmsRequestLatencyMs": latency_seconds * 1000,
    }
    print(json.dumps(event, separators=(",", ":")), file=sys.stdout, flush=True)


def request_with_retries(
    method: str,
    url: str,
    *,
    client: httpx.Client | None = None,
    rate_limiter: RequestRateLimiter | None = None,
    retry_statuses: set[int] | None = None,
    retry_transport_errors: bool = True,
    request_observer: RequestObserver | None = None,
    **kwargs: Any,
) -> httpx.Response:
    statuses_to_retry = TRANSIENT_STATUSES if retry_statuses is None else retry_statuses
    for retry_number in range(1, MAX_RETRIES + 2):
        try:
            if rate_limiter is not None:
                rate_limiter.wait()
            request_started = time.perf_counter()
            try:
                if client is None:
                    response = httpx.request(method, url, **kwargs)
                else:
                    response = client.request(method, url, **kwargs)
            except httpx.TransportError:
                if request_observer is not None:
                    request_observer(time.perf_counter() - request_started, None)
                raise
            if request_observer is not None:
                request_observer(
                    time.perf_counter() - request_started, response.status_code
                )
        except httpx.TransportError:
            if not retry_transport_errors or retry_number > MAX_RETRIES:
                raise
            delay = min(2 ** (retry_number - 1), 30)
            logger.warning(
                "Retrying transport failure method=%s retry=%s delay_seconds=%.2f",
                method,
                retry_number,
                delay,
            )
            time.sleep(delay)
            continue

        if response.status_code not in statuses_to_retry or retry_number > MAX_RETRIES:
            return response

        delay = _retry_delay(response.headers, retry_number)
        logger.warning(
            "Retrying transient response method=%s status=%s retry=%s delay_seconds=%.2f",
            method,
            response.status_code,
            retry_number,
            delay,
        )
        time.sleep(delay)

    raise RuntimeError("HTTP retry loop exited unexpectedly")
