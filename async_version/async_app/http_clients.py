import asyncio
import logging
import time
from collections.abc import AsyncIterator, Mapping
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlsplit

import httpx
from botocore.exceptions import ClientError

from async_app.config import (
    MAX_RETRIES,
    PAGE_SIZE,
    PRODUCT_API_KEY,
    PRODUCT_API_URL,
    PRODUCT_REQUESTS_PER_SECOND,
    WAREHOUSE_API_KEY,
    WAREHOUSE_API_URL,
    WAREHOUSE_REQUESTS_PER_SECOND,
)

logger = logging.getLogger(__name__)


class AsyncRateLimiter:
    def __init__(self, requests_per_second: float) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self._interval = 1 / requests_per_second
        self._next_request = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            delay = self._next_request - now
            if delay > 0:
                await asyncio.sleep(delay)
                now = loop.time()
            self._next_request = max(now, self._next_request) + self._interval


class DynamoDBRateLimiter:
    def __init__(self, client: Any, table_name: str, requests_per_second: int):
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self._client = client
        self._table_name = table_name
        self._interval_ms = max(1, round(1000 / requests_per_second))

    async def wait(self) -> None:
        while True:
            now_ms = time.time_ns() // 1_000_000
            try:
                await self._client.update_item(
                    TableName=self._table_name,
                    Key={"pk": {"S": "RATE#WMS"}, "sk": {"S": "GLOBAL"}},
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
                await asyncio.sleep(self._interval_ms / 1000)


async def _retry_delay(
    headers: Mapping[str, str], retry_number: int
) -> float:
    retry_after = headers.get("Retry-After")
    if retry_after is not None:
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(retry_after)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                return max(
                    0.0,
                    (retry_at - datetime.now(timezone.utc)).total_seconds(),
                )
            except (TypeError, ValueError, OverflowError):
                logger.warning("Ignoring invalid Retry-After header: %r", retry_after)
    return min(2 ** (retry_number - 1), 30)


async def request_with_retries(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    limiter: Any,
    retry_statuses: set[int],
    retry_transport_errors: bool,
    **kwargs: Any,
) -> httpx.Response:
    for retry_number in range(1, MAX_RETRIES + 2):
        await limiter.wait()
        try:
            response = await client.request(method, url, **kwargs)
        except httpx.TransportError:
            if not retry_transport_errors or retry_number > MAX_RETRIES:
                raise
            delay = min(2 ** (retry_number - 1), 30)
            logger.warning(
                "Retrying transport failure method=%s retry=%s delay=%.2fs",
                method,
                retry_number,
                delay,
            )
            await asyncio.sleep(delay)
            continue

        if response.status_code not in retry_statuses or retry_number > MAX_RETRIES:
            return response
        delay = await _retry_delay(response.headers, retry_number)
        logger.warning(
            "Retrying HTTP response method=%s status=%s retry=%s delay=%.2fs",
            method,
            response.status_code,
            retry_number,
            delay,
        )
        await asyncio.sleep(delay)
    raise RuntimeError("HTTP retry loop exited unexpectedly")


class ProductApi:
    def __init__(self, client: httpx.AsyncClient):
        self._client = client
        self._limiter = AsyncRateLimiter(PRODUCT_REQUESTS_PER_SECOND)

    async def iter_products(self) -> AsyncIterator[dict[str, Any]]:
        page = 1
        while True:
            response = await request_with_retries(
                self._client,
                "GET",
                f"{PRODUCT_API_URL}/products",
                limiter=self._limiter,
                retry_statuses={429, 500, 502, 503, 504},
                retry_transport_errors=True,
                params={"page": page, "page_size": PAGE_SIZE},
                headers={"X-API-Key": PRODUCT_API_KEY},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            if (
                not isinstance(payload, dict)
                or not isinstance(payload.get("products"), list)
                or not isinstance(payload.get("has_next"), bool)
            ):
                raise ValueError("Product API returned an invalid products page")
            for product in payload["products"]:
                if not isinstance(product, dict):
                    raise ValueError("Product API returned an invalid product")
                yield product
            if not payload["has_next"]:
                return
            next_page = payload.get("next_page")
            if type(next_page) is not int or next_page <= page:
                raise ValueError("Product API returned an invalid next_page")
            page = next_page


class WarehouseApi:
    def __init__(
        self, client: httpx.AsyncClient, limiter: Any | None = None
    ) -> None:
        self._client = client
        self._limiter = limiter or AsyncRateLimiter(
            WAREHOUSE_REQUESTS_PER_SECOND
        )
        self._retry_statuses = {429}
        if urlsplit(WAREHOUSE_API_URL).hostname in {
            "localhost",
            "127.0.0.1",
            "::1",
        }:
            self._retry_statuses.add(500)

    async def send_batch(
        self, products: list[dict[str, Any]]
    ) -> dict[str, Any]:
        if not products:
            raise ValueError("Cannot send an empty warehouse batch")
        response = await request_with_retries(
            self._client,
            "POST",
            f"{WAREHOUSE_API_URL}/products/batch",
            limiter=self._limiter,
            retry_statuses=self._retry_statuses,
            retry_transport_errors=False,
            json={"products": products},
            headers={"X-API-Key": WAREHOUSE_API_KEY},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Warehouse API returned an invalid batch response")
        return payload
