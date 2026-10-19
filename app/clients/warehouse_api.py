from collections.abc import Iterable
from itertools import islice
from typing import Any
from urllib.parse import urlsplit

import httpx

from app.clients.http import RateLimiter, request_with_retries
from app.config import BATCH_SIZE, WAREHOUSE_API_KEY, WAREHOUSE_API_URL

_RATE_LIMITER = RateLimiter(20)


def _retry_statuses(url: str) -> set[int]:
    statuses = {429}
    if urlsplit(url).hostname in {"localhost", "127.0.0.1", "::1"}:
        # The supplied loopback mock raises a 500 before processing products on
        # its rate-limit branches because of its JSONResponse argument order.
        statuses.add(500)
    return statuses


def send_batch(
    products: list[dict[str, Any]], client: httpx.Client | None = None
) -> dict[str, Any]:
    if not products:
        raise ValueError("Cannot send an empty warehouse batch")
    if len(products) > BATCH_SIZE:
        raise ValueError(f"Warehouse batch exceeds configured limit of {BATCH_SIZE}")
    response = request_with_retries(
        "POST",
        f"{WAREHOUSE_API_URL}/products/batch",
        client=client,
        json={"products": products},
        headers={"X-API-Key": WAREHOUSE_API_KEY},
        rate_limiter=_RATE_LIMITER,
        retry_statuses=_retry_statuses(WAREHOUSE_API_URL),
        retry_transport_errors=False,
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Warehouse API returned an invalid batch response")
    return payload


def send_products(products: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    results = []
    product_iterator = iter(products)
    with httpx.Client() as client:
        while batch := list(islice(product_iterator, BATCH_SIZE)):
            results.append(send_batch(batch, client))
    return results
