from collections.abc import Iterator
from typing import Any

import httpx

from app.clients.http import RateLimiter, request_with_retries
from app.config import PAGE_SIZE, PRODUCT_API_KEY, PRODUCT_API_URL

_RATE_LIMITER = RateLimiter(10)


def iter_products() -> Iterator[dict[str, Any]]:
    page = 1
    with httpx.Client() as client:
        while True:
            response = request_with_retries(
                "GET",
                f"{PRODUCT_API_URL}/products",
                client=client,
                params={"page": page, "page_size": PAGE_SIZE},
                headers={"X-API-Key": PRODUCT_API_KEY},
                rate_limiter=_RATE_LIMITER,
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

            yield from payload["products"]
            if not payload["has_next"]:
                return

            next_page = payload.get("next_page")
            if type(next_page) is not int or next_page <= page:
                raise ValueError("Product API returned an invalid next_page")
            page = next_page


def fetch_all_products() -> list[dict[str, Any]]:
    return list(iter_products())
