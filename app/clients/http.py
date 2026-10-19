import logging
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from threading import Lock
from typing import Any

import httpx

TRANSIENT_STATUSES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
logger = logging.getLogger(__name__)


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


def request_with_retries(
    method: str,
    url: str,
    *,
    client: httpx.Client | None = None,
    rate_limiter: RateLimiter | None = None,
    retry_statuses: set[int] | None = None,
    retry_transport_errors: bool = True,
    **kwargs: Any,
) -> httpx.Response:
    statuses_to_retry = TRANSIENT_STATUSES if retry_statuses is None else retry_statuses
    for retry_number in range(1, MAX_RETRIES + 2):
        try:
            if rate_limiter is not None:
                rate_limiter.wait()
            if client is None:
                response = httpx.request(method, url, **kwargs)
            else:
                response = client.request(method, url, **kwargs)
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
