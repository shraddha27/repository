import httpx
import pytest

from app.clients import http


def test_rate_limiter_spaces_requests(monkeypatch):
    clock = [0.0]
    timestamps = []
    monkeypatch.setattr(http.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        http.time,
        "sleep",
        lambda delay: clock.__setitem__(0, clock[0] + delay),
    )
    limiter = http.RateLimiter(10)

    for _ in range(3):
        limiter.wait()
        timestamps.append(clock[0])

    assert timestamps == pytest.approx([0.0, 0.1, 0.2])


def test_request_with_retries_honors_retry_after(monkeypatch):
    responses = iter(
        [
            httpx.Response(429, headers={"Retry-After": "2"}),
            httpx.Response(200),
        ]
    )
    delays = []
    monkeypatch.setattr(http.httpx, "request", lambda *args, **kwargs: next(responses))
    monkeypatch.setattr(http.time, "sleep", delays.append)

    response = http.request_with_retries("GET", "https://example.test")

    assert response.status_code == 200
    assert delays == [2.0]


def test_send_products_sends_bounded_batches(monkeypatch):
    from app.clients import warehouse_api

    batches = []

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return {"accepted": []}

    def request(method, url, **kwargs):
        batches.append(kwargs["json"]["products"])
        return Response()

    monkeypatch.setattr(warehouse_api, "BATCH_SIZE", 2)
    monkeypatch.setattr(warehouse_api, "request_with_retries", request)

    result = warehouse_api.send_products({"sku": str(i)} for i in range(5))

    assert [len(batch) for batch in batches] == [2, 2, 1]
    assert len(result) == 3


def test_warehouse_post_does_not_retry_ambiguous_failures(monkeypatch):
    from app.clients import warehouse_api

    request_options = {}

    class Response:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return {"accepted": ["P1"], "rejected": []}

    def request(method, url, **kwargs):
        request_options.update(kwargs)
        return Response()

    monkeypatch.setattr(warehouse_api, "request_with_retries", request)

    warehouse_api.send_batch([{"sku": "P1"}])

    assert request_options["retry_statuses"] == {429, 500}
    assert request_options["retry_transport_errors"] is False


def test_warehouse_only_retries_500_for_local_mock():
    from app.clients.warehouse_api import _retry_statuses

    assert _retry_statuses("http://localhost:8002") == {429, 500}
    assert _retry_statuses("https://warehouse.example.com") == {429}
