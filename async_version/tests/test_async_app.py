import asyncio
import csv

import httpx

from async_app import http_clients, pipeline
from async_app.dynamo_checkpoints import DynamoCheckpoints
from async_app.http_clients import AsyncRateLimiter, ProductApi
from async_app.postgres_checkpoints import PostgresCheckpoints


def test_dynamo_batch_number_reads_legacy_key():
    assert DynamoCheckpoints._batch_number({"sk": "BATCH#00000012"}) == 12


def test_async_entrypoint_imports():
    import async_app.main

    assert callable(async_app.main.main)


def test_async_rate_limiter_spaces_concurrent_calls():
    async def exercise():
        limiter = AsyncRateLimiter(50)
        starts = []

        async def take_slot():
            await limiter.wait()
            starts.append(asyncio.get_running_loop().time())

        await asyncio.gather(*(take_slot() for _ in range(4)))
        return starts

    starts = sorted(asyncio.run(exercise()))

    assert len(starts) == 4
    assert all(later - earlier >= 0.015 for earlier, later in zip(starts, starts[1:]))


def test_product_api_reads_pages_incrementally(monkeypatch):
    monkeypatch.setattr(http_clients, "PRODUCT_REQUESTS_PER_SECOND", 1000)
    requests = []

    async def respond(request):
        requests.append(request)
        page = int(request.url.params["page"])
        if page == 1:
            return httpx.Response(
                200,
                json={
                    "products": [{"id": "p1"}],
                    "has_next": True,
                    "next_page": 2,
                },
            )
        return httpx.Response(
            200,
            json={"products": [{"id": "p2"}], "has_next": False},
        )

    async def exercise():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(respond)
        ) as client:
            return [product async for product in ProductApi(client).iter_products()]

    products = asyncio.run(exercise())

    assert products == [{"id": "p1"}, {"id": "p2"}]
    assert len(requests) == 2
    assert all(request.url.params["page_size"] == "500" for request in requests)


def test_postgres_checkpoints_use_async_driver(monkeypatch):
    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return False

    class Connection:
        async def execute(self, query, *args):
            self.last_query = query
            return "OK"

        async def fetchval(self, query, *args):
            if "INSERT INTO run_lock" in query:
                return args[0]
            if "SELECT status FROM runs" in query:
                return "RUNNING"
            if "SELECT EXISTS" in query:
                return False
            raise AssertionError(f"Unexpected query: {query}")

        def transaction(self):
            return Transaction()

    class Acquire:
        async def __aenter__(self):
            return connection

        async def __aexit__(self, *_):
            return False

    class Pool:
        def acquire(self):
            return Acquire()

        async def close(self):
            pass

    connection = Connection()

    async def create_pool(*args, **kwargs):
        return Pool()

    monkeypatch.setattr(
        "async_app.postgres_checkpoints.asyncpg.create_pool", create_pool
    )

    async def exercise():
        checkpoints = PostgresCheckpoints("postgresql://test")
        await checkpoints.initialize()
        await checkpoints.acquire_run_lock("run-1")
        status = await checkpoints.start_run("run-1")
        await checkpoints.close()
        return status

    assert asyncio.run(exercise()) == "RUNNING"


def test_pipeline_runs_bounded_async_requests(tmp_path, monkeypatch):
    csv_path = tmp_path / "products.csv"
    products = [
        {
            "id": f"p{number}",
            "name": f"Product {number}",
            "category": "HOME",
            "price": "10",
            "currency": "INR",
            "updated_at": "2026-01-01T00:00:00Z",
        }
        for number in range(6)
    ]

    with csv_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=products[0].keys())
        writer.writeheader()
        writer.writerows(products)
    monkeypatch.setattr(pipeline, "BATCH_SIZE", 1)

    class Checkpoints:
        def __init__(self):
            self.started = set()
            self.completed = set()

        async def completed_batches(self, run_id):
            return set()

        async def begin_batch(self, run_id, number, batch):
            self.started.add(number)

        async def complete_batch(self, run_id, number, accepted, rejected):
            self.completed.add(number)

        async def mark_batch(self, run_id, number, status, error):
            raise AssertionError(f"Unexpected batch error: {status}: {error}")

    class Warehouse:
        def __init__(self):
            self.active = 0
            self.max_active = 0
            self.guard = asyncio.Lock()

        async def send_batch(self, batch):
            async with self.guard:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
            await asyncio.sleep(0.02)
            async with self.guard:
                self.active -= 1
            return {"accepted": [batch[0]["sku"]], "rejected": []}

    async def exercise():
        checkpoints = Checkpoints()
        warehouse = Warehouse()
        await pipeline.sync_catalogue(
            str(csv_path), "run-1", checkpoints, warehouse, concurrency=3
        )
        return checkpoints, warehouse

    checkpoints, warehouse = asyncio.run(exercise())

    assert checkpoints.started == set(range(1, 7))
    assert checkpoints.completed == set(range(1, 7))
    assert 1 < warehouse.max_active <= 3
