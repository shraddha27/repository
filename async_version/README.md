# Async catalogue synchronizer

This folder is a separate, native-async implementation. It does not import the
existing `app` package. HTTP uses `httpx.AsyncClient`, AWS uses `aioboto3`,
PostgreSQL uses `asyncpg`, and bounded batch concurrency uses asyncio tasks.

## Install and run locally

Start the local PostgreSQL checkpoint database, then install and run from this
folder with Python 3.11 or newer:

```powershell
docker compose up -d postgres
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m async_app.main
```

By default, the app reads the local Product API at port 8001 and writes its
catalogue under `runtime/` and checkpoints in PostgreSQL at
`postgresql://catalogue:catalogue@localhost:5432/catalogue_async`. Start the
existing mock APIs from the repository root before running it. Configure
`PRODUCT_API_URL`, `WAREHOUSE_API_URL`, API keys, `EXPORT_DIR`, and
`DATABASE_URL` through environment variables as needed.

`WMS_CONCURRENCY` controls how many asyncio tasks can have WMS requests in
flight (default 16). The shared limiter still caps WMS traffic at 20
requests/second.
PIM traffic remains limited to 10 page requests/second, with pages of up to 500
products and WMS batches of up to 100.

The application does not create worker threads, use SQS, or launch a separate
worker process. It schedules at most `WMS_CONCURRENCY` batch tasks at a time.
HTTP and AWS calls use native async clients. PostgreSQL uses `asyncpg`; local
CSV reads/writes and SQLite-based duplicate-SKU indexing are synchronous file
operations on the event loop so no file-I/O thread pool is created.
The application does not explicitly start worker threads; networking runtimes
may use internal resolver helpers, so this should not be treated as a guarantee
that the Python process will never create any thread.

## AWS mode

The package supports S3 exports and DynamoDB checkpoints without SQS. Set
`S3_BUCKET` and `DYNAMODB_TABLE`, plus AWS region/credentials, API URLs, and
keys. `AWS_ENDPOINT_URL` can target LocalStack. Do not set `SQS_QUEUE_URL`.
The DynamoDB table must have string partition and sort keys named `pk` and `sk`.
Run one producer task using
`python -m async_app.main`; it fetches the catalogue and submits batches
asynchronously from that task. Build this folder's Dockerfile and deploy it as
a separate ECS task definition with access to the configured bucket and table.

## One-million-product target

At current limits, one million products require at least 2,000 PIM page
requests (about 3 minutes 20 seconds) and 10,000 WMS batch requests (about 8
minutes 20 seconds). The ideal minimum is therefore about 11 minutes 40
seconds, before API latency, backoff, export, and checkpoint overhead.
The async implementation overlaps network waits and can help keep requests in
flight; it cannot exceed the API limits or guarantee a 30-minute completion.
Benchmark against representative API latency and AWS capacity before treating
the target as met.

## Important safety behavior

WMS 429s are retried with `Retry-After` support. A remote WMS 5xx or transport
failure is marked ambiguous and is not automatically replayed because the WMS
may have accepted the batch. Completed batches are checkpointed and skipped on
resume. A restarted in-flight batch is ambiguous and requires reconciliation.
