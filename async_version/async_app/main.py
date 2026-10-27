import argparse
import asyncio
import json
import logging
import os
import re
import tempfile
import time
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx

from async_app.aws import open_client
from async_app.catalogue import export_catalogue
from async_app.checkpoints import CheckpointStore
from async_app.config import (
    DATABASE_URL,
    DYNAMODB_TABLE,
    RUN_COMPLETION_TIMEOUT_SECONDS,
    RUN_ID,
    S3_BUCKET,
    WMS_CONCURRENCY,
)
from async_app.dynamo_checkpoints import DynamoCheckpoints
from async_app.http_clients import (
    DynamoDBRateLimiter,
    ProductApi,
    WarehouseApi,
)
from async_app.pipeline import sync_catalogue
from async_app.postgres_checkpoints import PostgresCheckpoints
from async_app.s3_store import AsyncS3Store

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


async def run(run_id: str | None = None) -> dict[str, object]:
    run_id = run_id or RUN_ID or uuid4().hex
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", run_id):
        raise ValueError("run_id must contain 1-80 letters, digits, hyphens, or underscores")
    if bool(S3_BUCKET) != bool(DYNAMODB_TABLE):
        raise ValueError("S3_BUCKET and DYNAMODB_TABLE must be configured together")
    started = time.monotonic()
    async with AsyncExitStack() as stack:
        dynamodb = None
        if DYNAMODB_TABLE:
            dynamodb = await stack.enter_async_context(
                open_client("dynamodb")
            )
            checkpoints = DynamoCheckpoints(dynamodb, DYNAMODB_TABLE)
        else:
            checkpoints = PostgresCheckpoints(DATABASE_URL)
            await checkpoints.initialize()
            stack.push_async_callback(checkpoints.close)

        await checkpoints.acquire_run_lock(run_id)
        try:
            return await _run_locked(run_id, started, checkpoints, dynamodb)
        finally:
            await checkpoints.release_run_lock(run_id)


async def _run_locked(
    run_id: str,
    started: float,
    checkpoints: CheckpointStore,
    dynamodb: Any | None,
) -> dict[str, object]:
    status = await checkpoints.start_run(run_id)
    if status == "COMPLETED":
        return await checkpoints.run_summary(run_id)
    if status != "RUNNING":
        raise RuntimeError(
            f"Run {run_id} is {status}; investigate its checkpoint before retrying"
        )

    store = AsyncS3Store(S3_BUCKET)
    export_key = await checkpoints.get_export_key(run_id)
    local_export: str | None = None
    downloaded_export: str | None = None
    try:
        async with httpx.AsyncClient(
            limits=httpx.Limits(
                max_connections=WMS_CONCURRENCY * 2,
                max_keepalive_connections=WMS_CONCURRENCY,
            )
        ) as http:
            product_api = ProductApi(http)
            if export_key is None:
                export_remaining = RUN_COMPLETION_TIMEOUT_SECONDS - (
                    time.monotonic() - started
                )
                if export_remaining <= 0:
                    raise TimeoutError("Catalogue run exceeded its time limit")
                async with asyncio.timeout(export_remaining):
                    local_export = await export_catalogue(product_api, run_id)
                export_key = f"catalogue/{run_id}/catalogue.csv"
                await store.put_file(local_export, export_key)
                await checkpoints.set_export_key(run_id, export_key)

            if S3_BUCKET:
                with tempfile.NamedTemporaryFile(
                    prefix=f"catalogue-{run_id}-",
                    suffix=".csv",
                    delete=False,
                ) as temporary:
                    downloaded_export = temporary.name
                catalogue_path = await store.download_file(
                    export_key, downloaded_export
                )
            else:
                catalogue_path = await store.get_file(export_key)

            limiter = (
                DynamoDBRateLimiter(dynamodb, DYNAMODB_TABLE, 20)
                if dynamodb is not None
                else None
            )
            warehouse_api = WarehouseApi(http, limiter)
            remaining = RUN_COMPLETION_TIMEOUT_SECONDS - (
                time.monotonic() - started
            )
            if remaining <= 0:
                raise TimeoutError("Catalogue run exceeded its time limit")
            async with asyncio.timeout(remaining):
                await sync_catalogue(
                    str(catalogue_path),
                    run_id,
                    checkpoints,
                    warehouse_api,
                )

        await checkpoints.finish_run(run_id)
    except Exception as error:
        summary = await checkpoints.run_summary(run_id)
        if summary["status"] == "RUNNING":
            await checkpoints.fail_run(run_id, str(error))
        raise
    finally:
        if local_export is not None:
            Path(local_export).unlink(missing_ok=True)
        if downloaded_export is not None:
            Path(downloaded_export).unlink(missing_ok=True)

    summary = await checkpoints.run_summary(run_id)
    logger.info(
        "Async catalogue run complete run_id=%s accepted=%s rejected=%s",
        run_id,
        summary["accepted"],
        summary["rejected"],
    )
    return summary


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Synchronize a product catalogue using async I/O"
    )
    parser.add_argument("--run-id", help="Resume a prior run with this ID")
    args = parser.parse_args()
    print(json.dumps(await run(args.run_id), sort_keys=True))


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
