import asyncio
import csv
import logging
import sqlite3
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx

from async_app.config import BATCH_SIZE, WMS_CONCURRENCY
from async_app.checkpoints import CheckpointStore
from async_app.http_clients import WarehouseApi

logger = logging.getLogger(__name__)


def transform(row: dict[str, str]) -> dict[str, Any]:
    return {
        "sku": row["id"],
        "description": row["name"],
        "selling_price": float(row["price"]),
        "currency": row["currency"],
        "category_code": row["category"],
        "source_updated_at": row["updated_at"],
    }


def validate_result(
    result: dict[str, Any], products: list[dict[str, Any]]
) -> tuple[list[str], list[dict[str, str]]]:
    accepted = result.get("accepted")
    rejected = result.get("rejected")
    if not isinstance(accepted, list) or not isinstance(rejected, list):
        raise ValueError("Warehouse response must include accepted and rejected arrays")
    if not all(isinstance(sku, str) for sku in accepted):
        raise ValueError("Warehouse accepted array contains an invalid SKU")

    expected = {product["sku"] for product in products}
    outcomes = list(accepted)
    normalized_rejections = []
    for item in rejected:
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("sku"), str)
            or not isinstance(item.get("reason"), str)
        ):
            raise ValueError("Warehouse rejected array contains an invalid result")
        outcomes.append(item["sku"])
        normalized_rejections.append({"sku": item["sku"], "reason": item["reason"]})
    if len(outcomes) != len(set(outcomes)) or set(outcomes) != expected:
        raise ValueError("Warehouse response did not account for each submitted SKU exactly once")
    return accepted, normalized_rejections


async def iter_batches(
    path: str, completed: set[int]
) -> AsyncIterator[tuple[int, list[dict[str, Any]]]]:
    with tempfile.TemporaryDirectory(prefix="async-catalogue-skus-") as directory:
        index = sqlite3.connect(Path(directory) / "seen.sqlite3")
        try:
            index.execute("PRAGMA journal_mode = OFF")
            index.execute("PRAGMA synchronous = OFF")
            index.execute("CREATE TABLE seen_skus (sku TEXT PRIMARY KEY)")
            with open(path, newline="", encoding="utf-8") as source:
                reader = csv.DictReader(source)
                batch_number = 1
                batch: list[dict[str, Any]] = []
                indexed = 0
                for row in reader:
                    product = transform(row)
                    try:
                        index.execute(
                            "INSERT INTO seen_skus (sku) VALUES (?)",
                            (product["sku"],),
                        )
                    except sqlite3.IntegrityError as error:
                        raise ValueError(
                            f"Duplicate product SKU in catalogue: {product['sku']}"
                        ) from error
                    batch.append(product)
                    indexed += 1
                    if indexed % 10_000 == 0:
                        index.commit()
                    if len(batch) == BATCH_SIZE:
                        if batch_number not in completed:
                            yield batch_number, batch
                        batch_number += 1
                        batch = []
                if batch and batch_number not in completed:
                    yield batch_number, batch
        finally:
            index.close()


async def process_batch(
    products: list[dict[str, Any]],
    run_id: str,
    number: int,
    checkpoints: CheckpointStore,
    warehouse_api: WarehouseApi,
) -> None:
    await checkpoints.begin_batch(run_id, number, products)
    try:
        response = await warehouse_api.send_batch(products)
        accepted, rejected = validate_result(response, products)
    except asyncio.CancelledError:
        await checkpoints.mark_batch(
            run_id, number, "AMBIGUOUS", "Batch cancelled while WMS outcome was unknown"
        )
        raise
    except httpx.TransportError as error:
        await checkpoints.mark_batch(run_id, number, "AMBIGUOUS", str(error))
        raise RuntimeError(
            f"WMS outcome is ambiguous for run {run_id}, batch {number}; reconcile before retrying"
        ) from error
    except httpx.HTTPStatusError as error:
        status = error.response.status_code
        state = "AMBIGUOUS" if status >= 500 else "FAILED"
        await checkpoints.mark_batch(run_id, number, state, str(error))
        if state == "AMBIGUOUS":
            raise RuntimeError(
                f"WMS outcome is ambiguous for run {run_id}, batch {number}; reconcile before retrying"
            ) from error
        raise
    except (ValueError, KeyError) as error:
        await checkpoints.mark_batch(run_id, number, "AMBIGUOUS", str(error))
        raise RuntimeError(
            f"WMS returned an unreadable result for run {run_id}, batch {number}"
        ) from error

    await checkpoints.complete_batch(run_id, number, accepted, rejected)
    logger.info(
        "Async WMS batch completed run_id=%s batch=%s products=%s",
        run_id,
        number,
        len(products),
    )


async def sync_catalogue(
    path: str,
    run_id: str,
    checkpoints: CheckpointStore,
    warehouse_api: WarehouseApi,
    concurrency: int = WMS_CONCURRENCY,
) -> None:
    completed = await checkpoints.completed_batches(run_id)
    pending: set[asyncio.Task[None]] = set()

    async def finish_completed() -> None:
        nonlocal pending
        completed_tasks, pending = await asyncio.wait(
            pending, return_when=asyncio.FIRST_COMPLETED
        )
        for task in completed_tasks:
            task.result()

    try:
        async for number, products in iter_batches(path, completed):
            if len(pending) >= concurrency:
                await finish_completed()
            pending.add(
                asyncio.create_task(
                    process_batch(
                        products, run_id, number, checkpoints, warehouse_api
                    )
                )
            )
        if pending:
            results = await asyncio.gather(*pending, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result
    except BaseException:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        raise
