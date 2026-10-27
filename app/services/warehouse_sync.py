import csv
import logging
import sqlite3
import tempfile
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from collections.abc import Generator
from pathlib import Path
from typing import Any

import httpx

from app.clients.warehouse_api import send_batch
from app.config import BATCH_SIZE, WMS_WORKERS
from app.storage.checkpoint import CheckpointBackend

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


def _validate_result(
    result: dict[str, Any], products: list[dict[str, Any]]
) -> tuple[list[str], list[dict[str, str]]]:
    accepted = result.get("accepted")
    rejected = result.get("rejected")
    if not isinstance(accepted, list) or not isinstance(rejected, list):
        raise ValueError("Warehouse response must include accepted and rejected arrays")

    expected = {product["sku"] for product in products}
    outcomes: list[str] = []
    normalized_rejections: list[dict[str, str]] = []
    if not all(isinstance(sku, str) for sku in accepted):
        raise ValueError("Warehouse accepted array contains an invalid SKU")
    outcomes.extend(accepted)

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


def process_batch(
    products: list[dict[str, Any]],
    run_id: str,
    number: int,
    checkpoints: CheckpointBackend,
    client: httpx.Client,
) -> None:
    checkpoints.begin_batch(run_id, number, products)
    started_at = time.monotonic()
    try:
        response = send_batch(products, client)
    except httpx.TransportError as error:
        checkpoints.mark_batch(run_id, number, "AMBIGUOUS", str(error))
        logger.error(
            "WMS batch outcome ambiguous run_id=%s batch=%s reason=transport_error",
            run_id,
            number,
        )
        raise RuntimeError(
            f"WMS outcome is ambiguous for run {run_id}, batch {number}; reconcile before retrying"
        ) from error
    except httpx.HTTPStatusError as error:
        status = error.response.status_code
        state = "AMBIGUOUS" if status >= 500 else "FAILED"
        checkpoints.mark_batch(run_id, number, state, str(error))
        logger.error(
            "WMS batch failed run_id=%s batch=%s status=%s outcome=%s",
            run_id,
            number,
            status,
            state,
        )
        if state == "AMBIGUOUS":
            raise RuntimeError(
                f"WMS outcome is ambiguous for run {run_id}, batch {number}; reconcile before retrying"
            ) from error
        raise
    except ValueError as error:
        checkpoints.mark_batch(run_id, number, "AMBIGUOUS", str(error))
        logger.error(
            "WMS batch outcome ambiguous run_id=%s batch=%s reason=invalid_response",
            run_id,
            number,
        )
        raise RuntimeError(
            f"WMS returned an unreadable result for run {run_id}, batch {number}"
        ) from error

    try:
        accepted, rejected = _validate_result(response, products)
    except ValueError as error:
        checkpoints.mark_batch(run_id, number, "AMBIGUOUS", str(error))
        logger.error(
            "WMS batch outcome ambiguous run_id=%s batch=%s reason=unaccountable_response",
            run_id,
            number,
        )
        raise RuntimeError(
            f"WMS returned an unaccountable result for run {run_id}, batch {number}"
        ) from error

    checkpoints.complete_batch(run_id, number, accepted, rejected)
    logger.info(
        "WMS batch completed run_id=%s batch=%s products=%s accepted=%s rejected=%s duration_seconds=%.3f",
        run_id,
        number,
        len(products),
        len(accepted),
        len(rejected),
        time.monotonic() - started_at,
    )


def sync_catalogue(path: str, run_id: str, checkpoints: CheckpointBackend) -> None:
    completed_batches = checkpoints.completed_batches(run_id)

    def submit_batch(
        products: list[dict[str, Any]], number: int, client: httpx.Client
    ) -> None:
        process_batch(products, run_id, number, checkpoints, client)

    def batches() -> Generator[tuple[int, list[dict[str, Any]]], None, None]:
        with tempfile.TemporaryDirectory(prefix="catalogue-sku-index-") as directory:
            index = sqlite3.connect(Path(directory) / "seen.sqlite3")
            try:
                index.execute("CREATE TABLE seen_skus (sku TEXT PRIMARY KEY)")
                batch_number = 1
                batch: list[dict[str, Any]] = []
                with open(path, newline="", encoding="utf-8") as source:
                    for row in csv.DictReader(source):
                        product = transform(row)
                        sku = product["sku"]
                        try:
                            index.execute(
                                "INSERT INTO seen_skus (sku) VALUES (?)", (sku,)
                            )
                        except sqlite3.IntegrityError as error:
                            message = f"Duplicate product SKU in catalogue: {sku}"
                            checkpoints.fail_run(run_id, message)
                            raise ValueError(message) from error

                        batch.append(product)
                        if len(batch) == BATCH_SIZE:
                            if batch_number not in completed_batches:
                                yield batch_number, batch
                            batch_number += 1
                            batch = []

                    if batch and batch_number not in completed_batches:
                        yield batch_number, batch
            finally:
                index.close()

    batch_iterator = iter(batches())
    first_error: Exception | None = None
    pending: dict[Future[None], int] = {}

    try:
        with httpx.Client() as client, ThreadPoolExecutor(
            max_workers=WMS_WORKERS, thread_name_prefix="wms-batch"
        ) as executor:
            while len(pending) < WMS_WORKERS:
                try:
                    number, products = next(batch_iterator)
                except StopIteration:
                    break
                except Exception as error:
                    first_error = error
                    break
                pending[executor.submit(submit_batch, products, number, client)] = number

            while pending:
                completed_futures, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in completed_futures:
                    pending.pop(future)
                    try:
                        future.result()
                    except Exception as error:
                        if first_error is None:
                            first_error = error

                if first_error is None:
                    while len(pending) < WMS_WORKERS:
                        try:
                            number, products = next(batch_iterator)
                        except StopIteration:
                            break
                        except Exception as error:
                            first_error = error
                            break
                        pending[executor.submit(submit_batch, products, number, client)] = number
    finally:
        batch_iterator.close()

    if first_error is not None:
        raise first_error
