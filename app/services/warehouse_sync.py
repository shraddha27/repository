import csv
import logging
import time
from typing import Any

import httpx

from app.clients.warehouse_api import send_batch
from app.config import BATCH_SIZE
from app.storage.checkpoint import CheckpointStore

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


def sync_catalogue(path: str, run_id: str, checkpoints: CheckpointStore) -> None:
    completed = checkpoints.completed_skus(run_id)
    batch: list[dict[str, Any]] = []
    batch_skus: set[str] = set()
    batch_number = checkpoints.next_batch_number(run_id)

    def submit_batch(
        products: list[dict[str, Any]], number: int, client: httpx.Client
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
        completed.update(accepted)
        completed.update(item["sku"] for item in rejected)

    with httpx.Client() as client:
        with open(path, newline="", encoding="utf-8") as source:
            for row in csv.DictReader(source):
                product = transform(row)
                sku = product["sku"]
                if sku in completed:
                    continue
                if sku in batch_skus:
                    checkpoints.fail_run(run_id, f"Duplicate product SKU in catalogue: {sku}")
                    raise ValueError(f"Duplicate product SKU in catalogue: {sku}")
                batch.append(product)
                batch_skus.add(sku)
                if len(batch) == BATCH_SIZE:
                    submit_batch(batch, batch_number, client)
                    batch_number += 1
                    batch = []
                    batch_skus = set()

            if batch:
                submit_batch(batch, batch_number, client)
