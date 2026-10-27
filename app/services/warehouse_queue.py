import csv
import json
import logging
import sqlite3
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from app.aws import client as aws_client
from app.config import BATCH_SIZE, SQS_QUEUE_URL
from app.services.warehouse_sync import transform
from app.storage.checkpoint import CheckpointBackend

logger = logging.getLogger(__name__)
MAX_SQS_MESSAGE_BYTES = 256 * 1024
MAX_SQS_BATCH_BYTES = 900 * 1024
MAX_SQS_BATCH_ENTRIES = 10


def _send_messages(queue: Any, queue_url: str, entries: list[dict[str, str]]) -> None:
    response = queue.send_message_batch(QueueUrl=queue_url, Entries=entries)
    failures = response.get("Failed", [])
    if failures:
        details = "; ".join(
            f"{failure.get('Id', 'unknown')}: {failure.get('Code', 'unknown')}"
            for failure in failures
        )
        raise RuntimeError(f"SQS rejected one or more catalogue batches: {details}")


def _batches(
    path: str, completed_batches: set[int]
) -> Iterator[tuple[int, list[dict[str, Any]]]]:
    with tempfile.TemporaryDirectory(prefix="catalogue-sku-index-") as directory:
        index = sqlite3.connect(Path(directory) / "seen.sqlite3")
        try:
            index.execute("CREATE TABLE seen_skus (sku TEXT PRIMARY KEY)")
            batch_number = 1
            batch: list[dict[str, Any]] = []
            with open(path, newline="", encoding="utf-8") as source:
                for row in csv.DictReader(source):
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
                    if len(batch) == BATCH_SIZE:
                        if batch_number not in completed_batches:
                            yield batch_number, batch
                        batch_number += 1
                        batch = []
                if batch:
                    if batch_number not in completed_batches:
                        yield batch_number, batch
        finally:
            index.close()


def dispatch_catalogue(
    path: str, run_id: str, checkpoints: CheckpointBackend
) -> int:
    if not SQS_QUEUE_URL:
        raise RuntimeError("SQS_QUEUE_URL must be configured for queued processing")
    queue = aws_client("sqs")
    completed = checkpoints.completed_batches(run_id)
    total_batches = 0
    queued_batches = 0
    entries: list[dict[str, str]] = []
    entry_bytes = 0

    def flush() -> None:
        nonlocal queued_batches, entry_bytes
        if not entries:
            return
        _send_messages(queue, SQS_QUEUE_URL, entries)
        queued_batches += len(entries)
        entries.clear()
        entry_bytes = 0

    for number, products in _batches(path, completed):
        total_batches = number
        body = json.dumps(
            {"run_id": run_id, "batch_number": number, "products": products},
            separators=(",", ":"),
        )
        if len(body.encode("utf-8")) > MAX_SQS_MESSAGE_BYTES:
            raise ValueError(
                f"Warehouse batch {number} exceeds the SQS message size limit"
            )
        body_bytes = len(body.encode("utf-8"))
        if entries and (
            len(entries) == MAX_SQS_BATCH_ENTRIES
            or entry_bytes + body_bytes > MAX_SQS_BATCH_BYTES
        ):
            flush()
        entries.append(
            {
                "Id": str(number),
                "MessageBody": body,
                "MessageGroupId": f"{run_id}-{number}",
                "MessageDeduplicationId": f"{run_id}-{number}",
            }
        )
        entry_bytes += body_bytes
    flush()

    if completed:
        total_batches = max(total_batches, max(completed))
    checkpoints.set_batch_total(run_id, total_batches)
    logger.info(
        "WMS batch dispatch complete run_id=%s total_batches=%s newly_queued=%s",
        run_id,
        total_batches,
        queued_batches,
    )
    return total_batches
