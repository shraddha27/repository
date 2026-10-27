import argparse
import json
import logging
import os
import re
import tempfile
import time
from pathlib import Path
from uuid import uuid4

from app.config import (
    DYNAMODB_TABLE,
    RUN_COMPLETION_TIMEOUT_SECONDS,
    RUN_ID,
    S3_BUCKET,
    SQS_QUEUE_URL,
    STATE_DB,
)
from app.services.catalogue_export import export_catalogue
from app.services.warehouse_queue import dispatch_catalogue
from app.services.warehouse_sync import sync_catalogue
from app.storage.checkpoint_dynamodb import DynamoCheckpointStore
from app.storage.checkpoint_sqlite import CheckpointStore
from app.storage.s3 import S3Store

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def run(run_id: str | None = None) -> dict[str, object]:
    run_id = run_id or RUN_ID or uuid4().hex
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", run_id):
        raise ValueError(
            "run_id must contain 1-80 letters, digits, hyphens, or underscores"
        )
    if bool(S3_BUCKET) != bool(DYNAMODB_TABLE):
        raise ValueError("S3_BUCKET and DYNAMODB_TABLE must be configured together")
    if SQS_QUEUE_URL and not (S3_BUCKET and DYNAMODB_TABLE):
        raise ValueError(
            "SQS_QUEUE_URL requires S3_BUCKET and DYNAMODB_TABLE for shared worker state"
        )
    logger.info("Catalogue run starting run_id=%s", run_id)
    run_started = time.monotonic()

    backend = (
        DynamoCheckpointStore(DYNAMODB_TABLE)
        if DYNAMODB_TABLE
        else CheckpointStore(STATE_DB)
    )
    backend.acquire_run_lock(run_id)
    status = backend.start_run(run_id)
    if status == "COMPLETED":
        backend.release_run_lock(run_id)
        logger.info("Catalogue run already completed run_id=%s", run_id)
        return backend.run_summary(run_id)
    if status != "RUNNING":
        logger.error("Catalogue run blocked run_id=%s status=%s", run_id, status)
        raise RuntimeError(
            f"Run {run_id} is {status}; investigate its checkpoint before starting another attempt"
        )

    store = S3Store(bucket=S3_BUCKET)
    export_key = backend.get_export_key(run_id)
    local_export: str | None = None
    temporary_download: str | None = None
    try:
        if export_key is None:
            local_export = export_catalogue(run_id)
            export_key = f"catalogue/{run_id}/catalogue.csv"
            store.put_file(local_export, export_key)
            backend.set_export_key(run_id, export_key)

        if S3_BUCKET:
            with tempfile.NamedTemporaryFile(
                prefix=f"catalogue-{run_id}-",
                suffix=".csv",
                delete=False,
            ) as file:
                temporary_download = file.name
            catalogue_path = store.download_file(export_key, temporary_download)
        else:
            catalogue_path = store.get_file(export_key)

        if SQS_QUEUE_URL:
            total_batches = dispatch_catalogue(str(catalogue_path), run_id, backend)
            deadline = run_started + RUN_COMPLETION_TIMEOUT_SECONDS
            while True:
                progress = backend.run_progress(run_id)
                if progress["status"] != "RUNNING":
                    raise RuntimeError(
                        f"Queued catalogue run {run_id} stopped with status "
                        f"{progress['status']}; inspect batch checkpoints"
                    )
                if progress["completed_batches"] == total_batches:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"Queued catalogue run {run_id} did not complete "
                        f"{total_batches} batches within "
                        f"{RUN_COMPLETION_TIMEOUT_SECONDS} seconds"
                    )
                time.sleep(2)
        else:
            sync_catalogue(str(catalogue_path), run_id, backend)
        backend.finish_run(run_id)
        backend.release_run_lock(run_id)
    except Exception as error:
        summary = backend.run_summary(run_id)
        if summary["status"] == "RUNNING":
            backend.fail_run(run_id, str(error))
        raise
    finally:
        if local_export is not None:
            Path(local_export).unlink(missing_ok=True)
        if temporary_download is not None:
            Path(temporary_download).unlink(missing_ok=True)

    summary = backend.run_summary(run_id)
    logger.info(
        "Catalogue run completed run_id=%s accepted=%s rejected=%s",
        run_id,
        summary["accepted"],
        summary["rejected"],
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Synchronize the product catalogue to the WMS"
    )
    parser.add_argument(
        "--run-id",
        help="Resume a previously started run (or provide an id for a new run)",
    )
    args = parser.parse_args()
    print(json.dumps(run(args.run_id), sort_keys=True))


if __name__ == "__main__":
    main()
