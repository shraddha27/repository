import argparse
import json
import logging
import os
import re
import tempfile
from pathlib import Path
from uuid import uuid4

from app.config import DYNAMODB_TABLE, RUN_ID, S3_BUCKET, STATE_DB
from app.services.catalogue_export import export_catalogue
from app.services.warehouse_sync import sync_catalogue
from app.storage.checkpoint import CheckpointStore, DynamoCheckpointStore
from app.storage.s3 import S3Store

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def run(run_id: str | None = None) -> dict[str, object]:
    run_id = run_id or RUN_ID or uuid4().hex
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", run_id):
        raise ValueError("run_id must contain 1-80 letters, digits, hyphens, or underscores")
    if bool(S3_BUCKET) != bool(DYNAMODB_TABLE):
        raise ValueError("S3_BUCKET and DYNAMODB_TABLE must be configured together")
    logger.info("Catalogue run starting run_id=%s", run_id)

    checkpoints = (
        DynamoCheckpointStore(DYNAMODB_TABLE)
        if DYNAMODB_TABLE
        else CheckpointStore(STATE_DB)
    )
    checkpoints.acquire_run_lock(run_id)
    status = checkpoints.start_run(run_id)
    if status == "COMPLETED":
        checkpoints.release_run_lock(run_id)
        logger.info("Catalogue run already completed run_id=%s", run_id)
        return checkpoints.run_summary(run_id)
    if status != "RUNNING":
        logger.error("Catalogue run blocked run_id=%s status=%s", run_id, status)
        raise RuntimeError(
            f"Run {run_id} is {status}; investigate its checkpoint before starting another attempt"
        )

    store = S3Store(bucket=S3_BUCKET)
    export_key = checkpoints.get_export_key(run_id)
    local_export: str | None = None
    temporary_download: str | None = None
    try:
        if export_key is None:
            local_export = export_catalogue(run_id)
            export_key = f"catalogue/{run_id}/catalogue.csv"
            store.put_file(local_export, export_key)
            checkpoints.set_export_key(run_id, export_key)

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

        sync_catalogue(str(catalogue_path), run_id, checkpoints)
        checkpoints.finish_run(run_id)
        checkpoints.release_run_lock(run_id)
    except Exception as error:
        summary = checkpoints.run_summary(run_id)
        if summary["status"] == "RUNNING":
            checkpoints.fail_run(run_id, str(error))
        raise
    finally:
        if local_export is not None:
            Path(local_export).unlink(missing_ok=True)
        if temporary_download is not None:
            Path(temporary_download).unlink(missing_ok=True)

    summary = checkpoints.run_summary(run_id)
    logger.info(
        "Catalogue run completed run_id=%s accepted=%s rejected=%s",
        run_id,
        summary["accepted"],
        summary["rejected"],
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Synchronize the product catalogue to the WMS")
    parser.add_argument(
        "--run-id",
        help="Resume a previously started run (or provide an id for a new run)",
    )
    args = parser.parse_args()
    print(json.dumps(run(args.run_id), sort_keys=True))


if __name__ == "__main__":
    main()
