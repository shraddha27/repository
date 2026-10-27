import json
import logging
import os
import threading
import time
from typing import Any

import httpx
from botocore.exceptions import BotoCoreError

from app.aws import client as aws_client
from app.config import (
    DYNAMODB_TABLE,
    SQS_QUEUE_URL,
    WORKER_POLL_SECONDS,
    WORKER_VISIBILITY_TIMEOUT,
)
from app.services.warehouse_sync import process_batch
from app.storage.checkpoint_dynamodb import DynamoCheckpointStore

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)
HEARTBEAT_SECONDS = 60


class QueueWorker:
    def __init__(self) -> None:
        if not SQS_QUEUE_URL or not DYNAMODB_TABLE:
            raise ValueError("SQS_QUEUE_URL and DYNAMODB_TABLE are required by a worker")
        self.queue = aws_client("sqs")
        self.checkpoints = DynamoCheckpointStore(DYNAMODB_TABLE)
        self.http_client = httpx.Client()
        metadata_uri = os.getenv("ECS_CONTAINER_METADATA_URI_V4")
        cluster_name = os.getenv("ECS_CLUSTER_NAME")
        self.scale_in_protection = (
            TaskScaleInProtection(metadata_uri, cluster_name)
            if metadata_uri and cluster_name
            else None
        )

    def _delete(self, receipt_handle: str) -> None:
        self.queue.delete_message(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=receipt_handle,
        )

    def _extend_visibility(
        self, receipt_handle: str, stop: threading.Event
    ) -> None:
        while not stop.wait(HEARTBEAT_SECONDS):
            try:
                self.queue.change_message_visibility(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=receipt_handle,
                    VisibilityTimeout=WORKER_VISIBILITY_TIMEOUT,
                )
            except BotoCoreError:
                logger.exception("Could not extend SQS message visibility")

    def process(self, message: dict[str, Any]) -> None:
        receipt_handle = message["ReceiptHandle"]
        payload = json.loads(message["Body"])
        run_id = payload.get("run_id")
        batch_number = payload.get("batch_number")
        products = payload.get("products")
        if (
            not isinstance(run_id, str)
            or not isinstance(batch_number, int)
            or batch_number < 1
            or not isinstance(products, list)
        ):
            raise ValueError("Malformed catalogue batch message")

        state = self.checkpoints.run_status(run_id)
        batch_state = self.checkpoints.batch_status(run_id, batch_number)
        if batch_state in {"COMPLETED", "AMBIGUOUS"}:
            self._delete(receipt_handle)
            return
        if state == "AMBIGUOUS":
            self._delete(receipt_handle)
            return
        if state != "RUNNING":
            self.queue.change_message_visibility(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=60,
            )
            return
        if batch_state == "IN_FLIGHT":
            reason = "SQS redelivered a batch whose previous WMS outcome is unknown"
            self.checkpoints.mark_batch(run_id, batch_number, "AMBIGUOUS", reason)
            logger.error(
                "WMS batch marked ambiguous after SQS redelivery run_id=%s batch=%s",
                run_id,
                batch_number,
            )
            self._delete(receipt_handle)
            return

        if self.scale_in_protection is not None:
            self.scale_in_protection.set_enabled(True)
        stop_heartbeat = threading.Event()
        heartbeat = threading.Thread(
            target=self._extend_visibility,
            args=(receipt_handle, stop_heartbeat),
            daemon=True,
            name=f"sqs-visibility-{batch_number}",
        )
        heartbeat.start()
        try:
            try:
                process_batch(
                    products,
                    run_id,
                    batch_number,
                    self.checkpoints,
                    self.http_client,
                )
            except Exception:
                batch_state = self.checkpoints.batch_status(run_id, batch_number)
                if batch_state in {"FAILED", "AMBIGUOUS", "COMPLETED"}:
                    self._delete(receipt_handle)
                    logger.exception(
                        "WMS batch stopped run_id=%s batch=%s status=%s",
                        run_id,
                        batch_number,
                        batch_state,
                    )
                    return
                raise
            self._delete(receipt_handle)
        finally:
            stop_heartbeat.set()
            heartbeat.join(timeout=HEARTBEAT_SECONDS + 1)
            if self.scale_in_protection is not None:
                try:
                    self.scale_in_protection.set_enabled(False)
                except (BotoCoreError, RuntimeError):
                    logger.exception("Could not remove ECS task scale-in protection")

    def run_forever(self) -> None:
        logger.info("SQS WMS worker started")
        while True:
            response = self.queue.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=WORKER_POLL_SECONDS,
                VisibilityTimeout=WORKER_VISIBILITY_TIMEOUT,
            )
            for message in response.get("Messages", []):
                try:
                    self.process(message)
                except Exception:
                    logger.exception("Failed to process SQS catalogue message")
                    time.sleep(1)

    def close(self) -> None:
        self.http_client.close()


class TaskScaleInProtection:
    def __init__(self, metadata_uri: str, cluster_name: str) -> None:
        response = httpx.get(f"{metadata_uri}/task", timeout=2, trust_env=False)
        response.raise_for_status()
        task_arn = response.json().get("TaskARN")
        if not isinstance(task_arn, str) or not task_arn:
            raise ValueError("ECS task metadata did not include TaskARN")
        self.cluster_name = cluster_name
        self.task_arn = task_arn
        self.client = aws_client("ecs")

    def set_enabled(self, enabled: bool) -> None:
        request: dict[str, Any] = {
            "cluster": self.cluster_name,
            "tasks": [self.task_arn],
            "protectionEnabled": enabled,
        }
        if enabled:
            request["expiresInMinutes"] = 120
        response = self.client.update_task_protection(**request)
        failures = response.get("failures", [])
        if failures:
            raise RuntimeError(f"ECS task protection update failed: {failures}")


def main() -> None:
    queue_worker = QueueWorker()
    try:
        queue_worker.run_forever()
    finally:
        queue_worker.close()


if __name__ == "__main__":
    main()
