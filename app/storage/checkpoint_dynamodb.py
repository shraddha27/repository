import hashlib
import json
from collections.abc import Iterator
from threading import RLock
from typing import Any

from botocore.exceptions import ClientError

from app.aws import client as aws_client
from app.aws import resource as aws_resource


class DynamoCheckpointStore:
    def __init__(self, table_name: str) -> None:
        self.table = aws_resource("dynamodb").Table(table_name)
        self.client = aws_client("dynamodb")
        self._resource_lock = RLock()

    @staticmethod
    def _partition(run_id: str) -> str:
        return f"RUN#{run_id}"

    def _get_run(self, run_id: str) -> dict[str, Any] | None:
        response = self.table.get_item(
            Key={"pk": self._partition(run_id), "sk": "META"},
            ConsistentRead=True,
        )
        return response.get("Item")

    def acquire_run_lock(self, run_id: str) -> None:
        try:
            self.table.update_item(
                Key={"pk": "LOCK", "sk": "GLOBAL"},
                UpdateExpression="SET run_id = :run",
                ConditionExpression="attribute_not_exists(run_id) OR run_id = :run",
                ExpressionAttributeValues={":run": run_id},
            )
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                raise
            lock = self.table.get_item(
                Key={"pk": "LOCK", "sk": "GLOBAL"},
                ConsistentRead=True,
            ).get("Item", {})
            raise RuntimeError(
                f"Catalogue run {lock.get('run_id', 'unknown')} currently holds the global run lock"
            ) from error

    def release_run_lock(self, run_id: str) -> None:
        self.table.delete_item(
            Key={"pk": "LOCK", "sk": "GLOBAL"},
            ConditionExpression="run_id = :run",
            ExpressionAttributeValues={":run": run_id},
        )

    def start_run(self, run_id: str) -> str:
        item = self._get_run(run_id)
        if item is None:
            self.table.put_item(
                Item={"pk": self._partition(run_id), "sk": "META", "status": "RUNNING"},
                ConditionExpression="attribute_not_exists(pk)",
            )
            return "RUNNING"

        status = item["status"]
        in_flight = [
            batch
            for batch in self._iter_query(run_id, "BATCH#")
            if batch.get("status") == "IN_FLIGHT"
        ]
        if status in {"RUNNING", "FAILED"} and (
            in_flight or "in_flight_batch" in item
        ):
            for batch in in_flight:
                self.table.update_item(
                    Key={"pk": self._partition(run_id), "sk": batch["sk"]},
                    UpdateExpression="SET #status = :ambiguous, #error = :error",
                    ExpressionAttributeNames={"#status": "status", "#error": "error"},
                    ExpressionAttributeValues={
                        ":ambiguous": "AMBIGUOUS",
                        ":error": "Process stopped during an in-flight WMS request",
                    },
                )
            self.table.update_item(
                Key={"pk": self._partition(run_id), "sk": "META"},
                UpdateExpression="SET #status = :status, #error = :error",
                ExpressionAttributeNames={"#status": "status", "#error": "error"},
                ExpressionAttributeValues={
                    ":status": "AMBIGUOUS",
                    ":error": "Process stopped during an in-flight WMS request",
                },
            )
            return "AMBIGUOUS"
        if status == "FAILED":
            self.table.update_item(
                Key={"pk": self._partition(run_id), "sk": "META"},
                UpdateExpression="SET #status = :running REMOVE #error",
                ExpressionAttributeNames={"#status": "status", "#error": "error"},
                ExpressionAttributeValues={":running": "RUNNING"},
            )
            return "RUNNING"
        return status

    def get_export_key(self, run_id: str) -> str | None:
        item = self._get_run(run_id)
        return item.get("export_key") if item else None

    def set_export_key(self, run_id: str, export_key: str) -> None:
        self.table.update_item(
            Key={"pk": self._partition(run_id), "sk": "META"},
            UpdateExpression="SET export_key = :key",
            ConditionExpression="#status = :running",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":key": export_key, ":running": "RUNNING"},
        )

    def set_batch_total(self, run_id: str, total: int) -> None:
        completed = len(self.completed_batches(run_id))
        self.table.update_item(
            Key={"pk": self._partition(run_id), "sk": "META"},
            UpdateExpression=(
                "SET expected_batches = :total, "
                "completed_batches = if_not_exists(completed_batches, :completed)"
            ),
            ConditionExpression="#status = :running",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":total": total,
                ":completed": completed,
                ":running": "RUNNING",
            },
        )

    def _iter_partition_query(
        self, partition: str, prefix: str
    ) -> Iterator[dict[str, Any]]:
        kwargs: dict[str, Any] = {
            "KeyConditionExpression": "pk = :pk AND begins_with(sk, :prefix)",
            "ExpressionAttributeValues": {
                ":pk": partition,
                ":prefix": prefix,
            },
            "ConsistentRead": True,
        }
        while True:
            response = self.table.query(**kwargs)
            yield from response["Items"]
            if "LastEvaluatedKey" not in response:
                return
            kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    def _iter_query(self, run_id: str, prefix: str) -> Iterator[dict[str, Any]]:
        yield from self._iter_partition_query(self._partition(run_id), prefix)

    def _iter_outcomes(self, run_id: str) -> Iterator[dict[str, Any]]:
        yield from self._iter_partition_query(self._partition(run_id), "SKU#")

    def _iter_batch_outcomes(self, run_id: str) -> Iterator[dict[str, Any]]:
        for batch in self._iter_query(run_id, "BATCH#"):
            if batch.get("status") == "COMPLETED" and batch.get("outcomes_json"):
                yield json.loads(batch["outcomes_json"])

    def completed_skus(self, run_id: str) -> set[str]:
        completed = {item["sku"] for item in self._iter_outcomes(run_id)}
        for outcomes in self._iter_batch_outcomes(run_id):
            completed.update(outcomes["accepted"])
            completed.update(item["sku"] for item in outcomes["rejected"])
        return completed

    def completed_batches(self, run_id: str) -> set[int]:
        return {
            int(item["sk"].removeprefix("BATCH#"))
            for item in self._iter_query(run_id, "BATCH#")
            if item.get("status") == "COMPLETED"
        }

    def batch_status(self, run_id: str, batch_number: int) -> str | None:
        item = self.table.get_item(
            Key={
                "pk": self._partition(run_id),
                "sk": f"BATCH#{batch_number:08d}",
            },
            ConsistentRead=True,
        ).get("Item")
        return item.get("status") if item else None

    def run_progress(self, run_id: str) -> dict[str, Any]:
        run = self._get_run(run_id)
        if run is None:
            raise KeyError(f"Unknown run: {run_id}")
        return {
            "status": run["status"],
            "expected_batches": run.get("expected_batches"),
            "completed_batches": int(run.get("completed_batches", 0)),
            "error": run.get("error"),
        }

    def run_status(self, run_id: str) -> str:
        run = self._get_run(run_id)
        if run is None:
            raise KeyError(f"Unknown run: {run_id}")
        return run["status"]

    def next_batch_number(self, run_id: str) -> int:
        batch_numbers = [
            int(item["sk"].removeprefix("BATCH#"))
            for item in self._iter_query(run_id, "BATCH#")
        ]
        return max(batch_numbers, default=0) + 1

    def begin_batch(
        self, run_id: str, batch_number: int, products: list[dict[str, Any]]
    ) -> None:
        batch_key = f"BATCH#{batch_number:08d}"
        products_hash = hashlib.sha256(
            json.dumps(products, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        self.client.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": self.table.name,
                        "Item": {
                            "pk": {"S": self._partition(run_id)},
                            "sk": {"S": batch_key},
                            "status": {"S": "IN_FLIGHT"},
                            "products_hash": {"S": products_hash},
                        },
                        "ConditionExpression": (
                            "attribute_not_exists(pk) OR "
                            "(#status = :failed AND products_hash = :products_hash)"
                        ),
                        "ExpressionAttributeNames": {"#status": "status"},
                        "ExpressionAttributeValues": {
                            ":failed": {"S": "FAILED"},
                            ":products_hash": {"S": products_hash},
                        },
                    }
                },
                {
                    "ConditionCheck": {
                        "TableName": self.table.name,
                        "Key": {
                            "pk": {"S": self._partition(run_id)},
                            "sk": {"S": "META"},
                        },
                        "ConditionExpression": "#status = :running",
                        "ExpressionAttributeNames": {"#status": "status"},
                        "ExpressionAttributeValues": {
                            ":running": {"S": "RUNNING"},
                        },
                    }
                },
            ]
        )

    def complete_batch(
        self,
        run_id: str,
        batch_number: int,
        accepted: list[str],
        rejected: list[dict[str, Any]],
    ) -> None:
        batch_key = f"BATCH#{batch_number:08d}"
        with self._resource_lock:
            self.client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": self.table.name,
                            "Key": {
                                "pk": {"S": self._partition(run_id)},
                                "sk": {"S": batch_key},
                            },
                            "UpdateExpression": (
                                "SET #status = :completed, outcomes_json = :outcomes, "
                                "accepted_count = :accepted, rejected_count = :rejected"
                            ),
                            "ConditionExpression": "#status = :in_flight",
                            "ExpressionAttributeNames": {"#status": "status"},
                            "ExpressionAttributeValues": {
                                ":completed": {"S": "COMPLETED"},
                                ":in_flight": {"S": "IN_FLIGHT"},
                                ":outcomes": {
                                    "S": json.dumps(
                                        {"accepted": accepted, "rejected": rejected}
                                    )
                                },
                                ":accepted": {"N": str(len(accepted))},
                                ":rejected": {"N": str(len(rejected))},
                            },
                        }
                    },
                    {
                        "Update": {
                            "TableName": self.table.name,
                            "Key": {
                                "pk": {"S": self._partition(run_id)},
                                "sk": {"S": "META"},
                            },
                            "UpdateExpression": "ADD completed_batches :one",
                            "ExpressionAttributeValues": {":one": {"N": "1"}},
                        }
                    },
                ]
            )

    def mark_batch(self, run_id: str, batch_number: int, status: str, error: str) -> None:
        batch_key = f"BATCH#{batch_number:08d}"
        with self._resource_lock:
            self.table.update_item(
                Key={"pk": self._partition(run_id), "sk": batch_key},
                UpdateExpression="SET #status = :status, #error = :error",
                ExpressionAttributeNames={"#status": "status", "#error": "error"},
                ExpressionAttributeValues={":status": status, ":error": error},
            )
            try:
                self.table.update_item(
                    Key={"pk": self._partition(run_id), "sk": "META"},
                    UpdateExpression="SET #status = :status, #error = :error",
                    ConditionExpression="#status <> :ambiguous",
                    ExpressionAttributeNames={"#status": "status", "#error": "error"},
                    ExpressionAttributeValues={
                        ":status": status,
                        ":error": error,
                        ":ambiguous": "AMBIGUOUS",
                    },
                )
            except ClientError as update_error:
                if update_error.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                    raise

    def finish_run(self, run_id: str) -> None:
        if any(
            item.get("status") == "IN_FLIGHT"
            for item in self._iter_query(run_id, "BATCH#")
        ):
            raise RuntimeError(f"Run {run_id} still has in-flight batches")
        self.table.update_item(
            Key={"pk": self._partition(run_id), "sk": "META"},
            UpdateExpression="SET #status = :completed REMOVE #error",
            ConditionExpression="#status = :running",
            ExpressionAttributeNames={"#status": "status", "#error": "error"},
            ExpressionAttributeValues={
                ":completed": "COMPLETED",
                ":running": "RUNNING",
            },
        )

    def fail_run(self, run_id: str, error: str) -> None:
        self.table.update_item(
            Key={"pk": self._partition(run_id), "sk": "META"},
            UpdateExpression="SET #status = :failed, #error = :error",
            ExpressionAttributeNames={"#status": "status", "#error": "error"},
            ExpressionAttributeValues={":failed": "FAILED", ":error": error},
        )

    def run_summary(self, run_id: str) -> dict[str, Any]:
        run = self._get_run(run_id)
        if run is None:
            raise KeyError(f"Unknown run: {run_id}")
        batches = list(self._iter_query(run_id, "BATCH#"))
        counts = {"accepted": 0, "rejected": 0}
        legacy_batches = [
            item
            for item in batches
            if item.get("status") == "COMPLETED" and "accepted_count" not in item
        ]
        if legacy_batches:
            legacy_batch_numbers = {
                int(item["sk"].removeprefix("BATCH#"))
                for item in legacy_batches
            }
            for item in self._iter_outcomes(run_id):
                if int(item.get("batch_number", 0)) in legacy_batch_numbers:
                    if item["status"] == "ACCEPTED":
                        counts["accepted"] += 1
                    elif item["status"] == "REJECTED":
                        counts["rejected"] += 1
        for item in batches:
            if "accepted_count" in item:
                counts["accepted"] += int(item["accepted_count"])
                counts["rejected"] += int(item.get("rejected_count", 0))
        return {
            "run_id": run_id,
            "status": run["status"],
            "export_key": run.get("export_key"),
            "error": run.get("error"),
            "accepted": counts["accepted"],
            "rejected": counts["rejected"],
            "expected_batches": run.get("expected_batches"),
            "completed_batches": int(
                run.get(
                    "completed_batches",
                    sum(1 for item in batches if item.get("status") == "COMPLETED"),
                )
            ),
        }
