import hashlib
import json
from typing import Any

from botocore.exceptions import ClientError


class DynamoCheckpoints:
    def __init__(self, client: Any, table_name: str):
        self._client = client
        self._table_name = table_name

    @staticmethod
    def _partition(run_id: str) -> str:
        return f"RUN#{run_id}"

    @staticmethod
    def _batch_number(item: dict[str, Any]) -> int:
        if "batch_number" in item:
            return int(item["batch_number"])
        return int(item["sk"].removeprefix("BATCH#"))

    async def _get_run(self, run_id: str) -> dict[str, Any] | None:
        response = await self._client.get_item(
            TableName=self._table_name,
            Key={"pk": {"S": self._partition(run_id)}, "sk": {"S": "META"}},
            ConsistentRead=True,
        )
        item = response.get("Item")
        return {key: _from_attribute(value) for key, value in item.items()} if item else None

    async def _query(self, run_id: str, prefix: str) -> list[dict[str, Any]]:
        items = []
        request = {
            "TableName": self._table_name,
            "KeyConditionExpression": "pk = :pk AND begins_with(sk, :prefix)",
            "ExpressionAttributeValues": {
                ":pk": {"S": self._partition(run_id)},
                ":prefix": {"S": prefix},
            },
            "ConsistentRead": True,
        }
        while True:
            response = await self._client.query(**request)
            items.extend(
                {key: _from_attribute(value) for key, value in item.items()}
                for item in response["Items"]
            )
            if "LastEvaluatedKey" not in response:
                return items
            request["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    async def acquire_run_lock(self, run_id: str) -> None:
        try:
            await self._client.update_item(
                TableName=self._table_name,
                Key={"pk": {"S": "LOCK"}, "sk": {"S": "GLOBAL"}},
                UpdateExpression="SET run_id = :run",
                ConditionExpression="attribute_not_exists(run_id) OR run_id = :run",
                ExpressionAttributeValues={":run": {"S": run_id}},
            )
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") != (
                "ConditionalCheckFailedException"
            ):
                raise
            response = await self._client.get_item(
                TableName=self._table_name,
                Key={"pk": {"S": "LOCK"}, "sk": {"S": "GLOBAL"}},
                ConsistentRead=True,
            )
            holder = response.get("Item", {}).get("run_id", {}).get("S", "unknown")
            raise RuntimeError(
                f"Catalogue run {holder} currently holds the global run lock"
            ) from error

    async def release_run_lock(self, run_id: str) -> None:
        await self._client.delete_item(
            TableName=self._table_name,
            Key={"pk": {"S": "LOCK"}, "sk": {"S": "GLOBAL"}},
            ConditionExpression="run_id = :run",
            ExpressionAttributeValues={":run": {"S": run_id}},
        )

    async def start_run(self, run_id: str) -> str:
        run = await self._get_run(run_id)
        if run is None:
            await self._client.put_item(
                TableName=self._table_name,
                Item={
                    "pk": {"S": self._partition(run_id)},
                    "sk": {"S": "META"},
                    "status": {"S": "RUNNING"},
                    "completed_batches": {"N": "0"},
                },
                ConditionExpression="attribute_not_exists(pk)",
            )
            return "RUNNING"
        status = run["status"]
        in_flight = [
            batch
            for batch in await self._query(run_id, "BATCH#")
            if batch.get("status") == "IN_FLIGHT"
        ]
        if status in {"RUNNING", "FAILED"} and in_flight:
            reason = "Process stopped during an in-flight WMS request"
            for batch in in_flight:
                await self.mark_batch(
                    run_id, self._batch_number(batch), "AMBIGUOUS", reason
                )
            await self._client.update_item(
                TableName=self._table_name,
                Key={"pk": {"S": self._partition(run_id)}, "sk": {"S": "META"}},
                UpdateExpression="SET #status = :status, #error = :error",
                ExpressionAttributeNames={"#status": "status", "#error": "error"},
                ExpressionAttributeValues={
                    ":status": {"S": "AMBIGUOUS"},
                    ":error": {"S": reason},
                },
            )
            return "AMBIGUOUS"
        if status == "FAILED":
            await self._client.update_item(
                TableName=self._table_name,
                Key={"pk": {"S": self._partition(run_id)}, "sk": {"S": "META"}},
                UpdateExpression="SET #status = :running REMOVE #error",
                ExpressionAttributeNames={"#status": "status", "#error": "error"},
                ExpressionAttributeValues={":running": {"S": "RUNNING"}},
            )
            return "RUNNING"
        return status

    async def get_export_key(self, run_id: str) -> str | None:
        run = await self._get_run(run_id)
        return run.get("export_key") if run else None

    async def set_export_key(self, run_id: str, key: str) -> None:
        await self._client.update_item(
            TableName=self._table_name,
            Key={"pk": {"S": self._partition(run_id)}, "sk": {"S": "META"}},
            UpdateExpression="SET export_key = :key",
            ConditionExpression="#status = :running",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":key": {"S": key},
                ":running": {"S": "RUNNING"},
            },
        )

    async def completed_batches(self, run_id: str) -> set[int]:
        return {
            self._batch_number(item)
            for item in await self._query(run_id, "BATCH#")
            if item.get("status") == "COMPLETED"
        }

    async def set_batch_total(self, run_id: str, total: int) -> None:
        completed = len(await self.completed_batches(run_id))
        await self._client.update_item(
            TableName=self._table_name,
            Key={"pk": {"S": self._partition(run_id)}, "sk": {"S": "META"}},
            UpdateExpression=(
                "SET expected_batches = :total, "
                "completed_batches = if_not_exists(completed_batches, :completed)"
            ),
            ConditionExpression="#status = :running",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":total": {"N": str(total)},
                ":completed": {"N": str(completed)},
                ":running": {"S": "RUNNING"},
            },
        )

    async def begin_batch(
        self, run_id: str, number: int, products: list[dict[str, Any]]
    ) -> None:
        key = f"BATCH#{number:08d}"
        digest = hashlib.sha256(
            json.dumps(products, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        await self._client.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": self._table_name,
                        "Item": {
                            "pk": {"S": self._partition(run_id)},
                            "sk": {"S": key},
                            "batch_number": {"N": str(number)},
                            "status": {"S": "IN_FLIGHT"},
                            "products_hash": {"S": digest},
                        },
                        "ConditionExpression": (
                            "attribute_not_exists(pk) OR "
                            "(#status = :failed AND products_hash = :digest)"
                        ),
                        "ExpressionAttributeNames": {"#status": "status"},
                        "ExpressionAttributeValues": {
                            ":failed": {"S": "FAILED"},
                            ":digest": {"S": digest},
                        },
                    }
                },
                {
                    "ConditionCheck": {
                        "TableName": self._table_name,
                        "Key": {
                            "pk": {"S": self._partition(run_id)},
                            "sk": {"S": "META"},
                        },
                        "ConditionExpression": "#status = :running",
                        "ExpressionAttributeNames": {"#status": "status"},
                        "ExpressionAttributeValues": {":running": {"S": "RUNNING"}},
                    }
                },
            ]
        )

    async def complete_batch(
        self,
        run_id: str,
        number: int,
        accepted: list[str],
        rejected: list[dict[str, Any]],
    ) -> None:
        key = f"BATCH#{number:08d}"
        outcomes = json.dumps({"accepted": accepted, "rejected": rejected})
        await self._client.transact_write_items(
            TransactItems=[
                {
                    "Update": {
                        "TableName": self._table_name,
                        "Key": {
                            "pk": {"S": self._partition(run_id)},
                            "sk": {"S": key},
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
                            ":outcomes": {"S": outcomes},
                            ":accepted": {"N": str(len(accepted))},
                            ":rejected": {"N": str(len(rejected))},
                        },
                    }
                },
                {
                    "Update": {
                        "TableName": self._table_name,
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

    async def mark_batch(
        self, run_id: str, number: int, status: str, error: str
    ) -> None:
        key = f"BATCH#{number:08d}"
        await self._client.update_item(
            TableName=self._table_name,
            Key={"pk": {"S": self._partition(run_id)}, "sk": {"S": key}},
            UpdateExpression="SET #status = :status, #error = :error, batch_number = :number",
            ExpressionAttributeNames={"#status": "status", "#error": "error"},
            ExpressionAttributeValues={
                ":status": {"S": status},
                ":error": {"S": error},
                ":number": {"N": str(number)},
            },
        )
        try:
            await self._client.update_item(
                TableName=self._table_name,
                Key={"pk": {"S": self._partition(run_id)}, "sk": {"S": "META"}},
                UpdateExpression="SET #status = :status, #error = :error",
                ConditionExpression="#status <> :ambiguous",
                ExpressionAttributeNames={"#status": "status", "#error": "error"},
                ExpressionAttributeValues={
                    ":status": {"S": status},
                    ":error": {"S": error},
                    ":ambiguous": {"S": "AMBIGUOUS"},
                },
            )
        except ClientError as update_error:
            if update_error.response.get("Error", {}).get("Code") != (
                "ConditionalCheckFailedException"
            ):
                raise

    async def batch_status(self, run_id: str, number: int) -> str | None:
        response = await self._client.get_item(
            TableName=self._table_name,
            Key={
                "pk": {"S": self._partition(run_id)},
                "sk": {"S": f"BATCH#{number:08d}"},
            },
            ConsistentRead=True,
        )
        item = response.get("Item")
        return item.get("status", {}).get("S") if item else None

    async def run_status(self, run_id: str) -> str:
        run = await self._get_run(run_id)
        if run is None:
            raise KeyError(f"Unknown run: {run_id}")
        return run["status"]

    async def run_progress(self, run_id: str) -> dict[str, Any]:
        run = await self._get_run(run_id)
        if run is None:
            raise KeyError(f"Unknown run: {run_id}")
        return {
            "status": run["status"],
            "expected_batches": run.get("expected_batches"),
            "completed_batches": int(run.get("completed_batches", 0)),
            "error": run.get("error"),
        }

    async def finish_run(self, run_id: str) -> None:
        if any(
            item.get("status") == "IN_FLIGHT"
            for item in await self._query(run_id, "BATCH#")
        ):
            raise RuntimeError(f"Run {run_id} still has in-flight batches")
        await self._client.update_item(
            TableName=self._table_name,
            Key={"pk": {"S": self._partition(run_id)}, "sk": {"S": "META"}},
            UpdateExpression="SET #status = :completed REMOVE #error",
            ConditionExpression="#status = :running",
            ExpressionAttributeNames={"#status": "status", "#error": "error"},
            ExpressionAttributeValues={
                ":completed": {"S": "COMPLETED"},
                ":running": {"S": "RUNNING"},
            },
        )

    async def fail_run(self, run_id: str, error: str) -> None:
        try:
            await self._client.update_item(
                TableName=self._table_name,
                Key={"pk": {"S": self._partition(run_id)}, "sk": {"S": "META"}},
                UpdateExpression="SET #status = :failed, #error = :error",
                ConditionExpression="#status <> :ambiguous",
                ExpressionAttributeNames={"#status": "status", "#error": "error"},
                ExpressionAttributeValues={
                    ":failed": {"S": "FAILED"},
                    ":error": {"S": error},
                    ":ambiguous": {"S": "AMBIGUOUS"},
                },
            )
        except ClientError as update_error:
            if update_error.response.get("Error", {}).get("Code") != (
                "ConditionalCheckFailedException"
            ):
                raise

    async def run_summary(self, run_id: str) -> dict[str, Any]:
        run = await self._get_run(run_id)
        if run is None:
            raise KeyError(f"Unknown run: {run_id}")
        batches = await self._query(run_id, "BATCH#")
        legacy_numbers = {
            self._batch_number(item)
            for item in batches
            if item.get("status") == "COMPLETED"
            and "accepted_count" not in item
        }
        accepted = sum(int(item.get("accepted_count", 0)) for item in batches)
        rejected = sum(int(item.get("rejected_count", 0)) for item in batches)
        if legacy_numbers:
            for outcome in await self._query(run_id, "SKU#"):
                if int(outcome.get("batch_number", 0)) not in legacy_numbers:
                    continue
                if outcome.get("status") == "ACCEPTED":
                    accepted += 1
                elif outcome.get("status") == "REJECTED":
                    rejected += 1
        return {
            "run_id": run_id,
            "status": run["status"],
            "export_key": run.get("export_key"),
            "error": run.get("error"),
            "accepted": accepted,
            "rejected": rejected,
            "expected_batches": run.get("expected_batches"),
            "completed_batches": int(run.get("completed_batches", 0)),
        }


def _from_attribute(value: dict[str, Any]) -> Any:
    if "S" in value:
        return value["S"]
    if "N" in value:
        return int(value["N"])
    if "BOOL" in value:
        return value["BOOL"]
    if "NULL" in value:
        return None
    raise ValueError(f"Unsupported DynamoDB attribute: {value}")
