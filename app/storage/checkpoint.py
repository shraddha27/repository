import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from app.aws import client as aws_client
from app.aws import resource as aws_resource


class CheckpointStore:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    export_key TEXT,
                    error TEXT
                );
                CREATE TABLE IF NOT EXISTS run_lock (
                    lock_name TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS batches (
                    run_id TEXT NOT NULL,
                    batch_number INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    products_json TEXT NOT NULL,
                    error TEXT,
                    PRIMARY KEY (run_id, batch_number),
                    FOREIGN KEY (run_id) REFERENCES runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS product_outcomes (
                    run_id TEXT NOT NULL,
                    sku TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT,
                    batch_number INTEGER NOT NULL,
                    PRIMARY KEY (run_id, sku),
                    FOREIGN KEY (run_id) REFERENCES runs(run_id)
                );
                """
            )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def acquire_run_lock(self, run_id: str) -> None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT run_id FROM run_lock WHERE lock_name = 'catalogue'"
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO run_lock (lock_name, run_id) VALUES ('catalogue', ?)",
                    (run_id,),
                )
            elif row[0] != run_id:
                raise RuntimeError(
                    f"Catalogue run {row[0]} currently holds the global run lock"
                )

    def release_run_lock(self, run_id: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM run_lock WHERE lock_name = 'catalogue' AND run_id = ?",
                (run_id,),
            )

    def start_run(self, run_id: str) -> str:
        with self._connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO runs (run_id, status) VALUES (?, 'RUNNING')",
                (run_id,),
            )
            row = connection.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            status = row[0]
            in_flight = connection.execute(
                "SELECT 1 FROM batches WHERE run_id = ? AND status = 'IN_FLIGHT' LIMIT 1",
                (run_id,),
            ).fetchone()
            if status in {"RUNNING", "FAILED"} and in_flight:
                connection.execute(
                    "UPDATE runs SET status = 'AMBIGUOUS', error = ? WHERE run_id = ?",
                    ("Process stopped during an in-flight WMS request", run_id),
                )
                status = "AMBIGUOUS"
            elif status == "FAILED":
                connection.execute(
                    "UPDATE runs SET status = 'RUNNING', error = NULL WHERE run_id = ?",
                    (run_id,),
                )
                status = "RUNNING"
            return status

    def get_export_key(self, run_id: str) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT export_key FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            return row[0] if row else None

    def set_export_key(self, run_id: str, export_key: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE runs SET export_key = ? WHERE run_id = ? AND status = 'RUNNING'",
                (export_key, run_id),
            )

    def completed_skus(self, run_id: str) -> set[str]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT sku FROM product_outcomes WHERE run_id = ?", (run_id,)
            )
            return {row[0] for row in rows}

    def next_batch_number(self, run_id: str) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(batch_number), 0) + 1 FROM batches WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            return row[0]

    def begin_batch(
        self, run_id: str, batch_number: int, products: list[dict[str, Any]]
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO batches (run_id, batch_number, status, products_json)
                VALUES (?, ?, 'IN_FLIGHT', ?)
                """,
                (run_id, batch_number, json.dumps(products)),
            )

    def complete_batch(
        self,
        run_id: str,
        batch_number: int,
        accepted: list[str],
        rejected: list[dict[str, Any]],
    ) -> None:
        with self._connect() as connection:
            connection.executemany(
                """
                INSERT INTO product_outcomes
                    (run_id, sku, status, reason, batch_number)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (run_id, sku, "ACCEPTED", None, batch_number)
                    for sku in accepted
                ]
                + [
                    (run_id, item["sku"], "REJECTED", item["reason"], batch_number)
                    for item in rejected
                ],
            )
            connection.execute(
                """
                UPDATE batches SET status = 'COMPLETED'
                WHERE run_id = ? AND batch_number = ?
                """,
                (run_id, batch_number),
            )

    def mark_batch(self, run_id: str, batch_number: int, status: str, error: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE batches SET status = ?, error = ? WHERE run_id = ? AND batch_number = ?",
                (status, error, run_id, batch_number),
            )
            connection.execute(
                "UPDATE runs SET status = ?, error = ? WHERE run_id = ?",
                (status, error, run_id),
            )

    def finish_run(self, run_id: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE runs SET status = 'COMPLETED', error = NULL WHERE run_id = ?",
                (run_id,),
            )

    def fail_run(self, run_id: str, error: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE runs SET status = 'FAILED', error = ? WHERE run_id = ?",
                (error, run_id),
            )

    def run_summary(self, run_id: str) -> dict[str, Any]:
        with self._connect() as connection:
            run = connection.execute(
                "SELECT status, export_key, error FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None:
                raise KeyError(f"Unknown run: {run_id}")
            counts = connection.execute(
                """
                SELECT status, COUNT(*) FROM product_outcomes
                WHERE run_id = ? GROUP BY status
                """,
                (run_id,),
            ).fetchall()
            results = {status.lower(): count for status, count in counts}
            return {
                "run_id": run_id,
                "status": run[0],
                "export_key": run[1],
                "error": run[2],
                "accepted": results.get("accepted", 0),
                "rejected": results.get("rejected", 0),
            }


class DynamoCheckpointStore:
    def __init__(self, table_name: str) -> None:
        self.table = aws_resource("dynamodb").Table(table_name)
        self.client = aws_client("dynamodb")

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
        if status in {"RUNNING", "FAILED"} and "in_flight_batch" in item:
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

    def _iter_query(self, run_id: str, prefix: str) -> Iterator[dict[str, Any]]:
        kwargs: dict[str, Any] = {
            "KeyConditionExpression": "pk = :pk AND begins_with(sk, :prefix)",
            "ExpressionAttributeValues": {
                ":pk": self._partition(run_id),
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

    def completed_skus(self, run_id: str) -> set[str]:
        return {item["sku"] for item in self._iter_query(run_id, "SKU#")}

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
        self.client.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": self.table.name,
                        "Item": {
                            "pk": {"S": self._partition(run_id)},
                            "sk": {"S": batch_key},
                            "status": {"S": "IN_FLIGHT"},
                            "products_json": {"S": json.dumps(products)},
                        },
                        "ConditionExpression": "attribute_not_exists(pk)",
                    }
                },
                {
                    "Update": {
                        "TableName": self.table.name,
                        "Key": {
                            "pk": {"S": self._partition(run_id)},
                            "sk": {"S": "META"},
                        },
                        "UpdateExpression": "SET in_flight_batch = :batch",
                        "ConditionExpression": "#status = :running AND attribute_not_exists(in_flight_batch)",
                        "ExpressionAttributeNames": {"#status": "status"},
                        "ExpressionAttributeValues": {
                            ":batch": {"S": batch_key},
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
        with self.table.batch_writer() as writer:
            for sku in accepted:
                writer.put_item(
                    Item={
                        "pk": self._partition(run_id),
                        "sk": f"SKU#{sku}",
                        "sku": sku,
                        "status": "ACCEPTED",
                        "batch_number": batch_number,
                    }
                )
            for item in rejected:
                writer.put_item(
                    Item={
                        "pk": self._partition(run_id),
                        "sk": f"SKU#{item['sku']}",
                        "sku": item["sku"],
                        "status": "REJECTED",
                        "reason": item["reason"],
                        "batch_number": batch_number,
                    }
                )
        self.table.update_item(
            Key={"pk": self._partition(run_id), "sk": batch_key},
            UpdateExpression="SET #status = :completed",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":completed": "COMPLETED"},
        )
        self.table.update_item(
            Key={"pk": self._partition(run_id), "sk": "META"},
            UpdateExpression="REMOVE in_flight_batch",
            ConditionExpression="in_flight_batch = :batch",
            ExpressionAttributeValues={":batch": batch_key},
        )

    def mark_batch(self, run_id: str, batch_number: int, status: str, error: str) -> None:
        batch_key = f"BATCH#{batch_number:08d}"
        self.table.update_item(
            Key={"pk": self._partition(run_id), "sk": batch_key},
            UpdateExpression="SET #status = :status, #error = :error",
            ExpressionAttributeNames={"#status": "status", "#error": "error"},
            ExpressionAttributeValues={":status": status, ":error": error},
        )
        update_expression = (
            "SET #status = :status, #error = :error REMOVE in_flight_batch"
            if status == "FAILED"
            else "SET #status = :status, #error = :error"
        )
        self.table.update_item(
            Key={"pk": self._partition(run_id), "sk": "META"},
            UpdateExpression=update_expression,
            ExpressionAttributeNames={"#status": "status", "#error": "error"},
            ExpressionAttributeValues={":status": status, ":error": error},
        )

    def finish_run(self, run_id: str) -> None:
        self.table.update_item(
            Key={"pk": self._partition(run_id), "sk": "META"},
            UpdateExpression="SET #status = :completed REMOVE #error",
            ConditionExpression="attribute_not_exists(in_flight_batch)",
            ExpressionAttributeNames={"#status": "status", "#error": "error"},
            ExpressionAttributeValues={":completed": "COMPLETED"},
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
        counts = {"ACCEPTED": 0, "REJECTED": 0}
        for item in self._iter_query(run_id, "SKU#"):
            counts[item["status"]] += 1
        return {
            "run_id": run_id,
            "status": run["status"],
            "export_key": run.get("export_key"),
            "error": run.get("error"),
            "accepted": counts["ACCEPTED"],
            "rejected": counts["REJECTED"],
        }
