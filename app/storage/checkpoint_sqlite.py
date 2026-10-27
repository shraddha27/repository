import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any


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
                    error TEXT,
                    expected_batches INTEGER,
                    completed_batches INTEGER NOT NULL DEFAULT 0
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
            run_columns = {
                row[1] for row in connection.execute("PRAGMA table_info(runs)")
            }
            for column, column_type in (
                ("expected_batches", "INTEGER"),
                ("completed_batches", "INTEGER NOT NULL DEFAULT 0"),
            ):
                if column not in run_columns:
                    connection.execute(
                        f"ALTER TABLE runs ADD COLUMN {column} {column_type}"
                    )
                    if column == "completed_batches":
                        connection.execute(
                            """
                            UPDATE runs SET completed_batches = (
                                SELECT COUNT(*) FROM batches
                                WHERE batches.run_id = runs.run_id
                                    AND batches.status = 'COMPLETED'
                            )
                            """
                        )
            batch_columns = {
                row[1] for row in connection.execute("PRAGMA table_info(batches)")
            }
            for column, column_type in (
                ("outcomes_json", "TEXT"),
                ("accepted_count", "INTEGER"),
                ("rejected_count", "INTEGER"),
            ):
                if column not in batch_columns:
                    connection.execute(
                        f"ALTER TABLE batches ADD COLUMN {column} {column_type}"
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
                "SELECT batch_number FROM batches WHERE run_id = ? AND status = 'IN_FLIGHT'",
                (run_id,),
            ).fetchall()
            if status in {"RUNNING", "FAILED"} and in_flight:
                connection.execute(
                    "UPDATE batches SET status = 'AMBIGUOUS', error = ? "
                    "WHERE run_id = ? AND status = 'IN_FLIGHT'",
                    ("Process stopped during an in-flight WMS request", run_id),
                )
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

    def set_batch_total(self, run_id: str, total: int) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE runs SET expected_batches = ? "
                "WHERE run_id = ? AND status = 'RUNNING'",
                (total, run_id),
            )

    def completed_skus(self, run_id: str) -> set[str]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT sku FROM product_outcomes WHERE run_id = ?", (run_id,)
            )
            completed = {row[0] for row in rows}
            batches = connection.execute(
                "SELECT outcomes_json FROM batches "
                "WHERE run_id = ? AND status = 'COMPLETED' AND outcomes_json IS NOT NULL",
                (run_id,),
            )
            for (outcomes_json,) in batches:
                outcomes = json.loads(outcomes_json)
                completed.update(outcomes["accepted"])
                completed.update(item["sku"] for item in outcomes["rejected"])
            return completed

    def completed_batches(self, run_id: str) -> set[int]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT batch_number FROM batches WHERE run_id = ? AND status = 'COMPLETED'",
                (run_id,),
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
            cursor = connection.execute(
                """
                INSERT INTO batches (run_id, batch_number, status, products_json)
                VALUES (?, ?, 'IN_FLIGHT', ?)
                ON CONFLICT(run_id, batch_number) DO UPDATE SET
                    status = 'IN_FLIGHT', error = NULL
                WHERE batches.status = 'FAILED'
                    AND batches.products_json = excluded.products_json
                """,
                (run_id, batch_number, json.dumps(products)),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"Batch {batch_number} for run {run_id} is not available to claim"
                )

    def complete_batch(
        self,
        run_id: str,
        batch_number: int,
        accepted: list[str],
        rejected: list[dict[str, Any]],
    ) -> None:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE batches
                SET status = 'COMPLETED', outcomes_json = ?,
                    accepted_count = ?, rejected_count = ?, error = NULL
                WHERE run_id = ? AND batch_number = ? AND status = 'IN_FLIGHT'
                """,
                (
                    json.dumps({"accepted": accepted, "rejected": rejected}),
                    len(accepted),
                    len(rejected),
                    run_id,
                    batch_number,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"Batch {batch_number} for run {run_id} is not in flight"
                )
            connection.execute(
                "UPDATE runs SET completed_batches = completed_batches + 1 "
                "WHERE run_id = ?",
                (run_id,),
            )

    def batch_status(self, run_id: str, batch_number: int) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT status FROM batches WHERE run_id = ? AND batch_number = ?",
                (run_id, batch_number),
            ).fetchone()
            return row[0] if row else None

    def run_progress(self, run_id: str) -> dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT status, expected_batches, completed_batches, error "
                "FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown run: {run_id}")
            return {
                "status": row[0],
                "expected_batches": row[1],
                "completed_batches": row[2],
                "error": row[3],
            }

    def run_status(self, run_id: str) -> str:
        return self.run_progress(run_id)["status"]

    def mark_batch(self, run_id: str, batch_number: int, status: str, error: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE batches SET status = ?, error = ? WHERE run_id = ? AND batch_number = ?",
                (status, error, run_id, batch_number),
            )
            connection.execute(
                """
                UPDATE runs
                SET status = CASE
                        WHEN status = 'AMBIGUOUS' THEN status
                        WHEN ? = 'AMBIGUOUS' THEN 'AMBIGUOUS'
                        ELSE ?
                    END,
                    error = CASE
                        WHEN status = 'AMBIGUOUS' THEN error
                        WHEN ? = 'AMBIGUOUS' THEN ?
                        ELSE ?
                    END
                WHERE run_id = ?
                """,
                (status, status, status, error, error, run_id),
            )

    def finish_run(self, run_id: str) -> None:
        with self._connect() as connection:
            in_flight = connection.execute(
                "SELECT 1 FROM batches WHERE run_id = ? AND status = 'IN_FLIGHT' LIMIT 1",
                (run_id,),
            ).fetchone()
            if in_flight:
                raise RuntimeError(f"Run {run_id} still has in-flight batches")
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
                "SELECT status, export_key, error, expected_batches, completed_batches "
                "FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if run is None:
                raise KeyError(f"Unknown run: {run_id}")
            counts = connection.execute(
                """
                SELECT COALESCE(SUM(accepted_count), 0),
                       COALESCE(SUM(rejected_count), 0)
                FROM batches WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            legacy = connection.execute(
                """
                SELECT status, COUNT(*) FROM product_outcomes
                WHERE run_id = ? AND batch_number IN (
                    SELECT batch_number FROM batches
                    WHERE run_id = ? AND accepted_count IS NULL
                )
                GROUP BY status
                """,
                (run_id, run_id),
            ).fetchall()
            results = {status: count for status, count in legacy}
            return {
                "run_id": run_id,
                "status": run[0],
                "export_key": run[1],
                "error": run[2],
                "accepted": counts[0] + results.get("ACCEPTED", 0),
                "rejected": counts[1] + results.get("REJECTED", 0),
                "expected_batches": run[3],
                "completed_batches": run[4],
            }
