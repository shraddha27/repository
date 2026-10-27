import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import asyncpg


class PostgresCheckpoints:
    def __init__(self, database_url: str):
        self._database_url = database_url
        self._pool: asyncpg.Pool | None = None

    async def initialize(self) -> None:
        self._pool = await asyncpg.create_pool(
            self._database_url, min_size=1, max_size=32, command_timeout=30
        )
        async with self._connection() as connection:
            await connection.execute(
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
                    run_id TEXT NOT NULL REFERENCES runs(run_id),
                    batch_number INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    products_json TEXT NOT NULL,
                    outcomes_json TEXT,
                    accepted_count INTEGER,
                    rejected_count INTEGER,
                    error TEXT,
                    PRIMARY KEY (run_id, batch_number)
                );
                """
            )

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    @asynccontextmanager
    async def _connection(self) -> AsyncIterator[asyncpg.Connection]:
        if self._pool is None:
            raise RuntimeError("Checkpoint pool is not initialized")
        async with self._pool.acquire() as connection:
            yield connection

    async def acquire_run_lock(self, run_id: str) -> None:
        async with self._connection() as connection:
            acquired = await connection.fetchval(
                """
                INSERT INTO run_lock (lock_name, run_id)
                VALUES ('catalogue', $1)
                ON CONFLICT (lock_name) DO UPDATE SET run_id = EXCLUDED.run_id
                WHERE run_lock.run_id = EXCLUDED.run_id
                RETURNING run_id
                """,
                run_id,
            )
            if acquired is None:
                holder = await connection.fetchval(
                    "SELECT run_id FROM run_lock WHERE lock_name = 'catalogue'"
                )
                raise RuntimeError(
                    f"Catalogue run {holder} currently holds the global run lock"
                )

    async def release_run_lock(self, run_id: str) -> None:
        async with self._connection() as connection:
            await connection.execute(
                "DELETE FROM run_lock WHERE lock_name = 'catalogue' AND run_id = $1",
                run_id,
            )

    async def start_run(self, run_id: str) -> str:
        async with self._connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    "INSERT INTO runs (run_id, status) VALUES ($1, 'RUNNING') "
                    "ON CONFLICT (run_id) DO NOTHING",
                    run_id,
                )
                status = await connection.fetchval(
                    "SELECT status FROM runs WHERE run_id = $1", run_id
                )
                in_flight = await connection.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM batches "
                    "WHERE run_id = $1 AND status = 'IN_FLIGHT')",
                    run_id,
                )
                if status in {"RUNNING", "FAILED"} and in_flight:
                    reason = "Process stopped during an in-flight WMS request"
                    await connection.execute(
                        "UPDATE batches SET status = 'AMBIGUOUS', error = $1 "
                        "WHERE run_id = $2 AND status = 'IN_FLIGHT'",
                        reason,
                        run_id,
                    )
                    await connection.execute(
                        "UPDATE runs SET status = 'AMBIGUOUS', error = $1 "
                        "WHERE run_id = $2",
                        reason,
                        run_id,
                    )
                    return "AMBIGUOUS"
                if status == "FAILED":
                    await connection.execute(
                        "UPDATE runs SET status = 'RUNNING', error = NULL "
                        "WHERE run_id = $1",
                        run_id,
                    )
                    return "RUNNING"
                return status

    async def get_export_key(self, run_id: str) -> str | None:
        async with self._connection() as connection:
            return await connection.fetchval(
                "SELECT export_key FROM runs WHERE run_id = $1", run_id
            )

    async def set_export_key(self, run_id: str, key: str) -> None:
        async with self._connection() as connection:
            await connection.execute(
                "UPDATE runs SET export_key = $1 "
                "WHERE run_id = $2 AND status = 'RUNNING'",
                key,
                run_id,
            )

    async def completed_batches(self, run_id: str) -> set[int]:
        async with self._connection() as connection:
            rows = await connection.fetch(
                "SELECT batch_number FROM batches "
                "WHERE run_id = $1 AND status = 'COMPLETED'",
                run_id,
            )
            return {row["batch_number"] for row in rows}

    async def set_batch_total(self, run_id: str, total: int) -> None:
        async with self._connection() as connection:
            await connection.execute(
                "UPDATE runs SET expected_batches = $1 "
                "WHERE run_id = $2 AND status = 'RUNNING'",
                total,
                run_id,
            )

    async def begin_batch(
        self, run_id: str, number: int, products: list[dict[str, Any]]
    ) -> None:
        products_json = json.dumps(products, separators=(",", ":"))
        async with self._connection() as connection:
            claimed = await connection.fetchval(
                """
                INSERT INTO batches (run_id, batch_number, status, products_json)
                VALUES ($1, $2, 'IN_FLIGHT', $3)
                ON CONFLICT (run_id, batch_number) DO UPDATE SET
                    status = 'IN_FLIGHT', error = NULL
                WHERE batches.status = 'FAILED'
                    AND batches.products_json = EXCLUDED.products_json
                RETURNING batch_number
                """,
                run_id,
                number,
                products_json,
            )
            if claimed is None:
                raise RuntimeError(
                    f"Batch {number} for run {run_id} is not available to claim"
                )

    async def complete_batch(
        self,
        run_id: str,
        number: int,
        accepted: list[str],
        rejected: list[dict[str, Any]],
    ) -> None:
        async with self._connection() as connection:
            async with connection.transaction():
                completed = await connection.fetchval(
                    """
                    UPDATE batches
                    SET status = 'COMPLETED', outcomes_json = $1,
                        accepted_count = $2, rejected_count = $3, error = NULL
                    WHERE run_id = $4 AND batch_number = $5 AND status = 'IN_FLIGHT'
                    RETURNING batch_number
                    """,
                    json.dumps(
                        {"accepted": accepted, "rejected": rejected},
                        separators=(",", ":"),
                    ),
                    len(accepted),
                    len(rejected),
                    run_id,
                    number,
                )
                if completed is None:
                    raise RuntimeError(
                        f"Batch {number} for run {run_id} is not in flight"
                    )
                await connection.execute(
                    "UPDATE runs SET completed_batches = completed_batches + 1 "
                    "WHERE run_id = $1",
                    run_id,
                )

    async def mark_batch(
        self, run_id: str, number: int, status: str, error: str
    ) -> None:
        async with self._connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    "UPDATE batches SET status = $1, error = $2 "
                    "WHERE run_id = $3 AND batch_number = $4",
                    status,
                    error,
                    run_id,
                    number,
                )
                await connection.execute(
                    """
                    UPDATE runs
                    SET status = CASE
                            WHEN status = 'AMBIGUOUS' THEN status
                            WHEN $1 = 'AMBIGUOUS' THEN 'AMBIGUOUS'
                            ELSE $1
                        END,
                        error = CASE
                            WHEN status = 'AMBIGUOUS' THEN error
                            WHEN $1 = 'AMBIGUOUS' THEN $2
                            ELSE $2
                        END
                    WHERE run_id = $3
                    """,
                    status,
                    error,
                    run_id,
                )

    async def batch_status(self, run_id: str, number: int) -> str | None:
        async with self._connection() as connection:
            return await connection.fetchval(
                "SELECT status FROM batches WHERE run_id = $1 AND batch_number = $2",
                run_id,
                number,
            )

    async def run_status(self, run_id: str) -> str:
        async with self._connection() as connection:
            status = await connection.fetchval(
                "SELECT status FROM runs WHERE run_id = $1", run_id
            )
            if status is None:
                raise KeyError(f"Unknown run: {run_id}")
            return status

    async def run_progress(self, run_id: str) -> dict[str, Any]:
        async with self._connection() as connection:
            row = await connection.fetchrow(
                "SELECT status, expected_batches, completed_batches, error "
                "FROM runs WHERE run_id = $1",
                run_id,
            )
            if row is None:
                raise KeyError(f"Unknown run: {run_id}")
            return dict(row)

    async def finish_run(self, run_id: str) -> None:
        async with self._connection() as connection:
            async with connection.transaction():
                in_flight = await connection.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM batches "
                    "WHERE run_id = $1 AND status = 'IN_FLIGHT')",
                    run_id,
                )
                if in_flight:
                    raise RuntimeError(f"Run {run_id} still has in-flight batches")
                result = await connection.execute(
                    "UPDATE runs SET status = 'COMPLETED', error = NULL "
                    "WHERE run_id = $1 AND status = 'RUNNING'",
                    run_id,
                )
                if result != "UPDATE 1":
                    raise RuntimeError(f"Run {run_id} is not running")

    async def fail_run(self, run_id: str, error: str) -> None:
        async with self._connection() as connection:
            await connection.execute(
                """
                UPDATE runs
                SET status = CASE WHEN status = 'AMBIGUOUS' THEN status ELSE 'FAILED' END,
                    error = CASE WHEN status = 'AMBIGUOUS' THEN error ELSE $1 END
                WHERE run_id = $2
                """,
                error,
                run_id,
            )

    async def run_summary(self, run_id: str) -> dict[str, Any]:
        async with self._connection() as connection:
            row = await connection.fetchrow(
                """
                SELECT runs.run_id, runs.status, runs.export_key, runs.error,
                       runs.expected_batches, runs.completed_batches,
                       COALESCE(SUM(batches.accepted_count), 0) AS accepted,
                       COALESCE(SUM(batches.rejected_count), 0) AS rejected
                FROM runs
                LEFT JOIN batches ON batches.run_id = runs.run_id
                WHERE runs.run_id = $1
                GROUP BY runs.run_id
                """,
                run_id,
            )
            if row is None:
                raise KeyError(f"Unknown run: {run_id}")
            return dict(row)
