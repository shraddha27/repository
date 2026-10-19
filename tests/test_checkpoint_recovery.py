import csv

import httpx
import pytest

from app.services import warehouse_sync
from app.storage.checkpoint import CheckpointStore


def _write_catalogue(path):
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["id", "name", "category", "price", "currency", "updated_at"],
        )
        writer.writeheader()
        writer.writerows(
            [
                {
                    "id": "P0000001",
                    "name": "Accepted",
                    "category": "HOME",
                    "price": "10.00",
                    "currency": "INR",
                    "updated_at": "2026-09-25T08:30:00Z",
                },
                {
                    "id": "P0000002",
                    "name": "Rejected",
                    "category": "HOME",
                    "price": "20.00",
                    "currency": "INR",
                    "updated_at": "2026-09-25T08:30:00Z",
                },
            ]
        )


def test_sync_checkpoints_partial_success_and_skips_completed_rows(tmp_path, monkeypatch):
    checkpoint = CheckpointStore(str(tmp_path / "state.sqlite3"))
    checkpoint.start_run("run123")
    catalogue = tmp_path / "catalogue.csv"
    _write_catalogue(catalogue)
    sent_batches = []

    def send_batch(products, client=None):
        sent_batches.append(products)
        return {
            "accepted": ["P0000001"],
            "rejected": [{"sku": "P0000002", "reason": "invalid product"}],
        }

    monkeypatch.setattr(warehouse_sync, "send_batch", send_batch)

    warehouse_sync.sync_catalogue(str(catalogue), "run123", checkpoint)
    warehouse_sync.sync_catalogue(str(catalogue), "run123", checkpoint)

    summary = checkpoint.run_summary("run123")
    assert len(sent_batches) == 1
    assert summary["accepted"] == 1
    assert summary["rejected"] == 1


def test_ambiguous_wms_failure_blocks_replay(tmp_path, monkeypatch):
    checkpoint = CheckpointStore(str(tmp_path / "state.sqlite3"))
    checkpoint.start_run("run123")
    catalogue = tmp_path / "catalogue.csv"
    _write_catalogue(catalogue)

    def fail_after_send(products, client=None):
        raise httpx.ConnectError("connection lost")

    monkeypatch.setattr(warehouse_sync, "send_batch", fail_after_send)

    with pytest.raises(RuntimeError, match="outcome is ambiguous"):
        warehouse_sync.sync_catalogue(str(catalogue), "run123", checkpoint)

    assert checkpoint.start_run("run123") == "AMBIGUOUS"


def test_restart_during_in_flight_batch_marks_run_ambiguous(tmp_path):
    checkpoint = CheckpointStore(str(tmp_path / "state.sqlite3"))
    checkpoint.start_run("run123")
    checkpoint.begin_batch("run123", 1, [{"sku": "P0000001"}])

    assert checkpoint.start_run("run123") == "AMBIGUOUS"
    assert checkpoint.run_summary("run123")["status"] == "AMBIGUOUS"


def test_definitive_failure_can_resume_without_replaying_completed_skus(tmp_path):
    checkpoint = CheckpointStore(str(tmp_path / "state.sqlite3"))
    checkpoint.start_run("run123")
    checkpoint.fail_run("run123", "temporary PIM failure before WMS processing")

    assert checkpoint.start_run("run123") == "RUNNING"
    assert checkpoint.completed_skus("run123") == set()


def test_global_lock_prevents_overlapping_runs(tmp_path):
    checkpoint = CheckpointStore(str(tmp_path / "state.sqlite3"))
    checkpoint.acquire_run_lock("run123")
    checkpoint.acquire_run_lock("run123")

    with pytest.raises(RuntimeError, match="holds the global run lock"):
        checkpoint.acquire_run_lock("run456")

    checkpoint.release_run_lock("run123")
    checkpoint.acquire_run_lock("run456")
