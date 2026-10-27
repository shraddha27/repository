import csv
import threading
import time

import httpx
import pytest

from app.services import warehouse_sync
from app.storage.checkpoint import CheckpointStore


def _write_catalogue(path, count):
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["id", "name", "category", "price", "currency", "updated_at"],
        )
        writer.writeheader()
        for number in range(1, count + 1):
            writer.writerow(
                {
                    "id": f"P{number:07d}",
                    "name": f"Product {number}",
                    "category": "HOME",
                    "price": "10.00",
                    "currency": "INR",
                    "updated_at": "2026-09-25T08:30:00Z",
                }
            )


def test_sync_uses_bounded_concurrent_workers(tmp_path, monkeypatch):
    checkpoint = CheckpointStore(str(tmp_path / "state.sqlite3"))
    checkpoint.start_run("parallel-run")
    catalogue = tmp_path / "catalogue.csv"
    _write_catalogue(catalogue, 12)
    monkeypatch.setattr(warehouse_sync, "BATCH_SIZE", 1)
    monkeypatch.setattr(warehouse_sync, "WMS_WORKERS", 3)
    active = 0
    maximum_active = 0
    guard = threading.Lock()

    def send_batch(products, client=None):
        nonlocal active, maximum_active
        with guard:
            active += 1
            maximum_active = max(maximum_active, active)
        time.sleep(0.02)
        with guard:
            active -= 1
        return {"accepted": [products[0]["sku"]], "rejected": []}

    monkeypatch.setattr(warehouse_sync, "send_batch", send_batch)

    warehouse_sync.sync_catalogue(str(catalogue), "parallel-run", checkpoint)

    assert 1 < maximum_active <= 3
    assert checkpoint.completed_batches("parallel-run") == set(range(1, 13))
    assert checkpoint.run_summary("parallel-run")["accepted"] == 12


def test_resume_skips_completed_batches_and_retries_failed_batch(tmp_path, monkeypatch):
    checkpoint = CheckpointStore(str(tmp_path / "state.sqlite3"))
    checkpoint.start_run("resume-run")
    catalogue = tmp_path / "catalogue.csv"
    _write_catalogue(catalogue, 3)
    monkeypatch.setattr(warehouse_sync, "BATCH_SIZE", 1)
    monkeypatch.setattr(warehouse_sync, "WMS_WORKERS", 1)
    with catalogue.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source))

    checkpoint.begin_batch("resume-run", 1, [warehouse_sync.transform(rows[0])])
    checkpoint.complete_batch("resume-run", 1, ["P0000001"], [])
    checkpoint.begin_batch("resume-run", 2, [warehouse_sync.transform(rows[1])])
    checkpoint.mark_batch("resume-run", 2, "FAILED", "temporary failure")
    checkpoint.start_run("resume-run")
    sent = []

    def send_batch(products, client=None):
        sent.append(products[0]["sku"])
        return {"accepted": [products[0]["sku"]], "rejected": []}

    monkeypatch.setattr(warehouse_sync, "send_batch", send_batch)

    warehouse_sync.sync_catalogue(str(catalogue), "resume-run", checkpoint)

    assert sent == ["P0000002", "P0000003"]
    assert checkpoint.completed_batches("resume-run") == {1, 2, 3}


def test_sync_stops_scheduling_after_error_and_drains_in_flight_batches(
    tmp_path, monkeypatch
):
    checkpoint = CheckpointStore(str(tmp_path / "state.sqlite3"))
    checkpoint.start_run("stop-run")
    catalogue = tmp_path / "catalogue.csv"
    _write_catalogue(catalogue, 5)
    monkeypatch.setattr(warehouse_sync, "BATCH_SIZE", 1)
    monkeypatch.setattr(warehouse_sync, "WMS_WORKERS", 2)
    sent = []
    second_started = threading.Event()

    def send_batch(products, client=None):
        sku = products[0]["sku"]
        sent.append(sku)
        if sku == "P0000001":
            second_started.wait(timeout=1)
            raise httpx.ConnectError("connection lost")
        if sku == "P0000002":
            second_started.set()
            time.sleep(0.03)
        return {"accepted": [sku], "rejected": []}

    monkeypatch.setattr(warehouse_sync, "send_batch", send_batch)

    with pytest.raises(RuntimeError, match="outcome is ambiguous"):
        warehouse_sync.sync_catalogue(str(catalogue), "stop-run", checkpoint)

    assert set(sent) == {"P0000001", "P0000002"}
    assert checkpoint.completed_batches("stop-run") == {2}
    assert checkpoint.run_summary("stop-run")["status"] == "AMBIGUOUS"


def test_duplicate_skus_across_batches_fail_without_resending_duplicate(
    tmp_path, monkeypatch
):
    checkpoint = CheckpointStore(str(tmp_path / "state.sqlite3"))
    checkpoint.start_run("duplicate-run")
    catalogue = tmp_path / "catalogue.csv"
    _write_catalogue(catalogue, 2)
    with catalogue.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source))
    rows[1]["id"] = rows[0]["id"]
    with catalogue.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    monkeypatch.setattr(warehouse_sync, "BATCH_SIZE", 1)
    monkeypatch.setattr(warehouse_sync, "WMS_WORKERS", 1)
    sent = []

    def send_batch(products, client=None):
        sent.append(products[0]["sku"])
        return {"accepted": [products[0]["sku"]], "rejected": []}

    monkeypatch.setattr(warehouse_sync, "send_batch", send_batch)

    with pytest.raises(ValueError, match="Duplicate product SKU"):
        warehouse_sync.sync_catalogue(str(catalogue), "duplicate-run", checkpoint)

    assert sent == ["P0000001"]
    assert checkpoint.run_summary("duplicate-run")["status"] == "FAILED"
