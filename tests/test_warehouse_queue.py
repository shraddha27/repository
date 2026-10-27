import csv
import json

import pytest

from app.services import warehouse_queue


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


def test_dispatch_queues_uncompleted_batches_and_sets_expected_total(
    tmp_path, monkeypatch
):
    catalogue = tmp_path / "catalogue.csv"
    _write_catalogue(catalogue, 3)
    sent = []
    totals = []

    requests = []

    class Queue:
        def send_message_batch(self, **request):
            request_copy = {**request, "Entries": list(request["Entries"])}
            requests.append(request_copy)
            sent.extend(request_copy["Entries"])
            return {"Successful": [{"Id": entry["Id"]} for entry in request["Entries"]]}

    class Checkpoints:
        def completed_batches(self, run_id):
            assert run_id == "queued-run"
            return {1}

        def set_batch_total(self, run_id, total):
            totals.append((run_id, total))

    monkeypatch.setattr(warehouse_queue, "BATCH_SIZE", 1)
    monkeypatch.setattr(warehouse_queue, "SQS_QUEUE_URL", "queue-url")
    monkeypatch.setattr(warehouse_queue, "aws_client", lambda name: Queue())

    total = warehouse_queue.dispatch_catalogue(
        str(catalogue), "queued-run", Checkpoints()
    )

    assert total == 3
    assert totals == [("queued-run", 3)]
    assert len(sent) == 2
    assert len(requests) == 1
    assert len(requests[0]["Entries"]) == 2
    assert all(message["MessageGroupId"].startswith("queued-run-") for message in sent)
    payloads = [json.loads(message["MessageBody"]) for message in sent]
    assert {item["batch_number"] for item in payloads} == {2, 3}
    assert all(item["run_id"] == "queued-run" for item in payloads)


def test_dispatch_uses_sqs_batch_limit_and_surfaces_partial_failures(
    tmp_path, monkeypatch
):
    catalogue = tmp_path / "catalogue.csv"
    _write_catalogue(catalogue, 21)
    request_sizes = []

    class Queue:
        def send_message_batch(self, **request):
            request_sizes.append(len(request["Entries"]))
            return {"Failed": [{"Id": request["Entries"][0]["Id"], "Code": "InternalError"}]}

    class Checkpoints:
        def completed_batches(self, run_id):
            return set()

        def set_batch_total(self, run_id, total):
            raise AssertionError("total should not be recorded after a failed queue write")

    monkeypatch.setattr(warehouse_queue, "BATCH_SIZE", 1)
    monkeypatch.setattr(warehouse_queue, "SQS_QUEUE_URL", "queue-url")
    monkeypatch.setattr(warehouse_queue, "aws_client", lambda name: Queue())

    with pytest.raises(RuntimeError, match="SQS rejected"):
        warehouse_queue.dispatch_catalogue(
            str(catalogue), "failed-queue-run", Checkpoints()
        )

    assert request_sizes == [10]
