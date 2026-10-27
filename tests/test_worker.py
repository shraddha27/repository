import json

from app import worker
from app.config import RUN_COMPLETION_TIMEOUT_SECONDS


def test_queued_run_timeout_defaults_to_four_hours():
    assert RUN_COMPLETION_TIMEOUT_SECONDS == 4 * 60 * 60


def _message(batch_number=1):
    return {
        "ReceiptHandle": "receipt",
        "Body": json.dumps(
            {
                "run_id": "worker-run",
                "batch_number": batch_number,
                "products": [{"sku": "P0000001"}],
            }
        ),
    }


def test_worker_processes_and_deletes_completed_batch(monkeypatch):
    class Queue:
        deleted = []

        def delete_message(self, **kwargs):
            self.deleted.append(kwargs)

    class Checkpoints:
        state = "RUNNING"
        batch = None

        def run_status(self, run_id):
            return self.state

        def batch_status(self, run_id, number):
            return self.batch

    class Protection:
        changes = []

        def set_enabled(self, enabled):
            self.changes.append(enabled)

    queue = Queue()
    checkpoints = Checkpoints()
    protection = Protection()
    worker_instance = worker.QueueWorker.__new__(worker.QueueWorker)
    worker_instance.queue = queue
    worker_instance.checkpoints = checkpoints
    worker_instance.http_client = object()
    worker_instance.scale_in_protection = protection

    def process_batch(products, run_id, number, store, client):
        checkpoints.batch = "COMPLETED"

    monkeypatch.setattr(worker, "SQS_QUEUE_URL", "queue-url")
    monkeypatch.setattr(worker, "process_batch", process_batch)

    worker_instance.process(_message())

    assert queue.deleted == [{"QueueUrl": "queue-url", "ReceiptHandle": "receipt"}]
    assert protection.changes == [True, False]


def test_worker_marks_redelivered_in_flight_batch_ambiguous(monkeypatch):
    class Queue:
        deleted = []

        def delete_message(self, **kwargs):
            self.deleted.append(kwargs)

    class Checkpoints:
        def run_status(self, run_id):
            return "RUNNING"

        def batch_status(self, run_id, number):
            return "IN_FLIGHT"

        def mark_batch(self, run_id, number, status, reason):
            self.marked = (run_id, number, status, reason)

    queue = Queue()
    checkpoints = Checkpoints()
    worker_instance = worker.QueueWorker.__new__(worker.QueueWorker)
    worker_instance.queue = queue
    worker_instance.checkpoints = checkpoints
    worker_instance.http_client = object()
    worker_instance.scale_in_protection = None
    monkeypatch.setattr(worker, "SQS_QUEUE_URL", "queue-url")

    worker_instance.process(_message())

    assert checkpoints.marked[:3] == ("worker-run", 1, "AMBIGUOUS")
    assert queue.deleted


def test_task_scale_in_protection_updates_task_protection(monkeypatch):
    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return {"TaskARN": "arn:aws:ecs:region:account:task/cluster/task-id"}

    calls = []

    class ECS:
        def update_task_protection(self, **kwargs):
            calls.append(kwargs)
            return {"failures": []}

    monkeypatch.setattr(
        worker.httpx,
        "get",
        lambda url, **kwargs: Response(),
    )
    monkeypatch.setattr(worker, "aws_client", lambda service: ECS())

    protection = worker.TaskScaleInProtection("http://metadata", "catalogue")
    protection.set_enabled(True)
    protection.set_enabled(False)

    assert calls == [
        {
            "cluster": "catalogue",
            "tasks": ["arn:aws:ecs:region:account:task/cluster/task-id"],
            "protectionEnabled": True,
            "expiresInMinutes": 120,
        },
        {
            "cluster": "catalogue",
            "tasks": ["arn:aws:ecs:region:account:task/cluster/task-id"],
            "protectionEnabled": False,
        },
    ]


def test_worker_closes_reused_http_client():
    class Client:
        closed = False

        def close(self):
            self.closed = True

    client = Client()
    worker_instance = worker.QueueWorker.__new__(worker.QueueWorker)
    worker_instance.http_client = client

    worker_instance.close()

    assert client.closed


def test_worker_reuses_http_client_across_batches(monkeypatch):
    class Queue:
        def delete_message(self, **kwargs):
            pass

    class Checkpoints:
        def run_status(self, run_id):
            return "RUNNING"

        def batch_status(self, run_id, number):
            return None

    clients = []
    worker_instance = worker.QueueWorker.__new__(worker.QueueWorker)
    worker_instance.queue = Queue()
    worker_instance.checkpoints = Checkpoints()
    worker_instance.http_client = object()
    worker_instance.scale_in_protection = None

    def process_batch(products, run_id, number, store, client):
        clients.append(client)

    monkeypatch.setattr(worker, "SQS_QUEUE_URL", "queue-url")
    monkeypatch.setattr(worker, "process_batch", process_batch)

    worker_instance.process(_message(1))
    worker_instance.process(_message(2))

    assert len(clients) == 2
    assert clients[0] is clients[1] is worker_instance.http_client
