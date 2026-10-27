from app import aws
from app.storage import checkpoint, s3


def test_s3_store_targets_localstack_with_path_style(monkeypatch):
    options = {}
    client = object()

    def create_client(service_name, **kwargs):
        options["service"] = service_name
        options.update(kwargs)
        return client

    monkeypatch.setattr(aws, "AWS_ENDPOINT_URL", "http://localhost:4566")
    monkeypatch.setattr(aws.boto3, "client", create_client)

    store = s3.S3Store(bucket="catalogue")

    assert store.client is client
    assert options["service"] == "s3"
    assert options["endpoint_url"] == "http://localhost:4566"
    assert options["config"].s3["addressing_style"] == "path"


def test_dynamodb_checkpoint_store_targets_localstack(monkeypatch):
    options = {}
    client_options = {}
    table = object()
    client = object()

    class Resource:
        def Table(self, name):
            assert name == "catalogue-runs"
            return table

    def create_client(service_name, **kwargs):
        client_options["service"] = service_name
        client_options.update(kwargs)
        return client

    def create_resource(service_name, **kwargs):
        options["service"] = service_name
        options.update(kwargs)
        return Resource()

    monkeypatch.setattr(aws, "AWS_ENDPOINT_URL", "http://localhost:4566")
    monkeypatch.setattr(aws.boto3, "resource", create_resource)
    monkeypatch.setattr(aws.boto3, "client", create_client)

    store = checkpoint.DynamoCheckpointStore("catalogue-runs")

    assert store.table is table
    assert store.client is client
    assert options == {
        "service": "dynamodb",
        "endpoint_url": "http://localhost:4566",
    }
    assert client_options == {
        "service": "dynamodb",
        "endpoint_url": "http://localhost:4566",
    }


def test_dynamodb_batch_transaction_uses_low_level_client(monkeypatch):
    captured = {}

    class Table:
        name = "catalogue-runs"

    class Resource:
        def Table(self, name):
            assert name == "catalogue-runs"
            return Table()

    class Client:
        def transact_write_items(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(aws, "AWS_ENDPOINT_URL", "http://localhost:4566")
    monkeypatch.setattr(aws.boto3, "resource", lambda *args, **kwargs: Resource())
    monkeypatch.setattr(aws.boto3, "client", lambda *args, **kwargs: Client())

    store = checkpoint.DynamoCheckpointStore("catalogue-runs")
    store.begin_batch("test-run", 1, [{"sku": "sku-1"}])

    assert captured["TransactItems"][0]["Put"]["Item"]["pk"] == {
        "S": "RUN#test-run"
    }
    assert "products_hash" in captured["TransactItems"][0]["Put"]["Item"]
    assert "products_json" not in captured["TransactItems"][0]["Put"]["Item"]


def test_dynamodb_batch_completion_stores_outcomes_and_counts(monkeypatch):
    import json

    class Table:
        name = "catalogue-runs"

    table = Table()

    class Resource:
        def Table(self, name):
            assert name == "catalogue-runs"
            return table

    monkeypatch.setattr(aws, "AWS_ENDPOINT_URL", "http://localhost:4566")
    monkeypatch.setattr(aws.boto3, "resource", lambda *args, **kwargs: Resource())
    class Client:
        def transact_write_items(self, **kwargs):
            captured.update(kwargs)

    captured = {}

    monkeypatch.setattr(aws.boto3, "client", lambda *args, **kwargs: Client())

    store = checkpoint.DynamoCheckpointStore("catalogue-runs")
    store.complete_batch(
        "test-run",
        1,
        ["SKU-1"],
        [{"sku": "SKU-2", "reason": "invalid"}],
    )

    update = captured["TransactItems"][0]["Update"]
    values = update["ExpressionAttributeValues"]
    assert json.loads(values[":outcomes"]["S"]) == {
        "accepted": ["SKU-1"],
        "rejected": [{"sku": "SKU-2", "reason": "invalid"}],
    }
    assert values[":accepted"] == {"N": "1"}
    assert values[":rejected"] == {"N": "1"}
    assert captured["TransactItems"][1]["Update"]["UpdateExpression"] == (
        "ADD completed_batches :one"
    )


def test_dynamodb_run_summary_counts_legacy_and_batch_outcomes():
    class Table:
        def get_item(self, **kwargs):
            return {"Item": {"status": "COMPLETED"}}

        def query(self, **kwargs):
            prefix = kwargs["ExpressionAttributeValues"][":prefix"]
            if prefix == "BATCH#":
                return {
                    "Items": [
                        {"sk": "BATCH#00000001", "status": "COMPLETED"},
                        {
                            "sk": "BATCH#00000002",
                            "status": "COMPLETED",
                            "accepted_count": 2,
                            "rejected_count": 1,
                        },
                        {"sk": "BATCH#00000003", "status": "FAILED"},
                    ]
                }
            if prefix == "SKU#":
                return {
                    "Items": [
                        {
                            "batch_number": 1,
                            "status": "ACCEPTED",
                        },
                        {
                            "batch_number": 1,
                            "status": "REJECTED",
                        },
                        {
                            "batch_number": 3,
                            "status": "ACCEPTED",
                        },
                    ]
                }
            raise AssertionError(f"Unexpected query prefix: {prefix}")

    store = object.__new__(checkpoint.DynamoCheckpointStore)
    store.table = Table()

    summary = store.run_summary("test-run")

    assert summary["accepted"] == 3
    assert summary["rejected"] == 2


def test_distributed_rate_limiter_waits_for_shared_slot(monkeypatch):
    from botocore.exceptions import ClientError

    from app.clients import http

    calls = []

    class Client:
        def update_item(self, **kwargs):
            assert kwargs["TableName"] == "catalogue-runs"
            calls.append(kwargs)
            if len(calls) == 1:
                raise ClientError(
                    {"Error": {"Code": "ConditionalCheckFailedException"}},
                    "UpdateItem",
                )

    monkeypatch.setattr(aws, "AWS_ENDPOINT_URL", "http://localhost:4566")
    monkeypatch.setattr(aws.boto3, "client", lambda *args, **kwargs: Client())
    monkeypatch.setattr(http.time, "sleep", lambda _: None)
    monkeypatch.setattr(http.time, "time_ns", lambda: 1_000_000_000)

    http.DynamoDBRateLimiter("catalogue-runs", 20).wait()

    assert len(calls) == 2
    assert calls[1]["Key"] == {
        "pk": {"S": "RATE#WMS"},
        "sk": {"S": "GLOBAL"},
    }
