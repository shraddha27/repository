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
