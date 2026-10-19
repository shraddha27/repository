import boto3
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient
from botocore.config import Config

from app.config import AWS_ENDPOINT_URL


def client(service_name: str) -> BaseClient:
    options = {}
    if AWS_ENDPOINT_URL:
        options["endpoint_url"] = AWS_ENDPOINT_URL
        if service_name == "s3":
            options["config"] = Config(s3={"addressing_style": "path"})
    return boto3.client(service_name, **options)


def resource(service_name: str) -> ServiceResource:
    options = {"endpoint_url": AWS_ENDPOINT_URL} if AWS_ENDPOINT_URL else {}
    return boto3.resource(service_name, **options)
