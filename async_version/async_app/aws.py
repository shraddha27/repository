from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import aioboto3
from botocore.config import Config

from async_app.config import AWS_ENDPOINT_URL


def session() -> aioboto3.Session:
    return aioboto3.Session()


def client_options(service_name: str) -> dict[str, Any]:
    options: dict[str, Any] = {}
    if AWS_ENDPOINT_URL:
        options["endpoint_url"] = AWS_ENDPOINT_URL
    if AWS_ENDPOINT_URL and service_name == "s3":
        options["config"] = Config(s3={"addressing_style": "path"})
    return options


@asynccontextmanager
async def open_client(service_name: str) -> AsyncIterator[Any]:
    async with session().client(service_name, **client_options(service_name)) as client:
        yield client

