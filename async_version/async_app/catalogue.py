import asyncio
import csv
import os
import re
import tempfile
from pathlib import Path
from uuid import uuid4

import httpx

from async_app.config import EXPORT_DIR
from async_app.http_clients import ProductApi

FIELDS = ["id", "name", "category", "price", "currency", "updated_at"]


async def export_catalogue(
    product_api: ProductApi, run_id: str | None = None
) -> str:
    run_id = run_id or uuid4().hex
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", run_id):
        raise ValueError("run_id must contain 1-80 letters, digits, hyphens, or underscores")
    os.makedirs(EXPORT_DIR, exist_ok=True)
    target = Path(EXPORT_DIR) / f"catalogue-{run_id}.csv"
    temporary_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            newline="",
            encoding="utf-8",
            dir=EXPORT_DIR,
            prefix=".catalogue-",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = temporary.name
        with open(temporary_path, "w", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=FIELDS)
            writer.writeheader()
            async for product in product_api.iter_products():
                writer.writerow(product)
        os.replace(temporary_path, target)
        temporary_path = None
        return str(target)
    finally:
        if temporary_path is not None and os.path.exists(temporary_path):
            os.unlink(temporary_path)

