import csv
import os
import re
import tempfile
from pathlib import Path
from uuid import uuid4

from app.clients.product_api import iter_products
from app.config import EXPORT_DIR


def export_catalogue(run_id: str | None = None) -> str:
    run_id = run_id or uuid4().hex
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", run_id):
        raise ValueError("run_id must contain 1-80 letters, digits, hyphens, or underscores")
    os.makedirs(EXPORT_DIR, exist_ok=True)
    path = os.path.join(EXPORT_DIR, f"catalogue-{run_id}.csv")
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            newline="",
            encoding="utf-8",
            dir=EXPORT_DIR,
            prefix=".catalogue-",
            suffix=".tmp",
            delete=False,
        ) as output:
            temporary_path = output.name
            writer = csv.DictWriter(
                output,
                fieldnames=["id", "name", "category", "price", "currency", "updated_at"],
            )
            writer.writeheader()
            writer.writerows(iter_products())
        os.replace(temporary_path, path)
        temporary_path = None
    finally:
        if temporary_path is not None and os.path.exists(temporary_path):
            os.unlink(temporary_path)
    return str(Path(path))
