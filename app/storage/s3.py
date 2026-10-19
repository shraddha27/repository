import shutil
from pathlib import Path

from app.aws import client as aws_client


class S3Store:
    def __init__(self, root: str = "./runtime/s3", bucket: str | None = None) -> None:
        self.root = Path(root)
        self.bucket = bucket
        self.client = aws_client("s3") if bucket else None

    def _local_path(self, key: str) -> Path:
        key_path = Path(key)
        if key_path.is_absolute() or ".." in key_path.parts:
            raise ValueError("S3 object key must be a relative path")
        return self.root / key_path

    def put_file(self, source: str | Path, key: str) -> str:
        if self.bucket:
            self.client.upload_file(str(source), self.bucket, key)
            return key
        destination = self._local_path(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        return str(destination)

    def get_file(self, key: str) -> Path:
        if self.bucket:
            raise RuntimeError("Use download_file with a destination for S3 objects")
        return self._local_path(key)

    def download_file(self, key: str, destination: str | Path) -> Path:
        destination = Path(destination)
        if self.bucket:
            self.client.download_file(self.bucket, key, str(destination))
        else:
            shutil.copyfile(self._local_path(key), destination)
        return destination
