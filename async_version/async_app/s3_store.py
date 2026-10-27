import shutil
from pathlib import Path

from async_app.aws import open_client


class AsyncS3Store:
    def __init__(self, bucket: str | None, root: str = "./runtime/s3"):
        self._bucket = bucket
        self._root = Path(root)

    def _local_path(self, key: str) -> Path:
        path = Path(key)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("S3 object key must be a relative path")
        return self._root / path

    async def put_file(self, source: str, key: str) -> str:
        if self._bucket:
            async with open_client("s3") as client:
                upload = await client.create_multipart_upload(
                    Bucket=self._bucket, Key=key
                )
                upload_id = upload["UploadId"]
                parts = []
                try:
                    with open(source, "rb") as input_file:
                        part_number = 1
                        while chunk := input_file.read(16 * 1024 * 1024):
                            result = await client.upload_part(
                                Bucket=self._bucket,
                                Key=key,
                                UploadId=upload_id,
                                PartNumber=part_number,
                                Body=chunk,
                            )
                            parts.append(
                                {
                                    "PartNumber": part_number,
                                    "ETag": result["ETag"],
                                }
                            )
                            part_number += 1
                    if not parts:
                        parts.append(
                            {
                                "PartNumber": 1,
                                "ETag": (
                                    await client.upload_part(
                                        Bucket=self._bucket,
                                        Key=key,
                                        UploadId=upload_id,
                                        PartNumber=1,
                                        Body=b"",
                                    )
                                )["ETag"],
                            }
                        )
                    await client.complete_multipart_upload(
                        Bucket=self._bucket,
                        Key=key,
                        UploadId=upload_id,
                        MultipartUpload={"Parts": parts},
                    )
                except BaseException:
                    await client.abort_multipart_upload(
                        Bucket=self._bucket, Key=key, UploadId=upload_id
                    )
                    raise
            return key
        target = self._local_path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        return str(target)

    async def get_file(self, key: str) -> Path:
        if self._bucket:
            raise RuntimeError("Use download_file for S3 objects")
        return self._local_path(key)

    async def download_file(self, key: str, destination: str) -> Path:
        target = Path(destination)
        if not self._bucket:
            source = self._local_path(key)
            shutil.copyfile(source, target)
            return target

        async with open_client("s3") as client:
            response = await client.get_object(Bucket=self._bucket, Key=key)
            async with response["Body"] as body:
                with open(target, "wb") as output_file:
                    while chunk := await body.read(1024 * 1024):
                        output_file.write(chunk)
        return target
