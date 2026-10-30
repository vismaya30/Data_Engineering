import json
from collections.abc import Iterable
from pathlib import Path
from urllib.parse import urlparse

import boto3


def is_s3(path: str) -> bool:
    return path.startswith(("s3://", "s3a://"))


def _split(path: str) -> tuple[str, str]:
    parsed = urlparse(path)
    return parsed.netloc, parsed.path.lstrip("/")


def write_text(path: str, content: str) -> None:
    if is_s3(path):
        bucket, key = _split(path)
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")


def write_jsonl(path: str, records: Iterable[dict]) -> None:
    write_text(path, "".join(json.dumps(record, default=str) + "\n" for record in records))


def write_json(path: str, payload: dict) -> None:
    write_text(path, json.dumps(payload, indent=2, default=str))


def exists(path: str) -> bool:
    if is_s3(path):
        bucket, key = _split(path)
        response = boto3.client("s3").list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
        return response.get("KeyCount", 0) > 0
    return Path(path).exists()
