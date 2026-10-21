"""Offline S3 snapshot/restore of Delta data, independent of Spark checkpoints.

Pause all lakehouse writers before backup and all readers/writers of the empty
restore target. The caller owns both buckets; this module never deletes objects.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

from streaming.s3_config import ssl_enabled

PREFIXES = ("bronze/", "silver/", "gold/")
CHUNK_SIZE = 1024 * 1024


def inventory(client, bucket: str) -> list[dict]:
    objects = []
    for prefix in PREFIXES:
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            objects.extend(
                {"key": obj["Key"], "size": obj["Size"], "etag": obj["ETag"]}
                for obj in page.get("Contents", [])
            )
    return sorted(objects, key=lambda obj: obj["key"])


def backup(client, bucket: str, destination: Path, *, writers_paused: bool) -> dict:
    if not writers_paused:
        raise ValueError("Pause lakehouse writers and explicitly confirm before backup")
    objects = inventory(client, bucket)
    if not objects:
        raise ValueError("No Delta data under bronze/, silver/ or gold/")
    destination.mkdir(mode=0o700, parents=False, exist_ok=False)
    entries = []
    for index, obj in enumerate(objects):
        filename = f"{index:08d}.bin"  # S3 keys are never used as filesystem paths.
        response = client.get_object(Bucket=bucket, Key=obj["key"], IfMatch=obj["etag"])
        digest = hashlib.sha256()
        size = 0
        body = response["Body"]
        try:
            with (destination / filename).open("xb") as output:
                while chunk := body.read(CHUNK_SIZE):
                    output.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
        finally:
            body.close()
        if size != obj["size"]:
            raise ValueError("Source size changed during backup")
        entries.append(
            {
                **obj,
                "file": filename,
                "sha256": digest.hexdigest(),
                "content_type": response.get("ContentType", "application/octet-stream"),
            }
        )
    if inventory(client, bucket) != objects:
        raise ValueError("Source changed during backup; no complete manifest was written")
    manifest = {
        "format": 1,
        "source_bucket": bucket,
        "prefixes": list(PREFIXES),
        "objects": entries,
    }
    with (destination / "manifest.json").open("x", encoding="utf-8") as output:
        json.dump(manifest, output, indent=2)
    return manifest


def _verified_entries(directory: Path) -> list[dict]:
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    entries = manifest.get("objects")
    if manifest.get("format") != 1 or not isinstance(entries, list) or not entries:
        raise ValueError("Invalid or empty backup manifest")
    keys = set()
    for index, entry in enumerate(entries):
        key = entry["key"]
        filename = f"{index:08d}.bin"
        if (
            not isinstance(key, str)
            or not key.startswith(PREFIXES)
            or key in keys
            or entry["file"] != filename
        ):
            raise ValueError("Invalid object key, duplicate or local filename")
        keys.add(key)
        path = directory / filename
        if path.is_symlink() or not path.is_file():
            raise ValueError("Backup object must be a regular file")
        with path.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        if path.stat().st_size != entry["size"] or digest != entry["sha256"]:
            raise ValueError("Backup object failed size/SHA-256 verification")
    return entries


def restore(client, bucket: str, directory: Path) -> int:
    entries = _verified_entries(directory)  # Check every file before the first write.
    if client.list_objects_v2(Bucket=bucket, MaxKeys=1).get("Contents"):
        raise ValueError("Restore target must be an existing empty, isolated bucket")
    for entry in entries:
        with (directory / entry["file"]).open("rb") as source:
            client.put_object(
                Bucket=bucket,
                Key=entry["key"],
                Body=source,
                ContentType=entry["content_type"],
                IfNoneMatch="*",
            )
    return len(entries)


def main() -> None:
    import boto3

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["backup", "restore"])
    parser.add_argument("bucket")
    parser.add_argument("directory", type=Path)
    parser.add_argument("--writers-paused", action="store_true")
    args = parser.parse_args()
    endpoint = os.environ["S3_ENDPOINT"]
    ssl_enabled(endpoint)  # Validate before passing credentials; HTTPS verification stays on.
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
    )
    if args.operation == "backup":
        count = len(
            backup(client, args.bucket, args.directory, writers_paused=args.writers_paused)[
                "objects"
            ]
        )
    else:
        count = restore(client, args.bucket, args.directory)
    print(f"{args.operation}: {count} objects")  # noqa: T201


if __name__ == "__main__":
    main()
