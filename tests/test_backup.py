"""Safety regressions for offline backups; live S3 restoration is opt-in."""

import hashlib
import io

import pytest

from lakehouse.backup import backup, restore


class MemoryS3:
    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.writes = []

    def get_paginator(self, name):
        return self

    def paginate(self, Bucket, Prefix):
        yield {
            "Contents": [
                {"Key": key, "Size": len(data), "ETag": hashlib.sha256(data).hexdigest()}
                for key, data in self.objects.items()
                if key.startswith(Prefix)
            ]
        }

    def get_object(self, Bucket, Key, IfMatch):
        data = self.objects[Key]
        assert hashlib.sha256(data).hexdigest() == IfMatch
        return {"Body": io.BytesIO(data)}

    def list_objects_v2(self, **kwargs):
        return {"Contents": list(self.objects)}

    def put_object(self, Bucket, Key, Body, **kwargs):
        assert kwargs["IfNoneMatch"] == "*"
        assert Key not in self.objects
        self.objects[Key] = Body.read()
        self.writes.append(Key)


def test_backup_restore_preserves_delta_log_and_data_but_excludes_checkpoints(tmp_path):
    data = {"gold/t/_delta_log/000.json": b'{"add":{}}', "bronze/a.parquet": b"data"}
    source = MemoryS3({**data, "checkpoints/bronze/offsets/1": b"offset"})
    directory = tmp_path / "backup"
    manifest = backup(source, "source", directory, writers_paused=True)
    assert len(manifest["objects"]) == 2
    target = MemoryS3()
    assert restore(target, "target", directory) == 2
    assert target.objects == data
    with pytest.raises(FileExistsError):
        backup(source, "source", directory, writers_paused=True)


def test_backup_requires_quiescent_source(tmp_path):
    with pytest.raises(ValueError, match="Pause"):
        backup(MemoryS3(), "source", tmp_path / "backup", writers_paused=False)
    assert not (tmp_path / "backup").exists()


def test_restore_rejects_corrupt_last_file_before_writing_anything(tmp_path):
    source = MemoryS3({"gold/a": b"a", "gold/b": b"b"})
    directory = tmp_path / "backup"
    backup(source, "source", directory, writers_paused=True)
    (directory / "00000001.bin").write_bytes(b"corrupt")
    target = MemoryS3()
    with pytest.raises(ValueError, match="SHA-256"):
        restore(target, "target", directory)
    assert not target.writes


def test_restore_never_overwrites_existing_bucket(tmp_path):
    directory = tmp_path / "backup"
    backup(MemoryS3({"gold/a": b"new"}), "source", directory, writers_paused=True)
    target = MemoryS3({"gold/a": b"user-data"})
    with pytest.raises(ValueError, match="empty"):
        restore(target, "target", directory)
    assert target.objects == {"gold/a": b"user-data"}


def test_source_change_does_not_produce_complete_manifest(tmp_path):
    class MutatingS3(MemoryS3):
        def get_object(self, **kwargs):
            result = super().get_object(**kwargs)
            self.objects["gold/new"] = b"changed"
            return result

    directory = tmp_path / "backup"
    with pytest.raises(ValueError, match="Source changed"):
        backup(MutatingS3({"gold/a": b"a"}), "source", directory, writers_paused=True)
    assert not (directory / "manifest.json").exists()


def test_s3_keys_cannot_escape_backup_directory(tmp_path):
    directory = tmp_path / "backup"
    source = MemoryS3({"gold/../../outside": b"data"})
    backup(source, "source", directory, writers_paused=True)
    assert sorted(p.name for p in directory.iterdir()) == ["00000000.bin", "manifest.json"]
    assert not (tmp_path / "outside").exists()
