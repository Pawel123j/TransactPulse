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


def test_restored_gold_snapshot_keeps_row_counts_and_sums(tmp_path):
    """Delta-level check: restored gold reads the same latest snapshot, not every file."""
    from datetime import date
    from pathlib import Path

    deltalake = pytest.importorskip("deltalake")
    pa = pytest.importorskip("pyarrow")

    source_dir = tmp_path / "source"

    def daily(rows):
        cols = ("event_date", "country", "tx_count", "total_volume_pln")
        return pa.Table.from_pylist([dict(zip(cols, row, strict=True)) for row in rows])

    # v0: 3 stale rows; v1 overwrites with the current 2 rows (v0 files stay on disk).
    daily_path = source_dir / "gold" / "daily_volume_by_country"
    deltalake.write_deltalake(daily_path, daily([(date(2026, 6, 12), "PL", 9, 999.0)] * 3))
    deltalake.write_deltalake(
        daily_path,
        daily([(date(2026, 6, 13), "PL", 2, 530.0), (date(2026, 6, 13), "DE", 2, 93.5)]),
        mode="overwrite",
    )
    deltalake.write_deltalake(
        source_dir / "gold" / "scored_transactions", pa.table({"amount_pln": [100.0, 430.0]})
    )

    objects = {
        path.relative_to(source_dir).as_posix(): path.read_bytes()
        for path in sorted(source_dir.rglob("*"))
        if path.is_file()
    }
    objects["checkpoints/bronze/offsets/1"] = b"offset"  # never restored
    target = MemoryS3()
    backup(MemoryS3(objects), "source", tmp_path / "snapshot", writers_paused=True)
    assert restore(target, "target", tmp_path / "snapshot") == len(objects) - 1

    restored_dir = tmp_path / "restored"
    for key, data in target.objects.items():
        path = restored_dir / Path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def totals(root, table, column):
        snapshot = deltalake.DeltaTable(root / "gold" / table).to_pyarrow_table()
        return snapshot.num_rows, round(sum(snapshot.column(column).to_pylist()), 2)

    # Expected by hand: v1 only -> 2 rows, 530.0 + 93.5; scoring 100.0 + 430.0.
    expected = {
        ("daily_volume_by_country", "total_volume_pln"): (2, 623.5),
        ("scored_transactions", "amount_pln"): (2, 530.0),
    }
    for (table, column), value in expected.items():
        assert totals(source_dir, table, column) == value
        assert totals(restored_dir, table, column) == value
    # Every parquet file (incl. removed v0 ones) is restored, so only the Delta
    # snapshot gives the right answer; a raw file scan would count 5 rows.
    restored_files = list((restored_dir / "gold" / "daily_volume_by_country").glob("*.parquet"))
    assert len(restored_files) == 2
