import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest import bronze  # noqa: E402


NOW = datetime(2026, 7, 9, 12, 0, tzinfo=timezone.utc)


class FakeS3Client:
    def __init__(self, pages):
        self.pages = list(pages)
        self.aborted = []
        self.list_kwargs = []

    def list_multipart_uploads(self, **kwargs):
        self.list_kwargs.append(kwargs)
        return self.pages.pop(0)

    def abort_multipart_upload(self, *, Bucket, Key, UploadId):
        self.aborted.append((Bucket, Key, UploadId))


def upload(key, upload_id, age_seconds):
    return {
        "Key": key,
        "UploadId": upload_id,
        "Initiated": NOW - timedelta(seconds=age_seconds),
    }


def test_aborts_uploads_older_than_threshold():
    client = FakeS3Client(
        [
            {
                "Uploads": [
                    upload("table/data/a.parquet", "u1", 7200),
                    upload("table/metadata/b.avro", "u2", 4000),
                ],
                "IsTruncated": False,
            }
        ]
    )

    count = bronze._abort_stale_multipart_uploads(
        client, "seoul-dev", "table/", older_than_seconds=3600, now=NOW
    )

    assert count == 2
    assert ("seoul-dev", "table/data/a.parquet", "u1") in client.aborted
    assert ("seoul-dev", "table/metadata/b.avro", "u2") in client.aborted
    assert client.list_kwargs[0]["Prefix"] == "table/"


def test_keeps_recent_uploads_from_live_writers():
    client = FakeS3Client(
        [{"Uploads": [upload("table/data/live.parquet", "u3", 120)], "IsTruncated": False}]
    )

    count = bronze._abort_stale_multipart_uploads(
        client, "seoul-dev", "table/", older_than_seconds=3600, now=NOW
    )

    assert count == 0
    assert client.aborted == []


def test_follows_truncated_listings():
    client = FakeS3Client(
        [
            {
                "Uploads": [upload("table/data/a.parquet", "u1", 7200)],
                "IsTruncated": True,
                "NextKeyMarker": "table/data/a.parquet",
                "NextUploadIdMarker": "u1",
            },
            {
                "Uploads": [upload("table/data/b.parquet", "u2", 7200)],
                "IsTruncated": False,
            },
        ]
    )

    count = bronze._abort_stale_multipart_uploads(
        client, "seoul-dev", "table/", older_than_seconds=3600, now=NOW
    )

    assert count == 2
    assert client.list_kwargs[1]["KeyMarker"] == "table/data/a.parquet"
    assert client.list_kwargs[1]["UploadIdMarker"] == "u1"


def test_pyiceberg_table_runs_stale_upload_cleanup(monkeypatch):
    cleaned = []

    class FakeTable:
        pass

    fake_table = FakeTable()

    class FakeCatalog:
        def load_table(self, identifier):
            assert identifier == f"dev_x.{bronze.BRONZE_TABLE}"
            return fake_table

    monkeypatch.setattr(bronze, "_pyiceberg_catalog", lambda: FakeCatalog())
    monkeypatch.setattr(bronze, "_cleanup_stale_uploads", cleaned.append)

    table = bronze._pyiceberg_table("dev_x")

    assert table is fake_table
    assert cleaned == [fake_table]
