import sys
import urllib.error
from email.message import Message
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest.common import runtime  # noqa: E402


class FakeResponse:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self):
        return b"ok"


def test_fetch_url_retries_configured_429(monkeypatch):
    calls = []
    sleeps = []

    def fake_urlopen(request, timeout):
        calls.append((request, timeout))
        if len(calls) == 1:
            headers = Message()
            headers["Retry-After"] = "0"
            raise urllib.error.HTTPError(request.full_url, 429, "Too Many Requests", headers, None)
        return FakeResponse()

    monkeypatch.setattr(runtime.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(runtime.time, "sleep", sleeps.append)

    assert runtime.fetch_url(
        "https://example.test/data",
        "ask-seoul-test/1.0",
        max_attempts=2,
        retry_statuses=(429,),
        retry_base_delay_seconds=30,
    ) == (200, b"ok")
    assert len(calls) == 2
    assert sleeps == [0.0]
