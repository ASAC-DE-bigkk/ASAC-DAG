import sys
import types
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest import link_reference_ingest as runtime  # noqa: E402


INFO_PAYLOAD = b"""<LinkInfo><list_total_count>1</list_total_count>
<RESULT><CODE>INFO-000</CODE><MESSAGE>ok</MESSAGE></RESULT>
<row><LINK_ID>1220003800</LINK_ID><ROAD_NAME>test-road</ROAD_NAME></row></LinkInfo>"""
VERTEX_PAYLOAD = b"""<LinkVerInfo><list_total_count>1</list_total_count>
<RESULT><CODE>INFO-000</CODE><MESSAGE>ok</MESSAGE></RESULT>
<row><LINK_ID>1220003800</LINK_ID><VER_SEQ>1</VER_SEQ><GRS80TM_X>194000</GRS80TM_X><GRS80TM_Y>451000</GRS80TM_Y></row></LinkVerInfo>"""


def test_runtime_uses_path_auth_without_putting_the_seoul_key_in_url_metadata(
    monkeypatch,
):
    calls = []

    class Http:
        def get(self, url, *, auth):
            calls.append((url, auth))
            payload = INFO_PAYLOAD if "/LinkInfo/" in url else VERTEX_PAYLOAD
            return types.SimpleNamespace(status=200, content=payload)

    class Store:
        def __init__(self):
            self.objects = {}

        def write_bytes(self, key, payload, content_type):
            self.objects[key] = (payload, content_type)

    store = Store()
    monkeypatch.setattr(runtime, "_build_link_reference_http", lambda: Http())
    monkeypatch.setattr(runtime, "traffic_api_key", lambda: "fixture-secret")
    monkeypatch.setattr(runtime, "_build_s3_client", lambda: object())
    monkeypatch.setattr(runtime, "r2_env", lambda name: "seoul-dev")
    monkeypatch.setattr(runtime, "R2RawObjectStore", lambda *_args, **_kwargs: store)

    result = runtime.build_traffic_link_reference_landing().collect(
        link_ids=["1220003800"],
        dag_run_id="manual__link-ref",
        landing_load_date="2026-08-10",
    )

    assert len(result["raw_objects"]) == 2
    assert ["/LinkInfo/" in url for url, _auth in calls] == [True, False]
    assert all("{api_key}" in url for url, _auth in calls)
    assert all("fixture-secret" not in url for url, _auth in calls)
    assert all(
        auth.apply(url, None, None).url.count("fixture-secret") == 1
        for url, auth in calls
    )
