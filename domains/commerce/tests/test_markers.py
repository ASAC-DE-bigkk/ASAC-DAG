"""bronze.markers — run_id 목록/최신·완료 short·미완료(재수집) 대상 단위 테스트."""
from bronze import markers
from commerce_core import paths
from commerce_core.storage import Storage


class FakeStorage(Storage):
    def __init__(self, keys):
        self.keys = list(keys)

    def write_bytes(self, key, data):
        self.keys.append(key)

    def read_bytes(self, key):
        return b""

    def exists(self, key):
        return key in self.keys

    def list_keys(self, prefix):
        return sorted(k for k in self.keys if k.startswith(prefix))

    def delete(self, key):
        if key in self.keys:
            self.keys.remove(key)


def test_list_and_latest_run_id_chronological():
    st = FakeStorage([
        paths.bronze_object_key(run_id="2026-06-30_100000_000", short="clinic"),
        paths.bronze_object_key(run_id="2026-06-30_120000_000", short="clinic"),
        paths.bronze_object_key(run_id="2026-06-29_235959_999", short="hospital"),
    ])
    assert markers.list_run_ids(st) == [
        "2026-06-29_235959_999", "2026-06-30_100000_000", "2026-06-30_120000_000"]
    assert markers.latest_run_id(st) == "2026-06-30_120000_000"   # 사전식=시간순
    assert markers.latest_run_id(FakeStorage([])) is None


def test_completed_shorts_ignores_run_marker():
    rid = "2026-06-30_120000_000"
    base = paths.bronze_run_dir(run_id=rid)
    st = FakeStorage([
        f"{base}/clinic.jsonl",
        f"{base}/_markers/clinic.completed",
        f"{base}/_markers/hospital.incomplete",
        f"{base}/_markers/_RUN.incomplete",      # 실행 마커는 short 로 세지 않음
    ])
    assert markers.completed_shorts(st, "", rid) == {"clinic"}


def test_incomplete_targets_picks_not_completed():
    rid = "2026-06-30_120000_000"
    base = paths.bronze_run_dir(run_id=rid)
    st = FakeStorage([
        f"{base}/_markers/clinic.completed",
        f"{base}/_markers/hospital.incomplete",  # 미완료 → 대상
    ])                                            # pharmacy 마커 없음 → 미시도 → 대상
    enabled = ["clinic", "hospital", "pharmacy"]
    assert markers.incomplete_targets(st, "", rid, enabled) == ["hospital", "pharmacy"]


def test_incomplete_targets_no_prior_run_is_all():
    assert markers.incomplete_targets(FakeStorage([]), "", None, ["a", "b"]) == ["a", "b"]


def test_same_day_completed_summary_groups_by_run():
    # 0종 완료 알림의 근거 — 동일 KST 일자의 run 별 완료 종수 + 완료 short 합집합.
    d = "2026-07-31"
    r1, r2, prev = f"{d}_014858_879", f"{d}_120000_000", "2026-07-30_235959_999"
    st = FakeStorage([
        f"{paths.markers_run_dir(run_id=r1)}/clinic.completed",
        f"{paths.markers_run_dir(run_id=r1)}/hospital.incomplete",     # 미완료는 세지 않음
        f"{paths.markers_run_dir(run_id=r2)}/hospital.completed",
        f"{paths.markers_run_dir(run_id=r2)}/not_enabled.completed",   # 수집 대상 밖 → 제외
        f"{paths.markers_run_dir(run_id=prev)}/pharmacy.completed",    # 다른 일자 → 제외
    ])
    s = markers.same_day_completed_summary(st, "", d, ["clinic", "hospital", "pharmacy"])
    assert s["date"] == d
    assert s["completed"] == ["clinic", "hospital"]
    assert s["runs"] == {r1: 1, r2: 1}                                 # prev run 은 미포함


def test_honors_storage_prefix():
    rid = "2026-06-30_120000_000"
    base = paths.bronze_run_dir(prefix="dev/x", run_id=rid)
    st = FakeStorage([f"{base}/_markers/clinic.completed"])
    assert markers.latest_run_id(st, "dev/x") == rid
    assert markers.completed_shorts(st, "dev/x", rid) == {"clinic"}
