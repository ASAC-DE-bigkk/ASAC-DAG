"""적재 백로그 관측·경보·시간 예산 단위 테스트 (ASK-Seoul#719).

2026-08-06 사고: bronze loader 런 1건이 26시간 32분 '실행 중'으로 돌면서 서빙 2종의
freshness 가 SLO(75분)를 161·200분 초과했는데, 수집·변환·게시가 전부 성공이라 어떤
경보에도 걸리지 않았다. 여기서 검증하는 것은 그 침묵을 깨는 세 장치다.

- alerts.backlog_exceeded : 나이(주) 또는 잔량(보) 임계 판정
- load_pending 의 관측    : 런이 끝난 뒤 **실제 남은** pending 을 재서 보고
- 런 시간 예산            : 기본 OFF(전량 처리 = #369 동작 유지), 켜면 이월
"""
import importlib.util
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

_TRANSIT = Path(__file__).resolve().parents[1]        # domains/transit
_DAGS = Path(__file__).resolve().parents[3]           # dags 루트
for p in (str(_DAGS), str(_TRANSIT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from seoul_transit import alerts, config, loader  # noqa: E402


# ── 임계 판정 ────────────────────────────────────────────────────────────────
def test_backlog_exceeded_by_age_even_when_count_is_small():
    """나이가 주 판정 — 한 건만 남아도 그 한 건이 오래됐으면 창고가 그만큼 늙었다."""
    assert alerts.backlog_exceeded(1, config.LOADER_BACKLOG_AGE_WARN_MINUTES)
    assert alerts.backlog_exceeded(1, config.LOADER_BACKLOG_AGE_WARN_MINUTES + 100)
    assert not alerts.backlog_exceeded(1, config.LOADER_BACKLOG_AGE_WARN_MINUTES - 1)


def test_backlog_exceeded_by_count_when_age_is_unknown_or_young():
    """유입이 드레인을 앞지르는 국면 — 아직 안 늙었어도 쌓이는 중이면 잡는다."""
    assert alerts.backlog_exceeded(config.LOADER_BACKLOG_WARN, 0)
    assert alerts.backlog_exceeded(config.LOADER_BACKLOG_WARN, None)
    assert not alerts.backlog_exceeded(config.LOADER_BACKLOG_WARN - 1, None)


def test_backlog_age_threshold_fires_before_serving_slo():
    """경보는 서빙 freshness SLO(75분)를 넘기기 **전에** 울려야 조치할 시간이 있다."""
    assert config.LOADER_BACKLOG_AGE_WARN_MINUTES < 75


def test_warn_backlog_is_noop_under_threshold(monkeypatch):
    sent = []
    monkeypatch.setattr(alerts, "send_embed", lambda **kw: sent.append(kw) or True)
    assert alerts.warn_backlog(pending=0, oldest_age_minutes=None) is False
    assert sent == []


def test_warn_backlog_sends_with_age_and_oldest_key(monkeypatch):
    sent = []
    monkeypatch.setattr(alerts, "send_embed", lambda **kw: sent.append(kw) or True)
    assert alerts.warn_backlog(
        pending=113, oldest_age_minutes=151.4,
        oldest_key="ops/control/state/transit/loader_pending/parking/20260806T114501Z__x.json",
        deferred=7,
    ) is True
    assert len(sent) == 1
    body = sent[0]["description"]
    assert "113" in sent[0]["title"] and "151분" in sent[0]["title"]
    assert "7건" in body                       # 이월 수량이 드러난다
    assert "20260806T114501Z" in body          # 어느 마커부터 밀렸는지


def test_warn_backlog_is_not_suppressed_by_quiet_hours(monkeypatch):
    """0행 경보와 달리 백로그는 심야에 정상인 상태가 아니다(주차는 24시간 수집)."""
    sent = []
    monkeypatch.setattr(alerts, "send_embed", lambda **kw: sent.append(kw) or True)
    monkeypatch.setattr(alerts, "in_quiet_hours", lambda *a, **k: True)
    assert alerts.warn_backlog(pending=200, oldest_age_minutes=300) is True
    assert len(sent) == 1


# ── DAG 오케스트레이션(가짜 Airflow) ─────────────────────────────────────────
def _install_airflow_fakes():
    airflow = types.ModuleType("airflow")

    class FakeDAG:
        _stack: list = []

        def __init__(self, dag_id, **kwargs):
            self.dag_id, self.kwargs, self.task_dict = dag_id, kwargs, {}

        def __enter__(self):
            self._stack.append(self)
            return self

        def __exit__(self, *_):
            self._stack.pop()

    class FakePythonOperator:
        def __init__(self, task_id, python_callable, **kwargs):
            self.task_id, self.python_callable, self.kwargs = task_id, python_callable, kwargs
            FakeDAG._stack[-1].task_dict[task_id] = self

    airflow.DAG = FakeDAG
    providers = types.ModuleType("airflow.providers")
    standard = types.ModuleType("airflow.providers.standard")
    operators = types.ModuleType("airflow.providers.standard.operators")
    python_mod = types.ModuleType("airflow.providers.standard.operators.python")
    python_mod.PythonOperator = FakePythonOperator
    sys.modules.update({
        "airflow": airflow,
        "airflow.providers": providers,
        "airflow.providers.standard": standard,
        "airflow.providers.standard.operators": operators,
        "airflow.providers.standard.operators.python": python_mod,
    })


def _load_loader_dag():
    _install_airflow_fakes()
    path = _TRANSIT / "transit_bronze_loader.py"
    spec = importlib.util.spec_from_file_location("transit_bronze_loader_under_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def dag_module():
    return _load_loader_dag()


def _fake_markers(n, dataset="parking", start_min=0):
    """실제 ingest_ts 형식(`%Y%m%dT%H%M%SZ`)으로 만든다.

    ⚠ 초판 픽스처는 `T` 를 빠뜨린 `20260806100000Z` 를 만들었고, 그래서 이 파일의
    순서·나이 검증이 전부 '파싱 불가 키' 위에서 헛돌았다(#719 리뷰에서 적발). 픽스처가
    진짜 형식을 지키는지부터 아래 test_fixture_makes_parsable_markers 가 고정한다.
    """
    base = datetime(2026, 8, 6, 10, 0, tzinfo=timezone.utc)
    return [
        loader.pending_key(
            dataset,
            (base + timedelta(minutes=start_min + i)).strftime("%Y%m%dT%H%M%SZ"),
            f"scheduled__2026-08-06T10_00_00_00_00_{i}",   # 실제 run_id 처럼 `__` 포함
        )
        for i in range(n)
    ]


def test_dag_bounds_runtime_with_dagrun_timeout(dag_module):
    """매달린 런이 단일 active 슬롯을 무한정 점유하지 못하게 한다(#719).

    백로그 경보는 런이 **끝날 때** 나가므로 '실행 중'인 채로 멈춘 런은 못 잡는다 —
    2026-08-06 의 26시간 32분이 그 형태였다. weather_vilage_fcst_bronze 선례와 동형.
    스케줄(*/10)보다는 넉넉하되(정상 백로그 드레인을 죽이지 않게) 시간 단위로 매달리는
    것은 끊는 범위.
    """
    dag = dag_module.dag
    assert dag.kwargs["dagrun_timeout"] is not None
    assert timedelta(minutes=30) <= dag.kwargs["dagrun_timeout"] <= timedelta(hours=3)
    assert dag.kwargs["max_active_runs"] == 1     # 상한이 필요한 이유 자체


def test_fixture_makes_parsable_markers():
    """픽스처 자체 검증 — 이게 깨지면 아래 테스트들이 조용히 무의미해진다."""
    keys = _fake_markers(3)
    assert all(loader.marker_ingest_ts(k) is not None for k in keys)
    assert [loader.marker_ingest_ts(k).minute for k in keys] == [0, 1, 2]


class _Harness:
    """_wire 결과 묶음 — 처리된 키·발신된 경보·가짜 시계 조작."""

    def __init__(self, loaded, sent, clock, module):
        self.loaded, self.sent, self._clock, self._module = loaded, sent, clock, module

    def advance(self, seconds):
        self._clock["t"] += seconds

    def fail_every_marker(self, monkeypatch, message="trino down", seconds=0.0):
        """모든 마커를 실패시키되 시계는 흐르게 — 실패 + 예산 소진을 함께 재현한다."""
        def boom(cursor, marker_key, marker, ensured):
            self._clock["t"] += seconds
            raise RuntimeError(message)

        monkeypatch.setattr(self._module, "_load_marker", boom)


def _wire(module, monkeypatch, keys, *, load_seconds=0.0):
    """R2·Trino·경보를 전부 가짜로 — 순수하게 루프 제어만 본다."""
    remaining = list(keys)
    loaded: list[str] = []
    clock = {"t": 0.0}

    monkeypatch.setattr(module, "list_keys",
                        lambda prefix: list(remaining) if "loader_pending" in prefix else [])
    monkeypatch.setattr(module.config, "LOADER_PENDING_LEGACY_PREFIXES", ())
    monkeypatch.setattr(module, "_trino_cursor", lambda: object())
    monkeypatch.setattr(module, "get_json", lambda key: {"dataset": "parking"})
    # DAG 모듈의 `time` 참조만 갈아끼운다 — stdlib time.monotonic 을 직접 patch 하면
    # 테스트가 도는 동안 프로세스 전체(runmetrics 소요시간 측정 등)의 시계가 멈춘다.
    monkeypatch.setattr(module, "time", types.SimpleNamespace(monotonic=lambda: clock["t"]))

    def fake_load(cursor, marker_key, marker, ensured):
        loaded.append(marker_key)
        remaining.remove(marker_key)
        clock["t"] += load_seconds
        return 1

    monkeypatch.setattr(module, "_load_marker", fake_load)
    sent = []
    monkeypatch.setattr(module.alerts, "warn_backlog", lambda **kw: sent.append(kw) or True)
    return _Harness(loaded, sent, clock, module)


def test_budget_off_by_default_drains_everything(dag_module, monkeypatch):
    """기본값은 #369 그대로 — 전량 처리. 예산 기능이 기본 동작을 바꾸지 않는다."""
    assert config.LOADER_RUN_BUDGET_MINUTES == 0
    keys = _fake_markers(12)
    w = _wire(dag_module, monkeypatch, keys, load_seconds=600)  # 건당 10분
    monkeypatch.setattr(dag_module.config, "LOADER_RUN_BUDGET_MINUTES", 0)

    out = dag_module.load_pending()
    assert len(w.loaded) == 12 and out["deferred"] == 0
    assert out["pending"] == 0


def test_budget_defers_remainder_to_next_run(dag_module, monkeypatch):
    keys = _fake_markers(12)
    w = _wire(dag_module, monkeypatch, keys, load_seconds=120)  # 건당 2분
    monkeypatch.setattr(dag_module.config, "LOADER_RUN_BUDGET_MINUTES", 10)

    out = dag_module.load_pending()
    # 10분 예산 / 건당 2분 → 5건 처리 후 6번째 진입 시 소진
    assert len(w.loaded) == 5
    assert out["markers"] == 5 and out["deferred"] == 7
    # 이월분이 잔량으로 보고되고 경보로도 나간다 — 조용한 적체 금지
    assert out["pending"] == 7
    assert w.sent and w.sent[0]["deferred"] == 7


def test_backlog_measured_after_run_not_from_start_list(dag_module, monkeypatch):
    """런 도중 도착한 마커도 잔량에 포함된다 — '다 비웠다'는 착시 방지."""
    keys = _fake_markers(3)
    remaining_late = loader.pending_key("subway_arrival", "20260806T130000Z", "late")
    w = _wire(dag_module, monkeypatch, keys)

    original = dag_module.list_keys
    calls = {"n": 0}

    def listing(prefix):
        calls["n"] += 1
        out = original(prefix)
        # 2번째 나열(= 런 종료 후 재나열)에는 도중 도착분이 섞여 있다.
        return out + [remaining_late] if calls["n"] > 1 else out

    monkeypatch.setattr(dag_module, "list_keys", listing)
    out = dag_module.load_pending()
    assert len(w.loaded) == 3
    assert out["pending"] == 1                     # 도중 도착분이 잡힌다
    assert out["oldest_age_minutes"] is not None


def test_processing_order_is_global_time_not_dataset(dag_module, monkeypatch):
    """#719 회귀 — 한 dataset 이 통째로 앞서지 않는다."""
    keys = [
        loader.pending_key("subway_arrival", "20260806T100000Z", "a"),
        loader.pending_key("bus_position", "20260806T110000Z", "b"),
        loader.pending_key("parking", "20260806T120000Z", "c"),
    ]
    w = _wire(dag_module, monkeypatch, keys)
    dag_module.load_pending()
    assert [k.split("/")[-2] for k in w.loaded] == [
        "subway_arrival", "bus_position", "parking"]


def test_one_unparsable_marker_does_not_blind_the_age_alarm(dag_module, monkeypatch):
    """#719 리뷰 회귀 — 정렬의 첫 키를 '최고령'으로 쓰면 안 된다.

    sort_markers 는 ingest_ts 를 못 읽는 키를 굶기지 않으려고 맨 앞에 둔다. 그 키를
    그대로 나이 측정에 쓰면, 실패해서 계속 남는(그리고 maintenance 만료 스윕도 건너뛰는)
    마커 **하나**가 나이 신호를 영구히 지운다 — 잔량이 임계 미만이면 완전 무경보가 되어
    이 브랜치가 없애려는 26시간 침묵이 그대로 재현된다.
    """
    # 적재가 통째로 막힌 상황(Trino 다운) — 마커는 전부 보존되므로 백로그에는
    # '오래된 정상 마커 3건 + 파싱 불가 1건' 이 함께 남는다. 이게 결함의 재현 조건이다.
    keys = _fake_markers(3) + [config.LOADER_PENDING_PREFIX + "parking/hand-made.json"]
    w = _wire(dag_module, monkeypatch, keys)
    w.fail_every_marker(monkeypatch)

    with pytest.raises(RuntimeError):
        dag_module.load_pending()

    assert w.sent, "나이가 임계를 넘으면 경보가 나가야 한다"
    assert w.sent[0]["pending"] == 4
    assert w.sent[0]["oldest_age_minutes"] is not None, "파싱 불가 키가 나이를 지우면 안 된다"
    assert "hand-made" not in (w.sent[0]["oldest_key"] or "")
    # 최고령은 '가장 오래된 **정상** 마커'여야 한다 — 정렬 첫 키가 아니라.
    assert "20260806T100000Z" in w.sent[0]["oldest_key"]


def test_budget_always_attempts_at_least_one_marker(dag_module, monkeypatch):
    """고정 오버헤드가 예산을 넘겨도 큐가 영영 안 줄어드는 상태가 되면 안 된다."""
    keys = _fake_markers(5)
    w = _wire(dag_module, monkeypatch, keys, load_seconds=60)
    monkeypatch.setattr(dag_module.config, "LOADER_RUN_BUDGET_MINUTES", 10)
    # 커서 연결만으로 이미 예산 초과인 상황(고정 오버헤드 > 예산)
    cursor = dag_module._trino_cursor
    monkeypatch.setattr(dag_module, "_trino_cursor",
                        lambda: (w.advance(11 * 60), cursor())[1])

    out = dag_module.load_pending()
    assert out["markers"] == 1, "런당 최소 1건은 처리해야 큐가 줄어든다"
    assert out["deferred"] == len(keys) - 1


def test_observation_failure_does_not_fail_the_task(dag_module, monkeypatch):
    """관측이 곁다리다 — R2 나열이 죽었다고 성공한 적재 런이 빨개지면 안 된다."""
    keys = _fake_markers(2)
    w = _wire(dag_module, monkeypatch, keys)
    monkeypatch.setattr(dag_module, "_backlog_stats",
                        lambda: (_ for _ in ()).throw(RuntimeError("R2 endpoint down")))

    out = dag_module.load_pending()               # 예외가 새어나오면 실패
    assert out["markers"] == 2 and out["pending"] is None


def test_observation_failure_does_not_mask_the_real_load_failure(dag_module, monkeypatch):
    """적재 실패 런에서는 '어느 마커가 왜 실패했는지'가 살아남아야 한다."""
    keys = _fake_markers(2)
    w = _wire(dag_module, monkeypatch, keys)
    monkeypatch.setattr(dag_module, "_load_marker",
                        lambda *a: (_ for _ in ()).throw(RuntimeError("trino down")))
    monkeypatch.setattr(dag_module, "_backlog_stats",
                        lambda: (_ for _ in ()).throw(RuntimeError("R2 endpoint down")))

    with pytest.raises(RuntimeError) as exc:
        dag_module.load_pending()
    assert "적재 실패" in str(exc.value) and "trino down" in str(exc.value)


def test_failure_denominator_counts_attempts_not_deferred(dag_module, monkeypatch):
    """예산을 켠 런에서 '3/100 실패' 같은 실제보다 순한 수치가 나오면 안 된다."""
    keys = _fake_markers(20)
    w = _wire(dag_module, monkeypatch, keys)
    monkeypatch.setattr(dag_module.config, "LOADER_RUN_BUDGET_MINUTES", 10)
    w.fail_every_marker(monkeypatch, message="boom", seconds=120)   # 건당 2분씩 흐름

    with pytest.raises(RuntimeError) as exc:
        dag_module.load_pending()
    # 10분 예산 / 건당 2분 → 5건 시도 후 이월. 분모는 시도분(5)이지 나열 전체(20)가 아니다.
    assert "5/5건 적재 실패" in str(exc.value)
    assert "/20건" not in str(exc.value)


def test_backlog_alert_fires_even_when_a_marker_failed(dag_module, monkeypatch):
    """실패 예외로 백로그 신호가 유실되면, 정작 밀린 상황에서만 조용해진다."""
    keys = _fake_markers(2)
    w = _wire(dag_module, monkeypatch, keys)

    def boom(cursor, marker_key, marker, ensured):
        raise RuntimeError("trino down")

    monkeypatch.setattr(dag_module, "_load_marker", boom)
    with pytest.raises(RuntimeError):
        dag_module.load_pending()
    assert w.sent, "실패로 마감하기 전에 백로그 경보가 나가야 한다"
