"""버스 수집 시간창(#440 후속) — collect_plan 판정과 일일 호출 예산 회귀 테스트.

시간창 설계: 평일은 출퇴근(07~09·17~19시) 10분 간격, 주말은 낮(09~20시) 20분 간격,
그 외 창 내 시각은 시간당 1런, 01~05시는 제외(00시는 막차라 포함).

이 테스트의 핵심은 마지막 예산 테스트다 — 창·간격 env 를 넓히면 호출량이 늘어
운영계정 10,000콜/일 상한을 넘고, #440 쿼터 가드가 그날 남은 수집을 통째로 실패시킨다.
창을 바꾸는 PR 은 이 테스트가 먼저 깨지므로 예산 재계산을 강제받는다.

Airflow 를 가짜 모듈로 대체해 DAG 파일을 import 만 한다(실호출 없음).
"""
import importlib.util
import sys
import types
from datetime import datetime
from pathlib import Path

_TRANSIT = Path(__file__).resolve().parents[1]        # domains/transit
_DAGS = Path(__file__).resolve().parents[3]           # dags 루트
for p in (str(_DAGS), str(_TRANSIT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from seoul_transit import config  # noqa: E402


class _FakeDAG:
    def __init__(self, dag_id, **kwargs):
        self.dag_id, self.kwargs = dag_id, kwargs

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        pass


class _FakePythonOperator:
    def __init__(self, task_id, python_callable=None, **kwargs):
        self.task_id, self.python_callable, self.kwargs = task_id, python_callable, kwargs


def _load_bus_dag():
    airflow = types.ModuleType("airflow")
    airflow.DAG = _FakeDAG
    providers = types.ModuleType("airflow.providers")
    standard = types.ModuleType("airflow.providers.standard")
    operators = types.ModuleType("airflow.providers.standard.operators")
    python_mod = types.ModuleType("airflow.providers.standard.operators.python")
    python_mod.PythonOperator = _FakePythonOperator
    sys.modules.update({
        "airflow": airflow,
        "airflow.providers": providers,
        "airflow.providers.standard": standard,
        "airflow.providers.standard.operators": operators,
        "airflow.providers.standard.operators.python": python_mod,
    })
    path = _TRANSIT / "transit_bus_bronze.py"
    spec = importlib.util.spec_from_file_location("bus_bronze_under_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


_BUS = _load_bus_dag()


def _at(y, m, d, hh, mm):
    """KST 벽시계 시각 — collect_plan 은 tz-aware 를 받지만 판정에는 요일·시·분만 쓴다."""
    return datetime(y, m, d, hh, mm, tzinfo=config.KST)


# 2026-07-27 은 월요일, 08-01 은 토요일, 08-02 는 일요일.
# 모두 재개 게이트(BUS_COLLECT_NOT_BEFORE) 이후 날짜여야 한다 — 게이트 이전 날짜를 쓰면
# 창 판정과 무관하게 전부 False 가 되어 이 파일의 테스트가 통째로 무의미해진다.
MON, SAT, SUN = (2026, 7, 27), (2026, 8, 1), (2026, 8, 2)


# ── 재개 게이트(정책 전환일 분리) ──────────────────────────────────────────────
def test_collect_gate_blocks_before_resume_time(monkeypatch):
    monkeypatch.setattr(config, "BUS_COLLECT_NOT_BEFORE", "2026-07-21T09:00")
    # 게이트 직전: 출퇴근 dense 시각이어도 호출하지 않는다.
    assert _BUS.collect_plan(_at(2026, 7, 21, 8, 50))[0] is False
    # 게이트 시각부터: 09시는 dense + tier2 시각이라 전 노선 스냅샷으로 열린다.
    assert _BUS.collect_plan(_at(2026, 7, 21, 9, 0)) == (True, True)


def test_collect_gate_disabled_when_blank(monkeypatch):
    monkeypatch.setattr(config, "BUS_COLLECT_NOT_BEFORE", "")
    assert _BUS.collect_plan(_at(2026, 7, 20, 18, 0))[0] is True


# ── 제외 시간대 ────────────────────────────────────────────────────────────────
def test_dawn_hours_are_skipped_both_daytypes():
    # 01~05시는 실측상 운행이 사실상 없다(02시 16대·03시 19대) → 전 요일 호출 없음.
    for hh in (1, 2, 3, 4, 5):
        assert _BUS.collect_plan(_at(*MON, hh, 0))[0] is False, f"평일 {hh}시"
        assert _BUS.collect_plan(_at(*SAT, hh, 0))[0] is False, f"주말 {hh}시"


def test_last_train_hour_is_collected():
    # 00시는 막차·심야버스 시간대(실측 12,355건) — 새벽 제외에서 살려둔 시각.
    assert _BUS.collect_plan(_at(*MON, 0, 0))[0] is True
    assert _BUS.collect_plan(_at(*SAT, 0, 0))[0] is True


# ── 평일: 출퇴근 dense(10분) vs 그 외 시간당 1런 ────────────────────────────────
def test_weekday_rush_hours_run_every_10min():
    for hh in (7, 8, 9, 17, 18, 19):
        for mm in (0, 10, 20, 30, 40, 50):
            assert _BUS.collect_plan(_at(*MON, hh, mm))[0] is True, f"{hh}:{mm}"


def test_weekday_offpeak_runs_once_per_hour():
    # 창 안이지만 dense 가 아닌 시각(낮·저녁)은 정시 런만.
    for hh in (6, 10, 13, 16, 20, 22):
        assert _BUS.collect_plan(_at(*MON, hh, 0))[0] is True, f"{hh}:00"
        for mm in (10, 20, 30, 40, 50):
            assert _BUS.collect_plan(_at(*MON, hh, mm))[0] is False, f"{hh}:{mm}"


# ── 주말: 낮 dense(20분) vs 그 외 시간당 1런 ────────────────────────────────────
def test_weekend_daytime_runs_every_20min():
    for hh in (9, 12, 15, 20):
        for mm in (0, 20, 40):
            assert _BUS.collect_plan(_at(*SAT, hh, mm))[0] is True, f"{hh}:{mm}"
        for mm in (10, 30, 50):
            assert _BUS.collect_plan(_at(*SUN, hh, mm))[0] is False, f"{hh}:{mm}"


def test_weekend_offpeak_runs_once_per_hour():
    for hh in (6, 8, 21, 23):
        assert _BUS.collect_plan(_at(*SAT, hh, 0))[0] is True, f"{hh}:00"
        assert _BUS.collect_plan(_at(*SAT, hh, 30))[0] is False, f"{hh}:30"


# ── tier2(전 노선 스냅샷) ──────────────────────────────────────────────────────
def test_tier2_included_only_on_the_hour_of_tier2_hours():
    # 기본 09·19시 — 두 요일 유형의 수집 창에 모두 들어가는 시각이어야 한다.
    for day in (MON, SAT):
        for hh in sorted(config.BUS_TIER2_HOURS):
            assert _BUS.collect_plan(_at(*day, hh, 0)) == (True, True), f"{day} {hh}:00"


def test_tier2_not_duplicated_within_the_hour():
    # dense 시간대라 여러 런이 돌아도 tier2 는 정시 1회만(중복 호출 = 예산 초과 위험).
    hh = sorted(config.BUS_TIER2_HOURS)[0]
    later = [_BUS.collect_plan(_at(*MON, hh, mm))[1] for mm in (10, 20, 30, 40, 50)]
    assert not any(later)


def test_tier2_hours_are_inside_both_collection_windows():
    # 창 밖 시각을 tier2 로 두면 그 요일 유형에서 전 노선 수집이 영영 안 된다.
    assert config.BUS_TIER2_HOURS <= config.BUS_WEEKDAY_HOURS
    assert config.BUS_TIER2_HOURS <= config.BUS_WEEKEND_HOURS


# ── 일일 호출 예산 (운영계정 10,000콜/일) ───────────────────────────────────────
DAILY_CALL_BUDGET = 10_000
TIER1_ROUTES = 165   # 간선(3)+광역(6) 실측 규모
TIER2_ROUTES = 563   # 그 외 서울 인면허(#440 실측: 총 728 - tier1)


def _daily_calls(day) -> int:
    """하루를 1분 단위로 돌려 실제 호출량을 센다(설계표가 아니라 코드 기준)."""
    total = 0
    for hh in range(24):
        for mm in range(0, 60, 10):  # DAG 스케줄 */10 이 만드는 런
            should, tier2 = _BUS.collect_plan(_at(*day, hh, mm))
            if should:
                total += TIER1_ROUTES + (TIER2_ROUTES if tier2 else 0)
    return total


def test_daily_call_budget_within_quota():
    weekday, weekend = _daily_calls(MON), _daily_calls(SAT)
    assert weekday == 9_211, f"평일 호출량 변경됨: {weekday} (예산표 갱신 필요)"
    assert weekend == 8_221, f"주말 호출량 변경됨: {weekend} (예산표 갱신 필요)"
    assert weekday <= DAILY_CALL_BUDGET
    assert weekend <= DAILY_CALL_BUDGET


def test_dense_interval_is_multiple_of_dag_tick():
    # */10 런 중 배수 분만 고르는 구현이라, 10의 배수가 아니면 그 시간대는
    # 정시 1런으로 조용히 퇴화한다(의도치 않은 커버리지 손실).
    assert config.BUS_WEEKDAY_DENSE_INTERVAL_MIN % 10 == 0
    assert config.BUS_WEEKEND_DENSE_INTERVAL_MIN % 10 == 0
