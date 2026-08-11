"""파이프라인 기대 주기 — 도메인별 등록을 한 스키마로 모은다 (ASK-Seoul#78 §9).

**값의 정본은 DAG 선언(코드)이고 조회 DB 는 사본이다**(`S-1`). 두 곳에 두면 스케줄을 바꿀 때
어긋나므로, 각 도메인은 아래 :class:`Expectation` 로 자기 DAG 를 등록하고 그 표가 조회 DB 로 간다.

**왜 필요한가** — 알림이 "기록이 없다"를 곧바로 장애로 부르면 안 되고, **기대 주기 초과와 함께**
판정해야 한다(`C-9`). 기대치가 없으면 매일 도는 파이프라인이 죽어도 "원래 안 도는 시간"으로
처리된다. 초안 표에서 실제로 한 번 났던 사고다(`S-5`).

도메인이 등록하는 법 — 자기 번들에 파일 하나:

    # domains/<domain>/include/<pkg>/ops_expectations.py
    from common.ops.expectations import Expectation, register

    register("weather", owner="masondev1024", confirmed_on="2026-08-03", items=[
        Expectation("weather_vilage_fcst_bronze", "schedule", "1시간", max_delay_minutes=180),
        Expectation("weather_transform", "asset", upstream="weather_..._bronze",
                    max_delay_minutes=180),
        Expectation("weather_manual_backfill", "manual", monitored=False),   # S-4 감시 제외
    ])

등록된 것만 조회 DB 로 간다. 등록 안 한 도메인은 **비어 있는 채로 남고**, 그건 "감시 대상이
아니다"가 아니라 **"아직 등록 안 됨"** 이다 — 화면에서 그렇게 읽어야 한다.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Literal

TriggerType = Literal["schedule", "asset", "manual", "external"]

#: 도메인 → 등록 내용. `register()` 가 채운다.
_REGISTRY: dict[str, "DomainExpectations"] = {}


@dataclass(frozen=True)
class Expectation:
    """DAG 하나의 기대치.

    - `schedule`(고정 주기) 는 `expected_interval` 을 적는다.
    - **상류 완료로 도는 DAG 는 고정 주기 대신 트리거 방식 + 상류 + 최대 허용 지연**으로
      등록한다(`S-3`) — 고정 주기를 적으면 상류가 늦어질 때마다 오탐이 난다.
    - **수동 실행 전용은 감시에서 뺀다**(`S-4`) — 안 도는 것이 정상이라 알림이 소음이 된다.
    """

    dag_id: str
    trigger_type: TriggerType
    expected_interval: str | None = None
    upstream: str | None = None
    max_delay_minutes: int | None = None
    monitored: bool = True
    schedule_timezone: str = "Asia/Seoul"

    def __post_init__(self) -> None:
        if self.trigger_type not in ("schedule", "asset", "manual", "external"):
            raise ValueError(f"{self.dag_id}: 알 수 없는 트리거 방식 {self.trigger_type!r}")
        if self.monitored and not self.max_delay_minutes:
            raise ValueError(
                f"{self.dag_id}: 감시 대상에는 max_delay_minutes 가 필요하다(S-2) — "
                "없으면 늦은 것과 죽은 것을 가를 수 없다")
        if self.trigger_type == "asset" and not self.upstream:
            raise ValueError(f"{self.dag_id}: 상류 이벤트형은 upstream 을 밝혀야 한다(S-3)")


@dataclass(frozen=True)
class DomainExpectations:
    domain: str
    owner: str
    confirmed_on: str
    items: tuple[Expectation, ...] = field(default_factory=tuple)


def register(domain: str, *, owner: str, confirmed_on: str,
             items: Iterable[Expectation]) -> None:
    """도메인 하나의 기대 주기를 등록한다.

    ``owner``·``confirmed_on`` 은 필수다 — **등록 전 도메인 오너 확인이 필수**이고(`S-5`),
    누가 언제 확인했는지가 없으면 그 표를 믿을 근거가 없다.
    """
    if not owner or not confirmed_on:
        raise ValueError(f"{domain}: owner 와 confirmed_on 은 필수다(S-5)")
    entries = tuple(items)
    seen = {e.dag_id for e in entries}
    if len(seen) != len(entries):
        raise ValueError(f"{domain}: dag_id 가 중복됐다")
    _REGISTRY[domain] = DomainExpectations(domain, owner, confirmed_on, entries)


def registered_domains() -> tuple[str, ...]:
    return tuple(sorted(_REGISTRY))


def rows(*, updated_at: str, domains: Iterable[str] | None = None) -> list[dict[str, Any]]:
    """조회 DB ``_ops_pipeline_expectation`` 행. 자연키가 ``dag_id`` 라 도메인별로 안전하다(D-4)."""
    wanted = set(domains) if domains else set(_REGISTRY)
    out: list[dict[str, Any]] = []
    for domain in sorted(wanted & set(_REGISTRY)):
        entry = _REGISTRY[domain]
        for item in entry.items:
            out.append({
                "dag_id": item.dag_id,
                "domain": entry.domain,
                "trigger_type": item.trigger_type,
                "expected_interval": item.expected_interval,
                "upstream": item.upstream,
                "max_delay_minutes": item.max_delay_minutes,
                "schedule_timezone": item.schedule_timezone,
                "monitored": 1 if item.monitored else 0,
                "owner": entry.owner,
                "owner_confirmed_on": entry.confirmed_on,
                "updated_at": updated_at,
            })
    return out


#: 등록 모듈 후보 — **닫힌 집합**이다. 도메인을 늘리면 여기와 통합 census 테스트
#: (`common/tests/test_ops_expectations_completeness.py`)에 같이 늘린다. 후보에 있는데
#: 모듈이 없으면 런타임은 조용히 건너뛰지만(아래 load_all — 실행 환경 사정), **테스트는
#: 실패한다** — transit 이 이 침묵 스킵 뒤에 숨어 40개 DAG 가 판정 근거 없이 돌던 것이
#: #733 의 사고다. 같은 일이 다시는 조용히 지나가지 않게 한다.
EXPECTED_MODULES: tuple[str, ...] = (
    "commerce_core.ops_expectations",
    "citydata_ingest.ops_expectations",
    "culture_ingest.ops_expectations",
    "traffic_ingest.ops_expectations",
    "seoul_transit.ops_expectations",      # transit 번들 실제 패키지명(#733 — transit_ingest 아님)
    "weather_ingest.ops_expectations",
    "common.ops.ops_expectations",         # 공용 축(로더·로그십) — #733 "등록 주체" 확정분
)


def load_all() -> tuple[str, ...]:
    """등록 모듈을 임포트해 레지스트리를 채운다. 없는 도메인은 조용히 건너뛴다.

    각 도메인이 자기 번들에 `ops_expectations` 모듈을 두면 여기서 자동으로 잡힌다.
    임포트 실패는 **그 도메인만** 비우고 나머지는 계속 — 한 도메인의 문법 오류가 전체
    등록을 막지 않는다. (완결성은 런타임이 아니라 `common/tests` 통합 census 가 강제한다.)
    """
    import importlib
    import logging

    logger = logging.getLogger(__name__)
    candidates = EXPECTED_MODULES + (
        "transit_ingest.ops_expectations",   # 과거 후보명 — 혹시 그 이름으로 만든 배포가 있어도 잡히게
    )
    for name in candidates:
        try:
            importlib.import_module(name)
        except ImportError:
            continue
        except Exception as exc:  # noqa: BLE001 - 한 도메인의 오류가 전체를 막지 않는다
            logger.warning("[ops.expectations] %s 등록 실패(건너뜀): %s", name, type(exc).__name__)
    return registered_domains()
