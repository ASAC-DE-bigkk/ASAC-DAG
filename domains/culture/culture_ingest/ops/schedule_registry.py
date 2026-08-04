"""culture 기대 주기 등록값 — ASAC-DAG#619 확정안 '기대 주기 등록'.

확정안이 이 항목에 요구한 것은 셋이다.

* **culture 는 주 1회가 아니라 일 1회다.** 본문 표가 주간 정비 DAG(``culture_maintenance``·
  ``culture_facility_refresh``)만 보고 도메인 전체를 주 1회로 등록할 뻔했다.
* **등록 전 도메인 오너 확인 필수** — 표에서 이미 한 번 틀렸기 때문에 붙은 절차다.
* **상류 완료 시 이어 도는 DAG 는 고정 주기 대신 "트리거 방식 + 상류 + 최대 허용 지연"**.
  ``culture_transform`` 이 정확히 그 경우라, 고정 주기 칸에 넣으면 등록 자체가 거짓이 된다.

오너 확인 결과가 코멘트로만 남으면 다음에 또 틀린다. 그래서 **선언을 코드에 두고
테스트가 실제 DAG 정의와 대조**한다(``tests/test_schedule_registry.py``) — 스케줄을
바꾸면서 등록값을 안 고치면 테스트가 깨진다. 확인자는 culture 오너(#619 코멘트).

``max_delay_min`` 은 "이 시각까지 안 끝나면 지연으로 본다"는 값이지 SLA 가 아니다.
공유 대시보드의 '대기 건수·최장 대기 시간' 지표가 도메인마다 다른 기준을 필요로 해서
등록 항목에 들어갔다.
"""

from __future__ import annotations

from dataclasses import dataclass

# 트리거 방식 — 확정안이 고정 주기와 구분하라고 한 축.
TRIGGER_SCHEDULE = "schedule"   # cron 고정 주기
TRIGGER_ASSET = "asset"         # 상류 Asset 발행에 이어 돎


@dataclass(frozen=True)
class ExpectedCadence:
    """DAG 1개의 기대 주기 등록 항목."""

    dag_id: str
    trigger_type: str
    cron: str | None            # TRIGGER_SCHEDULE 일 때만. 타임존은 아래 TIMEZONE.
    upstream: str | None        # TRIGGER_ASSET 일 때 상류 dag_id
    max_delay_min: int          # 기대 시점 이후 이만큼 지나면 지연
    note: str
    # 공유 화면에 그대로 뜨는 사람 말 표기. cron 을 그대로 보여주면 도메인마다 표기가
    # 갈려(전 도메인은 "일 1회 00:00" 식) 같은 칸에 두 어휘가 섞인다. cron 이 정본이고
    # 이건 그 사본이라, 아래 테스트가 둘의 시각을 대조한다.
    interval_ko: str = ""


# 모든 cron 은 DAG 타임존 해석 — culture DAG 는 전부 KST 로 정의돼 있다.
TIMEZONE = "Asia/Seoul"

REGISTRY: tuple[ExpectedCadence, ...] = (
    ExpectedCadence(
        dag_id="culture_bronze",
        trigger_type=TRIGGER_SCHEDULE,
        cron="0 3 * * *",
        upstream=None,
        # 통상 run ~30분(7/5 14분 → 7/7 28분), 세종 88MB 적재가 13분을 먹는다.
        # DAG 자체의 침묵 감시는 2h(deadline) — 여기 90분은 그보다 앞서는 '지연' 선.
        max_delay_min=90,
        interval_ko="일 1회 03:00",
        note="일 1회 수집·bronze 적재. 확정안 표의 '주 1회'는 오류(주간 정비 DAG만 본 값)",
    ),
    ExpectedCadence(
        dag_id="culture_transform",
        trigger_type=TRIGGER_ASSET,
        cron=None,
        upstream="culture_bronze",
        max_delay_min=60,
        interval_ko="상류 완료 시",
        note="bronze Asset 발행에 이어 돎 — 고정 시각 없음. 상류가 늦으면 같이 늦는 게 정상",
    ),
    ExpectedCadence(
        dag_id="culture_serving_export",
        trigger_type=TRIGGER_SCHEDULE,
        cron="30 4 * * *",
        upstream=None,
        max_delay_min=60,
        interval_ko="일 1회 04:30",
        note="일 1회 gold → 서빙 D1 내보내기",
    ),
    ExpectedCadence(
        dag_id="culture_slo",
        trigger_type=TRIGGER_SCHEDULE,
        cron="0 5 * * *",
        upstream=None,
        max_delay_min=60,
        interval_ko="일 1회 05:00",
        note="일 1회 SLO 마트 — 본류(03:00→~04:00) 뒤·facility_refresh(05:30) 앞 슬롯",
    ),
    ExpectedCadence(
        dag_id="culture_maintenance",
        trigger_type=TRIGGER_SCHEDULE,
        cron="30 4 * * 0",
        upstream=None,
        max_delay_min=120,
        interval_ko="주 1회 일요일 04:30",
        note="주 1회 정비(일요일). 도메인 주기가 아니라 정비 주기",
    ),
    ExpectedCadence(
        dag_id="culture_facility_refresh",
        trigger_type=TRIGGER_SCHEDULE,
        cron="30 5 * * 0",
        upstream=None,
        max_delay_min=120,
        interval_ko="주 1회 일요일 05:30",
        note="주 1회 시설 전량 갱신(일요일). 도메인 주기가 아니라 갱신 주기",
    ),
)

BY_DAG_ID = {c.dag_id: c for c in REGISTRY}

# 도메인 대표 주기 — "culture 는 얼마나 자주 도나"에 대한 한 줄 답. 본류(수집) 기준이다.
DOMAIN_CADENCE_DAG_ID = "culture_bronze"


def as_rows() -> list[dict]:
    """등록 테이블에 그대로 넣을 수 있는 dict 목록 (공유 D1 적재용).

    컬럼 이름은 확정안의 등록 항목 표현을 그대로 쓴다 — 도메인마다 다른 이름으로
    올리면 합류하는 쪽이 매핑을 또 만들어야 한다.
    """
    return [
        {
            "domain": "culture",
            "dag_id": c.dag_id,
            "trigger_type": c.trigger_type,
            "cron": c.cron,
            "timezone": TIMEZONE if c.cron else None,
            "upstream": c.upstream,
            "max_delay_min": c.max_delay_min,
            "note": c.note,
        }
        for c in REGISTRY
    ]
