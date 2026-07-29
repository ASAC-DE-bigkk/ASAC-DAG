"""culture 도메인 설정: 적재 루트 + 소스 API 키.

공용 R2 적재 대상은 ``culture_ingest.common.config``에서 가져온다. 여기에는
culture 전용 소스 키와 bronze 적재 루트만 둔다.
"""

from __future__ import annotations

from dataclasses import dataclass

from culture_ingest.common.config import load_env_file, pick

# 받아온 원본 객체는 culture 도메인의 bronze 레이어 prefix 아래에 적재된다.
LANDING_ROOT = "raw/culture"

# --- R2 존 구분 (ASK-Seoul#60 약속 ②) ------------------------------------------
# 존을 가르는 질문은 하나 — "이 파일, 지워지면 무슨 일이 나는가?"
#   raw/          지우면 원본 재현 불가(다시 못 받는 API 응답) → 영구 보존·이동 금지
#   ops/reports/  지워도 지나간 기록일 뿐 → 자동 삭제(TTL) 대상
#   ops/control/  지우면 **다음 실행이 오작동** → 자동 삭제 금지, 최신본 유지
#
# run 리포트는 역할이 둘이라 한 곳에 못 둔다: SLO·대시보드가 읽는 관측 기록이면서,
# 동시에 #147 볼륨 가드의 기준선(HWM) 공급원이다. 후자를 TTL 대상 구역에 두면
# lifecycle 이 걸리는 순간 가드가 **조용히** 꺼진다(load_baselines 는 fail-open).
# 그래서 리포트는 reports 로, 기준선은 control 로 나눈다.
OPS_REPORTS_ROOT = "ops/reports/culture"
OPS_CONTROL_ROOT = "ops/control/state/culture"
VOLUME_HWM_KEY = f"{OPS_CONTROL_ROOT}/volume_hwm.json"

# 과도기 dual-read 용 — 2026-07-29 이전 리포트 62건은 raw 에 남아 있다(#60 "기존 객체
# 이동 0건", 전환은 새 쓰기부터). 읽는 쪽이 양쪽을 봐야 이력이 끊기지 않는다.
LEGACY_REPORTS_PREFIX = f"{LANDING_ROOT}/_reports/"

KOPIS_KEY_ENV = "KOPIS_SERVICE_KEY"   # KOPIS 인증키 환경변수 이름
SEOUL_KEY_ENV = "SEOUL_API_KEY_CULT"   # 서울 열린데이터 인증키 환경변수 이름
CULT_KEY_ENV = "PUBLIC_DATA_API_KEY_CULT"   # 한눈에보는문화정보(KCISA, data.go.kr) 인증키
KOBIS_KEY_ENV = "KOBIS_SERVICE_KEY"   # KOBIS(영화진흥위원회) 인증키 환경변수 이름(#197)


@dataclass(frozen=True)
class SourceKeys:
    """네 소스의 인증키 묶음."""

    kopis: str
    seoul: str
    cult: str
    kobis: str


def source_keys(env_file: str | None = None) -> SourceKeys:
    """환경변수(+선택적 .env)에서 소스 인증키를 읽어 온다."""
    env = load_env_file(env_file)
    return SourceKeys(
        kopis=pick(KOPIS_KEY_ENV, env),
        seoul=pick(SEOUL_KEY_ENV, env),
        cult=pick(CULT_KEY_ENV, env),
        kobis=pick(KOBIS_KEY_ENV, env),
    )


def missing_keys(keys: SourceKeys) -> list[str]:
    """비어 있는 인증키 환경변수 이름 목록 (사전 점검용)."""
    missing: list[str] = []
    if not keys.kopis:
        missing.append(KOPIS_KEY_ENV)
    if not keys.seoul:
        missing.append(SEOUL_KEY_ENV)
    if not keys.cult:
        missing.append(CULT_KEY_ENV)
    if not keys.kobis:
        missing.append(KOBIS_KEY_ENV)
    return missing
