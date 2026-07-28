"""transit 저빈도 마스터 수집 — 순수 로직(테스트 대상) (#162).

서울 열린데이터광장 마스터 2종을 **주간 스냅샷**으로 수집한다:

1. subwayStationMaster — 전체 지하철역 마스터(~784행, WGS84 LAT/LOT 포함)
2. GetParkInfo         — 공영주차장 마스터(~2,204행, 주소·좌표 포함)

⚠️ GetParkInfo(마스터) ≠ GetParkingInfo(실시간 점유, transit_parking_bronze). 별개 서비스.

원천은 둘 다 서울 열린데이터광장(openapi.seoul.go.kr:8088)이라 **경로 키(PathKey)** 방식이다.
공통 어댑터 SeoulOpenApiClient(#78, common.http.seoul)를 주입받아 쓰고, 키는 URL 경로에
박히지만 HttpCore 의 redaction(로그·예외 URL 마스킹)으로 로그에 남지 않는다. 인증키는
seoul_transit.config.load_key()(env SEOUL_API_KEY_TRAN).

이 파일 = 순수 로직(수집 iter·종료조건·R2 랜딩·manifest). 오케스트레이션/Trino 적재는
domains/transit/transit_master_bronze.py.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator

from common.http import HttpCore, SeoulOpenApiClient
from common.http.contract import Transport

from . import config

LOGGER = logging.getLogger(__name__)

# 서울 OpenAPI 한 요청당 최대 행수(start/end 창 크기). 원천 상한 1000.
DEFAULT_PER_PAGE = 1000
# 위생 가드 — 종료 조건(short page / collected>=total) 오류로 인한 무한 페이지네이션 방지.
# 가장 큰 GetParkInfo(2,204행)도 3페이지면 끝 → 20 초과 시 RuntimeError.
_MAX_PAGES = 20
# 정상 응답 코드(서울 OpenAPI). INFO-200 = 해당하는 데이터가 없음(빈 스냅샷 → 종료).
_OK_CODE = "INFO-000"
_NO_DATA_CODE = "INFO-200"


@dataclass(frozen=True)
class MasterSpec:
    """마스터 원천 1종의 계약 — 서비스명/필드/적재 대상까지 한 곳에.

    fields 는 **실호출 응답(2026-07-06 검증)** 필드명(대문자)이다. 브론즈 컬럼명은
    소문자 스네이크(= f.lower())로 파생한다(코드류는 varchar — 선행 0 보존).
    """

    dataset: str            # R2 경로·manifest 용 (subway_station_master / park_info_master)
    service: str            # 서울 OpenAPI 서비스명(= 응답 엔벨로프 키)
    source_system: str      # 계보 컬럼·에러콜백·manifest 용(기존 transit DAG 값과 정합)
    table: str              # Iceberg 브론즈 테이블
    fields: tuple[str, ...]  # 원천 필드명(대문자)
    expected_rows: int | None = None  # verify 로그용 기대 행수(있으면)


# ── 원천 스펙(실호출 검증 필드) ─────────────────────────────────────────────────
SUBWAY_STATION_MASTER = MasterSpec(
    dataset="subway_station_master",
    service="subwayStationMaster",
    source_system="seoul_subway",
    table="bronze_subway_station_master",
    fields=("BLDN_ID", "BLDN_NM", "ROUTE", "LAT", "LOT"),
    expected_rows=784,
)

PARK_INFO_MASTER = MasterSpec(
    dataset="park_info_master",
    service="GetParkInfo",
    source_system="seoul_parking",
    table="bronze_park_info_master",
    fields=(
        "PKLT_NM", "ADDR", "PKLT_CD", "PKLT_KND", "PKLT_KND_NM", "OPER_SE", "OPER_SE_NM",
        "TELNO", "PRK_NOW_INFO_PVSN_YN", "PRK_NOW_INFO_PVSN_YN_NM", "TPKCT",
        "CHGD_FREE_SE", "CHGD_FREE_NM", "NGHT_FREE_OPN_YN", "NGHT_FREE_OPN_YN_NAME",
        "WD_OPER_BGNG_TM", "WD_OPER_END_TM", "WE_OPER_BGNG_TM", "WE_OPER_END_TM",
        "LHLDY_BGNG", "LHLDY", "LAST_DATA_SYNC_TM", "SAT_CHGD_FREE_SE", "SAT_CHGD_FREE_NM",
        "LHLDY_YN", "LHLDY_NM", "MNTL_CMUT_CRG", "CRB_PKLT_MNG_GROUP_NO", "PRK_CRG",
        "PRK_HM", "ADD_CRG", "ADD_UNIT_TM_MNT", "BUS_PRK_CRG", "BUS_PRK_HM",
        "BUS_PRK_ADD_HM", "BUS_PRK_ADD_CRG", "DLY_MAX_CRG", "LAT", "LOT",
    ),
    expected_rows=None,  # 마스터가 커서(2,204±) 로그 고정 기대치는 두지 않음
)

SPECS: dict[str, MasterSpec] = {
    SUBWAY_STATION_MASTER.dataset: SUBWAY_STATION_MASTER,
    PARK_INFO_MASTER.dataset: PARK_INFO_MASTER,
}


def column_names(spec: MasterSpec) -> list[str]:
    """원천 필드명(대문자) → 브론즈 컬럼명(소문자 스네이크)."""
    return [f.lower() for f in spec.fields]


# ── HTTP 클라이언트 조립 ─────────────────────────────────────────────────────────
def load_key() -> str:
    """서울 열린데이터광장 인증키 — seoul_transit.config.load_key()(SEOUL_API_KEY_TRAN)."""
    return config.load_key()


def build_core(transport: Transport | None = None) -> HttpCore:
    """서울 OpenAPI 소스용 HttpCore — 멱등 GET 재시도 안전(#78).

    timeout=60s(대용량 페이지), max_attempts=3(429/5xx 백오프+jitter),
    rate_limit=None(마스터는 주간 1런, 원천 명시 제한 준수는 별도). transport 주입은 테스트용.
    """
    return HttpCore(
        source="seoul_openapi",
        transport=transport,
        timeout=60.0,
        max_attempts=3,
        rate_limit=None,
    )


def build_client(core: HttpCore, key: str) -> SeoulOpenApiClient:
    """SeoulOpenApiClient(#78) — 경로 키 치환은 PathKey(요청 직전)가, redaction 은 HttpCore 가."""
    return SeoulOpenApiClient(core, key)


# ── 수집(페이지네이션) ────────────────────────────────────────────────────────────
def fetch_page(
    client: SeoulOpenApiClient, spec: MasterSpec, start: int, end: int,
) -> tuple[bytes, dict]:
    """단일 페이지 조회 → (raw_bytes(json 원문), parsed dict).

    키는 SeoulOpenApiClient 가 PathKey 로 URL 경로에 치환(로그엔 redact). raw 는 R2 원본 보존용.
    """
    response = client.fetch_bytes(spec.service, start, end, fmt="json")
    raw = response.content
    document = json.loads(raw.decode("utf-8"))
    return raw, document


def _envelope(document: dict, spec: MasterSpec) -> dict:
    """응답에서 서비스 엔벨로프를 꺼낸다.

    서울 OpenAPI는 인증오류/데이터없음 시 서비스 엔벨로프({service:{...}}) 없이 **최상위
    ``{"RESULT": {"CODE": ...}}``** 만 돌려준다(commerce/culture clients 동형). 이 경우
    최상위 RESULT 를 빈 엔벨로프(row=[])로 승격해, 상위(iter_master)의 RESULT.CODE 분기
    (INFO-200 조용히 종료 / 그 외 코드는 raise)가 그대로 동작하게 한다. 서비스 키도
    최상위 RESULT.CODE 도 없는 **완전 무형식** 응답만 기존 '엔벨로프 없음' 에러를 유지한다.
    """
    env = document.get(spec.service)
    if isinstance(env, dict):
        return env
    result = document.get("RESULT")
    if isinstance(result, dict) and result.get("CODE"):
        return {"RESULT": result, "row": [], "list_total_count": 0}
    raise RuntimeError(
        f"{spec.service} 응답 엔벨로프 없음 — 서비스명/권한 확인 필요"
    )


def iter_master(
    client: SeoulOpenApiClient, spec: MasterSpec, per_page: int = DEFAULT_PER_PAGE,
) -> Iterator[tuple[int, bytes, list[dict]]]:
    """마스터 전량 페이지 제너레이터 → (page_no, raw_bytes, rows).

    종료: 누적 수집행수 >= list_total_count 또는 short page(rows < per_page).
    위생 가드: page > _MAX_PAGES 이면 RuntimeError. RESULT.CODE 가 정상(INFO-000)이 아니면
    INFO-200(데이터 없음)은 조용히 종료, 그 외 코드는 RuntimeError.
    """
    page = 1
    collected = 0
    total: int | None = None
    while True:
        if page > _MAX_PAGES:
            raise RuntimeError(
                f"페이지 폭주 가드 발동 — {_MAX_PAGES}페이지 초과({spec.service}). "
                "종료 조건(short page/collected>=total) 오류 의심"
            )
        start = (page - 1) * per_page + 1
        end = page * per_page
        raw, document = fetch_page(client, spec, start, end)
        env = _envelope(document, spec)

        code = (env.get("RESULT") or {}).get("CODE")
        rows = env.get("row") or []
        if total is None:
            total = env.get("list_total_count")

        if code and code != _OK_CODE:
            if code == _NO_DATA_CODE:
                break
            message = (env.get("RESULT") or {}).get("MESSAGE")
            raise RuntimeError(
                f"{spec.service} API 오류 — RESULT.CODE={code} MESSAGE={message}"
            )

        yield page, raw, rows
        collected += len(rows)

        if total is not None and collected >= total:
            break
        if len(rows) < per_page:
            break
        page += 1


# ── load_date 멱등/자가치유 결정 (순수 로직 — DAG load 태스크가 소비) ──────────────
def load_action(existing_rows: int, landed_rows: int) -> str:
    """이 load_date 재적재 여부를 결정한다 — 순수 함수(테스트 대상).

    existing_rows: 이 load_date 로 브론즈에 이미 적재된 행수.
    landed_rows:   이번 런에서 R2 로 랜딩된 행수.

    반환:
      "insert" — existing==0(최초 적재).
      "skip"   — existing>0 이고 landed 와 정확히 일치(진짜 멱등 재실행).
      "reload" — existing>0 이지만 landed 와 불일치. 배치 중간 실패로 남은 partial 이거나
                 당일 재실행에서 원천 행수가 바뀐 경우 → DELETE 후 재적재(self-heal).
                 (기존 'existing 있으면 무조건 skip' 은 이 두 경우 verify 를 영구 실패시켰다.)
    """
    if existing_rows == 0:
        return "insert"
    if existing_rows == landed_rows:
        return "skip"
    return "reload"


# ── R2 랜딩 (common.storage #109) ────────────────────────────────────────────────
def _r2_env(name: str) -> str:
    """R2 자격증명 — R2_DEV_<name> 우선(멘티 dev 게이트), 없으면 R2_<name> 폴백."""
    dev = os.environ.get("R2_DEV_" + name)
    if dev:
        return dev
    value = os.environ.get("R2_" + name)
    if not value:
        raise RuntimeError(f"R2 자격증명 누락 — R2_DEV_{name} 또는 R2_{name}")
    return value


def build_r2_storage():
    """common.storage.build_storage(backend='r2') — 기존 transit R2 자격증명 규약과 동일."""
    from common.storage import build_storage

    return build_storage(
        "r2",
        endpoint=_r2_env("ENDPOINT"),
        key=_r2_env("ACCESS_KEY_ID"),
        secret=_r2_env("SECRET_ACCESS_KEY"),
        bucket=_r2_env("BUCKET_NAME"),
    )


def land_master(
    pages: Iterator[tuple[int, bytes, list[dict]]],
    spec: MasterSpec,
    run_id: str,
    *,
    storage=None,
    load_date: str | None = None,
    ingest_ts: str | None = None,
) -> dict:
    """마스터 페이지들을 R2 에 랜딩 + _manifest.json.

    경로 규약(transit 문서 규약 — source_system 세그먼트 포함):
        raw/transit/<source_system>/<dataset>/load_date=YYYY-MM-DD/ingest_ts=YYYYMMDDTHHMMSSZ/page-NNNN.json
        + .../_manifest.json

    storage 주입 가능(테스트) — 미지정 시 build_r2_storage(). 반환: 랜딩 결과 dict.
    마스터는 절대 0행일 수 없으므로 랜딩 결과가 0행이면 RuntimeError(빈 스냅샷 방치 방지).
    """
    store = storage if storage is not None else build_r2_storage()
    now = datetime.now(timezone.utc)
    load_date = load_date or now.strftime("%Y-%m-%d")
    ingest_ts = ingest_ts or now.strftime("%Y%m%dT%H%M%SZ")
    base = (
        f"raw/transit/{spec.source_system}/{spec.dataset}"
        f"/load_date={load_date}/ingest_ts={ingest_ts}"
    )

    object_keys: list[str] = []
    total_rows = 0
    total_bytes = 0
    for page_no, raw, rows in pages:
        object_key = f"{base}/page-{page_no:04d}.json"
        store.write_bytes(object_key, raw)
        object_keys.append(object_key)
        total_rows += len(rows)
        total_bytes += len(raw)

    if total_rows == 0:
        # 마스터 스냅샷이 0행 — 원천 이상(인증 만료·서비스 장애) 의심. 빈 스냅샷을
        # green 으로 흘리면 다운스트림이 조용히 비므로 여기서 끊는다(이전 스냅샷 유지됨).
        raise RuntimeError(
            f"빈 마스터 스냅샷 [{spec.dataset}] — 원천 이상 의심, 이전 스냅샷 유지됨 (rows=0)"
        )

    # 완결 확인서(ASK-Seoul#60 약속③) — 전 페이지 업로드 후 마지막에 쓴다(R1).
    # completed_at 은 업로드 완료 시각 — 함수 시작의 now(load_date/ingest_ts 기준)와 다르다.
    manifest = {
        "dataset": spec.dataset,
        "source_system": spec.source_system,
        "service": spec.service,
        "pages": len(object_keys),
        "rows": total_rows,
        "expected_rows": spec.expected_rows,
        "bytes": total_bytes,
        "run_id": run_id,
        "load_date": load_date,
        "ingest_ts": ingest_ts,
        "object_keys": object_keys,
        "status": "ok",
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest_key = f"{base}/_manifest.json"
    store.write_json(manifest_key, manifest)

    return {
        "object_keys": object_keys,
        "manifest_key": manifest_key,
        "rows": total_rows,
        "bytes": total_bytes,
        "load_date": load_date,
    }


def rows_from_document(document: dict, spec: MasterSpec) -> list[dict]:
    """R2 재다운로드한 페이지 원문 → 행 리스트(load 태스크가 파싱에 사용)."""
    return _envelope(document, spec).get("row") or []
