"""행정동 마스터 수집 — 순수 로직(테스트 대상) (#154).

원천: 공공데이터포털 ODcloud '행정동/법정동 연계 정보'
    GET https://api.odcloud.kr/api/15136368/v1/uddi:bbc26865-3049-42f5-a4b4-92c7f123a777

데이터 구조: **개정일자별 전국 전체 스냅샷 누적**(개정일자당 ~21,700행). 최신 개정일자
스냅샷만 수집한다. totalCount(1,048,575)는 엑셀 한도라 무의미 — **종료 판정은
currentCount < perPage** 로 한다. 최신 개정일자는 page1(perPage=1000)의 data 에서
max(개정일자)로 탐지한다(데이터가 최신 개정부터 정렬돼 있음을 실측 확인, max 로 이중 안전).

보안:
- 인증키는 env 에서만 로드(load_service_key). PUBLIC_DATA_API_KEY 우선,
  없으면 PUBLIC_DATA_API_KEY_BUS 폴백.
- serviceKey 는 QueryKey 전략으로 **params 로만** 전달한다. HttpCore 는 로그/예외에
  URL 만(redact 후) 남기고 params 는 로깅하지 않으므로(core.py request()) 키가 로그에
  남지 않는다. 방어 심화로 redaction 의 named-key 패턴이 'serviceKey' 도 커버한다
  (common/security/redaction.py `service[_-]?key` — 값이 URL 에 박혀도 마스킹).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Iterator

from common.http import HttpCore, QueryKey
from common.http.contract import Transport

LOGGER = logging.getLogger(__name__)

ODCLOUD_URL = (
    "https://api.odcloud.kr/api/15136368/v1/"
    "uddi:bbc26865-3049-42f5-a4b4-92c7f123a777"
)

DATASET = "admin_dong"
SOURCE_SYSTEM = "mods_dong_linkage"

DEFAULT_PER_PAGE = 1000
# 위생 가드 — 최신 스냅샷은 ~22페이지(perPage=1000) 예상. 종료 조건 오류로 인한
# 무한 페이지네이션(비용/rate 폭주) 방지. 40 초과 시 RuntimeError.
_MAX_PAGES = 40

# 인증키 env 이름 폴백 체인(현 루트 .env 엔 BUS 만 존재 — 후속으로 이름 정리 명시).
_KEY_ENV_NAMES = ("PUBLIC_DATA_API_KEY", "PUBLIC_DATA_API_KEY_BUS")


def load_service_key() -> str:
    """공공데이터포털 인증키 로드 — 폴백 체인, 없으면 RuntimeError."""
    for name in _KEY_ENV_NAMES:
        value = os.environ.get(name)
        if value:
            return value
    raise RuntimeError(
        "공공데이터포털 인증키 없음 — "
        + " 또는 ".join(_KEY_ENV_NAMES)
        + " 환경변수를 설정하세요"
    )


def build_core(transport: Transport | None = None) -> HttpCore:
    """odcloud 소스용 HttpCore — 멱등 GET 재시도 안전(#78).

    timeout=60s(대용량 페이지), max_attempts=3(429/5xx 백오프+jitter),
    rate_limit=None(원천에 명시 제한 없음 — 기존 동작 보존).
    transport 주입은 단위 테스트용(가짜 Transport).
    """
    return HttpCore(
        source="odcloud",
        transport=transport,
        timeout=60.0,
        max_attempts=3,
        rate_limit=None,
    )


def fetch_page(
    core: HttpCore,
    key: str,
    page: int,
    per_page: int = DEFAULT_PER_PAGE,
    revision: str | None = None,
) -> tuple[bytes, dict]:
    """단일 페이지 조회 → (raw_bytes(json 원문), parsed dict).

    serviceKey 는 QueryKey 로 params 에 실린다(URL 에 넣지 않음 → 로그 미노출).
    revision 이 주어지면 `cond[개정일자::EQ]` 로 특정 스냅샷만 조회한다.
    """
    params = {"page": str(page), "perPage": str(per_page)}
    if revision:
        params["cond[개정일자::EQ]"] = revision
    response = core.get(ODCLOUD_URL, params=params, auth=QueryKey("serviceKey", key))
    raw = response.content
    document = json.loads(raw.decode("utf-8"))
    return raw, document


def latest_revision(core: HttpCore, key: str) -> str:
    """최신 개정일자 탐지 — page1(perPage=1000)의 data 에서 max(개정일자)."""
    _, document = fetch_page(core, key, page=1, per_page=DEFAULT_PER_PAGE)
    rows = document.get("data") or []
    revisions = [r.get("개정일자") for r in rows if r.get("개정일자")]
    if not revisions:
        raise RuntimeError(
            "최신 개정일자 탐지 실패 — page1 data 가 비었거나 '개정일자' 필드가 없음"
        )
    return max(revisions)


def iter_snapshot(
    core: HttpCore,
    key: str,
    revision: str,
    per_page: int = DEFAULT_PER_PAGE,
) -> Iterator[tuple[int, bytes, list[dict]]]:
    """개정일자 스냅샷 페이지 제너레이터 → (page_no, raw_bytes, rows).

    종료: currentCount < per_page. 위생 가드: page > 40 이면 RuntimeError.
    """
    page = 1
    while True:
        if page > _MAX_PAGES:
            raise RuntimeError(
                f"페이지 폭주 가드 발동 — {_MAX_PAGES}페이지 초과"
                f"(개정일자={revision}). 종료 조건(currentCount<perPage) 오류 의심"
            )
        raw, document = fetch_page(
            core, key, page=page, per_page=per_page, revision=revision
        )
        rows = document.get("data") or []
        current = document.get("currentCount")
        if current is None:
            current = len(rows)
        yield page, raw, rows
        if current < per_page:
            break
        page += 1


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
    """common.storage.build_storage(backend='r2') — env 규약은 여기(어댑터)가 정한다."""
    from common.storage import build_storage

    return build_storage(
        "r2",
        endpoint=_r2_env("ENDPOINT"),
        key=_r2_env("ACCESS_KEY_ID"),
        secret=_r2_env("SECRET_ACCESS_KEY"),
        bucket=_r2_env("BUCKET_NAME"),
    )


def land_snapshot(
    pages: Iterator[tuple[int, bytes, list[dict]]],
    revision: str,
    run_id: str,
    *,
    storage=None,
    load_date: str | None = None,
    ingest_ts: str | None = None,
) -> dict:
    """스냅샷 페이지들을 R2 에 랜딩 + _manifest.json.

    경로 규약(transit r2_landing.land 와 동형):
        raw/common/admin_dong/load_date=YYYY-MM-DD/ingest_ts=YYYYMMDDTHHMMSSZ/page-NNNN.json
        + .../_manifest.json

    storage 주입 가능(테스트) — 미지정 시 build_r2_storage(). 반환: 랜딩 결과 dict.
    """
    store = storage if storage is not None else build_r2_storage()
    now = datetime.now(timezone.utc)
    load_date = load_date or now.strftime("%Y-%m-%d")
    ingest_ts = ingest_ts or now.strftime("%Y%m%dT%H%M%SZ")
    base = f"raw/common/{DATASET}/load_date={load_date}/ingest_ts={ingest_ts}"

    object_keys: list[str] = []
    total_rows = 0
    total_bytes = 0
    for page_no, raw, rows in pages:
        object_key = f"{base}/page-{page_no:04d}.json"
        store.write_bytes(object_key, raw)
        object_keys.append(object_key)
        total_rows += len(rows)
        total_bytes += len(raw)

    manifest = {
        "dataset": DATASET,
        "source_system": SOURCE_SYSTEM,
        "revision": revision,
        "pages": len(object_keys),
        "rows": total_rows,
        "bytes": total_bytes,
        "request_params": {"cond[개정일자::EQ]": revision},
        "run_id": run_id,
        "load_date": load_date,
        "ingest_ts": ingest_ts,
        "object_keys": object_keys,
    }
    manifest_key = f"{base}/_manifest.json"
    store.write_json(manifest_key, manifest)

    return {
        "dataset": DATASET,
        "revision": revision,
        "object_keys": object_keys,
        "manifest_key": manifest_key,
        "rows": total_rows,
        "bytes": total_bytes,
        "load_date": load_date,
        "ingest_ts": ingest_ts,
        "base": base,
    }
