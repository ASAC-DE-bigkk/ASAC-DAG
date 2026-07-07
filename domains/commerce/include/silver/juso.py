"""Juso(도로명주소) API 클라이언트 — 도로명 → 지번주소/법정동 조회 (silver 지번 결측 보강).

원천(LOCALDATA) 인허가 행 중 지번주소(SITEWHLADDR·LOTNO_ADDR 모두)가 없는 도로명주소를
지번주소로 보강한다. 인허가 도로명주소는 상세동·층·괄호 등 노이즈가 많아 **정규화 래더**
(여러 정규식 후보를 순서대로 시도)로 질의를 만들고, 첫 매치(totalCount>=1)에서 멈춘다.
어떤 패턴으로 몇 번 호출했는지는 결과 row 에 남겨 enrichment 테이블/로그로 감사한다.

- 인증키: env `JUSO_CONFM_KEY` (.env.commerce — `*_KEY` 네이밍이라 보안 모듈이 자동 마스킹).
- 계약(실호출 검증 2026-07-06): GET https://business.juso.go.kr/addrlink/addrLinkApi.do
  (resultType=json) → results.common.errorCode('0'=정상)/totalCount,
  results.juso[].jibunAddr/admCd(**법정동코드 10자리**)/emdNm/sggNm/siNm/roadAddr.
  참고문서: https://business.juso.go.kr/jst/jstRoadNmAddrApiSearch
- 래더를 고치면 LADDER_VERSION 을 올릴 것 — not_found 캐시가 새 래더로 재시도된다.
"""
from __future__ import annotations

import logging
import os
import re
import time

from security import redact
from security.netio import http_get

log = logging.getLogger(__name__)

JUSO_URL_DEFAULT = "https://business.juso.go.kr/addrlink/addrLinkApi.do"
LADDER_VERSION = 1              # 정규화 래더 개정판(패턴 추가/수정 시 +1)
_MAX_RESPONSE_BYTES = 1_000_000


class JusoApiError(RuntimeError):
    """Juso 가 오류 코드를 반환(인증키/파라미터 오류 등) — 주소를 바꿔도 소용없는 실패."""


def normalize_road_key(raw: str | None) -> str | None:
    """도로명 정규화 **조인 키** — dbt silver 의 road_address_norm 과 규칙이 반드시 동일해야 함.

    규칙(v1): '(' 이후 절단 → 연속 공백 1개 → trim → 빈값 None.
    (dbt: regexp_replace(regexp_replace(x,'\\(.*$',''),'\\s+',' ') 후 trim/nullif)
    """
    if raw is None:
        return None
    s = re.sub(r"\(.*$", "", raw)
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


# Juso 가 차단/오해하는 특수문자(SQL 예약문자 포함) — 공백 치환 후 재정리
_FORBIDDEN = re.compile(r"[%=<>&!{}\[\]\"';|\\]")
# 유효 질의 최소 조건: 도로명(로/길) + 건물번호 숫자
_VALID_QUERY = re.compile(r"(로|길)\s?\d")
# 도로명+번호 코어 추출(상세주소 앞부분): '... 올림픽로 300' / '... 세종대로 110-1'
_CORE_PREFIX = re.compile(r"^(.*?(?:로|길)\s?\d+(?:-\d+)?)")
# 원문 어디서든 '<구> <도로명> <번호>' 를 재조립(주소가 비정형일 때 최후 수단)
_CORE_ANY = re.compile(r"(\S+구)\s+(\S+(?:로|길))\s*(\d+(?:-\d+)?)")


def candidate_queries(raw: str | None) -> list[tuple[str, str]]:
    """정규화 래더 — (pattern_id, query) 순서 목록. 유효성 필터 + 중복 제거.

    p1 괄호 절단(=조인 키) → p2 콤마(상세주소) 절단 → p3 도로명+번호 접두 추출
    → p4 원문 재조립(서울특별시 <구> <로> <번호>) → p5 시도 생략형.
    """
    base = normalize_road_key(raw) or ""
    cands: list[tuple[str, str]] = [("p1_paren_cut", base)]
    comma = base.split(",")[0].strip()
    cands.append(("p2_comma_cut", comma))
    m = _CORE_PREFIX.match(comma)
    if m:
        cands.append(("p3_core_prefix", m.group(1).strip()))
    m2 = _CORE_ANY.search(raw or "")
    if m2:
        gu, road, bun = m2.group(1), m2.group(2), m2.group(3)
        cands.append(("p4_core_extract", f"서울특별시 {gu} {road} {bun}"))
        cands.append(("p5_no_sido", f"{gu} {road} {bun}"))
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for pid, q in cands:
        q = _FORBIDDEN.sub(" ", q)
        q = re.sub(r"\s+", " ", q).strip()
        if len(q) >= 8 and _VALID_QUERY.search(q) and q not in seen:
            seen.add(q)
            out.append((pid, q))
    return out


def search_one(query: str, *, confm_key: str, url: str = JUSO_URL_DEFAULT,
               timeout: float = 10.0) -> tuple[int, dict | None]:
    """1회 조회 → (totalCount, 첫 결과 dict|None). 오류 코드는 JusoApiError.

    호출은 보안 래퍼(http_get)로 — timeout 주입·TLS 강제·예외 메시지 마스킹.
    """
    resp = http_get(url, params={
        "confmKey": confm_key, "currentPage": 1, "countPerPage": 1,
        "keyword": query, "resultType": "json",
    }, timeout=timeout, max_response_bytes=_MAX_RESPONSE_BYTES)
    data = resp.json()
    common = (data.get("results") or {}).get("common") or {}
    code = str(common.get("errorCode", "")).strip()
    if code != "0":
        # errorMessage 는 정적 안내문이지만 방어적으로 마스킹해 전달
        raise JusoApiError(f"juso errorCode={code} msg={redact(str(common.get('errorMessage', '')))}")
    total = int(common.get("totalCount") or 0)
    juso = (data.get("results") or {}).get("juso") or []
    return total, (juso[0] if total >= 1 and juso else None)


def fill_one(raw_road: str, *, confm_key: str, url: str = JUSO_URL_DEFAULT,
             delay_seconds: float = 0.15, search=search_one) -> dict:
    """래더를 순서대로 시도해 지번주소를 찾는다 — 첫 성공에서 멈춤.

    반환 row(성공/실패 공통 스키마): status filled|not_found|error,
    pattern_id(성공 패턴), attempts(시도 패턴 수), api_calls(실호출 수),
    jibun_address/legal_dong_code(admCd)/legal_dong_name(emdNm)/sgg_name/si_name/
    matched_road_addr/juso_total_count.
    JusoApiError(키·파라미터 오류)는 상위로 전파 — 주소를 바꿔도 소용없어 태스크 실패가 맞다.
    """
    row = {
        "road_address_norm": normalize_road_key(raw_road), "source_road_address": raw_road,
        "jibun_address": None, "matched_road_addr": None,
        "legal_dong_code": None, "legal_dong_name": None, "sgg_name": None, "si_name": None,
        "juso_total_count": 0, "pattern_id": None, "attempts": 0, "api_calls": 0,
        "status": "not_found", "error_summary": None, "ladder_version": LADDER_VERSION,
    }
    for pid, query in candidate_queries(raw_road):
        row["attempts"] += 1
        row["api_calls"] += 1
        try:
            total, first = search(query, confm_key=confm_key, url=url)
        except JusoApiError:
            raise
        except Exception as exc:                     # 네트워크 등 일시 오류 — 이 주소만 error 로 기록
            row["status"] = "error"
            row["error_summary"] = redact(str(exc))[:300]
            log.warning("juso 조회 실패(주소 단위, 다음 실행 재시도): %s", row["error_summary"])
            return row
        time.sleep(delay_seconds)                    # 과금/차단 방지 매너 딜레이
        if total >= 1 and first:
            row.update({
                "status": "filled", "pattern_id": pid, "juso_total_count": total,
                "jibun_address": first.get("jibunAddr"),
                "matched_road_addr": first.get("roadAddr"),
                "legal_dong_code": first.get("admCd"),
                "legal_dong_name": first.get("emdNm"),
                "sgg_name": first.get("sggNm"), "si_name": first.get("siNm"),
            })
            log.info("juso 매치: pattern=%s calls=%d '%s' -> '%s'",
                     pid, row["api_calls"], query, row["jibun_address"])
            return row
    log.info("juso 매치 실패(래더 %d회 소진): '%s'", row["api_calls"], row["road_address_norm"])
    return row


def get_confm_key() -> str:
    key = os.getenv("JUSO_CONFM_KEY", "").strip()
    if not key:
        raise RuntimeError("JUSO_CONFM_KEY 미설정 — .env.commerce 에 승인키를 넣어야 함")
    return key
