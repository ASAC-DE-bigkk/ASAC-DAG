"""silver.juso — 정규화 래더/조회 파싱/fill_one 상태 머신 단위 테스트 (네트워크 없음)."""
from __future__ import annotations

import pytest

from silver import juso


# ── normalize_road_key: dbt road_address_norm 규칙과 동일해야 함(조인 키 계약) ──
@pytest.mark.parametrize("raw,expected", [
    ("서울특별시 송파구 올림픽로 300, 롯데월드몰 행사장내 5층 (신천동)",
     "서울특별시 송파구 올림픽로 300, 롯데월드몰 행사장내 5층"),
    ("서울특별시  중구   세종대로 110, 지하2층 (태평로1가, 서울시청신매점)",
     "서울특별시 중구 세종대로 110, 지하2층"),
    ("  (괄호로 시작) 전부 절단  ", None),      # '(' 이후 절단 → 빈값 → None
    ("", None),
    (None, None),
])
def test_normalize_road_key(raw, expected):
    assert juso.normalize_road_key(raw) == expected


# ── candidate_queries: 래더 순서·유효성 필터·중복 제거 ──
def test_ladder_orders_and_dedupes():
    raw = "서울특별시 송파구 올림픽로 300, 롯데월드몰 행사장내 5층 (신천동)"
    cands = juso.candidate_queries(raw)
    ids = [pid for pid, _ in cands]
    queries = [q for _, q in cands]
    assert ids[0] == "p1_paren_cut"
    assert "서울특별시 송파구 올림픽로 300" in queries          # 콤마 절단이 만들어짐
    assert len(queries) == len(set(queries))                    # 중복 없음
    for _, q in cands:                                          # 전부 유효(도로명+번호)
        assert juso._VALID_QUERY.search(q)


def test_ladder_extracts_core_from_noisy_detail():
    # 콤마 없이 상세가 이어지는 형태 — p3(도로명+번호 접두)로 코어를 살려야 함
    raw = "서울특별시 강남구 테헤란로 152 지하2층 205호"
    queries = [q for _, q in juso.candidate_queries(raw)]
    assert "서울특별시 강남구 테헤란로 152" in queries


def test_ladder_skips_invalid_and_forbidden():
    # 도로명·번호가 없는 주소 → 후보 0 (호출 자체를 만들지 않음)
    assert juso.candidate_queries("서울특별시 강남구") == []
    # 금지문자는 공백 치환 후 정리
    raw = "서울특별시 강남구 테헤란로 152; DROP -- (주석)"
    for _, q in juso.candidate_queries(raw):
        assert ";" not in q and "%" not in q


def test_ladder_rebuilds_from_messy_prefix():
    # 시도 표기가 깨져도 p4/p5 가 '<구> <로> <번호>' 를 재조립
    raw = "서울 특별시송파구 올림픽로 300 (신천동)"
    queries = [q for _, q in juso.candidate_queries(raw)]
    assert any(q.endswith("올림픽로 300") for q in queries)


# ── fill_one: 상태 머신(성공/전패/오류) + 호출 수 기록 ──
def _juso_record():
    return {"jibunAddr": "서울특별시 송파구 신천동 29 롯데월드타워앤드롯데월드몰",
            "roadAddr": "서울특별시 송파구 올림픽로 300 (신천동)",
            "admCd": "1171010200", "emdNm": "신천동", "sggNm": "송파구", "siNm": "서울특별시"}


def test_fill_one_stops_at_first_match():
    calls = []

    def fake_search(q, **kw):
        calls.append(q)
        return (1, _juso_record()) if len(calls) >= 2 else (0, None)   # 2번째 패턴에서 성공

    row = juso.fill_one("서울특별시 송파구 올림픽로 300, 롯데월드몰 5층 (신천동)",
                        confm_key="k", delay_seconds=0, search=fake_search)
    assert row["status"] == "filled"
    assert row["api_calls"] == 2 and len(calls) == 2
    assert row["legal_dong_code"] == "1171010200" and row["legal_dong_name"] == "신천동"
    assert row["road_address_norm"] == "서울특별시 송파구 올림픽로 300, 롯데월드몰 5층"


def test_fill_one_exhausts_ladder_to_not_found():
    n = [0]

    def fake_search(q, **kw):
        n[0] += 1
        return (0, None)

    row = juso.fill_one("서울특별시 중구 세종대로 110, 지하2층 (태평로1가)",
                        confm_key="k", delay_seconds=0, search=fake_search)
    assert row["status"] == "not_found"
    assert row["api_calls"] == n[0] > 0
    assert row["ladder_version"] == juso.LADDER_VERSION


def test_fill_one_records_transient_error():
    def fake_search(q, **kw):
        raise ConnectionError("boom")

    row = juso.fill_one("서울특별시 중구 세종대로 110", confm_key="k",
                        delay_seconds=0, search=fake_search)
    assert row["status"] == "error" and row["error_summary"]


def test_fill_one_propagates_api_error():
    def fake_search(q, **kw):
        raise juso.JusoApiError("juso errorCode=E0001")

    with pytest.raises(juso.JusoApiError):
        juso.fill_one("서울특별시 중구 세종대로 110", confm_key="k",
                      delay_seconds=0, search=fake_search)
