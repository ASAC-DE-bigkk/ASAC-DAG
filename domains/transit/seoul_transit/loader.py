"""bronze 적재 분리(#369) — pending 마커 기반 R2→Iceberg 적재. 순수 로직(테스트 대상).

1분 수집을 버티기 위해 collector 와 적재를 분리한다:
  collector(고빈도) : API → R2 랜딩 + pending 마커(ops/control/state/transit/loader_pending/…)
  loader(저빈도)    : 마커 나열 → manifest·페이지 재다운로드 → 파싱 → 청크 INSERT → 마커 삭제

멱등성: 마커 단위로 `DELETE WHERE dag_run_id=<collector run>` 후 재적재 —
loader 가 중간에 죽어도 마커가 남아 다음 런이 통째로 다시 처리한다(중복 없음).
INSERT 는 문자 길이 캡(LOADER_INSERT_MAX_CHARS)으로 청크 — 버스 XML(노선당 ~10KB+)이
한 문장에 뭉치면 Trino QUERY_TEXT_TOO_LARGE(100만자) — 기존 노선당 1 INSERT 교훈.

오케스트레이션(Trino 커넥션·R2 IO·태스크)은 transit_bronze_loader.py — 이 파일은
파싱·마커·청크의 순수 함수만 담는다.
"""
from __future__ import annotations

import json
import os
import re
from typing import Iterator

from . import config
from .records import envelope

_HEADER_CD = re.compile(r"<headerCd>(\d+)</headerCd>")
_SAFE_RUN_ID = re.compile(r"[^A-Za-z0-9_.-]")

# envelope 형 테이블(지하철·주차) 공통 컬럼 — 기존 bronze 스키마와 동일해야 한다.
ENVELOPE_COLUMNS = ("source", "ts_source", "ts_collected", "lat", "lon", "raw")
# 버스 테이블(1행=1노선, raw=XML) 컬럼 — 기존 bronze_bus_position 스키마와 동일.
BUS_COLUMNS = ("source", "dataset", "bus_route_id", "ts_collected", "rows_cnt", "raw")
# 공통 계보 컬럼(loader 가 채움)
LINEAGE_COLUMNS = ("ingested_at", "dag_run_id")

# dataset → (table, shape). shape: "envelope" | "bus"
TABLE_SPECS: dict[str, tuple[str, str]] = {
    "subway_arrival":  ("bronze_subway_arrival",  "envelope"),
    "subway_position": ("bronze_subway_position", "envelope"),
    "parking":         ("bronze_parking",         "envelope"),
    "bus_arrival":     ("bronze_bus_arrival",     "bus"),
    "bus_position":    ("bronze_bus_position",    "bus"),
}

# dataset → 응답 리스트 키(envelope 형 JSON 파싱용) + ts_source 필드
_JSON_LIST_KEYS: dict[str, tuple[str, str]] = {
    "subway_arrival":  ("realtimeArrivalList",  "recptnDt"),
    "subway_position": ("realtimePositionList", "recptnDt"),
    "parking":         ("__parking__",          "NOW_PRK_VHCL_UPDT_TM"),
}


# ── Trino 대상 해석 — transit_master_bronze 의 dev 게이트 규약과 동일 (리뷰 #369:
# 파괴적 DELETE(보존 집행)가 env 하나(TRINO_ICEBERG_CATALOG)에만 의존하지 않게) ────
def is_dev_target() -> bool:
    return os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod")) == "dev"


def trino_catalog() -> str:
    # canonical 키 하나 — 값이 배포 환경을 따라간다(미설정 시 기본만 타깃별).
    return os.environ.get("TRINO_ICEBERG_CATALOG") or (
        "iceberg_dev" if is_dev_target() else "iceberg")


def transit_schema() -> str:
    # 도메인 공용 스키마(#367): prod/dev 동일 transit, 환경 분리는 카탈로그가 담당.
    return os.environ.get("TRANSIT_SCHEMA", "transit")


# ── SQL 리터럴 (기존 DAG 관례와 동일) ─────────────────────────────────────────────
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def sql_identifier(value: str) -> str:
    if not _IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"Unsafe SQL identifier: {value}")
    return value


def sql_str(value) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


# ── pending 마커 ─────────────────────────────────────────────────────────────────
def pending_key(dataset: str, ingest_ts: str, run_id: str) -> str:
    """마커 키 — ingest_ts 프리픽스라 사전순 나열 = 시간순 처리."""
    safe = _SAFE_RUN_ID.sub("_", run_id)
    return f"{config.LOADER_PENDING_PREFIX}{dataset}/{ingest_ts}__{safe}.json"


def make_marker(*, dataset: str, source: str, manifest_key: str,
                run_id: str, ts_collected: str) -> dict:
    """마커 본문. table/shape 는 관측용 스냅샷 — loader 는 적재 시점에 TABLE_SPECS
    에서 재파생한다(마커에 박힌 구값이 스펙 변경 후 잘못된 테이블로 적재되는 것 방지)."""
    if dataset not in TABLE_SPECS:
        raise ValueError(f"loader 미지원 dataset: {dataset}")
    table, shape = TABLE_SPECS[dataset]
    return {
        "dataset": dataset,
        "table": table,
        "shape": shape,
        "source": source,
        "manifest_key": manifest_key,
        "run_id": run_id,
        "ts_collected": ts_collected,
    }


def enqueue_pending(*, dataset: str, source: str, landed: dict, run_id: str,
                    ts_collected: str | None) -> None:
    """collector 공용 pending 마커 등록 — 마커 계약(키·본문) 변경을 한 곳으로 모은다.

    landed 는 r2_landing.land() 반환값(manifest_key·ingest_ts 포함).
    """
    from .r2_landing import put_json

    marker = make_marker(
        dataset=dataset, source=source, manifest_key=landed["manifest_key"],
        run_id=run_id, ts_collected=ts_collected,
    )
    put_json(pending_key(dataset, landed["ingest_ts"], run_id), marker)


# ── 원본 페이지 → bronze 행 파싱 ──────────────────────────────────────────────────
def build_rows(marker: dict, manifest: dict, pages: list[bytes]) -> list[tuple]:
    """마커 + manifest + 페이지 원문 → 테이블 shape 별 값 튜플 리스트.

    envelope 형: (source, ts_source, ts_collected, lat, lon, raw_json_str)
    bus 형:      (source, dataset, bus_route_id, ts_collected, rows_cnt, raw_xml)
    """
    shape = marker["shape"]
    if shape == "envelope":
        return _envelope_rows(marker, pages)
    if shape == "bus":
        return _bus_rows(marker, manifest, pages)
    raise ValueError(f"unknown shape: {shape}")


def _envelope_rows(marker: dict, pages: list[bytes]) -> list[tuple]:
    # source 컬럼은 dataset 명 — 분리 전 collector 인라인 적재가 넣던 값
    # ('subway_arrival'/'parking')과의 연속성 유지(리뷰 #369; marker['source'] 는
    # R2 경로용 source system 명이라 값이 달랐다).
    dataset, tc = marker["dataset"], marker["ts_collected"]
    source = dataset
    list_key, ts_field = _JSON_LIST_KEYS[dataset]
    rows: list[tuple] = []
    for page in pages:
        d = json.loads(page.decode("utf-8"))
        if dataset == "parking":
            items = d.get("GetParkingInfo", {}).get("row", [])
        else:
            items = d.get(list_key, [])
        for r in items:
            e = envelope(
                source, r, ts_source=r.get(ts_field), ts_collected=tc,
                lat=(r.get("LAT") or None) if dataset == "parking" else None,
                lon=(r.get("LOT") or None) if dataset == "parking" else None,
            )
            rows.append((
                e["source"], e["ts_source"], e["ts_collected"], e["lat"], e["lon"],
                json.dumps(e["raw"], ensure_ascii=False),
            ))
    return rows


def _bus_rows(marker: dict, manifest: dict, pages: list[bytes]) -> list[tuple]:
    """버스 → 1행=1노선. 두 랜딩 형식 지원:

    - 번들(JSONL, #369 번들링): 1객체에 1행=1노선 {busRouteId, rows, raw(xml)} —
      manifest request_params.bundle == "jsonl" 로 식별.
    - 구형(노선당 1페이지 XML): 전환 시점에 남아 있던 pending 마커 처리용.
    """
    if manifest.get("request_params", {}).get("bundle") == "jsonl":
        return _bus_rows_bundle(marker, pages)
    dataset, source, tc = marker["dataset"], marker["source"], marker["ts_collected"]
    routes = manifest.get("request_params", {}).get("busRouteId", [])
    if len(routes) != len(pages):
        raise ValueError(
            f"bus manifest 노선수({len(routes)}) != 페이지수({len(pages)}) — "
            f"manifest={marker['manifest_key']}"
        )
    rows: list[tuple] = []
    for route, page in zip(routes, pages):
        xml = page.decode("utf-8")
        cd = _HEADER_CD.search(xml)
        ok = cd and cd.group(1) == "0"
        rows.append((
            source, dataset, route, tc,
            xml.count("<itemList>") if ok else -1,
            xml,
        ))
    return rows


def _bus_rows_bundle(marker: dict, pages: list[bytes]) -> list[tuple]:
    """번들 JSONL 파싱 — rows_cnt 는 collector 가 계산한 값을 신뢰(원본 XML 동봉)."""
    dataset, source, tc = marker["dataset"], marker["source"], marker["ts_collected"]
    rows: list[tuple] = []
    for page in pages:
        for line in page.decode("utf-8").splitlines():
            if not line.strip():
                continue
            item = json.loads(line)
            rows.append((
                source, dataset, item["busRouteId"], tc,
                int(item["rows"]), item["raw"],
            ))
    return rows


# ── 청크 INSERT 값 생성 ──────────────────────────────────────────────────────────
def value_literal(row: tuple, *, ingested_at: str, dag_run_id: str, shape: str) -> str:
    """행 튜플 → VALUES 원소. rows_cnt(bus 5번째)만 정수, 나머지 문자열/NULL."""
    parts = []
    for i, v in enumerate(row):
        if shape == "bus" and i == 4:
            parts.append(str(int(v)))
        else:
            parts.append(sql_str(v))
    parts.append(f"TIMESTAMP {sql_str(ingested_at)}")
    parts.append(sql_str(dag_run_id))
    return "(" + ", ".join(parts) + ")"


def chunk_values(values: list[str], max_chars: int | None = None) -> Iterator[list[str]]:
    """VALUES 원소들을 문장당 문자 길이 캡으로 청크. 원소 1개가 캡을 넘어도 단독 통과
    (버스 XML 대형 응답 — 문장당 1행이 기존 노선당 1 INSERT 와 동치)."""
    cap = max_chars or config.LOADER_INSERT_MAX_CHARS
    chunk: list[str] = []
    size = 0
    for v in values:
        if chunk and size + len(v) > cap:
            yield chunk
            chunk, size = [], 0
        chunk.append(v)
        size += len(v)
    if chunk:
        yield chunk


def insert_columns(shape: str) -> str:
    base = ENVELOPE_COLUMNS if shape == "envelope" else BUS_COLUMNS
    return ", ".join(sql_identifier(c) for c in base + LINEAGE_COLUMNS)


def create_table_ddl(qualified_table: str, shape: str) -> str:
    """기존 collector DDL 과 동일 스키마 — loader 로 이관(#369)."""
    if shape == "envelope":
        cols = (
            "source varchar, ts_source varchar, ts_collected varchar, "
            "lat varchar, lon varchar, raw varchar"
        )
    else:
        cols = (
            "source varchar, dataset varchar, bus_route_id varchar, "
            "ts_collected varchar, rows_cnt integer, raw varchar"
        )
    return (
        f"CREATE TABLE IF NOT EXISTS {qualified_table} ({cols}, "
        f"ingested_at timestamp(6), dag_run_id varchar) WITH (format = 'PARQUET')"
    )
