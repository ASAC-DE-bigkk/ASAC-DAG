"""필드 실측 — 적재된 bronze record_json 키를 dataset 별로 수집(Trino).

카탈로그의 권위 소스는 라이브 API 가 아니라 **이미 적재된 bronze** 다(우리가 실제 가진 데이터).
전 행 스캔은 과대하므로 dataset 별 최근 N행의 키 합집합으로 측정한다 — LOCALDATA 응답은
행 간 키가 동일(스키마 고정)이라 소수 표본으로 충분하고, 드리프트는 다음 실행이 잡는다.
"""
from __future__ import annotations

import logging

from bronze.warehouse import _connect, _qualified
from security.dbio import is_identifier

log = logging.getLogger(__name__)

_SQL = """
select dataset, array_distinct(flatten(array_agg(ks))) as fields
from (
    select dataset,
           map_keys(cast(json_parse(record_json) as map(varchar, json))) as ks,
           row_number() over (partition by dataset order by collected_at desc) as rn
    from {qschema}.bronze_localdata_license
) t
where rn <= {sample_rows}
group by dataset
"""


def measure_fields(sample_rows: int = 50) -> dict[str, set[str]]:
    """dataset(short) → 응답 필드명 set. 적재된 데이터셋만 반환(미적재는 카탈로그가 다음 실행에 흡수)."""
    catalog, schema, qschema = _qualified()
    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        cur.execute(  # security: allow-sql - qschema 는 _qualified() 검증 식별자, 상수 쿼리.
            _SQL.format(qschema=qschema, sample_rows=int(sample_rows)))
        rows = cur.fetchall()
    finally:
        conn.close()
    # 유입 경계(C1 §20): 외부 API 응답 유래 필드명은 무검증으로 DDL/JSONPath 에 흘려보내지 않는다.
    # 비식별자(따옴표·공백·`$` 등)는 여기서 걸러 카탈로그에서 배제하고, 무엇을 뺐는지 §19.1 규격
    # 품질 이벤트로 남긴다(파이프라인은 정상 필드로 계속 진행).
    out: dict[str, set[str]] = {}
    dropped: dict[str, list[str]] = {}
    for r in rows:
        fields = set(r[1])
        bad = sorted(f for f in fields if not is_identifier(f))
        if bad:
            dropped[r[0]] = bad
        out[r[0]] = {f for f in fields if is_identifier(f)}
    if dropped:
        from security import log_event
        log_event("gold.build_catalog.non_identifier_fields_dropped", level="error",
                  where="measure_fields", task="commerce_load_gold.build_catalog",
                  message="비식별자 필드명 제외(injection guard)", dropped=dropped)
    log.info("field measure: datasets=%d (bronze 적재분 기준)", len(out))
    return out
