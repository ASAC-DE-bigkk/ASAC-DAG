"""silver dbt 청크 실행 — 전체 재빌드 시 dataset 배치로 나눠 OOM 회피.

전체 재빌드(테이블 drop 후 최초/`--full-refresh`)는 `silver_license_history` 의 window 연산이 전 행을
한 번에 올려 Trino 노드 메모리를 초과(EXCEEDED_LOCAL_MEMORY_LIMIT). dataset 를 **행수 기준 배치**로
묶어 `include_datasets`(화이트리스트, macros/exclusions.sql)로 순차 실행하면 각 배치가 메모리에 든다
(가장 큰 단일 dataset 이 한 배치에 들어갈 정도면 절대 OOM 안 남). 평소 증분은 unmarked run 만 처리해
배치가 작아 부담 미미. 절차 문서: dbt/domains/commerce/docs/rebuild-and-ops.md §6.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess

log = logging.getLogger(__name__)


def dataset_row_counts() -> dict[str, int]:
    """bronze dataset 별 행수(배치 크기 산정용). bronze.warehouse 접속 재사용."""
    from bronze.warehouse import _connect, _qualified

    catalog, schema, qschema = _qualified()
    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        cur.execute(  # security: allow-sql - qschema 는 _qualified() 검증 식별자, 상수 쿼리.
            f"select cast(dataset as varchar), count(*) "
            f"from {qschema}.bronze_localdata_license group by 1")
        return {r[0]: int(r[1]) for r in cur.fetchall()}
    finally:
        conn.close()


def plan_batches(counts: dict[str, int], budget: int) -> list[list[str]]:
    """행수 기준 greedy-pack. budget 이상 단일 dataset 은 단독 배치(그 이하로 못 쪼갬)."""
    batches: list[list[str]] = []
    cur: list[str] = []
    cur_rows = 0
    for ds, n in sorted(counts.items(), key=lambda x: -x[1]):
        if n >= budget:
            batches.append([ds])
            continue
        if cur and cur_rows + n > budget:
            batches.append(cur)
            cur, cur_rows = [], 0
        cur.append(ds)
        cur_rows += n
    if cur:
        batches.append(cur)
    return batches


def _dbt(project_dir: str, dbt_bin: str, target: str, args: str) -> str:
    import shlex

    return (
        "set -euo pipefail\n"
        f"cd {shlex.quote(project_dir)}\n"
        f"DBT_PROJECT_DIR={shlex.quote(project_dir)} DBT_PROFILES_DIR={shlex.quote(project_dir)} "
        f"{shlex.quote(dbt_bin)} --no-use-colors --target {shlex.quote(target)} {args}")


def silver_history_rows() -> int:
    """silver_license_history 행수. 테이블 부재(첫 빌드)면 0 — 청크 여부 판단용."""
    from bronze.warehouse import _connect, _qualified

    catalog, schema, qschema = _qualified()
    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        cur.execute(f"select count(*) from {qschema}.silver_license_history")  # security: allow-sql
        return int(cur.fetchone()[0])
    except Exception:                     # 테이블 없음 등 → 전체 재빌드로 간주
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _run(cmd: str, label: str) -> None:
    res = subprocess.run(["/bin/bash", "-c", cmd], capture_output=True, text=True)
    if res.returncode != 0:
        tail = (res.stdout or "")[-2000:] + (res.stderr or "")[-500:]
        log.error("silver %s 실패:\n%s", label, tail)
        raise RuntimeError(f"silver {label} 실패")
    log.info("silver %s OK", label)


def run_silver_chunked(*, select: str, project_dir: str, dbt_bin: str, target: str,
                       budget: int | None = None) -> dict:
    """select 모델을 dataset 배치로 나눠 dbt run. 배치 실패 시 예외(다음 실행이 이어감).

    budget = 배치당 최대 bronze 행수(COMMERCE_SILVER_BATCH_ROWS, 기본 80만 — 노드 3.7GB 한도 여유).
    """
    budget = int(budget or os.getenv("COMMERCE_SILVER_BATCH_ROWS", "800000"))
    counts = dataset_row_counts()
    if not counts:
        log.info("bronze 비어 있음 — silver 실행 없음")
        return {"batches": 0, "datasets": 0}
    batches = plan_batches(counts, budget)
    log.info("silver 청크 실행: %d 배치(budget=%d행, dataset=%d)", len(batches), budget, len(counts))
    for i, batch in enumerate(batches, 1):
        vars_json = json.dumps({"include_datasets": batch}, ensure_ascii=False)
        # include_datasets 값은 레지스트리 유래 short(검증 식별자) — 셸 인용은 _dbt/json 이 처리.
        cmd = _dbt(project_dir, dbt_bin, target, f"run --select {select} --vars {json.dumps(vars_json)}")
        log.info("배치 %d/%d: %d dataset (~%d행)", i, len(batches), len(batch),
                 sum(counts[d] for d in batch))
        _run(cmd, f"청크 배치 {i}/{len(batches)}")
    return {"mode": "chunked", "batches": len(batches), "datasets": len(counts), "budget": budget}


def run_dbt_test(*, select: str, project_dir: str, dbt_bin: str, target: str) -> None:
    """seed(cold start) 검증 — 청크 전량 빌드 직후 `dbt test`. 실패 시 예외(→ 마킹으로 진행 안 됨).

    Cosmos 전환(#54) 후 cold-start 는 seed 태스크가 build→test→mark 를 캡슐화한다. 평상시 증분의
    test 는 Cosmos(dbt_silver)가 모델별로 렌더하므로 이 헬퍼는 seed 경로 전용이다.
    """
    _run(_dbt(project_dir, dbt_bin, target, f"test --select {select}"), "seed test")


def run_silver(*, select: str, project_dir: str, dbt_bin: str, target: str,
               budget: int | None = None) -> dict:
    """silver dbt run 진입점 — **전체 재빌드(silver 비었음)면 청크, 평소 증분이면 단일 실행**.

    증분은 unmarked run 만 처리해 소량이라 단일 dbt run 이 안전·빠름. silver_license_history 가
    비어 있으면(테이블 drop 후 최초) window 연산이 전 행을 올려 OOM 나므로 dataset 배치로 나눈다.

    참고: Cosmos 전환(#54) 이후 commerce_load_silver DAG 는 증분을 Cosmos(dbt_silver)로 돌리고,
    cold-start 청크만 run_silver_chunked 로 호출한다. 이 함수(청크/증분 자동 분기)는 수동 재빌드·
    다른 진입점용으로 남겨 둔다.
    """
    if silver_history_rows() == 0:
        log.info("silver_license_history 비어 있음 → 전체 재빌드(청크 모드)")
        return run_silver_chunked(select=select, project_dir=project_dir, dbt_bin=dbt_bin,
                                  target=target, budget=budget)
    log.info("silver 증분(단일 실행)")
    _run(_dbt(project_dir, dbt_bin, target, f"run --select {select}"), "증분 run")
    return {"mode": "incremental", "batches": 1}
