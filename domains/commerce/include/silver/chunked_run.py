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


def seed_state() -> dict:
    """seed 완료/재개 판정의 정본 — 마커 커버리지 + current 정합(행수>0 프록시 금지).

    "rows>0 → skip" 은 부분 빌드 후 재시도에서 잘못 skip 하고 Cosmos 에 무스코프 증분(=전량,
    OOM 클래스)을 넘기는 결함이 실측됐다(change-log #59). 올바른 신호:
    - history_incomplete: DONE 마커 없는 publishable run 이 있는 dataset (그 dataset 의 history 는
      미완 — 부분 행이 있어도 지우고 다시 빌드해야 함).
    - current_incomplete: current 행수 != history distinct grain 수인 dataset (history 는 완성됐지만
      current 단계 전에 중단된 경우 — current 만 재계산).
    complete = 둘 다 빈 목록. 마커/테이블 부재는 cold-start(전체 미완)로 간주한다."""
    from bronze.warehouse import _connect, _qualified

    catalog, schema, qschema = _qualified()
    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"""
select distinct cast(b.dataset as varchar)
from {qschema}.bronze_localdata_license b
inner join {qschema}.bronze_collection_run_manifest m
    on cast(b.dataset as varchar) = cast(m.dataset as varchar)
   and cast(b.bronze_run_id as varchar) = cast(m.bronze_run_id as varchar)
   and m.status = 'SUCCESS' and m.is_publishable
where not exists (
    select 1 from {qschema}.silver_load_run_marker k
    where k.status = 'DONE'
      and cast(k.dataset as varchar) = cast(b.dataset as varchar)
      and cast(k.bronze_run_id as varchar) = cast(b.bronze_run_id as varchar))
""")  # security: allow-sql - qschema 검증 식별자, 상수 쿼리
            history_incomplete = sorted(r[0] for r in cur.fetchall())
        except Exception:                 # marker/bronze 부재 → cold-start
            return {"complete": False, "cold_start": True,
                    "history_incomplete": [], "current_incomplete": []}
        current_incomplete: list[str] = []
        try:
            cur.execute(f"""
select coalesce(h.dataset, c.dataset)
from (select cast(dataset as varchar) dataset,
             count(distinct (coalesce(opnsfteamcode,''), coalesce(mgtno,''))) grains
      from {qschema}.silver_license_history group by 1) h
full outer join (select cast(dataset as varchar) dataset, count(*) rows
      from {qschema}.silver_license_current group by 1) c
    on h.dataset = c.dataset
where coalesce(h.grains, -1) <> coalesce(c.rows, -2)
""")  # security: allow-sql
            current_incomplete = sorted(r[0] for r in cur.fetchall())
        except Exception:                 # current 테이블 부재 → history 있는 전 dataset 미완
            current_incomplete = ["__missing_current__"]
        if silver_history_rows() == 0:
            return {"complete": False, "cold_start": True,
                    "history_incomplete": history_incomplete, "current_incomplete": []}
        complete = not history_incomplete and not current_incomplete
        return {"complete": complete, "cold_start": False,
                "history_incomplete": history_incomplete,
                "current_incomplete": current_incomplete}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _delete_dataset_rows(table: str, datasets: list[str]) -> None:
    """재빌드 대상 dataset 의 부분 행 선삭제(비-json, 저비용) — 버킷 경로는 pre_hook 삭제를 끄므로
    재개 시 여기서 정리한다. 값은 레지스트리 short(식별자 게이트 통과 규격)만 온다."""
    from bronze.warehouse import _connect, _qualified

    if not datasets:
        return
    catalog, schema, qschema = _qualified()
    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        vals = ", ".join(f"'{d}'" for d in datasets)
        try:
            cur.execute(f"delete from {qschema}.{table} where dataset in ({vals})")  # security: allow-sql
            cur.fetchall()
        except Exception:                 # 테이블 부재(cold-start) → 삭제 불필요
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _unmark_datasets(datasets: list[str]) -> None:
    """재빌드 대상 dataset 의 DONE 마커 삭제 — **행 삭제와 언마크는 한 몸**이다.

    행만 지우고 마커를 남기면 history 모델의 증분 술어(silver_unmarked_publishable_predicate)가
    'DONE run' 을 재처리하지 않아 그 run 의 행이 **영구 소실**된다(2026-07-13 실측: 재개가 dataset
    행을 지운 뒤 마커 남은 run 10개가 재빌드에서 빠짐 — change-log #60 버그5). 재빌드 = 전 run
    재처리이므로 대상 dataset 의 마커를 반드시 함께 비운다(seed 완료 시 mark_silver_runs_done 이
    test 통과 후 다시 마킹)."""
    from bronze.warehouse import _connect, _qualified

    if not datasets:
        return
    catalog, schema, qschema = _qualified()
    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        vals = ", ".join(f"'{d}'" for d in datasets)
        try:
            cur.execute(  # security: allow-sql - dataset 은 레지스트리 short
                f"delete from {qschema}.silver_load_run_marker where cast(dataset as varchar) in ({vals})")
            cur.fetchall()
        except Exception:                 # marker 테이블 부재(cold-start) → 불필요
            pass
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


def _dynamic_budget(ceil_rows: int) -> int:
    """가용 heap 마진 기반 배치 행수(하드웨어 적응). 실측: silver 변환 ≈ 11KB/행(파싱+지오+윈도우).
    프로브 실패 시 ceil_rows(env/기본) 폴백 — 큰 노드는 자동으로 큰 배치(빠름), 작은 노드는 작게(안전)."""
    from commerce_core import trino_mem

    return trino_mem.dynamic_rows(
        int(os.getenv("COMMERCE_SILVER_ROW_BYTES", "12000")),
        margin=float(os.getenv("COMMERCE_SILVER_HEAP_MARGIN", "0.35")),
        ceil_rows=ceil_rows, floor_rows=50_000)


def run_silver_chunked(*, select: str, project_dir: str, dbt_bin: str, target: str,
                       budget: int | None = None, state: dict | None = None) -> dict:
    """silver 를 dataset 배치로 나눠 dbt run — cold-start 전체 또는 **마커 기반 재개**(state).

    - budget 미지정 시 **동적 산정**(가용 heap 마진 — 하드웨어 적응). env COMMERCE_SILVER_BATCH_ROWS 는
      상한(ceil)/폴백.
    - 대형 단일 dataset(예: mail_order_sale 934K 단일 스냅샷 run)은 include_datasets 로 못 쪼개므로
      **싼 비-json 컬럼 버킷**으로 서브청크: history=content_hash 버킷(동일 레코드=같은 버킷 →
      adjacent-dedup 보존), current=grain 키 버킷(키의 전 버전=같은 버킷 → latest 정확). 각 버킷이
      record_json 읽기를 1/K 로 바운드(실측: 행수 비례 heap, 비스필 — change-log #59).
    - 쿼리 사이 **pace**(heap 회복 대기)로 백투백 garbage 누적 OOM 을 차단.
    - state(seed_state()) 가 있으면 **미완 dataset 만** 정리·재빌드(재개). 부분 행은 선삭제.
    """
    from commerce_core import trino_mem

    ceil_rows = int(budget or os.getenv("COMMERCE_SILVER_BATCH_ROWS", "800000"))
    budget = _dynamic_budget(ceil_rows)
    counts = dataset_row_counts()
    if not counts:
        log.info("bronze 비어 있음 — silver 실행 없음")
        return {"batches": 0, "datasets": 0}

    # 재개 스코프: history 미완 dataset(재빌드) + current 만 미완인 dataset(현재만 재계산)
    hist_targets = sorted(counts)                        # cold-start 기본 = 전체
    cur_only: list[str] = []
    if state and not state.get("cold_start"):
        hist_targets = [d for d in state.get("history_incomplete", []) if d in counts]
        cur_inc = state.get("current_incomplete", [])
        if "__missing_current__" in cur_inc:             # current 테이블 자체가 없음 → 전 dataset 재계산
            cur_only = [d for d in counts if d not in hist_targets]
        else:
            cur_only = [d for d in cur_inc if d in counts and d not in hist_targets]
        # 부분 행 선삭제 + **언마크**(한 몸 — 마커가 남으면 증분 술어가 DONE run 을 재처리하지 않아
        # 지운 행이 영구 소실된다). 버킷 경로는 pre_hook 삭제를 끄므로 여기서 정리(비-json 저비용).
        _delete_dataset_rows("silver_license_history", hist_targets)
        _delete_dataset_rows("silver_license_current", hist_targets + cur_only)
        _unmark_datasets(hist_targets)

    batches = plan_batches({d: counts[d] for d in hist_targets}, budget) if hist_targets else []
    log.info("silver 청크 실행: %d 배치(budget=%d행 동적, hist대상=%d, current만=%d)",
             len(batches), budget, len(hist_targets), len(cur_only))
    bucketed = 0
    for i, batch in enumerate(batches, 1):
        if len(batch) == 1 and counts.get(batch[0], 0) > budget:
            ds = batch[0]
            k = max(1, -(-counts[ds] // budget))         # ceil(rows/budget)
            log.info("배치 %d/%d: %s %d행 → history content_bucket ×%d + current key_bucket ×%d(저메모리)",
                     i, len(batches), ds, counts[ds], k, k)
            if not (state and not state.get("cold_start")):
                _delete_dataset_rows("silver_license_history", [ds])   # 재시도 부분행 방어(멱등)
                _unmark_datasets([ds])                                  # 삭제·언마크 한 몸(방어)
            for b in range(k):                            # phase 1 — history 전 버킷(완전 빌드)
                trino_mem.pace(f"{ds} hist {b + 1}/{k}")
                vj = json.dumps({"include_datasets": [ds], "content_bucket": [b, k]}, ensure_ascii=False)
                cmd = _dbt(project_dir, dbt_bin, target,
                           f"run --select silver_license_history --vars {json.dumps(vj)}")
                _run(cmd, f"청크 배치 {i} history content_bucket {b + 1}/{k}")
            for b in range(k):                            # phase 2 — current(완성된 history 참조)
                trino_mem.pace(f"{ds} cur {b + 1}/{k}")
                vj = json.dumps({"include_datasets": [ds], "key_bucket": [b, k]}, ensure_ascii=False)
                cmd = _dbt(project_dir, dbt_bin, target,
                           f"run --select silver_license_current --vars {json.dumps(vj)}")
                _run(cmd, f"청크 배치 {i} current key_bucket {b + 1}/{k}")
            bucketed += 1
            continue
        trino_mem.pace(f"batch {i}")
        vars_json = json.dumps({"include_datasets": batch}, ensure_ascii=False)
        # include_datasets 값은 레지스트리 유래 short(검증 식별자) — 셸 인용은 _dbt/json 이 처리.
        cmd = _dbt(project_dir, dbt_bin, target, f"run --select {select} --vars {json.dumps(vars_json)}")
        log.info("배치 %d/%d: %d dataset (~%d행)", i, len(batches), len(batch),
                 sum(counts[d] for d in batch))
        _run(cmd, f"청크 배치 {i}/{len(batches)}")

    # current 만 미완인 dataset 재계산(배치·버킷 동일 규칙)
    for j, cbatch in enumerate(plan_batches({d: counts[d] for d in cur_only}, budget) if cur_only else [], 1):
        if len(cbatch) == 1 and counts.get(cbatch[0], 0) > budget:
            ds = cbatch[0]
            k = max(1, -(-counts[ds] // budget))
            for b in range(k):
                trino_mem.pace(f"{ds} cur-only {b + 1}/{k}")
                vj = json.dumps({"include_datasets": [ds], "key_bucket": [b, k]}, ensure_ascii=False)
                _run(_dbt(project_dir, dbt_bin, target,
                          f"run --select silver_license_current --vars {json.dumps(vj)}"),
                     f"current 재계산 {j} 키버킷 {b + 1}/{k}")
            continue
        trino_mem.pace(f"cur-only {j}")
        vj = json.dumps({"include_datasets": cbatch}, ensure_ascii=False)
        _run(_dbt(project_dir, dbt_bin, target,
                  f"run --select silver_license_current --vars {json.dumps(vj)}"),
             f"current 재계산 {j}")
    return {"mode": "chunked", "batches": len(batches), "datasets": len(hist_targets),
            "current_only": len(cur_only), "budget": budget, "bucketed_datasets": bucketed}


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
