"""commerce_load_silver — silver 보강 + dbt(Cosmos) 변환 오케스트레이션.

bronze 적재(commerce_load_bronze, 04:00 KST) 이후 ① silver 사전 보강(행정동↔법정동 참조
전량 교체 + 지번 결측 Juso 보강 캐시) ② cold-start 시드 가드 ③ dbt/domains/commerce 의
silver 모델 반영(Cosmos DbtTaskGroup — 모델당 run+test) ④ 완료 마킹·리포트 순으로 실행한다.

dbt 실행 계약(Cosmos):
- Cosmos(astronomer-cosmos)는 Airflow 이미지(airflow env)에 설치하고, 실제 dbt 는 기존 별도
  venv(`DBT_BIN`, 기본 /home/airflow/dbt-venv/bin/dbt — common_dbt_smoke 와 동일 계약)로
  실행한다(ExecutionMode.LOCAL + dbt_executable_path → venv 경계 유지). 프로필은 기존
  dbt/domains/commerce/profiles.yml 을 그대로 재사용한다(Cosmos 가 새로 만들지 않음).
- Cosmos 는 **모델당 1태스크**(run → test AFTER_EACH)로 렌더한다 → 모델단위 관측성·lineage.
  선택 모델은 SILVER_SELECT(history, current). 기존 단일 BashOperator `dbt test` 를 대체한다.

cold-start(빈 silver) 안전 시드 — **왜 전체 재빌드를 Cosmos 단독으로 못 하는가**(요약; 상세는
docs/cosmos.md):
  전체 재빌드(테이블 drop 후 최초/`--full-refresh`)는 silver_license_history 의 window 연산이
  전 행을 한 번에 올려 Trino 노드 메모리를 초과(EXCEEDED_LOCAL_MEMORY_LIMIT)한다. 이를 막으려면
  dataset 를 행수 배치로 나눠 `--vars include_datasets` 로 순차 실행해야 하는데(chunked_run),
  Cosmos 는 파싱시 정적 렌더(모델당 1태스크)라 런타임 행수 기반 배치 루프를 못 한다. 게다가
  silver_license_history 는 pre_hook(delete_unmarked)+마커 기반 증분이라, 마킹되지 않은 전량
  빌드 위에 Cosmos 증분을 돌리면 pre_hook 이 그 전량을 지우고 비청크 단일 run 으로 재처리 → OOM.
  따라서 최초 전량 빌드만 seed_silver_if_empty 가 청크로 처리하고 **마킹까지 끝낸다** → 이후
  Cosmos 증분은 no-op(삭제 대상 없음). 평상시(테이블 존재)엔 seed 는 no-op 이고 Cosmos 가 증분
  run+test 를 담당한다. 절차: dbt/domains/commerce/docs/rebuild-and-ops.md §6.

  [enrich_admin_dong_ref, enrich_fill_jibun, ensure_silver_marker]
    ─> seed_silver_if_empty ─> dbt_silver(Cosmos: run+test)
    ─> notify_masked_address_summary ─> mark_silver_done ─> report_silver

보강 규약(주소·동·좌표): dbt/domains/commerce/docs/address-and-geo.md
"""
from __future__ import annotations

import sys
from pathlib import Path

# 자립(portable): 자기 카테고리의 include 를 import 경로에 올린다.
sys.path.insert(0, str(Path(__file__).resolve().parent / "include"))
# 공통 패키지(dags/common) — commerce_core.storage 가 common.storage 를 쓴다(#109).
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from commerce_core.env import load_commerce_env  # noqa: E402

load_commerce_env()

from security import install_security  # noqa: E402

install_security()

import json  # noqa: E402
import os  # noqa: E402

import pendulum  # noqa: E402
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException  # noqa: E402
from cosmos import (  # noqa: E402
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import ExecutionMode, InvocationMode, LoadMode, TestBehavior  # noqa: E402

from commerce_core.observability import ops_default_args  # noqa: E402
from common.ops import Layer  # noqa: E402

# dbt 실행 계약(호스트 이미지 env 우선, 없으면 기본값) — common_dbt_smoke 와 동일 형태.
DBT_PROJECT_DIR = os.getenv("COMMERCE_DBT_PROJECT_DIR", "/opt/airflow/dbt/domains/commerce")
DBT_BIN = os.getenv("DBT_BIN", "/home/airflow/dbt-venv/bin/dbt")
# 기본 dev(iceberg_dev/seoul-dev). prod 전환은 .env.commerce 또는 compose env 로.
DBT_TARGET = os.getenv("COMMERCE_DBT_TARGET") or os.getenv("DBT_TARGET", "dev")
# 모델 선택(리스트 = Cosmos RenderConfig.select / 문자열 join = chunked seed·비교용).
# 원형 정리본 entity 2종은 레이어 재분류(#70, PROJECT.md §4)로 silver 파이프라인에 편승 —
# ref 체인(current→entity, history→entity_history)은 Cosmos 가 그래프 순서로 실행한다.
SILVER_SELECT = ["silver_license_history", "silver_license_current",
                 "silver_license_entity", "silver_license_entity_history"]
SILVER_SELECT_STR = " ".join(SILVER_SELECT)
# 스코프 dbt vars(운영 노브) — Cosmos 는 정적 렌더라 런타임 청크 루프가 불가하므로(위 docstring),
# 저메모리 환경에서 dbt_silver 를 dataset 스코프로 돌릴 때 .env.commerce 에 JSON 으로 지정한다.
# 예: COMMERCE_DBT_VARS='{"include_datasets": ["golf_course"]}' — 비우면(기본) 전체 증분.
# 마커/pre_hook 은 include_datasets 스코프를 그대로 존중한다(silver_markers 매크로).
_DBT_VARS: dict = json.loads(os.getenv("COMMERCE_DBT_VARS") or "{}")

_DEFAULT_ARGS = {"owner": "data-eng", "retries": 1, "retry_delay": pendulum.duration(minutes=5),
                 **ops_default_args(Layer.SILVER)}

# ── Cosmos 설정 ──────────────────────────────────────────────────────────────
# 프로필은 기존 profiles.yml 재사용, 실행은 별도 dbt venv(LOCAL). commerce 프로젝트는
# packages.yml 이 없어 `dbt deps` 불필요(install_deps=False). load_method=DBT_LS 는 DAG 파싱시
# venv dbt 로 `dbt ls` 를 실행해 모델 그래프를 만든다(견고화 옵션은 docs/cosmos.md §manifest).
_profile_config = ProfileConfig(
    profile_name="commerce",
    target_name=DBT_TARGET,
    profiles_yml_filepath=Path(DBT_PROJECT_DIR) / "profiles.yml",
)
_project_config = ProjectConfig(dbt_project_path=DBT_PROJECT_DIR)
_execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    # cosmos>=1.15 는 Airflow env 에 dbt-core 가 보이면(openlineage extra 의존) DBT_RUNNER
    # (in-process)를 기본으로 잡는다 — 이 DAG 의 계약은 별도 dbt venv(DBT_BIN) 실행이므로
    # SUBPROCESS 를 명시해 venv 경계를 고정한다(위 docstring "dbt 실행 계약" 그대로).
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path=DBT_BIN,
)
_render_config = RenderConfig(
    select=SILVER_SELECT,
    test_behavior=TestBehavior.AFTER_EACH,
    load_method=LoadMode.DBT_LS,
    invocation_mode=InvocationMode.SUBPROCESS,   # 렌더(dbt ls)도 venv dbt — ExecutionConfig 와 동일 사유
    dbt_executable_path=DBT_BIN,
)


@dag(dag_id="commerce_load_silver", schedule="0 5 * * *",
     start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), catchup=False,
     max_active_runs=1, default_args=_DEFAULT_ARGS,
     tags=["seoul", "commerce", "silver", "dbt", "cosmos"], doc_md=__doc__)
def commerce_load_silver():
    @task
    def dbt_seed_taxonomy() -> dict:
        """dbt seed(분류 체계 등 참조 시드) 멱등 재적재 — from-zero/이관 자가 치유.

        prod 컷오버 실측(2026-07-29): seed 는 dev 에서 수동 1회만 실행돼 있어 신규 환경의
        gold 22종 + silver(detail_health)가 `commerce_dataset_taxonomy` 부재로 전멸했다.
        dbt 진입 레이어(silver)에서 매 run 실행해 환경 변경·이관 시에도 참조 시드가
        항상 존재하게 한다(152행·수 초 — 존재 시 전량 교체 멱등).
        """
        import logging
        import subprocess

        log = logging.getLogger(__name__)
        cmd = [DBT_BIN, "seed", "--project-dir", DBT_PROJECT_DIR,
               "--profiles-dir", DBT_PROJECT_DIR, "--target", DBT_TARGET]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        tail = (proc.stdout or "")[-2000:]
        if proc.returncode != 0:
            log.error("dbt seed 실패(rc=%s): %s", proc.returncode, tail)
            raise RuntimeError(f"dbt seed 실패 rc={proc.returncode}")
        seeded = sum(1 for ln in tail.splitlines() if " OK loaded seed file " in ln)
        log.info("dbt seed 완료: %d개 시드 적재(target=%s)", seeded, DBT_TARGET)
        return {"seeds_loaded": seeded, "target": DBT_TARGET}

    @task
    def enrich_admin_dong_ref() -> dict:
        """행정동↔법정동 참조(raw/common/admin_dong 최신본) → Iceberg 전량 교체."""
        from silver import enrich_tasks   # 지연 임포트 — DAG 파싱 경량 유지

        return enrich_tasks.load_admin_dong_ref()

    @task
    def enrich_fill_jibun() -> dict:
        """지번 결측 도로명 → Juso 래더 조회 → enrichment 캐시 upsert(+감사 로그)."""
        from silver import enrich_tasks

        return enrich_tasks.fill_jibun_from_road()

    @task
    def ensure_silver_marker() -> dict:
        """silver DONE marker 테이블 생성 + 기존 history marker 부트스트랩."""
        from silver import silver_markers

        return silver_markers.ensure_silver_marker_table()

    @task
    def seed_silver_if_empty() -> dict:
        """Cold start/재개 안전 시드 — Cosmos 가 못 하는 청크 전량 빌드·복구만 여기서.

        판정은 **마커 커버리지**(chunked_run.seed_state)로 하되, '빌드 미완'(미마킹 run 이 history
        에 행을 남김/DONE 전무)과 '신규 run 도착'(기존 DONE 존재 + 미빌드)을 구분한다 — 신규 run 은
        **Cosmos 증분 몫**(cosmos_pending)이라 seed 가 재빌드하지 않는다(구분 없이 재빌드하면 신규
        run 이 있는 날마다 기적재 이력 전체를 delete+재빌드 — 기적재분 재적재, 2026-07-13 진단).
        "행수>0 → skip" 프록시는 부분 빌드 후 재시도에서 잘못 skip 하고 Cosmos 에 무스코프
        증분(=전량, OOM 클래스)을 넘기는 결함이 실측됐다(change-log #59). 미완이면 해당 dataset 만
        정리·재빌드(재개) → dbt test → DONE 마킹. **마킹까지 끝내야** downstream Cosmos 증분이
        pre_hook 전역 삭제로 전량 재처리(OOM)하는 걸 막는다. 상세: docs/cosmos.md · rebuild-and-ops.md §6.
        """
        from datetime import datetime, timedelta, timezone

        from silver import chunked_run, silver_markers

        # cutoff = 빌드 시작 시각(KST, bronze_run_id 포맷) — 이 이전의 publishable run 은 이번 빌드가
        # 전부 처리하므로, dedup 으로 행이 0이어도 DONE 마킹 대상(영구 미완 오판 방지 — change-log #60).
        cutoff = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d_%H%M%S")
        state = chunked_run.seed_state()
        if state["complete"]:
            return {"seed": "skip", "reason": "marker coverage complete + current 정합",
                    "cosmos_pending": state.get("cosmos_pending", [])}
        built = chunked_run.run_silver_chunked(
            select=SILVER_SELECT_STR, project_dir=DBT_PROJECT_DIR, dbt_bin=DBT_BIN,
            target=DBT_TARGET, state=state)
        # 마킹 전 검증(실패 시 예외 → 마킹 안 됨 → 다음 실행이 이어감, 기존 invariant 동일).
        chunked_run.run_dbt_test(
            select=SILVER_SELECT_STR, project_dir=DBT_PROJECT_DIR, dbt_bin=DBT_BIN, target=DBT_TARGET)
        marked = silver_markers.mark_silver_runs_done(processed_cutoff_run_id=cutoff)
        return {"seed": "built", "build": built, "marked": marked,
                "resumed_hist": len(state.get("history_incomplete", [])),
                "cold_start": state.get("cold_start", False)}

    @task
    def mark_silver_done(**ctx) -> dict:
        """dbt test 통과(dbt_silver 성공) 후 silver history run 을 DONE marker 로 기록.

        cutoff(=이 DAG run 시작 시각, KST)을 함께 넘겨 **dedup 으로 행이 0인 run** 도 처리완료로
        마킹한다(영구 미완 오판 방지). 빌드 중 도착한 run(≥cutoff)은 다음 증분이 처리."""
        from datetime import timedelta

        from silver import silver_markers

        dr = ctx.get("dag_run")
        start = getattr(dr, "start_date", None) if dr else None
        cutoff = ((start + timedelta(hours=9)).strftime("%Y-%m-%d_%H%M%S") if start else None)
        return silver_markers.mark_silver_runs_done(processed_cutoff_run_id=cutoff)

    @task
    def notify_masked_address_summary() -> dict:
        """마스킹 주소 동단위 매핑 스킵 건수를 warning 알림으로 집계."""
        from silver import quality_tasks

        return quality_tasks.notify_masked_address_dong_skip_summary()

    @task
    def build_detail_catalog() -> dict:
        """실측(bronze record_json) → 카탈로그 규칙 → meta_detail_catalog(Iceberg) 갱신.

        silver detail(원형 — API 별 상이 컬럼) 생성용 스펙의 파생 과정(별도 meta_ 단위,
        PROJECT.md §4.3). 레이어 재분류(#70)로 gold DAG 에서 silver 파이프라인으로 편승."""
        from commerce_core import notify, registry
        from gold import catalog_rules, loader, measure

        fields = measure.measure_fields()
        meta = {d.short: {"fmt": d.fmt} for d in registry.enabled_for_schedule("daily")}
        cat = catalog_rules.build_catalog(fields, meta)
        _prev, prev_ver = loader.read_catalog()
        if prev_ver and prev_ver != cat["version"]:
            import logging
            logging.getLogger(__name__).warning("detail 카탈로그 드리프트: %s → %s", prev_ver, cat["version"])
        loader.upsert_catalog(cat["details"], cat["version"])

        # 이름이 없어 단건으로 떨어진 클러스터 — **적재는 정상이고 이름만 미정**이다.
        # 조용히 넘기면 영영 안 고쳐지므로, 규약 §19.1 형식의 품질 이벤트로 올려
        # 알림·운영 기록 양쪽에 남긴다(파이프라인은 계속 진행).
        pending = cat.get("pending_cluster_names") or []
        if pending:
            members = sorted({m for row in pending for m in row["members"]})
            notify.notify_quality_event(
                task="build_detail_catalog",
                level="warning",
                title="detail cluster 이름 미정 — 단건 테이블로 적재 중",
                description=(
                    "같은 모양으로 묶인 데이터셋에 도메인 이름이 없어, 클러스터 대신 개별 "
                    "detail 테이블로 적재했습니다. **데이터 손실은 없습니다.** "
                    "gold/catalog_rules.py 의 NAME_BY_MEMBER 에 이름을 추가하면 다음 빌드에서 "
                    "하나로 합쳐집니다."),
                metrics={
                    "affected_rows": len(members),
                    "settled_rows": len(cat["details"]),
                    "affected_ratio_pct": round(100.0 * len(members) / max(len(cat["details"]), 1), 1),
                    "unnamed_clusters": len(pending),
                },
                context={"members": members,
                         "clusters": [{"members": r["members"], "shared_n": r["shared_n"]}
                                      for r in pending]},
            )

        return {"version": cat["version"], "specs": len(cat["details"]),
                "pending_cluster_names": pending}

    @task
    def load_details() -> dict:
        """silver_<domain>_detail 증분 적재 — 카탈로그 구동, 멤버별 INSERT INTO SELECT(#70).

        마킹(mark_silver_done) **뒤**에 두어 detail 실패가 run 마킹을 막지 않게 한다 —
        detail 은 history 파생이라 자체 워터마크로 다음 실행이 이어간다(재개 표준 §3)."""
        from gold import loader

        details, version = loader.read_catalog()
        if not details:
            return {"loaded": {}}
        loaded = loader.run_load_details(details)
        return {"loaded": loaded, "objects": len(loaded), "rows": sum(loaded.values()),
                "catalog_version": version}

    @task
    def maintain_silver_gold_tables() -> list[dict]:
        """**silver 원형·detail + gold 집계** Iceberg 테이블 유지보수(#226 확장, 2026-07-15 재승인).

        대상(= 이 태스크가 실제로 손대는 레이어):
          - silver 원형: silver_license_entity, silver_license_entity_history
          - silver detail: silver_<domain>_detail 78종(카탈로그 구동)
          - gold 집계: AGG_TABLES 22종
          - meta: meta_detail_catalog(보조)
        **bronze 는 대상 아님** — bronze 유지보수는 commerce_load_bronze 의 iceberg_maintenance 가
        따로 담당한다(구 이름 `maintain_gold_tables` 는 실제 대상과 어긋나 오독을 유발 —
        2026-07-20 from-zero 드릴에서 지적, `maintain_silver_gold_tables` 로 개명).

        동작: **메타 통합(expire: 옛 버전 포인터 정리) + 본 데이터 병합(optimize: 소파일 컴팩션)
        + orphan 청소**. 데이터 행은 0 삭제(실증: history 가 동일 정책으로 매일 관리되며
        6/30 부터 전량 보존). OOM 근거: 최중량 silver_license_history(289만×record_json)의
        일일 optimize 가 이 박스에서 무사고 — 대상 테이블은 그보다 좁음."""
        from bronze import maintenance
        from gold import loader

        details, _ = loader.read_catalog()
        from gold.report import AGG_TABLES   # gold 집계 명단 정본(1곳 관리)

        tables = tuple(["silver_license_entity", "silver_license_entity_history",
                        "meta_detail_catalog"] + list(AGG_TABLES) + [d["object"] for d in details])
        return maintenance.run_table_maintenance(tables)

    @task(trigger_rule="all_done")
    def report_silver(**ctx) -> dict:
        """DAG 완료 리포트(#218, PROJECT.md §2) — **이번 실행이 silver 로 적재한 신규분만**
        API 별 표기(누적 현황 아님): 이 run 중 DONE 마킹된 run 의 history 적재행 집계.
        run 시작시각(start_date)이 '이번 실행' 마킹의 경계다. 실패해도 보고."""
        from datetime import datetime, timezone

        from silver import quality_tasks

        dr = ctx.get("dag_run")
        start = getattr(dr, "start_date", None) if dr else None
        elapsed = ((datetime.now(timezone.utc) - start).total_seconds() if start else None)
        result = quality_tasks.report_silver_run(elapsed_seconds=elapsed, run_started_at=start)

        # 리포트를 보낸 뒤 **상류 실패를 그대로 드러낸다**.
        # 이 태스크는 all_done 이라 앞이 실패해도 돌고, 이 DAG 의 **유일한 말단**이다.
        # 그래서 이게 성공하면 Airflow 가 DagRun 을 success 로 마킹해 **실패가 통째로 가려진다**
        # (실측: build_detail_catalog 실패 → load_details 미실행 → detail 0건인데 DAG 는 초록).
        # 리포트는 실패해도 나가야 하므로 all_done 은 유지하고, 보고 후 예외로 상태를 바로잡는다.
        failed = sorted(
            ti.task_id for ti in (dr.get_task_instances() if dr else [])
            if ti.task_id != "report_silver" and ti.state in ("failed", "upstream_failed")
        )
        if failed:
            raise AirflowException(
                "silver 실행 중 실패한 태스크가 있습니다(리포트는 발송됨) — "
                f"{', '.join(failed)}")
        return result

    # Cosmos: silver 모델 run+test(모델당 태스크). 증분은 여기서, 전량 빌드는 seed 가 선처리.
    dbt_silver = DbtTaskGroup(
        group_id="dbt_silver",
        project_config=_project_config,
        profile_config=_profile_config,
        execution_config=_execution_config,
        render_config=_render_config,
        operator_args={"install_deps": False, **({"vars": _DBT_VARS} if _DBT_VARS else {})},
    )

    seed = seed_silver_if_empty()
    [dbt_seed_taxonomy(), enrich_admin_dong_ref(), enrich_fill_jibun(),
     ensure_silver_marker()] >> seed
    # 원형 파이프라인 편승(#70): dbt(원형 4모델) → 마킹 → detail(카탈로그 구동) → 유지보수 → 리포트
    (seed >> dbt_silver >> notify_masked_address_summary() >> mark_silver_done()
     >> build_detail_catalog() >> load_details() >> maintain_silver_gold_tables() >> report_silver())


commerce_load_silver()
