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
from airflow.decorators import dag, task  # noqa: E402
from cosmos import (  # noqa: E402
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import ExecutionMode, InvocationMode, LoadMode, TestBehavior  # noqa: E402

# dbt 실행 계약(호스트 이미지 env 우선, 없으면 기본값) — common_dbt_smoke 와 동일 형태.
DBT_PROJECT_DIR = os.getenv("COMMERCE_DBT_PROJECT_DIR", "/opt/airflow/dbt/domains/commerce")
DBT_BIN = os.getenv("DBT_BIN", "/home/airflow/dbt-venv/bin/dbt")
# 기본 dev(iceberg_dev/seoul-dev). prod 전환은 .env.commerce 또는 compose env 로.
DBT_TARGET = os.getenv("COMMERCE_DBT_TARGET") or os.getenv("DBT_TARGET", "dev")
# 모델 선택(리스트 = Cosmos RenderConfig.select / 문자열 join = chunked seed·비교용).
SILVER_SELECT = ["silver_license_history", "silver_license_current"]
SILVER_SELECT_STR = " ".join(SILVER_SELECT)
# 스코프 dbt vars(운영 노브) — Cosmos 는 정적 렌더라 런타임 청크 루프가 불가하므로(위 docstring),
# 저메모리 환경에서 dbt_silver 를 dataset 스코프로 돌릴 때 .env.commerce 에 JSON 으로 지정한다.
# 예: COMMERCE_DBT_VARS='{"include_datasets": ["golf_course"]}' — 비우면(기본) 전체 증분.
# 마커/pre_hook 은 include_datasets 스코프를 그대로 존중한다(silver_markers 매크로).
_DBT_VARS: dict = json.loads(os.getenv("COMMERCE_DBT_VARS") or "{}")

_DEFAULT_ARGS = {"owner": "data-eng", "retries": 1, "retry_delay": pendulum.duration(minutes=5)}

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
        """Cold start(빈 silver) 전용 안전 시드 — Cosmos 가 못 하는 청크 전량 빌드만 여기서.

        silver_license_history 가 비어 있을 때만: dataset 청크로 전량 빌드 → dbt test →
        (통과 시) DONE 마킹(기존 cold-start 순서 build→test→mark 를 한 태스크에 캡슐화). **마킹까지
        끝내야** downstream Cosmos 증분(dbt_silver)이 pre_hook(delete_unmarked, include_datasets
        비어 전역 삭제)로 방금 빌드한 전량을 지우고 비청크 단일 run 으로 재처리(OOM)하는 걸 막는다.
        테이블이 있으면 no-op(평상시) → 증분은 Cosmos 담당. 상세: docs/cosmos.md · rebuild-and-ops.md §6.
        """
        from silver import chunked_run, silver_markers

        if chunked_run.silver_history_rows() > 0:
            return {"seed": "skip", "reason": "silver_license_history not empty"}
        built = chunked_run.run_silver_chunked(
            select=SILVER_SELECT_STR, project_dir=DBT_PROJECT_DIR, dbt_bin=DBT_BIN, target=DBT_TARGET)
        # 마킹 전 검증(실패 시 예외 → 마킹 안 됨 → 다음 실행이 이어감, 기존 invariant 동일).
        chunked_run.run_dbt_test(
            select=SILVER_SELECT_STR, project_dir=DBT_PROJECT_DIR, dbt_bin=DBT_BIN, target=DBT_TARGET)
        marked = silver_markers.mark_silver_runs_done()
        return {"seed": "built", "build": built, "marked": marked}

    @task
    def mark_silver_done() -> dict:
        """dbt test 통과(dbt_silver 성공) 후 silver history run 을 DONE marker 로 기록."""
        from silver import silver_markers

        return silver_markers.mark_silver_runs_done()

    @task
    def notify_masked_address_summary() -> dict:
        """마스킹 주소 동단위 매핑 스킵 건수를 warning 알림으로 집계."""
        from silver import quality_tasks

        return quality_tasks.notify_masked_address_dong_skip_summary()

    @task(trigger_rule="all_done")
    def report_silver(**ctx) -> dict:
        """DAG 완료 리포트(#218) — silver current 데이터셋(API)별 현재 행수 + 실행시간. 실패해도 보고."""
        from datetime import datetime, timezone

        from silver import quality_tasks

        dr = ctx.get("dag_run")
        elapsed = ((datetime.now(timezone.utc) - dr.start_date).total_seconds()
                   if dr and getattr(dr, "start_date", None) else None)
        return quality_tasks.report_silver_run(elapsed_seconds=elapsed)

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
    [enrich_admin_dong_ref(), enrich_fill_jibun(), ensure_silver_marker()] >> seed
    seed >> dbt_silver >> notify_masked_address_summary() >> mark_silver_done() >> report_silver()


commerce_load_silver()
