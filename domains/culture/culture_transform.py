"""Airflow DAG: culture silver/gold 변환 (dbt) — Asset 트리거.

``culture_bronze``의 load_bronze가 bronze Iceberg를 갱신하며 Asset을 발행하면
이 DAG이 자동 기동한다(cron 우연 결합 없음 — bronze 성공 시에만 변환).

체인: dbt_deps → dbt_source_freshness → dbt_seed → dbt_run → dbt_test → notify_transform_success
  * deps — packages.yml 의존성 설치(#564). 선언 수와 dbt_packages 설치 수가 어긋나면
    dbt는 파스 단계에서 죽어 뒤의 세 태스크가 시작조차 못 한다.
  * source freshness — sources.yml의 계약(경고 30h/에러 48h)을 실측. error만 실패.
  * seed — sema_branch_location(분관 좌표)·sejong_location + asac_axes 패키지 seed(행정동 크로스워크·경계).
  * run/test — silver 12종 + gold 13종 빌드 후 계약 테스트(tag:slo 4종은 `culture_slo` 몫).
  * notify — silver·gold 를 어떻게 만들었는지 Discord 요약(성공 경로).

알림은 두 쪽이 다 있어야 조용함을 읽을 수 있다: 실패는 `common.errors.airflow` 의 에러
embed(#161, 실패 노드·사유 포함), 성공은 이 DAG 의 마지막 태스크. 둘 다 best-effort 라
알림이 못 나가도 변환 결과는 그대로다.

dbt 프로젝트는 compose가 마운트한 ``/opt/airflow/dbt/domains/culture``(ASAC-DBT),
실행 바이너리는 이미지 전용 venv. target(dev/prod)은 카탈로그를 가른다(기본 = 런타임 env).

파라미터 (트리거 시 덮어쓰기 가능):
  target   "dev" | "prod"   (기본 = 런타임 env)
"""

from __future__ import annotations

import os
import shlex
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset, Param

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.discord.notify import send_payload  # noqa: E402
from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.runtime_guard import TARGET_CHOICES, default_target  # noqa: E402
from common.ops import Layer  # noqa: E402
from common.ops.observability import ops_default_args  # noqa: E402

from culture_ingest.common.config import CULTURE_BRONZE_ASSET  # noqa: E402
from culture_ingest.common import transform_notify  # noqa: E402

KST = "Asia/Seoul"

DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/culture"

# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
# dbt 변환 DAG 라 외부 소스가 없어 source_system 은 생략.
#
# `dbt_project_dir` 을 주면 실패 알림에 **어떤 모델·테스트가 왜** 깨졌는지가 붙는다(#161).
# 이게 없던 동안 7/21·7/29 의 `dbt_test` 실패 알림은 "dbt_test 가 실패했다"까지만 말해
# Discord 만 보고는 원인을 알 수 없었다. weather 가 쓰는 XCom 키 형은 실행기가
# attempt-local artifact 경로를 XCom 으로 넘겨줄 때 쓰는 것이고, culture 는 dbt CLI 를
# BashOperator 로 직접 돌려 결과가 `target/run_results.json` 에 그대로 남으므로
# citydata 와 같은 project_dir 형이 맞다.
record_culture_problem = problem_failure_callback(
    domain="culture", dbt_project_dir=DBT_PROJECT)

# target 기본값은 배포 env 를 따른다(ASK-Seoul#66) — 하드코딩 "dev" 는 prod 스택에서
# iceberg_dev 카탈로그를 찾다가 매 새벽 런을 전멸시킨다(traffic·weather #561 과 같은 패턴).
DEFAULT_PARAMS = {
    "target": Param(
        default=default_target(),
        type="string",
        enum=list(TARGET_CHOICES),
        description="dbt target profile. 기본값은 런타임 env(ASK_SEOUL_TARGET/DBT_TARGET).",
    )
}


def _dbt(args: str, *, snapshot_for: str | None = None) -> str:
    """dbt 하위명령을 culture 프로젝트/프로파일로 실행하는 bash 스니펫.

    ``snapshot_for`` 를 주면 **성공 직후** 그 태스크의 ``run_results.json`` 을 따로 복사한다.
    dbt 는 하위명령마다 이 파일을 덮어써서, test 가 끝나면 run 결과가 이미 사라진다 —
    성공 요약은 둘 다 필요하므로 각자 자기 것을 남겨 둔다. 실패하면 `set -e` 로 여기까지
    오지 못하고, 실패 콜백이 덮어쓰기 전 원본을 읽으므로 두 경로가 서로를 방해하지 않는다.
    """
    project = shlex.quote(DBT_PROJECT)
    command = (
        "set -euo pipefail\n"
        f"cd {project}\n"
        f"export DBT_PROFILES_DIR={project} DBT_PROJECT_DIR={project}\n"
        f"{shlex.quote(DBT_BIN)} {args} --target {{{{ params.target }}}} --no-use-colors"
    )
    if snapshot_for:
        snapshot = transform_notify.snapshot_path(DBT_PROJECT, snapshot_for)
        command += f"\ncp -f target/run_results.json {shlex.quote(snapshot)}"
    return command


def _notify_transform_success(**context: object) -> None:
    """silver·gold 를 어떻게 만들었는지 Discord 로 알린다(성공 경로).

    알림은 파이프라인을 죽이지 않는다 — 수집(`culture_bronze`)과 같은 규칙이다.
    스냅샷이 없으면(예: 이전 버전 코드로 만든 run) 조용히 건너뛴다.
    """
    run_results = transform_notify.read_run_results(
        transform_notify.snapshot_path(DBT_PROJECT, "dbt_run"))
    test_results = transform_notify.read_run_results(
        transform_notify.snapshot_path(DBT_PROJECT, "dbt_test"))
    if run_results is None and test_results is None:
        return

    dag_run = context.get("dag_run")
    params = context.get("params") or {}
    payload = transform_notify.build_transform_payload(
        transform_notify.summarize_models(run_results),
        transform_notify.summarize_tests(test_results),
        target=str(params.get("target")) if isinstance(params, dict) else None,
        dag_run_id=getattr(dag_run, "run_id", None),
    )
    try:
        send_payload(payload, domain=transform_notify.DOMAIN)
    except Exception as exc:  # noqa: BLE001 — 알림 실패가 변환 성공을 뒤집지 않는다
        print(f"[culture transform] discord 알림 실패(무시): {type(exc).__name__}")


with DAG(
    dag_id="culture_transform",
    description="Transform culture bronze -> silver/gold via dbt, triggered by bronze Asset.",
    start_date=pendulum.datetime(2026, 6, 1, tz=KST),
    schedule=[Asset(CULTURE_BRONZE_ASSET)],
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1, "retry_delay": timedelta(minutes=5),
        # ASK-Seoul#78 — 이 DAG 의 모든 태스크가 실행 기록을 남긴다. 실패 상세
        # (RFC 9457 Problem JSON)를 밀어내지 않고 뒤에 붙는다(교체가 아니라 추가).
        # layer 는 DAG 하나에 값 하나인데 이 DAG 은 silver·gold 를 `dbt run` 한 태스크로
        # 만든다. 태스크 단위(V-6 runs)로는 둘을 가를 수 없으므로 산출물 기준 gold 로
        # 등록하고, 모델별 단계 구분은 dbt 노드 단위(metrics 계열)의 몫으로 둔다.
        **ops_default_args("culture", Layer.GOLD, on_failure=record_culture_problem),
    },
    params=DEFAULT_PARAMS,
    tags=["transform", "culture", "silver", "gold", "dbt"],
) as dag:
    # packages.yml 의존성 설치(#564) — 체인 맨 앞이어야 한다. dbt 는 선언 수와 dbt_packages
    # 설치 수가 다르면 **파스 단계**에서 죽어(`dbt found N package(s) specified ... but only M
    # installed`) freshness 부터 test 까지 전부 시작조차 못 한다. 두 경로로 어긋난다:
    # ① packages.yml 에 패키지를 추가하는 PR(ASAC-DBT#347 의 dbt_utils) ② dbt_packages 유실
    # (gitignore 대상 — git clean·컨테이너 재생성). citydata·traffic 은 이미 매 런 돌린다.
    deps = BashOperator(task_id="dbt_deps", bash_command=_dbt("deps"))

    # bronze 신선도 게이트 — 48h 넘게 낡았으면 여기서 멈추고 수집부터 고치게 한다.
    freshness = BashOperator(task_id="dbt_source_freshness",
                             bash_command=_dbt("source freshness"))

    seed = BashOperator(task_id="dbt_seed", bash_command=_dbt("seed"))

    # asac_axes 패키지 자체 모델(dim_admin_dong)은 타 레포 로더(#154) 의존이라 이 환경에서 ERROR —
    # culture 변환은 자기 모델만 빌드/테스트한다 (패키지 seed·매크로·제네릭 테스트는 계속 사용).
    run_models = BashOperator(
        task_id="dbt_run", bash_command=_dbt(
            "run --exclude package:asac_axes tag:slo", snapshot_for="dbt_run"))

    test_models = BashOperator(
        task_id="dbt_test", bash_command=_dbt(
            "test --exclude package:asac_axes tag:slo", snapshot_for="dbt_test"))

    # 성공 쪽 말(#161 은 실패만 말한다) — 새벽 런이 조용할 때 "잘 돌았다"와 "안 돌았다"를
    # 가를 수 있게 한다. 알림 자체는 실패해도 이 태스크는 성공으로 둔다(변환 결과는 이미 났다).
    notify_success = PythonOperator(
        task_id="notify_transform_success", python_callable=_notify_transform_success)

    deps >> freshness >> seed >> run_models >> test_models >> notify_success
