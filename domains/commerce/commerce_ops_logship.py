"""commerce_ops_logship — ops 존의 운영 기록을 **한 관문으로** 저장소에 모으고 조회 DB 로 넘긴다.

두 가지 일을 한 DAG 에서 한다. 둘 다 "저장소가 원본, 조회 DB 는 사본"이라는 같은 규약
(ASK-Seoul#78 §10)을 따른다.

1. ``ship_logs`` — commerce 태스크 텍스트 로그를 도커 볼륨에 남기지 않고 run 단위 tar.gz 로
   ops 존에 옮긴 뒤 로컬에서 제거한다(업로드 검증 후 삭제 — 무손실).

       ops/logs/<domain>/observed_date=<run 시작일 KST>/<dag_id>/<run_id>.tar.gz

   경로는 공용 관문이 만든다(P-4 날짜 칸 ``observed_date=`` · P-9 도메인은 인자). 이미
   ``load_date=`` 로 쌓인 번들은 그 자리에 두고 읽을 때만 양쪽을 본다(G-1·G-4).

2. ``load_ops_to_d1`` — 규약이 정한 위치(``ops/<category>/…``)에 쌓인 **전 도메인**의 운영
   기록 파일을 감지해 기록 형식(F 표) 한 벌로 접고 조회 DB 에 적재한다. 기록기가 4벌이라
   모양이 제각각인데, 폴더를 합치는 대신 조회 단계에서 합친다(§8) — 저장소는 하나도 안 건드린다.

   중복은 **파일을 옮기거나 표식을 남겨서가 아니라 ``event_id`` 로** 가른다(C-6): "어디까지
   적재했는지"는 경로가 아니라 DB 안에 그 기록이 있는지 없는지로 판단한다. 그래서 같은 구간을
   매일 다시 훑어도 행이 늘지 않는다.

대상: `dag_id=commerce_*` 의 **종결(success|failed) run** 만 — 실행 중/미기록 run 로그는 보존.
타 도메인 **로그**는 건드리지 않는다(경로 필터 = commerce_ 접두). 타 도메인 **기록**은 읽어서
조회 DB 에 넣기만 하고, 그 파일은 읽기 전용이다.

매일 1회(01:30 KST). 순수 로직: include/commerce_core/logship.py · dags/common/ops/(contract,
d1_ops, ingest).py — 단위테스트 tests/test_logship.py · tests/test_ops_contract.py.
"""
from __future__ import annotations

import io
import logging
import os
import shutil
import sys
import tarfile
from datetime import timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from commerce_core.env import load_commerce_env  # noqa: E402

load_commerce_env()

from security import install_security, log_event  # noqa: E402

install_security()

import pendulum  # noqa: E402
from airflow.decorators import dag, task  # noqa: E402

from commerce_core import logship  # noqa: E402
from commerce_core.observability import ops_default_args  # noqa: E402
from commerce_core.storage import get_storage  # noqa: E402
from common.ops import Layer, OpsCategory  # noqa: E402

log = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))
_BASE = Path(os.getenv("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs"))
_DEFAULT_ARGS = {"owner": "data-eng", "retries": 1,
                 "retry_delay": pendulum.duration(minutes=10),
                 **ops_default_args(Layer.D1)}

#: 훑을 구간(일). 매일 도는 전제라 짧아도 되지만, 하루 걸러 실패해도 따라잡히도록 여유를 둔다.
_LOOKBACK_DAYS = int(os.getenv("COMMERCE_OPS_INGEST_LOOKBACK_DAYS", "3"))
#: 적재 대상 도메인. 비우면 **전 도메인**(조회 DB 는 팀 공용이고 §8 이 그렇게 설계됐다).
#: 다른 오너와의 합의 전까지 좁히고 싶으면 여기에 도메인 이름을 쉼표로 나열한다.
_INGEST_DOMAINS = [d.strip() for d in os.getenv("COMMERCE_OPS_INGEST_DOMAINS", "").split(",") if d.strip()]


@dag(dag_id="commerce_ops_logship", schedule="30 1 * * *",
     start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), catchup=False,
     max_active_runs=1, default_args=_DEFAULT_ARGS,
     tags=["seoul", "commerce", "ops", "logs", "d1"], doc_md=__doc__)
def commerce_ops_logship():
    @task
    def ship_logs() -> dict:
        """종결 run 로그 번들 → ops 존 업로드(검증) → 로컬 제거."""
        from airflow.models import DagRun
        from airflow.utils.session import create_session

        terminal: dict[tuple[str, str], str] = {}
        started: dict[tuple[str, str], object] = {}
        with create_session() as s:
            q = s.query(DagRun).filter(DagRun.dag_id.like("commerce_%"),
                                       DagRun.state.in_(("success", "failed")))
            for dr in q.all():
                terminal[(dr.dag_id, dr.run_id)] = dr.state
                started[(dr.dag_id, dr.run_id)] = dr.start_date

        run_dirs: list[tuple[str, str]] = []
        dir_of: dict[tuple[str, str], Path] = {}
        for dag_dir in sorted(_BASE.glob("dag_id=commerce_*")):
            dag_id = dag_dir.name.split("=", 1)[1]
            for run_dir in sorted(dag_dir.glob("run_id=*")):
                pair = (dag_id, run_dir.name.split("=", 1)[1])
                run_dirs.append(pair)
                dir_of[pair] = run_dir

        ship, keep = logship.plan_shippable(run_dirs, terminal)
        storage = get_storage()
        shipped = reaped = failed = already = 0
        bytes_up = 0
        for pair in ship:
            dag_id, run_id = pair
            run_dir = dir_of[pair]
            start = started.get(pair)
            observed_date = (start.astimezone(KST).strftime("%Y-%m-%d")
                             if start is not None else run_id[:10])
            key, legacy_key = logship.shipped_candidates(
                dag_id=dag_id, run_id=run_id, observed_date=observed_date)
            try:
                # 구경로(load_date=)에 이미 올라간 번들을 못 보면 같은 로그를 두 번 올린다(G-4).
                if storage.exists(key) or storage.exists(legacy_key):
                    already += 1
                    shutil.rmtree(run_dir)
                    reaped += 1
                    continue
                buf = io.BytesIO()
                with tarfile.open(fileobj=buf, mode="w:gz") as tf:
                    tf.add(run_dir, arcname=f"{dag_id}/{run_dir.name}")
                data = buf.getvalue()
                storage.write_bytes(key, data)
                if not storage.exists(key):                 # 업로드 검증 후에만 삭제
                    raise RuntimeError(f"업로드 검증 실패: {key}")
                bytes_up += len(data)
                shipped += 1
                shutil.rmtree(run_dir)                      # 도커 볼륨 무잔존(사용자 규약)
                reaped += 1
            except Exception as exc:                        # 개별 실패는 보존(다음 일자 재시도)
                failed += 1
                log.warning("logship 실패(로컬 보존): %s/%s — %s", dag_id, run_id, exc)
        # 빈 dag_id 폴더 정리
        for dag_dir in _BASE.glob("dag_id=commerce_*"):
            try:
                if not any(dag_dir.iterdir()):
                    dag_dir.rmdir()
            except OSError:
                pass

        return log_event("ops.logship", level="info", where="commerce_ops_logship",
                         task="ship_logs", shipped=shipped, deleted_local=reaped,
                         kept_active=len(keep), already_shipped=already, failed=failed,
                         bytes=bytes_up, category=OpsCategory.LOGS.value)

    @task
    def load_ops_to_d1(**ctx) -> dict:
        """ops 존 운영 기록 → 조회 DB(D1). 저장소는 읽기만 하고 파일을 옮기지 않는다(C-6)."""
        from commerce_core import ops_expectations
        from common.ops import d1_ops, resolve_environment
        from common.ops import ingest as ops_ingest
        from common.serving.runtime import build_d1_client_from_env

        storage = get_storage()
        client = build_d1_client_from_env()
        logical = ctx.get("logical_date") or pendulum.now("UTC")
        until = logical.in_timezone("Asia/Seoul").format("YYYY-MM-DD")
        since = (logical.in_timezone("Asia/Seoul")
                 .subtract(days=_LOOKBACK_DAYS).format("YYYY-MM-DD"))

        receipt = ops_ingest.ingest(
            list_keys=storage.list_keys,
            read_json=storage.read_json,
            d1_execute=client.execute,
            environment=resolve_environment().value,
            domains=_INGEST_DOMAINS or None,
            since=since, until=until,
        )
        # 기대 주기 사본(S-1) — 자연키가 dag_id 라 commerce 행만 갱신된다(D-4). 이게 있어야
        # 알림이 "기록 없음"을 기대 주기 초과와 함께 판정할 수 있다(C-9).
        expectations = ops_expectations.expectation_rows(
            updated_at=pendulum.now("UTC").to_iso8601_string())
        for statement in d1_ops.pipeline_expectation_upsert_statements(expectations):
            client.execute(statement)   # security: allow-sql — 공용 빌더(식별자 상수, 값 이스케이프)

        result = {"since": since, "until": until,
                  "expectations_registered": len(expectations), **receipt.as_dict()}
        # layer 가 없는 기록은 단계별 집계에서 빠진다. 조용히 넘기면 그 도메인은 화면에서
        # "이상 없음"으로 보이므로, 도메인별 건수를 경고로 남겨 쓰는 쪽이 고치게 한다.
        if receipt.layer_missing:
            log_event("ops.ingest.layer_missing", level="warning",
                      where="commerce_ops_logship", task="load_ops_to_d1",
                      by_domain=receipt.layer_missing,
                      note="공용 관문(common.ops.contract)으로 기록하면 layer 가 필수라 사라진다")
        return log_event("ops.ingest", level="info", where="commerce_ops_logship",
                         task="load_ops_to_d1", **result)

    ship_logs() >> load_ops_to_d1()


commerce_ops_logship()
