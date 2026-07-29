"""commerce_ops_logship — commerce 태스크 처리로그를 #60 존(`ops/logs/commerce/`)에 일단위 적재.

각 과정(수집/적재/변환/서빙 DAG)의 태스크 로그를 도커 볼륨(airflow_logs)에 남기지 않고,
run 단위 tar.gz 로 R2 ops 존에 옮긴 뒤 로컬에서 제거한다(업로드 검증 후 삭제 — 무손실).

  {COMMERCE_LOGS_LAYER}/load_date=<run 시작일 KST>/<dag_id>/<run_id>.tar.gz

- 대상: `dag_id=commerce_*` 의 **종결(success|failed) run** 만 — 실행 중/미기록 run 로그는 보존.
- 일단위: run 시작일(KST) 파티션(#60 약속① key=value) — 다른 존 산출물과 동일 패턴.
- 타 도메인 로그는 절대 건드리지 않는다(경로 필터 = commerce_ 접두).
- 매일 1회(01:30 KST). 멱등: 이미 적재된 키는 크기 검증 후 로컬만 정리.
순수 로직: include/commerce_core/logship.py (단위테스트 tests/test_logship.py).
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
from commerce_core.storage import get_storage  # noqa: E402

log = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))
_BASE = Path(os.getenv("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs"))
_DEFAULT_ARGS = {"owner": "data-eng", "retries": 1,
                 "retry_delay": pendulum.duration(minutes=10)}


@dag(dag_id="commerce_ops_logship", schedule="30 1 * * *",
     start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), catchup=False,
     max_active_runs=1, default_args=_DEFAULT_ARGS,
     tags=["seoul", "commerce", "ops", "logs"], doc_md=__doc__)
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
        shipped = reaped = failed = 0
        bytes_up = 0
        for pair in ship:
            dag_id, run_id = pair
            run_dir = dir_of[pair]
            start = started.get(pair)
            run_date = (start.astimezone(KST).strftime("%Y-%m-%d")
                        if start is not None else run_id[:10])
            key = logship.log_object_key(dag_id=dag_id, run_id=run_id, run_date=run_date)
            try:
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

        receipt = log_event("ops.logship", level="info", where="commerce_ops_logship",
                            task="ship_logs", shipped=shipped, deleted_local=reaped,
                            kept_active=len(keep), failed=failed, bytes=bytes_up,
                            layer=logship.LOGS_LAYER)
        return receipt

    ship_logs()


commerce_ops_logship()
