"""common_ops_logship — 이 인스턴스의 **모든 도메인** 태스크 로그를 ops 존으로 옮긴다.

Airflow 가 컨테이너 볼륨에 남기는 원문 로그를 run 단위 tar.gz 로 묶어 올리고 로컬에서 지운다.

    ops/logs/<domain>/observed_date=<run 시작일 KST>/<dag_id>/<run_id>.tar.gz

**왜 인스턴스마다 돌아야 하나** — 이 태스크가 다루는 것은 **그 인스턴스의 로컬 디스크 파일**이다.
다른 인스턴스의 로그는 읽을 수 없다. 여러 Airflow 가 같은 저장소를 공유하는 구성이므로, 이 DAG 는
**각 인스턴스에서 각각** 돌아야 그 인스턴스의 로그가 치워진다. (조회 DB 적재는 성격이 정반대라
`common_ops_d1_load` 로 분리했다 — 그쪽은 R2 만 읽으므로 어디서 돌든 같다.)

도메인은 **`dag_id` 의 첫 세그먼트**로 정한다(`commerce_load_bronze` → `commerce`). 저장소 규약이
도메인을 경로에 요구하는데(P-6·P-9), Airflow 가 알려주는 것은 dag_id 뿐이라 그 관행을 쓴다.
접두가 도메인명과 다른 것은 `DOMAIN_OVERRIDES` 에 명시한다.

대상은 **종결(success·failed) run 만** — 실행 중이거나 메타DB 에 없는 run 의 로그는 지우지 않는다.
업로드가 검증된 뒤에만 로컬을 지운다(무손실). 전환 전 경로(`load_date=`)에 이미 올라간 번들도
확인해 중복 업로드를 막는다(G-4).
"""
from __future__ import annotations

import io
import logging
import os
import shutil
import sys
import tarfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pendulum  # noqa: E402
from airflow.decorators import dag, task  # noqa: E402

from common.ops import Layer, OpsCategory, ops_key  # noqa: E402
from common.ops.contract import legacy_observation_key, safe_segment  # noqa: E402
from common.ops.observability import ops_default_args  # noqa: E402

log = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))
_BASE = Path(os.getenv("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs"))

#: dag_id 접두가 도메인명과 다른 것만 적는다. 없으면 첫 세그먼트를 그대로 쓴다.
DOMAIN_OVERRIDES: dict[str, str] = {
    "ask": "ask-seoul",   # ask_seoul_iceberg_maintenance — 공유 인프라 유지보수
    "common": "common",   # common_* — 도메인 공용
}

#: 이 인스턴스에서 옮길 도메인. 비우면 **전부**(권장 — 인스턴스에 있는 것이 곧 그 인스턴스의 몫).
_ONLY_DOMAINS = [d.strip() for d in os.getenv("OPS_LOGSHIP_DOMAINS", "").split(",") if d.strip()]

_DEFAULT_ARGS = {"owner": "data-eng", "retries": 1,
                 "retry_delay": pendulum.duration(minutes=10),
                 **ops_default_args("common", Layer.D1)}


def domain_of(dag_id: str) -> str:
    """dag_id → 저장 경로에 쓸 도메인. 첫 세그먼트가 기본, 예외만 매핑."""
    head = dag_id.split("_", 1)[0]
    return DOMAIN_OVERRIDES.get(head, head)


@dag(dag_id="common_ops_logship", schedule="30 2 * * *",
     start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), catchup=False,
     max_active_runs=1, default_args=_DEFAULT_ARGS,
     tags=["ops", "logs", "common"], doc_md=__doc__)
def common_ops_logship():
    @task
    def ship_logs() -> dict:
        """종결 run 로그 번들 → ops 존 업로드(검증) → 로컬 제거. 전 도메인."""
        from common.ops.airflow_meta import terminal_run_states
        from common.storage import resolve_storage

        # Airflow 3 은 태스크 프로세스의 ORM 접근을 막는다(BlockedDBSession) — REST API v2 로 읽는다.
        # 조회가 실패하면 여기서 죽는 게 맞다: 빈 목록으로 진행하면 종결 판정이 전부 거짓이 되어
        # **실행 중인 run 의 로그까지 지울** 수 있다(모른다 ≠ 없다).
        runs = terminal_run_states()
        terminal: dict[tuple[str, str], str] = {k: str(v["state"]) for k, v in runs.items()}
        started: dict[tuple[str, str], object] = {}
        for key, meta in runs.items():
            raw = meta.get("start_date")
            try:
                started[key] = datetime.fromisoformat(str(raw)) if raw else None
            except ValueError:
                started[key] = None

        # 배포 환경이 정한 저장소를 그대로 쓴다 — 로컬·운영을 코드가 아니라 값이 가른다.
        storage = resolve_storage()

        shipped = reaped = failed = already = kept = 0
        bytes_up = 0
        by_domain: dict[str, int] = {}
        for dag_dir in sorted(_BASE.glob("dag_id=*")):
            dag_id = dag_dir.name.split("=", 1)[1]
            domain = domain_of(dag_id)
            if _ONLY_DOMAINS and domain not in _ONLY_DOMAINS:
                continue
            for run_dir in sorted(dag_dir.glob("run_id=*")):
                run_id = run_dir.name.split("=", 1)[1]
                if terminal.get((dag_id, run_id)) not in ("success", "failed"):
                    kept += 1          # 실행 중·미기록 run 의 로그는 절대 지우지 않는다
                    continue
                start = started.get((dag_id, run_id))
                observed = (start.astimezone(KST).strftime("%Y-%m-%d")
                            if start is not None else run_id[:10])
                try:
                    key = ops_key(OpsCategory.LOGS, domain=domain, observed_date_kst=observed,
                                  subpath=(dag_id,),
                                  filename=f"{safe_segment(run_id)}.tar.gz")
                    # 전환 전 경로(load_date=)에 이미 올라간 것도 본다(G-4) — 못 보면 두 번 올린다.
                    legacy = legacy_observation_key(
                        OpsCategory.LOGS, domain=domain, date=observed,
                        subpath=(dag_id,), filename=f"{safe_segment(run_id)}.tar.gz")
                    if storage.exists(key) or storage.exists(legacy):
                        already += 1
                        shutil.rmtree(run_dir)
                        reaped += 1
                        continue
                    buf = io.BytesIO()
                    with tarfile.open(fileobj=buf, mode="w:gz") as archive:
                        archive.add(run_dir, arcname=f"{dag_id}/{run_dir.name}")
                    payload = buf.getvalue()
                    storage.write_bytes(key, payload)
                    if not storage.exists(key):        # 업로드 검증 후에만 삭제
                        raise RuntimeError(f"업로드 검증 실패: {key}")
                    bytes_up += len(payload)
                    shipped += 1
                    by_domain[domain] = by_domain.get(domain, 0) + 1
                    shutil.rmtree(run_dir)             # 볼륨 무잔존
                    reaped += 1
                except Exception as exc:               # 개별 실패는 보존(다음 일자 재시도)
                    failed += 1
                    log.warning("logship 실패(로컬 보존): %s/%s — %s", dag_id, run_id, exc)
            try:
                if not any(dag_dir.iterdir()):
                    dag_dir.rmdir()
            except OSError:
                pass

        result = {"shipped": shipped, "deleted_local": reaped, "already_shipped": already,
                  "kept_active": kept, "failed": failed, "bytes": bytes_up,
                  "by_domain": by_domain}
        log.info("[ops.logship] %s", result)
        return result

    ship_logs()


common_ops_logship()
