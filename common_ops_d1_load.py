"""common_ops_d1_load — ops 존에 쌓인 **전 도메인** 운영 기록을 조회 DB(D1)로 옮긴다.

    R2  ops/{runs,metrics,errors,reports,recovery,product-events,product-health}/…
      → D1 _ops_run_event · _ops_daily_metric · _ops_pipeline_state · _ops_pipeline_expectation

**어느 인스턴스에서 돌아도 된다.** 이 DAG 는 R2 만 읽으므로 인스턴스 로컬 상태에 의존하지 않고,
**여러 곳에서 동시에 돌아도 결과가 같다** — 중복 판정이 2단이고 적재가 자연키 upsert 라서다
(실측: 같은 범위 재실행 시 파일 127건 중 48건은 열지도 않고 건너뛰고 적재 0건).
그래서 인스턴스를 지정하는 스위치를 두지 않았다. 겹쳐 돌면 낭비되는 것은 목록 조회 몇 번뿐이다.

**3시간마다 도는 이유** — 하루 1회면 기록이 조회 DB 에 보이기까지 최대 하루가 걸리고, 그 사이
파이프라인이 죽어도 화면은 조용하다(`C-9`). 이미 넣은 것은 **파일을 열지도 않고** 건너뛰므로
자주 돌아도 비용이 거의 없다 — 새로 생긴 것이 없으면 목록 조회와 질의 한 번으로 끝난다.

한 실행이 다루는 구간은 최근 `OPS_D1_LOOKBACK_DAYS`(기본 3일)다. 그보다 오래된 구간을 넣어야
하면 정기 실행의 창을 키우지 말고 **날짜 단위 백필 스크립트**를 쓴다(적재기는 창을 전부 읽은 뒤
한 번에 쓰기 때문에 창이 크면 진행이 안 보이고 중단 시 통째로 버려진다).
상세: `dags/domains/commerce/docs/operations/ops-records.md`.
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pendulum  # noqa: E402
from airflow.decorators import dag, task  # noqa: E402

from common.ops import Layer  # noqa: E402
from common.ops.observability import ops_default_args  # noqa: E402

log = logging.getLogger(__name__)

#: 훑을 구간(일). 3시간마다 도는 전제라 짧아도 되지만, 하루 걸러 실패해도 따라잡히도록 여유를 둔다.
_LOOKBACK_DAYS = int(os.getenv("OPS_D1_LOOKBACK_DAYS", "3"))
#: 적재 대상 도메인. 비우면 **전 도메인**(조회 DB 는 팀 공용이고 규약 §8 이 그렇게 설계됐다).
_DOMAINS = [d.strip() for d in os.getenv("OPS_D1_DOMAINS", "").split(",") if d.strip()]

_DEFAULT_ARGS = {"owner": "data-eng", "retries": 1,
                 "retry_delay": pendulum.duration(minutes=15),
                 **ops_default_args("common", Layer.D1)}


@dag(dag_id="common_ops_d1_load", schedule="15 */3 * * *",
     start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), catchup=False,
     max_active_runs=1, default_args=_DEFAULT_ARGS,
     tags=["ops", "d1", "common"], doc_md=__doc__)
def common_ops_d1_load():
    @task
    def load_ops_records(**ctx) -> dict:
        """ops 존 → 조회 DB. 저장소는 읽기만 하고 파일을 옮기지 않는다(C-6)."""
        from common.ops import d1_ops, expectations, resolve_environment
        from common.ops import ingest as ops_ingest
        from common.serving.runtime import build_d1_client_from_env
        from common.storage import resolve_storage

        # 배포 환경이 정한 저장소를 그대로 쓴다 — 로컬·운영을 코드가 아니라 값이 가른다.
        storage = resolve_storage()
        client = build_d1_client_from_env()
        logical = ctx.get("logical_date") or pendulum.now("UTC")
        until = logical.in_timezone("Asia/Seoul").format("YYYY-MM-DD")
        since = (logical.in_timezone("Asia/Seoul")
                 .subtract(days=_LOOKBACK_DAYS).format("YYYY-MM-DD"))

        receipt = ops_ingest.ingest(
            list_keys=storage.list_keys, read_json=storage.read_json,
            d1_execute=client.execute, environment=resolve_environment().value,
            domains=_DOMAINS or None, since=since, until=until)

        # 기대 주기 사본(S-1) — 등록한 도메인만. 자연키가 dag_id 라 도메인별로 안전하다(D-4).
        registered = expectations.load_all()
        rows = expectations.rows(updated_at=pendulum.now("UTC").to_iso8601_string())
        for statement in d1_ops.pipeline_expectation_upsert_statements(rows):
            client.execute(statement)   # security: allow-sql — 공용 빌더(식별자 상수, 값 이스케이프)

        result = {"since": since, "until": until, "expectation_domains": list(registered),
                  "expectations": len(rows), **receipt.as_dict()}
        # layer 가 없는 기록은 단계별 집계에서 빠진다. 조용히 넘기면 그 도메인이 화면에서
        # "이상 없음"으로 보이므로, 도메인별 건수를 경고로 남겨 쓰는 쪽이 고치게 한다.
        if receipt.layer_missing:
            log.warning("[ops.d1] 단계 정보 없는 기록(공용 관문으로 기록하면 사라진다): %s",
                        receipt.layer_missing)
        log.info("[ops.d1] %s", result)
        return result

    load_ops_records()


common_ops_d1_load()
