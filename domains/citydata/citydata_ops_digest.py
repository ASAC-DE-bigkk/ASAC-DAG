"""Airflow DAG: citydata 일일 관측 digest — R2 ``runs/`` 집계 → Discord.

파일기반 관측(``common/ops/run_sink.record_run`` → R2 ``runs/``)으로 이전.
과거엔 ``ops.run_metadata``(Iceberg)를 읽었으나, citydata DAG 들이 record_run 으로 옮기며
그 테이블에 더 이상 쓰지 않아 digest 가 매일 빈 리포트를 냈다(소비자 미이전 버그). 이제
DAG 들이 실제로 기록하는 R2 ``runs/`` 를 직접 집계한다(Trino 무관, 데이터 품질 대시보드와 동일 소스).

경로: ``runs/observed_date=YYYY-MM-DD(KST)/domain=citydata/dag_id=<dag>/<run>__<task>__try<N>__<status>.json``
집계는 **파일명·경로만 파싱(list-only)** — 성공률·레이어별·실패상위. body GET 없음(값싸다).
⚠ ``expected_raw_objects`` 는 run 레코드에 없어 '수집 완전성 121장소' 는 제외(run 레코드 보강 시 부활 가능).

per-failure 알림(problem_failure_callback)과 별개 — 이건 "어제 하루 요약".
대상 날짜: 기본 어제(KST). params.target_date 로 임의 날짜 재생성 가능(백필/테스트).
"""

from __future__ import annotations

import os
import sys
from collections import defaultdict
from datetime import datetime  # noqa: F401  (params 파싱 호환)

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.ops.run_sink import runs_prefix  # noqa: E402  (target-aware runs/ prefix, #556)
from common.storage import r2_env_for  # noqa: E402  (target-aware R2 자격증명, #556)

KST = pendulum.timezone("Asia/Seoul")
# dag_id → 레이어 표기 (경로에서 도출). 미매핑은 dag_id 그대로.
_LAYER = {
    "citydata_bronze": "bronze",
    "citydata_transform_cosmos": "transform",
    "citydata_serving_export": "serving",
    "citydata_quality_export": "quality",
}

record_citydata_problem = problem_failure_callback(domain="citydata", source_system="seoul_citydata")


def _r2_client(target: str = "dev"):
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=r2_env_for("R2_ENDPOINT", target),
        aws_access_key_id=r2_env_for("R2_ACCESS_KEY_ID", target),
        aws_secret_access_key=r2_env_for("R2_SECRET_ACCESS_KEY", target),
        region_name="auto",
    )


def _list_keys(cli, prefix: str, target: str = "dev") -> list[str]:
    keys, token = [], None
    bucket = r2_env_for("R2_BUCKET_NAME", target)
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        resp = cli.list_objects_v2(**kw)
        keys.extend(o["Key"] for o in resp.get("Contents", []))
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys


def summary_from_runs(domain: str, dt: str, target: str = "dev") -> dict:
    """R2 ``runs/`` 하루치 집계 — 성공률·레이어별·실패상위 (파일명/경로만 파싱)."""
    cli = _r2_client(target)
    prefix = f"{runs_prefix(target)}/observed_date={dt}/domain={domain}/"
    keys = _list_keys(cli, prefix, target)

    total = failed = 0
    runs: set[str] = set()
    fail_by_task: dict[str, int] = defaultdict(int)
    layer_total: dict[str, int] = defaultdict(int)
    layer_fail: dict[str, int] = defaultdict(int)

    for key in keys:
        if not key.endswith(".json"):
            continue
        dag_id = key.split("dag_id=")[-1].split("/")[0] if "dag_id=" in key else "unknown"
        fname = key.rsplit("/", 1)[-1][:-5]  # ".json" 제거
        # 형식: <run_id>__<task_id>__try<N>__<status>. run_id 에 '__' 가 있어 오른쪽에서 3번만 분리.
        segs = fname.rsplit("__", 3)
        if len(segs) != 4:
            continue
        run_id, task_id, _tryn, status = segs
        layer = _LAYER.get(dag_id, dag_id)
        total += 1
        runs.add(run_id)
        layer_total[layer] += 1
        if status == "failed":
            failed += 1
            fail_by_task[task_id] += 1
            layer_fail[layer] += 1

    success_pct = round(100 * (total - failed) / total, 2) if total else 0.0
    top_failures = sorted(fail_by_task.items(), key=lambda x: -x[1])[:3]
    layers = [
        {
            "layer": L,
            "total": layer_total[L],
            "failed": layer_fail[L],
            "pct": round(100 * (layer_total[L] - layer_fail[L]) / layer_total[L], 1) if layer_total[L] else 0.0,
        }
        for L in sorted(layer_total)
    ]
    return {
        "dt": dt,
        "total": total,
        "runs": len(runs),
        "failed": failed,
        "success_pct": success_pct,
        "layers": layers,
        "top_failures": top_failures,
        "coverage_pct": None,   # run 레코드에 expected_raw_objects 없음 — 보강 시 부활
        "bronze_avg_s": None,   # body GET 없이 list-only 집계라 소요시간 제외
        "bronze_max_s": None,
    }


def _format(s: dict) -> str:
    if not s["total"]:
        return f"📊 citydata 일일 관측 — {s['dt']}\n(해당 날짜 실행 기록 없음)"
    lines = [
        f"📊 citydata 일일 관측 — {s['dt']}",
        f"• 실행 {s['runs']} run · {s['total']} 태스크",
        f"• 성공률 {s['success_pct']}% (실패 {s['failed']}건)",
    ]
    if s.get("layers"):
        lines.append("• 레이어별: " + " · ".join(f"{L['layer']} {L['pct']}%" for L in s["layers"]))
    if s["coverage_pct"] is not None:
        lines.append(f"• 수집 완전성 {s['coverage_pct']}% (121장소 기준)")
    if s["bronze_avg_s"] is not None:
        lines.append(f"• bronze 소요 평균 {s['bronze_avg_s']}s / 최대 {s['bronze_max_s']}s")
    if s["top_failures"]:
        top = ", ".join(f"{t}({c})" for t, c in s["top_failures"])
        lines.append(f"• 실패 상위: {top}")
    lines.append("• 소스: R2 runs/ (파일기반 관측)")
    return "\n".join(lines)


def _run_digest(**context) -> None:
    params = context["params"]
    target = params.get("target", "dev")
    target_date = params.get("target_date")
    if not target_date:
        target_date = pendulum.now(KST).subtract(days=1).format("YYYY-MM-DD")
    summary = summary_from_runs("citydata", target_date, target)
    msg = _format(summary)
    print(msg)
    try:
        from common.discord import resolve_webhook, send_text
        if not resolve_webhook("citydata"):
            print("[citydata ops digest] webhook 미설정 — 로그만")
            return
        if send_text(msg, domain="citydata"):
            print("[citydata ops digest] 전송 완료")
    except Exception as exc:  # noqa: BLE001 -- 전송 실패가 DAG 판정을 가리지 않게
        print(f"[citydata ops digest] 전송 실패(무시): {exc}")


with DAG(
    dag_id="citydata_ops_digest",
    description="citydata 일일 관측 digest — R2 runs/ 집계(성공률·레이어별·실패상위) → Discord.",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="7 8 * * *",   # 매일 08:07 KST — 전날 전체 집계
    catchup=False,
    max_active_runs=1,
    # 단일 env 노브(#556) — CITYDATA_TARGET=prod 로 컷오버, 미설정 시 dev(불변).
    # per-run 오버라이드 유지: 트리거 시 target=prod conf 가 이 기본값보다 우선.
    params={"target": os.environ.get("CITYDATA_TARGET", "dev"), "target_date": None},
    tags=["ops", "citydata", "digest", "slo"],
) as dag:
    digest = PythonOperator(
        task_id="run_digest",
        python_callable=_run_digest,
        on_failure_callback=record_citydata_problem,
    )
