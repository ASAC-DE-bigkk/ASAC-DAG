"""일배치 수집 리포트: 하루치 run_report.json 집계 → Discord 메시지 생성.

수집 DAG(``population_bronze``)가 run마다 R2에 남긴
``raw/population/_reports/load_date=<KST>/ingest_ts=<UTC>/run_report.json``을
하루치 모아 요약한다. 수집 경로와 완전히 분리돼 있어 수집 성능에 영향 없다.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone

from ..common.config import KST, build_r2_settings, missing_r2
from . import config as source_config


def _kst_hm(ingest_ts: str) -> str:
    """UTC ingest_ts(YYYYMMDDTHHMMSSZ)를 KST HH:MM 문자열로."""
    try:
        dt = datetime.strptime(ingest_ts, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        return dt.astimezone(KST).strftime("%H:%M")
    except (ValueError, TypeError):
        return ingest_ts or "?"


def _r2_client(target: str, env_file: str | None):
    """R2(S3 호환) 클라이언트 + 버킷 이름을 만든다."""
    import boto3

    settings = build_r2_settings(target, env_file)
    missing = missing_r2(settings)
    if missing:
        raise RuntimeError(f"Missing R2 config: {', '.join(missing)}")
    client = boto3.client(
        "s3",
        endpoint_url=settings.endpoint,
        aws_access_key_id=settings.access_key_id,
        aws_secret_access_key=settings.secret_access_key,
        region_name="auto",
    )
    return client, settings.bucket


def _read_reports(client, bucket: str, load_date: str) -> list[dict]:
    """해당 load_date의 모든 run_report.json을 읽어 리스트로 반환."""
    prefix = f"{source_config.LANDING_ROOT}/_reports/load_date={load_date}/"
    reports: list[dict] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("run_report.json"):
                body = client.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
                try:
                    reports.append(json.loads(body))
                except json.JSONDecodeError:
                    continue
    return reports


def aggregate_day(load_date: str, *, target: str = "dev", env_file: str | None = None) -> dict:
    """하루치 run_report를 집계한다(수집 run 수·커버리지·실패 장소)."""
    client, bucket = _r2_client(target, env_file)
    reports = _read_reports(client, bucket, load_date)

    total_runs = len(reports)
    fully_failed = 0
    coverage_pcts: list[float] = []
    area_failures: Counter = Counter()
    total_area_failures = 0
    failed_runs: list[tuple[str, list[str]]] = []  # (ingest_ts, [area_nm...])
    slo_breached: list[tuple[str, float]] = []  # (ingest_ts, coverage_pct) — landed>0인데 SLO 미달

    for r in reports:
        cov = r.get("coverage", {})
        landed = cov.get("landed", 0)
        cov_pct = cov.get("coverage_pct", 0.0)
        if landed == 0:
            fully_failed += 1
        elif cov_pct < source_config.COVERAGE_SLO_PCT:
            slo_breached.append((r.get("ingest_ts", ""), cov_pct))
        coverage_pcts.append(cov_pct)
        total_area_failures += cov.get("failed", 0)
        fails = r.get("failures", [])
        for f in fails:
            area_failures[f.get("area_nm", "?")] += 1
        if fails:
            failed_runs.append((r.get("ingest_ts", ""), [f.get("area_nm", "?") for f in fails]))

    failed_runs.sort()  # 시각 순
    slo_breached.sort()
    avg_cov = round(sum(coverage_pcts) / total_runs, 1) if total_runs else 0.0
    return {
        "load_date": load_date,
        "target": target,
        "total_runs": total_runs,
        "fully_failed_runs": fully_failed,
        "avg_coverage_pct": avg_cov,
        "total_area_failures": total_area_failures,
        "top_failing_areas": area_failures.most_common(8),
        "failed_runs": failed_runs,
        "slo_threshold_pct": source_config.COVERAGE_SLO_PCT,
        "slo_breached_runs": slo_breached,
    }


def format_message(summary: dict) -> str:
    """집계 결과를 Discord용 텍스트 메시지로 만든다."""
    s = summary
    lines = [
        f"📊 **인구혼잡도 수집 데일리 리포트** — {s['load_date']} (target={s['target']})",
        "",
    ]
    if s["total_runs"] == 0:
        lines.append("⚠️ 해당 날짜의 수집 리포트가 없습니다 (수집 run 없음 또는 리포트 미적재).")
        return "\n".join(lines)

    ok_runs = s["total_runs"] - s["fully_failed_runs"]
    breached = s.get("slo_breached_runs", [])
    slo_pct = s.get("slo_threshold_pct", 95.0)
    status = "✅" if s["fully_failed_runs"] == 0 and not breached else "⚠️"
    lines += [
        f"{status} 수집 run: **{s['total_runs']}회** (정상 {ok_runs} · 완전실패 {s['fully_failed_runs']} · SLO미달 {len(breached)})",
        f"📈 평균 장소 커버리지: **{s['avg_coverage_pct']}%** (SLO {slo_pct}%)",
        f"❌ 장소 수집 실패 누계: **{s['total_area_failures']}건**",
    ]
    if s["top_failing_areas"]:
        top = ", ".join(f"{name}({cnt})" for name, cnt in s["top_failing_areas"])
        lines += ["", f"🔻 자주 실패한 장소: {top}"]
    if s.get("failed_runs"):
        lines += ["", "🕒 **실패 run 상세** (시각 KST · 장소):"]
        for ts, areas in s["failed_runs"][:12]:
            lines.append(f"  {_kst_hm(ts)} — {', '.join(areas)}")
        if len(s["failed_runs"]) > 12:
            lines.append(f"  …외 {len(s['failed_runs']) - 12}개 run 실패")
    if breached:
        lines += ["", f"🚨 **SLO 미달 run** (커버리지 < {slo_pct}%):"]
        for ts, pct in breached[:12]:
            lines.append(f"  {_kst_hm(ts)} — {pct}%")
        if len(breached) > 12:
            lines.append(f"  …외 {len(breached) - 12}개 run")
    return "\n".join(lines)
