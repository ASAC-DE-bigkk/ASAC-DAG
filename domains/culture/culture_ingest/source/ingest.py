"""culture 적재 오케스트레이션.

fetch(원본 박제)와 load(bronze 적재)를 분리한 두 계열의 진입점을 제공한다:

* fetch -- :class:`Dataset` 을 원본 객체 + 매니페스트로 raw에 박제.
  ``run_batch`` 는 로컬 CLI용(실행 컨텍스트 1개로 여러 데이터셋), ``ingest_one``
  은 Airflow 매핑 태스크용(상류에서 만든 컨텍스트를 공유해 같은 ingest_ts 파티션).
* load -- fetch가 박제한 raw만 다시 읽어 bronze Iceberg에 멱등 적재(API 재호출 없음).
  ``load_bronze`` 는 R2·Trino를 만들어 주는 진입점, ``load_bronze_from_raw`` 는
  싱크·웨어하우스를 주입받는 코어.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import requests

from culture_ingest.common.checks import evaluate_landing, extract_record_fields
from culture_ingest.common.config import (
    RunContext,
    build_catalog_settings,
    build_r2_settings,
    missing_r2,
)
from culture_ingest.common.landing import DatasetResult, Landing, LocalSink, R2Sink
from culture_ingest.common.records import parse_records
from culture_ingest.common.security import redact, refresh_env_secrets, register_secret
from culture_ingest.common.warehouse import (
    BronzeWarehouse,
    PyicebergBronzeWarehouse,
    build_warehouse_settings,
)

from common.http.errors import HttpProblemError  # noqa: E402  (security 가 루트 보장 후)

from . import config as culture_config
from .clients import (
    KCISA_ROWS,
    KcisaClient,
    KobisClient,
    KopisClient,
    KopisError,
    SeoulClient,
)
from .datasets import ALL_DATASETS, BY_NAME, Dataset, select


@dataclass(frozen=True)
class IngestOptions:
    """적재 실행 옵션 (날짜창, 페이지/행 상한 등)."""

    date_from: str = ""  # 날짜창 엔드포인트용 시작일 YYYYMMDD
    date_to: str = ""  # 종료일 YYYYMMDD
    kopis_rows: int = 100  # KOPIS 목록 엔드포인트 페이지 크기
    max_pages: int | None = None  # KOPIS·KCISA 목록 페이지 상한 (None = 전체)
    max_rows: int | None = None  # 서울 행 수 상한 (None = 전체)
    max_detail: int = 200  # KOPIS 상세 엔드포인트에서 크롤할 id 상한
    include_detail: bool = False  # kopis_detail 데이터셋 실행 여부
    # 볼륨 HWM 계약(#147)용: {dataset: 직전 good 런 rows}. plan 이 직전 run_report 에서
    # 읽어 주입한다. None/미포함 데이터셋은 볼륨 검사 생략.
    baselines: dict | None = None


@dataclass
class Clients:
    """세 소스 클라이언트 묶음."""

    kopis: KopisClient
    seoul: SeoulClient
    kcisa: KcisaClient
    kobis: KobisClient


# stdate/eddate 날짜창이 필요한 엔드포인트 — 진실원은 Dataset.uses_date_window 하나뿐이며
# 여기서 파생한다(레지스트리에 창 데이터셋을 추가하면 자동 반영, 드리프트 제거). endpoint
# 문자열로 키잉하는 이유: kopis_detail 의 fallback id-fetch(_with_date_window 를
# id_source_endpoint 로 호출)가 목록과 같은 창을 받아 id 집합이 정합해야 하기 때문.
DATE_WINDOW_ENDPOINTS = {ds.endpoint for ds in ALL_DATASETS if ds.uses_date_window}


def _with_date_window(endpoint: str, params: dict, opts: IngestOptions) -> dict:
    """날짜창 엔드포인트면 base_params에 stdate/eddate를 끼워 넣는다."""
    params = dict(params)
    if endpoint in DATE_WINDOW_ENDPOINTS and opts.date_from and opts.date_to:
        params["stdate"] = opts.date_from
        params["eddate"] = opts.date_to
    return params


def _manifest(ds: Dataset, ctx: RunContext, result: DatasetResult, params: dict) -> dict:
    """이번 실행을 재구성할 수 있게 하는 _manifest.json 내용을 만든다."""
    return {
        "dataset": ds.name,
        "title": ds.title,
        "source": ds.source,
        "endpoint": ds.endpoint,
        "kind": ds.kind,
        "load_pattern": ds.load_pattern,
        "load_date": ctx.load_date,
        "ingest_ts": ctx.ingest_ts,
        "run_id": ctx.run_id,
        "request_params": params,
        "pages": result.pages,
        "rows": result.rows,
        "bytes": result.bytes_written,
        "object_keys": result.object_keys,
        "checks": result.checks,  # 수집 검증 결과(완전성·드리프트·freshness)
    }


def _ids_from_landed_list(ds: Dataset, landing: Landing, limit: int) -> list[str] | None:
    """같은 run 에 이미 랜딩된 sibling 목록 raw 에서 상세 크롤용 id 를 추출한다(#146).

    detail 이 목록을 API 로 **재조회**하던 것을 제거해 자정 KOPIS 호출을 줄인다.
    매니페스트(모든 페이지 기록 후 작성 = 완료 마커)가 없으면 None — 부분 랜딩된
    페이지로 id 를 덜 뽑는 침묵 절단을 막고, 호출측이 기존 API 경로로 폴백한다.
    """
    list_ds = next(
        (d for d in ALL_DATASETS if d.kind == "kopis_list" and d.endpoint == ds.id_source_endpoint),
        None,
    )
    if list_ds is None:
        return None
    prefix = landing.prefix_for(list_ds.source, list_ds.name)
    try:
        manifest = json.loads(landing.sink.get(f"{prefix}/_manifest.json"))
    except Exception:  # noqa: BLE001 -- 목록이 이 run 에 아직 없음(순서/부분실행) → 폴백
        return None
    id_re = re.compile(rf"<{ds.id_field}>(.*?)</{ds.id_field}>")
    ids: list[str] = []
    for key in manifest.get("object_keys") or []:
        try:
            body = landing.sink.get(key)
        except Exception:  # noqa: BLE001 -- 페이지 유실 = 신뢰 불가 → 폴백
            return None
        ids.extend(id_re.findall(body.decode("utf-8", "ignore")))
        if len(ids) >= limit:
            break
    return ids[:limit]


def ingest_dataset(
    ds: Dataset,
    clients: Clients,
    landing: Landing,
    opts: IngestOptions,
) -> DatasetResult:
    """데이터셋 1개를 받아 페이지 + 매니페스트를 적재한다. 에러는 예외로 던지지 않고
    결과(result)에 담아, 배치가 한 데이터셋 실패를 넘어 계속 돌 수 있게 한다.
    """
    prefix = landing.prefix_for(ds.source, ds.name)
    result = DatasetResult(name=ds.name, source=ds.source, endpoint=ds.endpoint, prefix=prefix)
    t0 = time.monotonic()
    sample_body: bytes | None = None  # 첫 페이지 = 드리프트(관측 스키마) 점검용 샘플

    def _record_page(body: bytes) -> None:
        nonlocal sample_body
        sample_body = sample_body or body

    try:
        if ds.kind == "kopis_list":
            # KOPIS 목록: 페이지를 끝까지 돌며 각 페이지를 page-NNNN.xml로 적재.
            params = _with_date_window(ds.endpoint, ds.base_params, opts)
            for page in clients.kopis.list_pages(ds.endpoint, params, opts.kopis_rows, opts.max_pages):
                filename = f"page-{page.index:04d}.xml"
                key = landing.write_page(prefix, filename, page.body, "xml")
                result.pages += 1
                result.rows += page.row_count
                result.bytes_written += len(page.body)
                result.object_keys.append(key)
                _record_page(page.body)

        elif ds.kind == "kopis_boxoffice":
            # 예매상황판: 페이징 없이 단일 GET 1건만 적재(page-0001.xml).
            params = _with_date_window(ds.endpoint, ds.base_params, opts)
            page = clients.kopis.fetch_once(ds.endpoint, params, ds.row_tag)
            key = landing.write_page(prefix, "page-0001.xml", page.body, "xml")
            result.pages += 1
            result.rows += page.row_count
            result.bytes_written += len(page.body)
            result.object_keys.append(key)
            _record_page(page.body)

        elif ds.kind == "kobis_boxoffice":
            # KOBIS 일별 박스오피스: 단일 GET 1건(page-0001.json). targetDt=전일 확정분
            # (DAG 03:00 KST 실행, load_date=당일 → 전일). date_from/date_to 창은 쓰지
            # 않는다 — 일배치가 창을 항상 당일로 채워, 존중하면 아직 확정 안 된 당일을
            # 조회하게 되기 때문. 과거 재수집은 logical date 재실행(operations.md). 서울은
            # base_params 에 wideAreaCd=0105001, 전국은 없음.
            target_dt = (
                date.fromisoformat(landing.ctx.load_date) - timedelta(days=1)
            ).strftime("%Y%m%d")
            wide = ds.base_params.get("wideAreaCd")
            page = clients.kobis.daily_boxoffice(target_dt, wide)
            key = landing.write_page(prefix, "page-0001.json", page.body, "json")
            result.pages += 1
            result.rows += page.row_count
            result.bytes_written += len(page.body)
            result.object_keys.append(key)
            _record_page(page.body)
            # write_manifest 용 params (설정 누락 시 UnboundLocalError → #196 실버그 교훈).
            params = {**ds.base_params, "targetDt": target_dt}

        elif ds.kind == "seoul_list":
            # 서울 목록: 1000행 윈도우를 page-NNNNNN.json으로 적재.
            params = {"service": ds.endpoint}
            for page in clients.seoul.list_pages(ds.endpoint, opts.max_rows):
                filename = f"page-{page.index:06d}.json"
                key = landing.write_page(prefix, filename, page.body, "json")
                result.pages += 1
                result.rows += page.row_count
                result.bytes_written += len(page.body)
                result.object_keys.append(key)
                _record_page(page.body)

        elif ds.kind == "kcisa_list":
            # KCISA area2: PageNo 페이징(numOfrows=KCISA_ROWS)을 page-NNNN.xml 로 적재.
            params = {**ds.base_params, "numOfrows": KCISA_ROWS}  # 매니페스트 기록용 요청 파라미터
            for page in clients.kcisa.list_pages(ds.endpoint, ds.base_params, rows=KCISA_ROWS,
                                                 max_pages=opts.max_pages):
                filename = f"page-{page.index:04d}.xml"
                key = landing.write_page(prefix, filename, page.body, "xml")
                result.pages += 1
                result.rows += page.row_count
                result.bytes_written += len(page.body)
                result.object_keys.append(key)
                _record_page(page.body)

        elif ds.kind == "kopis_detail":
            # 상세: 목록에서 id를 모아 건별 상세를 id=<값>.xml로 적재.
            if not opts.include_detail:
                result.error = "skipped (include_detail=False)"  # 옵션 꺼져 있으면 건너뜀
                return result
            # 목록 재조회 제거(#146): 같은 run 에 랜딩된 목록 raw 에서 id 재사용.
            ids = _ids_from_landed_list(ds, landing, opts.max_detail)
            if ids is None:
                # 폴백: 목록이 아직 안 랜딩된 실행 문맥(단독 실행·순서 역전)만 API 재조회.
                id_params = _with_date_window(ds.id_source_endpoint, ds.base_params, opts)
                ids = clients.kopis.list_ids(ds.id_source_endpoint, id_params, ds.id_field, opts.max_detail)
            else:
                print(f"  [detail] {ds.name}: 목록 재조회 생략 — 랜딩된 raw 에서 id {len(ids)}개 재사용(#146)")
            detail_errors: list[str] = []
            for identifier in ids:
                try:
                    page = clients.kopis.detail(ds.endpoint, identifier)
                except (HttpProblemError, requests.RequestException, KopisError) as exc:
                    # 개별 상세 실패(예: KOPIS 간헐적 400)는 그 id만 건너뛰고 계속 진행 —
                    # 한 건이 크롤 전체를 죽이지 않게. 과다 실패는 아래에서 태스크 실패로.
                    detail_errors.append(f"{identifier}: {type(exc).__name__}")
                    continue
                filename = f"id={identifier}.xml"
                key = landing.write_page(prefix, filename, page.body, "xml")
                result.pages += 1
                result.rows += page.row_count
                result.bytes_written += len(page.body)
                result.object_keys.append(key)
                _record_page(page.body)
            # 일시적 단건 실패는 관용하되, 하나도 못 받거나 과반이 실패하면 실질 장애로
            # 보고 태스크를 실패시켜 재시도·알림한다.
            if ids and (not result.pages or len(detail_errors) > len(ids) // 2):
                result.error = (
                    f"detail crawl failed: {len(detail_errors)}/{len(ids)} ids "
                    f"(e.g. {detail_errors[:3]})"
                )
                return result
            if detail_errors:
                print(
                    f"  [detail] {ds.name}: {len(detail_errors)}/{len(ids)} id 건너뜀 "
                    f"(일시 오류, 계속 진행) e.g. {detail_errors[:3]}"
                )
            params = {
                **ds.base_params,
                "id_field": ds.id_field,
                "max_detail": opts.max_detail,
                "ids": len(ids),
                "detail_skipped": len(detail_errors),
            }

        else:
            result.error = f"unknown kind: {ds.kind}"
            return result

        # 수집 검증 (계약 v0): 완전성·드리프트·freshness·볼륨HWM 점검 후 매니페스트에 동봉.
        observed = extract_record_fields(ds.source, sample_body, ds.row_tag, ds.endpoint) if sample_body else []
        baseline = (opts.baselines or {}).get(ds.name)
        result.checks = evaluate_landing(
            ds, result.rows, observed, landing.ctx.ingest_ts, baseline_rows=baseline
        )
        if result.checks["violations"]:
            print(f"  [contract] {ds.name}: " + " | ".join(result.checks["violations"]))
        landing.write_manifest(prefix, _manifest(ds, landing.ctx, result, params))
        # 볼륨 급락(#147)은 warn 이 아니라 **실패로 승격** — fetch_raw 태스크가 빨개져
        # 기존 retries 가 당일 재시도한다(서울은 당일만 복구 가능). 다른 위반(v0)은 현행
        # 유지(리포트 surface 만).
        volume_violations = [v for v in result.checks["violations"] if v.startswith("volume")]
        if volume_violations:
            result.error = volume_violations[0]
    except Exception as exc:  # noqa: BLE001 -- 데이터셋별로 잡아 두고 배치는 계속 진행
        # 2차 방어(#144): clients 의 scrub 을 우회한 예외(URL 키 포함 가능)도 여기서 마스킹 —
        # result.error 는 리포트·알림·태스크 로그로 퍼지는 문자열이라 항상 redact 를 거친다.
        result.error = redact(f"{type(exc).__name__}: {exc}")
    finally:
        result.duration_sec = round(time.monotonic() - t0, 1)
        result.finished_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return result


# --- run 리포트 (정량 측정: 커버리지·완전성·freshness, 계획안 Slide 6②·7) --------

def normalize_mapped_results(pulled) -> list[dict]:
    """매핑 태스크 XCom pull 결과를 항상 ``list[dict]`` 로 정규화한다(#87).

    Airflow 3에서 매핑 인스턴스가 1개면 ``xcom_pull(task_ids=...)`` 이 리스트가
    아니라 dict 하나를 줄 수 있다. 그대로 순회하면 dict 키(문자열)가 summary
    행세를 해 report 가 TypeError 로 죽는다. None 원소(실패 인스턴스)는 걸러낸다.
    """
    if not pulled:
        return []
    if isinstance(pulled, dict):
        return [pulled]
    return [r for r in pulled if r]


def build_run_report(
    summaries: list[dict], ctx: RunContext, expected_total: int, *, load_failed: bool = False
) -> dict:
    """데이터셋별 요약을 모아 run 단위 신뢰성 리포트를 만든다.

    "깨지면 얼마나 빨리 알고, 무엇이 영향인지 숫자로" — bronze v0의 SLO 측정점.
    ``load_failed`` = bronze Iceberg 적재(load) 단계 실패 — fetch가 전부 성공해도
    bronze가 미갱신이면 SLO 실패로 드러낸다(초록 리포트 뒤 침묵 방지).
    """
    # object_keys는 태스크 간 전달용 — 리포트 JSON에는 싣지 않는다(리니지는 _manifest.json).
    rows = [{k: v for k, v in s.items() if k != "object_keys"} for s in summaries if s]
    landed = [s for s in rows if not s["error"]]
    skipped = [s for s in rows if s["error"] and "skipped" in s["error"]]
    failed = [s for s in rows if s["error"] and "skipped" not in s["error"]]

    violations: list[dict] = []
    ages: list[float] = []
    for s in landed:
        ch = s.get("checks") or {}
        for v in ch.get("violations", []):
            violations.append({"dataset": s["name"], "violation": v})
        if ch.get("freshness_age_hours") is not None:
            ages.append(ch["freshness_age_hours"])

    # run 단위 SLO: 수집 실패 0 + 계약 위반 0 + bronze 적재 성공이면 통과.
    # expected=0(plan 전멸)은 분모가 없어 "실패 0"이 공허하게 참이 된다 — 7/7 사고(#182)
    # 리포트가 slo_passed=true 로 나온 구멍. 기대가 없으면 통과도 없다(#185).
    slo_passed = expected_total > 0 and not failed and not violations and not load_failed
    # 리포트는 R2·XCom·알림으로 퍼진다 — error 문자열 등에 시크릿이 남지 않게 통째 마스킹(#144).
    return redact({
        "domain": "culture",
        "layer": "bronze",
        "load_date": ctx.load_date,
        "ingest_ts": ctx.ingest_ts,
        "run_id": ctx.run_id,
        "coverage": {
            "expected": expected_total,
            "landed": len(landed),
            "skipped": len(skipped),
            "failed": len(failed),
            "coverage_pct": round(100.0 * len(landed) / expected_total, 1) if expected_total else 0.0,
        },
        "total_rows": sum(s["rows"] for s in landed),
        "total_iceberg_rows": sum(s.get("iceberg_rows", 0) for s in landed),
        "load_failed": load_failed,
        "freshness": {"max_age_hours": max(ages) if ages else None},
        "violation_count": len(violations),
        "violations": violations,
        "failed_datasets": [{"dataset": s["name"], "error": s["error"]} for s in failed],
        "slo_passed": slo_passed,
        "datasets": rows,
    })


def annihilation_reason(coverage: dict) -> str | None:
    """상류 전멸이면 report 태스크가 run 을 실패시켜야 하는 사유, 아니면 None (#185).

    report 는 all_done 리프라 상류가 전멸해도 성공하고, Airflow run 최종 상태는
    리프 기준이라 run 전체가 초록으로 위장된다(7/7 사고가 아침까지 미검출된 원인).
    리포트·알림을 다 보낸 **뒤** 이 판정으로 raise 해 관측 기능은 유지하고 run
    상태만 정직하게 만든다. 전멸만 잡는다:

    - ``expected == 0`` — plan 자체가 죽어 분모가 없음
    - ``landed == 0 and failed > 0`` — 계획은 됐지만 수집이 하나도 착지 못 함

    부분 실패는 기존대로 SLO surface 에 맡기고, 전부 의도적 skip(landed=0,
    failed=0)은 수집할 게 없던 run 이므로 전멸이 아니다.
    """
    expected = coverage.get("expected", 0)
    landed = coverage.get("landed", 0)
    failed = coverage.get("failed", 0)
    if expected == 0:
        return "plan 전멸 — 기대 데이터셋 0 (expected=0)"
    if landed == 0 and failed > 0:
        return f"수집 전멸 — {expected}개 계획, 0개 착지 (failed={failed})"
    return None


def write_run_report(
    report: dict,
    *,
    ctx: RunContext,
    target: str = "dev",
    env_file: str | None = None,
    dry_run: bool = False,
    local_dir: str = "./_dryrun",
    root: str = culture_config.LANDING_ROOT,
) -> str:
    """run 리포트를 적재 대상에 JSON으로 남긴다. 키를 반환."""
    key = f"{root}/_reports/load_date={ctx.load_date}/ingest_ts={ctx.ingest_ts}/run_report.json"
    body = json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8")
    if dry_run:
        sink = LocalSink(local_dir)
    else:
        settings = build_r2_settings(target, env_file)
        missing = missing_r2(settings)
        if missing:
            raise RuntimeError(f"Missing R2 config: {', '.join(missing)}")
        sink = R2Sink(settings)
    sink.put(key, body, "application/json")
    return key


# --- 런타임 빌더 ---------------------------------------------------------------

_BASELINE_SCAN_REPORTS = 5  # 부분 run(주간 refresh·백필)이 껴도 이 안에 전체 run 이 있도록


def load_baselines(sink, root: str, *, before_ingest_ts: str) -> dict[str, int]:
    """직전 run_report 들에서 {dataset: rows} 볼륨 HWM 을 읽는다(#147, 병합 #206).

    ``before_ingest_ts`` 이전(=이번 실행보다 과거) 리포트를 최신순으로 최대
    ``_BASELINE_SCAN_REPORTS`` 건 훑어 데이터셋별 가장 최근 rows 를 채운다 —
    부분 run(주간 facility refresh 등) 리포트가 최신이어도 나머지 데이터셋
    기준선이 과거 전체 run 에서 보충된다. 리포트 경로의 ingest_ts 는 UTC
    문자열이라 사전순 = 시간순. error 가 있던 데이터셋은 제외(실패 런의 부분
    rows 로 기준선을 끌어내리지 않기 위해). 리포트가 없거나 읽기 실패 시 {} —
    볼륨 검사가 조용히 생략될 뿐 수집 자체는 막지 않는다(fail-open).
    """
    try:
        keys = sink.list(f"{root}/_reports/")
        report_keys = [k for k in keys if k.endswith("run_report.json")]
        candidates = []
        for k in report_keys:
            m = re.search(r"ingest_ts=([0-9TZ]+)", k)
            if m and m.group(1) < before_ingest_ts:
                candidates.append((m.group(1), k))
        merged: dict[str, int] = {}
        for _, key in sorted(candidates, reverse=True)[:_BASELINE_SCAN_REPORTS]:
            report = json.loads(sink.get(key))
            for s in report.get("datasets", []):
                if s.get("rows") and not s.get("error") and s["name"] not in merged:
                    merged[s["name"]] = int(s["rows"])
        return merged
    except Exception as exc:  # noqa: BLE001 -- baseline 은 보조 신호, 수집을 막지 않는다
        print(f"[baselines] 직전 리포트 조회 실패(볼륨 검사 생략): {type(exc).__name__}")
        return {}


def load_baselines_for_target(
    target: str, *, before_ingest_ts: str, env_file: str | None = None
) -> dict[str, int]:
    """R2(target)에서 직전 리포트 기반 볼륨 HWM 을 읽는다 — DAG plan 용 편의 래퍼."""
    settings = build_r2_settings(target, env_file)
    if missing_r2(settings):
        return {}
    return load_baselines(
        R2Sink(settings), culture_config.LANDING_ROOT, before_ingest_ts=before_ingest_ts
    )


def build_clients(env_file: str | None = None) -> Clients:
    """인증키를 읽어 검증한 뒤 KOPIS/서울 클라이언트를 만든다.

    읽은 키는 redactor 에 **literal 등록**한다(#144) — KOPIS 의 bare `service=` 쿼리는
    공통 structural 패턴에 안 걸리고(실측), env_file 로 읽은 키는 os.environ 스캔
    (refresh_env_secrets)도 못 보므로, 값 자체 등록이 유일하게 확실한 방어다.
    """
    keys = culture_config.source_keys(env_file)
    missing = culture_config.missing_keys(keys)
    if missing:
        raise RuntimeError(f"Missing culture source keys: {', '.join(missing)}")
    register_secret(keys.kopis)
    register_secret(keys.seoul)
    register_secret(keys.cult)
    register_secret(keys.kobis)
    refresh_env_secrets()  # R2 자격증명 등 이름 기반 env 시크릿도 함께 등록
    return Clients(
        kopis=KopisClient(keys.kopis),
        seoul=SeoulClient(keys.seoul),
        kcisa=KcisaClient(keys.cult),
        kobis=KobisClient(keys.kobis),
    )


def build_landing(
    ctx: RunContext,
    *,
    target: str = "dev",
    env_file: str | None = None,
    dry_run: bool = False,
    local_dir: str = "./_dryrun",
    root: str = culture_config.LANDING_ROOT,
) -> Landing:
    """적재 싱크를 만든다. dry_run이면 로컬, 아니면 R2(사전 점검 포함)."""
    if dry_run:
        return Landing(LocalSink(local_dir), root, ctx)
    settings = build_r2_settings(target, env_file)
    missing = missing_r2(settings)
    if missing:
        raise RuntimeError(f"Missing R2 config: {', '.join(missing)}")
    return Landing(R2Sink(settings), root, ctx)


ENGINES = ("pyiceberg", "trino")  # trino = 전환기 롤백 레버(#203) — 일몰 계획은 operations.md


def build_warehouse(target: str = "dev", engine: str = "pyiceberg") -> BronzeWarehouse | PyicebergBronzeWarehouse:
    """bronze Iceberg 적재 웨어하우스. 기본 pyiceberg(커밋 1회), trino 는 롤백 레버."""
    if engine not in ENGINES:
        raise ValueError(f"engine must be one of {ENGINES}, got {engine!r}")
    settings = build_warehouse_settings(target)
    if engine == "trino":
        return BronzeWarehouse(settings)
    return PyicebergBronzeWarehouse(settings, build_catalog_settings(target))


def load_bronze_from_raw(
    ctx: RunContext,
    summaries: list[dict],
    *,
    sink,
    warehouse,
) -> dict[str, int]:
    """fetch 단계가 raw에 박제한 객체를 다시 읽어 bronze Iceberg에 멱등 적재한다.

    입력은 R2 raw뿐(API 재호출 없음) — bronze만 실패한 run은 이 단계만 재시도하면
    된다. 데이터셋 단위로 격리해 하나가 실패해도 나머지는 적재하고, 말미에 실패
    목록으로 예외를 던진다(fail loud). 적재 자체는 ``warehouse.load``의
    ingest_ts delete-then-insert 라 재실행이 중복을 만들지 않는다.
    반환: {dataset: 적재 행 수} (에러/skipped 데이터셋은 제외).
    """
    loaded: dict[str, int] = {}
    failures: list[str] = []
    for s in summaries:
        if not s or s.get("error"):
            continue  # 실패/skipped 데이터셋은 적재 대상 아님(리포트가 이미 드러냄)
        try:
            # 미등록 이름도 KeyError로 루프를 죽이지 않게 조회부터 try 안에서.
            ds = BY_NAME[s["name"]]
            records = []
            for key in s.get("object_keys") or []:
                filename = key.rsplit("/", 1)[-1]
                for rec in parse_records(ds.source, sink.get(key), ds.row_tag, ds.endpoint):
                    records.append((key, filename, rec))
            # fetch가 센 행수와 **적재 전** 대조 — 손상 raw를 parse_records가 빈 리스트로
            # 삼켜 0행 침묵 성공하는 구멍을 막고, 의심 데이터는 테이블에 쓰지 않는다.
            if len(records) != s.get("rows", len(records)):
                failures.append(
                    f"{ds.name}: 파싱 {len(records)}행 ≠ fetch {s['rows']}행 (raw 파싱 유실 의심)"
                )
                continue
            # 데이터셋별 소요 로그 — load_bronze 는 26분 블랙박스라(#202), 어느 데이터셋이
            # 몇 초 걸렸는지 남겨 병목(세종 88MB≈13분)을 눈으로 확인 가능하게 한다.
            t0 = time.monotonic()
            loaded[ds.name] = warehouse.load(ds, ctx, records)
            print(f"[load] {ds.name}: {loaded[ds.name]:,}행 · {time.monotonic() - t0:.1f}s")
        except Exception as exc:  # noqa: BLE001 -- 데이터셋별 격리, 말미 fail loud
            failures.append(redact(f"{s['name']}: {type(exc).__name__}: {exc}"))
    if failures:
        raise RuntimeError("bronze 적재 실패: " + " | ".join(failures))
    return loaded


def load_bronze(
    ctx: RunContext,
    summaries: list[dict],
    *,
    target: str = "dev",
    env_file: str | None = None,
    engine: str = "pyiceberg",
) -> dict[str, int]:
    """R2 싱크·웨어하우스를 만들어 ``load_bronze_from_raw``를 실행 (DAG/CLI 공용)."""
    settings = build_r2_settings(target, env_file)
    missing = missing_r2(settings)
    if missing:
        raise RuntimeError(f"Missing R2 config: {', '.join(missing)}")
    return load_bronze_from_raw(
        ctx, summaries, sink=R2Sink(settings), warehouse=build_warehouse(target, engine)
    )


def run_batch(
    names: list[str] | None,
    *,
    opts: IngestOptions,
    target: str = "dev",
    env_file: str | None = None,
    dry_run: bool = False,
    local_dir: str = "./_dryrun",
    run_id: str = "manual",
) -> tuple[RunContext, list[DatasetResult]]:
    """여러 데이터셋을 하나의 공유 실행 컨텍스트로 적재 (로컬 CLI 경로)."""
    ctx = RunContext.create(run_id=run_id)
    clients = build_clients(env_file)
    landing = build_landing(ctx, target=target, env_file=env_file, dry_run=dry_run, local_dir=local_dir)
    results = [ingest_dataset(ds, clients, landing, opts) for ds in select(names)]
    return ctx, results


def ingest_one(
    name: str,
    *,
    ctx: RunContext,
    opts: IngestOptions,
    target: str = "dev",
    env_file: str | None = None,
    dry_run: bool = False,
    local_dir: str = "./_dryrun",
) -> DatasetResult:
    """호출자가 넘긴 실행 컨텍스트로 데이터셋 1개를 적재 (Airflow 경로).

    공유 ``ctx`` 덕분에 한 DAG 실행의 모든 매핑 태스크가 같은 ``ingest_ts``
    파티션에 적재된다.
    """
    ds = BY_NAME[name]
    clients = build_clients(env_file)
    landing = build_landing(ctx, target=target, env_file=env_file, dry_run=dry_run, local_dir=local_dir)
    return ingest_dataset(ds, clients, landing, opts)
