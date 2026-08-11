"""culture silver/gold 변환 완료 Discord 알림 (성공 경로).

수집(`common/notify.py`)은 매 run 리포트를 보내지만 변환은 지금까지 **실패할 때만** 말이
있었다(`common.errors.airflow` 의 에러 embed). 그래서 새벽 런이 조용하면 "잘 돌았다"인지
"안 돌았다"인지 알림만으로는 못 갈랐다. 이 모듈이 성공 쪽 말을 채운다 — silver·gold 를
몇 개 어떻게 만들었고, 계약 테스트가 어떻게 나왔는지.

**왜 스냅샷을 읽는가**: dbt 는 하위명령마다 ``target/run_results.json`` 을 덮어쓴다.
성공 요약은 run 과 test 결과가 **둘 다** 필요한데, test 가 끝나면 run 결과는 이미 사라진 뒤다.
그래서 DAG 이 각 태스크 성공 직후 자기 결과를 ``run_results.<task_id>.json`` 으로 복사해 두고,
여기서는 그 둘을 읽는다. (실패 경로는 덮어쓰기 전 원본을 읽으므로 스냅샷과 무관하다.)

숫자는 **잰 것만** 적는다. Trino 는 모델 적재 행 수를 ``rows_affected=-1`` 로 돌려주는 일이
잦은데, 그걸 0 으로 적으면 그 순간부터 알림이 조용히 거짓말을 한다(수집 쪽 `#N-5` 와 같은 규칙).
못 잰 행 수는 아예 쓰지 않는다.

시크릿은 메시지에 넣지 않는다(전송 계층이 한 번 더 redaction 한다). 전송 실패는 삼킨다.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")
DOMAIN = "culture"

COLOR_PASS = 3066993   # 0x2ECC71 — run·test 전부 통과
COLOR_WARN = 15844367  # 0xF1C40F — 통과했지만 경고/스킵이 있다

#: 모델 이름 접두사로 가르는 단계. 이 DAG 이 만드는 건 이 둘뿐이다.
LAYERS = ("silver", "gold")

#: 알림에 이름을 적는 최대 건수 — 넘으면 "외 N건" 으로 접는다(임베드 길이 보호).
_MAX_NAMED = 5


def _split_uid(unique_id: Any) -> tuple[str, str]:
    """``model.culture.gold_culture_event_schedule`` → ``("model", "gold_...")``."""
    parts = str(unique_id or "").split(".")
    if not parts or not parts[0]:
        return "", ""
    return parts[0], parts[-1]


def _layer_of(model_name: str) -> str | None:
    """모델 이름 접두사로 단계를 정한다. 접두사가 없으면 None — 지어내지 않는다."""
    for layer in LAYERS:
        if model_name.startswith(layer + "_"):
            return layer
    return None


def _rows_of(result: Mapping[str, Any]) -> int | None:
    """적재 행 수. 어댑터가 못 재면(-1·비정수) None — 0 으로 바꾸지 않는다."""
    rows = (result.get("adapter_response") or {}).get("rows_affected")
    return rows if isinstance(rows, int) and not isinstance(rows, bool) and rows >= 0 else None


def summarize_models(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    """``dbt run`` 결과 → 단계별 요약 + 느린 모델 순위.

    반환: ``{"layers": {layer: {...}}, "slowest": [(name, seconds), ...],
    "elapsed": float|None, "unhealthy": [(name, status), ...]}``
    """
    results = list((payload or {}).get("results") or [])
    layers: dict[str, dict[str, Any]] = {
        layer: {"total": 0, "ok": 0, "seconds": 0.0, "rows": None} for layer in LAYERS
    }
    timings: list[tuple[str, float]] = []
    unhealthy: list[tuple[str, str]] = []

    for result in results:
        kind, name = _split_uid(result.get("unique_id"))
        if kind != "model":
            continue
        layer = _layer_of(name)
        if layer is None:
            continue
        bucket = layers[layer]
        bucket["total"] += 1
        status = str(result.get("status", "")).lower()
        if status == "success":
            bucket["ok"] += 1
        else:
            unhealthy.append((name, status or "unknown"))
        seconds = result.get("execution_time")
        if isinstance(seconds, (int, float)):
            bucket["seconds"] += float(seconds)
            timings.append((name, float(seconds)))
        rows = _rows_of(result)
        if rows is not None:
            bucket["rows"] = (bucket["rows"] or 0) + rows

    elapsed = (payload or {}).get("elapsed_time")
    return {
        "layers": layers,
        "slowest": sorted(timings, key=lambda pair: pair[1], reverse=True)[:3],
        "elapsed": float(elapsed) if isinstance(elapsed, (int, float)) else None,
        "unhealthy": unhealthy,
    }


def summarize_tests(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    """``dbt test`` 결과 → 상태별 건수 + 통과 못 한 테스트 이름.

    반환: ``{"total": int, "pass": int, "warn": int, "fail": int, "error": int,
    "skipped": int, "notable": [(status, name, failures), ...], "elapsed": float|None}``
    """
    counts = {"pass": 0, "warn": 0, "fail": 0, "error": 0, "skipped": 0}
    notable: list[tuple[str, str, Any]] = []
    total = 0

    for result in list((payload or {}).get("results") or []):
        kind, name = _split_uid(result.get("unique_id"))
        if kind != "test":
            continue
        total += 1
        status = str(result.get("status", "")).lower()
        if status in counts:
            counts[status] += 1
        if status in ("warn", "fail", "error"):
            notable.append((status, name, result.get("failures")))

    elapsed = (payload or {}).get("elapsed_time")
    return {
        "total": total,
        **counts,
        "notable": notable,
        "elapsed": float(elapsed) if isinstance(elapsed, (int, float)) else None,
    }


def _layer_line(layer: str, stats: Mapping[str, Any]) -> str:
    total, ok = stats["total"], stats["ok"]
    head = f"**{layer}** {total}개 · 성공 {ok}" if ok != total else f"**{layer}** {total}개 전부 성공"
    bits = []
    if stats["rows"] is not None:
        bits.append(f"{stats['rows']:,}행")
    if stats["seconds"]:
        bits.append(f"{stats['seconds']:.0f}초")
    return f"{head} — {' · '.join(bits)}" if bits else head


def _named(items: Iterable[str]) -> str:
    items = list(items)
    shown = " · ".join(items[:_MAX_NAMED])
    return shown + (f" 외 {len(items) - _MAX_NAMED}건" if len(items) > _MAX_NAMED else "")


def build_transform_payload(
    models: Mapping[str, Any],
    tests: Mapping[str, Any],
    *,
    label: str = "변환",
    target: str | None = None,
    dag_run_id: str | None = None,
    elapsed: float | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """요약 두 개 → Discord embed payload(전송은 하지 않는다 — 순수 조립).

    색은 판정을 따른다: 경고·스킵·비성공 모델이 하나라도 있으면 노랑, 아니면 초록.
    성공 경로에서만 불리므로 fail/error 는 원칙적으로 0 이지만, 그래도 세서 적는다 —
    "성공했는데 테스트가 깨져 있다"를 알림이 감추면 안 된다.

    ``elapsed`` 를 주면 그 값을 쓰고, 없으면 두 요약의 합으로 본다. 부르는 쪽이 갈리기
    때문이다 — `culture_transform` 은 run·test 가 **다른 명령**이라 합이 맞지만,
    `culture_slo` 는 `dbt build` **한 번**이라 같은 payload 를 둘로 요약해 넘긴다.
    거기서 합을 쓰면 소요가 정확히 두 배로 부풀어 오른다.
    """
    layers = models.get("layers") or {}
    built = {layer: (layers.get(layer) or {}).get("total", 0) for layer in LAYERS}
    title = f"culture {label} 완료 — " + " · ".join(f"{layer} {built[layer]}" for layer in LAYERS)

    lines = [_layer_line(layer, layers[layer]) for layer in LAYERS if layer in layers]

    bad = tests["fail"] + tests["error"]
    test_bits = [f"통과 {tests['pass']}"]
    if tests["warn"]:
        test_bits.append(f"경고 {tests['warn']}")
    if bad:
        test_bits.append(f"실패 {bad}")
    if tests["skipped"]:
        test_bits.append(f"스킵 {tests['skipped']}")
    lines.append(f"**테스트** {tests['total']}건 — {' · '.join(test_bits)}")

    if models.get("unhealthy"):
        lines.append("· 성공 못 한 모델: "
                     + _named(f"`{name}`({status})" for name, status in models["unhealthy"]))
    if tests.get("notable"):
        lines.append("· 짚어 볼 테스트: "
                     + _named(f"`{name}`({status}"
                              + (f" {failures}건" if isinstance(failures, int) and failures else "")
                              + ")"
                              for status, name, failures in tests["notable"]))
    if models.get("slowest"):
        lines.append("· 가장 오래: "
                     + " · ".join(f"`{name}` {sec:.0f}초" for name, sec in models["slowest"]))

    color = COLOR_WARN if (bad or tests["warn"] or models.get("unhealthy")) else COLOR_PASS

    stamped = (now or datetime.now(KST)).astimezone(KST).strftime("%Y-%m-%d %H:%M KST")
    footer_bits = [stamped]
    if target:
        footer_bits.append(f"target={target}")
    total_seconds = (elapsed if elapsed is not None
                     else sum(v for v in (models.get("elapsed"), tests.get("elapsed")) if v))
    if total_seconds:
        footer_bits.append(f"dbt {total_seconds:.0f}초")
    if dag_run_id:
        footer_bits.append(str(dag_run_id)[:60])

    return {"embeds": [{
        "title": title,
        "description": "\n".join(lines),
        "color": color,
        "footer": {"text": " · ".join(footer_bits)},
    }]}


def read_run_results(path: str) -> dict[str, Any] | None:
    """스냅샷 1개를 읽는다. 없거나 깨졌으면 None — 알림 때문에 DAG 을 죽이지 않는다."""
    try:
        with open(path, encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception as exc:  # noqa: BLE001 — 부재·파싱 실패 모두 관측 공백일 뿐이다
        log.warning("[culture transform] run_results 읽기 실패(%s): %s",
                    os.path.basename(path), type(exc).__name__)
        return None
    return payload if isinstance(payload, dict) else None


def snapshot_path(project_dir: str, task_id: str) -> str:
    """DAG 의 복사 대상과 이 모듈의 읽기 대상이 어긋나지 않게 경로를 한 곳에서 만든다."""
    return os.path.join(project_dir, "target", f"run_results.{task_id}.json")
