"""culture raw 적재 완료 Discord 알림.

`report` 태스크의 run_report 를 Discord 임베드로 포맷(build_report_payload)하고
webhook 으로 전송(DiscordWebhookNotifier)한다. env CULTURE_DISCORD_WEBHOOK_URL 이 없으면
NoopNotifier(전송 안 함) 라 코드만으로 안전하게 머지된다.

시크릿(웹훅 URL)은 메시지·로그에 절대 넣지 않는다. 전송 실패는 삼켜 파이프라인을 막지 않는다.
"""
from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

from culture_ingest.source.datasets import BY_NAME

log = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")
WEBHOOK_ENV = "CULTURE_DISCORD_WEBHOOK_URL"
COLOR_PASS = 3066993   # 0x2ECC71
COLOR_FAIL = 15158332  # 0xE74C3C


class Notifier(ABC):
    @abstractmethod
    def send(self, payload: dict) -> None:
        """Discord webhook payload 1건 전송."""


class NoopNotifier(Notifier):
    def send(self, payload: dict) -> None:
        title = ((payload.get("embeds") or [{}])[0]).get("title", "")
        log.info("[notify:noop] %s (전송 비활성 — URL 미설정)", title)


class DiscordWebhookNotifier(Notifier):
    def __init__(self, url: str, timeout: float = 10.0):
        self._url = url
        self._timeout = timeout

    def send(self, payload: dict) -> None:
        try:
            requests.post(self._url, json=payload, timeout=self._timeout)
        except Exception as exc:  # noqa: BLE001 -- best-effort.
            # 예외 타입 이름만 남긴다 — requests 예외 문자열/traceback 에 웹훅 URL 이
            # 섞여 로그로 새는 것을 막는다(설계 §7: URL 로그 금지).
            log.warning("[notify] Discord 전송 실패(무시): %s", type(exc).__name__)


def notifier_from_env(env: dict | None = None) -> Notifier:
    env = os.environ if env is None else env
    url = (env.get(WEBHOOK_ENV) or "").strip()
    return DiscordWebhookNotifier(url) if url else NoopNotifier()


def _kst_hms(ingest_ts: str) -> str:
    try:
        dt = datetime.strptime(ingest_ts, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        return dt.astimezone(KST).strftime("%H:%M:%S")
    except (ValueError, TypeError):
        return "--"


def _dur_str(seconds: float) -> str:
    total = int(seconds)
    m, s = divmod(total, 60)
    return f"{m}m{s:02d}s" if m else f"{s}s"


def _run_duration(ingest_ts: str, finish_tss: list[str]) -> str:
    if not finish_tss:
        return "--"
    try:
        t0 = datetime.strptime(ingest_ts, "%Y%m%dT%H%M%SZ")
        t1 = datetime.strptime(max(finish_tss), "%Y%m%dT%H%M%SZ")
        return _dur_str((t1 - t0).total_seconds())
    except (ValueError, TypeError):
        return "--"


def _title_of(name: str) -> str:
    return BY_NAME[name].title if name in BY_NAME else name


def _fmt_int(v) -> str:
    return "--" if v in (None, "") else f"{int(v):,}"


def _status(s: dict) -> str:
    err = s.get("error") or ""
    if err and "skipped" not in err:
        return "FAIL"
    if "skipped" in err:
        return "skip"
    checks = s.get("checks") or {}
    ib = s.get("iceberg_rows", 0)
    mismatch = bool(ib) and ib != s.get("rows", 0)
    if checks.get("passed") is False or mismatch:
        return "WARN"
    return "ok"


def _table(datasets: list[dict]) -> str:
    lines = [f"{'st':<4}  {'rows':>6}  {'pages':>6}  {'done':<8}  {'sec':>5}  dataset"]
    for s in datasets:
        done = _kst_hms(s["finished_ts"]) if s.get("finished_ts") else "--"
        dur = s.get("duration_sec")
        sec = f"{float(dur):.1f}" if dur is not None else "--"
        lines.append(
            f"{_status(s):<4}  {_fmt_int(s.get('rows')):>6}  {_fmt_int(s.get('pages')):>6}  "
            f"{done:<8}  {sec:>5}  {_title_of(s['name'])} · {s['name']}"
        )
    return "\n".join(lines)


def _issue_lines(report: dict, datasets: list[dict]) -> list[str]:
    out = []
    for f in report.get("failed_datasets", []):
        out.append(f"❌ FAIL — {_title_of(f['dataset'])}({f['dataset']}): {str(f.get('error',''))[:200]}")
    for v in report.get("violations", []):
        out.append(f"⚠️ 위반 — {_title_of(v['dataset'])}({v['dataset']}): {v['violation']}")
    for s in datasets:
        ib = s.get("iceberg_rows", 0)
        if ib and ib != s.get("rows", 0):
            out.append(f"⚠️ Iceberg 불일치 — {_title_of(s['name'])}({s['name']}): "
                       f"raw {_fmt_int(s.get('rows'))} ≠ iceberg {_fmt_int(ib)}")
    return out


def build_report_payload(report: dict) -> dict:
    datasets = report.get("datasets", [])
    cov = report.get("coverage", {})
    passed = report.get("slo_passed", False)

    start = _kst_hms(report.get("ingest_ts", ""))
    finish_tss = [s["finished_ts"] for s in datasets if s.get("finished_ts")]
    finish = _kst_hms(max(finish_tss)) if finish_tss else "--"
    dur = _run_duration(report.get("ingest_ts", ""), finish_tss)

    line1 = f"수집 {start} → 완료 {finish} KST · 소요 {dur}"
    parts = [
        f"커버리지 {cov.get('landed', 0)}/{cov.get('expected', 0)}",
        f"landed {cov.get('landed', 0)}",
        f"skipped {cov.get('skipped', 0)}",
        f"failed {cov.get('failed', 0)}",
        f"records {int(report.get('total_rows', 0)):,}",
    ]
    ib_total = report.get("total_iceberg_rows", 0)
    if ib_total:
        parts.append(f"Iceberg {int(ib_total):,}")
    fresh = (report.get("freshness") or {}).get("max_age_hours")
    if fresh is not None:
        parts.append(f"freshness {fresh}h")

    desc = f"{line1}\n{' · '.join(parts)}\n```\n{_table(datasets)}\n```"
    issues = _issue_lines(report, datasets)
    if issues:
        desc += "\n" + "\n".join(issues)

    return {
        "embeds": [{
            "title": f"culture raw 적재 리포트 · {report.get('load_date', '')} (KST)",
            "color": COLOR_PASS if passed else COLOR_FAIL,
            "description": desc[:4096],
            "footer": {"text": f"run_id={report.get('run_id', '')} · @daily"},
        }]
    }
