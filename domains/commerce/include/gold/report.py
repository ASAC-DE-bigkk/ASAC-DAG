"""gold(Iceberg) 적재 결과 → Discord 리포트 — 코어(dbt) 현황 + detail(카탈로그) 이번 적재행.

Postgres 시절 리포트의 승계판(객체 단위 렌더 원칙 유지): '적재 0행 = 이번 창 신규 버전 없음(정상)'
요약, 행>0 은 생략하지 않음. 길면 페이지 분할.
"""
from __future__ import annotations

import logging

from commerce_core.run_report import _MAX_DESC, _fmt_elapsed, _num, _paginate

log = logging.getLogger(__name__)
_DOMAIN = "commerce"

# 코어(dbt) gold 모델 — 현재 행수로 리포트(PROJECT.md §4.3).
CORE_TABLES = ("gold_license_entity", "gold_license_entity_history", "gold_license_dong_summary")


def core_counts() -> dict[str, int]:
    """코어 gold 테이블 행수(Trino). 부재 테이블은 -1(미빌드)."""
    from bronze.warehouse import _connect, _qualified

    catalog, schema, qschema = _qualified()
    conn = _connect(catalog, schema)
    out: dict[str, int] = {}
    try:
        cur = conn.cursor()
        for t in CORE_TABLES:
            try:
                cur.execute(f"SELECT count(*) FROM {qschema}.{t}")  # security: allow-sql - 상수 식별자
                out[t] = int(cur.fetchone()[0])
            except Exception:  # noqa: BLE001 — 미빌드(첫 실행)면 -1 로 표기
                out[t] = -1
    finally:
        conn.close()
    return out


def send_gold_report(*, catalog: dict | None, load: dict | None,
                     elapsed_seconds: float | None = None, error: str | None = None) -> dict:
    """catalog(build_catalog XCom) + load(load_details XCom {loaded:{obj:rows}}) → Discord."""
    from common.discord import COLOR_FAIL, COLOR_OK, COLOR_WARN, send_embed
    from security import redact

    if error or catalog is None or load is None:
        miss = [n for n, v in (("build_catalog", catalog), ("load_details", load)) if v is None]
        desc = (f"`commerce_load_gold` 실패 — {redact(str(error))[:200]}" if error
                else f"업스트림 미완료: `@{'`, `@'.join(miss)}` (XCom 없음)")
        if elapsed_seconds is not None:
            desc += f"\n⏱ 실행시간 {_fmt_elapsed(elapsed_seconds)}"
        try:
            send_embed("❌ [commerce] commerce_load_gold (gold 적재) — 실패", desc,
                       color=COLOR_FAIL, domain=_DOMAIN)
        except Exception as exc:  # noqa: BLE001
            log.warning("[commerce] gold report 전송 실패(무시): %s", type(exc).__name__)
        return {"status": "failed", "loaded_rows": 0, "objects": 0}

    loaded: dict[str, int] = {k: int(v) for k, v in (load.get("loaded") or {}).items()}
    total_rows = sum(loaded.values())
    nonzero = sorted(((k, v) for k, v in loaded.items() if v > 0), key=lambda x: -x[1])
    n_zero = sum(1 for v in loaded.values() if v == 0)
    drift = bool(catalog.get("drift"))

    try:
        cores = core_counts()
    except Exception:  # noqa: BLE001 — 리포트가 조회 실패로 죽지 않게
        cores = {}

    head = (f"**카탈로그** v`{str(catalog.get('version', '?'))[:8]}` · "
            f"cluster {catalog.get('clusters', '?')}·single {catalog.get('singles', '?')}"
            f" · 실측 {catalog.get('datasets', '?')}종"
            + (" · ⚠️ **드리프트(신규 API/필드)**" if drift else "")
            + f"\n**detail 적재** {_num(total_rows)}행 / {len(loaded)}객체 (Iceberg)")
    if elapsed_seconds is not None:
        head += f" · ⏱ {_fmt_elapsed(elapsed_seconds)}"

    body = []
    if cores:
        body.append("**코어(dbt) 현황** — " + " · ".join(
            f"`{t.removeprefix('gold_license_')}` {_num(v) if v >= 0 else '미빌드'}"
            for t, v in cores.items()))
    if nonzero:
        body.append(f"**상세(detail) 신규** ({len(nonzero)})\n"
                    + "\n".join(f"　◦ `{k}` · {_num(v)}행" for k, v in nonzero))
    if n_zero:
        body.append(f"**적재 0행** {n_zero}객체 — 이번 창 신규 버전 없음(정상)")

    desc = head + ("\n\n" + "\n".join(body) if body else "")
    icon = "⚠️" if drift else "✅"
    color = COLOR_WARN if drift else COLOR_OK
    title = f"{icon} [commerce] commerce_load_gold (gold 적재·Iceberg) — {len(loaded)}객체 {_num(total_rows)}행"

    pages = _paginate(desc, _MAX_DESC)
    for i, page in enumerate(pages, 1):
        t = title + (f" ({i}/{len(pages)})" if len(pages) > 1 else "")
        try:
            send_embed(t, page, color=color, domain=_DOMAIN)
        except Exception as exc:  # noqa: BLE001
            log.warning("[commerce] gold report 전송 실패(무시): %s", type(exc).__name__)
    counts = {"status": "ok", "loaded_rows": total_rows, "objects": len(loaded),
              "drift": drift, "pages": len(pages)}
    log.info("[commerce] gold report: %s", counts)
    return counts
