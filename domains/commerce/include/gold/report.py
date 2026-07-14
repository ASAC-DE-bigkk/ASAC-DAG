"""gold 적재 결과 → Discord 리포트(실행시간 + 카탈로그 + 객체별 적재행).

silver/collect 의 run_report(API별)와 달리 gold 는 **객체(테이블) 단위**라 별도 렌더.
'적재 0행'(이번 창에 신규 버전 없음 — 정상)만 요약하고, **행>0 은 생략하지 않는다**(run_report 와
동일 원칙). 길면 run_report._paginate 로 여러 임베드 분할.
"""
from __future__ import annotations

import logging

from commerce_core.run_report import _MAX_DESC, _fmt_elapsed, _num, _paginate

log = logging.getLogger(__name__)
_DOMAIN = "commerce"


def send_gold_report(*, catalog: dict | None, load: dict | None,
                     elapsed_seconds: float | None = None, error: str | None = None) -> dict:
    """catalog(build_catalog XCom) + load(load_gold XCom) → Discord. 반환: 요약 counts."""
    from common.discord import COLOR_FAIL, COLOR_OK, COLOR_WARN, send_embed
    from security import redact

    if error or catalog is None or load is None:
        # 실패(all_done 로 리포트는 항상 실행) — 어느 task 가 비었는지 명시
        miss = []
        if catalog is None:
            miss.append("build_catalog")
        if load is None:
            miss.append("load_gold")
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
    # **정확한 표기(재검증 #4)**: dim 3종(dataset/region/business_status)은 매 run **전량 delete+insert
    # 스냅샷**이라 그 행수는 '신규 적재'가 아니다. 이전엔 dim 스냅샷 행수를 신규 버전 적재와 합산해
    # (예: 신규 1건인데 region 스냅샷 1.5만행) 신규 데이터가 대량 유입된 것처럼 보였다. 이제 **신규
    # 버전 적재**(entity/history/detail = collected_at>워터마크 증분)와 **차원 스냅샷(전량 갱신)**을 분리한다.
    def _is_dim(k: str) -> bool:
        return k.startswith("commerce_dim_")

    new_loaded = {k: v for k, v in loaded.items() if not _is_dim(k)}   # 신규 버전(증분)
    dim_loaded = {k: v for k, v in loaded.items() if _is_dim(k)}       # 스냅샷(전량 갱신)
    new_rows = sum(new_loaded.values())
    dim_rows = sum(dim_loaded.values())
    nonzero = sorted(((k, v) for k, v in new_loaded.items() if v > 0), key=lambda x: -x[1])
    n_zero = sum(1 for v in new_loaded.values() if v == 0)             # 신규 없음(정상)은 증분 객체 기준
    drift = bool(catalog.get("drift"))
    skipped = load.get("skipped")

    head = (f"**카탈로그** v`{str(catalog.get('version', '?'))[:8]}` · "
            f"cluster {catalog.get('clusters', '?')}·single {catalog.get('singles', '?')}"
            f" · 실측 {catalog.get('datasets', '?')}종"
            + (" · ⚠️ **드리프트(신규 API/필드)**" if drift else "")
            + f"\n**신규 버전 적재** {_num(new_rows)}행 / {len(new_loaded)}객체"
            + (f" · 차원 스냅샷 {_num(dim_rows)}행/{len(dim_loaded)}종(전량 갱신)" if dim_loaded else "")
            + f" · hi=`{load.get('hi', '?')}`")
    if skipped:
        head += ("\n**⏭ 적재 0건 — 신규 없음(기적재만)**: silver 워터마크가 gold 마커 이하 → "
                 "적재·검증 생략(마커 기반 조기 스킵, 정상)")
    if elapsed_seconds is not None:
        head += f" · ⏱ {_fmt_elapsed(elapsed_seconds)}"

    def group(items, label):
        items = sorted(((k, v) for k, v in items if v > 0), key=lambda x: -x[1])
        if not items:
            return []
        return [f"**{label}** ({len(items)})"] + [f"　◦ `{k}` · {_num(v)}행" for k, v in items]

    body = []
    body += group([(k, v) for k, v in nonzero
                   if k in ("commerce_business_entity", "commerce_business_entity_history")],
                  "공통(신규 버전)")
    body += group([(k, v) for k, v in nonzero if k.endswith("_detail")], "상세(detail) 신규 버전")
    if n_zero:
        body.append(f"**적재 0행** {n_zero}객체 — 이번 창 신규 버전 없음(정상)")
    body += group(dim_loaded.items(), "차원 스냅샷(전량 갱신 · 신규 아님)")

    desc = head + ("\n\n" + "\n".join(body) if body else "")
    icon = "⚠️" if drift else "✅"
    color = COLOR_WARN if drift else COLOR_OK
    title = (f"{icon} [commerce] commerce_load_gold (gold 적재) — 신규 버전 {_num(new_rows)}행"
             + (f" · 차원 {_num(dim_rows)}행" if dim_rows else ""))

    pages = _paginate(desc, _MAX_DESC)
    for i, page in enumerate(pages, 1):
        t = title + (f" ({i}/{len(pages)})" if len(pages) > 1 else "")
        try:
            send_embed(t, page, color=color, domain=_DOMAIN)
        except Exception as exc:  # noqa: BLE001
            log.warning("[commerce] gold report 전송 실패(무시): %s", type(exc).__name__)
    counts = {"status": "ok", "loaded_rows": new_rows + dim_rows, "new_version_rows": new_rows,
              "dim_snapshot_rows": dim_rows, "objects": len(loaded), "drift": drift, "pages": len(pages)}
    log.info("[commerce] gold report: %s", counts)
    return counts
