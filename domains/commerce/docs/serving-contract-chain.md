# Serving 계약 추적 — commerce 자체 관리 + org 공통 계약(#478) 참고

> **commerce 의 D1 서빙은 commerce 안에서 자체 규약으로 관리한다.** 타 도메인(culture 등) 방식을
> 따라갈 필요 없다. 이 문서는 ① commerce 자체 서빙(정본)과 ② 별개로 존재하는 org 공통 계약
> ASAC-DAG #478(참고)을 구분해 정리한다. commerce 작업 시 정본은 **§1**, org 계약은 **§2(참고)**.

## 1. commerce 자체 D1 서빙 (정본 — self-managed)

commerce gold(Iceberg) → 공유 Cloudflare **D1(SQLite)** 선별 export. **미구현이 아니라 PR 로 진행 중.**
기존 serving Postgres 경로는 폐기(2026-07-14).

| 축 | 위치 | 내용 |
|---|---|---|
| **설계 정본** | dbt `domains/commerce/docs/DB/gold/serving-design.md` | tier 분류(iceberg_api/d1_rollup/d1_direct) · 22 gold → 6 화면 매핑 · 서빙 계약 |
| **계약(dbt)** | ASAC-DBT **#334 → PR #335** (`feat/334-commerce-serving-tier`) | gold `config.meta.serving.serving_tier` + `d1_table`·`publication_mode`(iceberg/rollup)·`product_id`(gold_*)·`product_question` |
| **export(dags)** | ASAC-DAG **#493 → PR #494** (`feat/493-commerce-serving-export`) | `commerce_serving_export` — gold Asset 트리거 **분리 DAG**, `include/gold/serving_export.py` |

**commerce 자체 규약 요지**:
- `serving_tier` = `d1_direct`(소형 → D1 전량 교체 스냅샷) · `d1_rollup`(원장 대용량 → export 시
  GROUP BY 로 화면 축만 사전 롤업한 소형 파생만 D1, 세부는 Trino 폴백) · `iceberg_api`(원장 초대용량
  → D1 금지, Trino 직조회).
- export = gold 빌드(`commerce_load_gold`)와 **분리된 DAG**, gold 완료 Asset(`iceberg://commerce/gold`)
  트리거로 기동. commerce 소유 `d1_*` 테이블만 DROP+CREATE, 공유 `_catalog`/`_request_log`/`d1_meta`
  는 upsert(DROP 금지 — transit 규약 승계). 스왑 전 **행수 밴드 게이트**(0행/2배 → `stale` 직전본 유지).
- 상태 마커: R2 `commerce_serve_state/_export_state.json`(silver/bronze state 대칭). 보안: `security.http_post`,
  토큰 `CLOUDFLARE_API_TOKEN`. **전부 commerce 번들 자립** — 공유 패키지 강제 소비 없음.
- 범위 밖(후속): dim 4종 · current-period 2종(`agg_license_daily/monthly`) · D1 스왑 원자성(`*_next`).

> commerce 서빙을 이어서 작업할 때는 위 3개(설계·#335·#494)만 보면 된다. 타 도메인 참조 불필요.

## 2. org 공통 Serving Contract #478 (참고 — commerce 정본 아님)

commerce 와 **별개로**, 여러 도메인이 공유 D1 서빙을 통일하려고 만든 org 공통 계약이 있다. commerce 는
이 계약에 강제 종속되지 않으나, 같은 공유 D1·`_catalog` 를 쓰므로 존재를 알아 둔다.

- **규격 정본**: ASAC-DAG [`docs/contracts/serving-contract-v1.md`](https://github.com/ASAC-DE-bigkk/ASAC-DAG/blob/dev/docs/contracts/serving-contract-v1.md)
  — `meta.serving.enabled/external/product_id/grain/primary_key/publication_mode(snapshot|upsert|append)/
  zero_policy/publication_trigger` (commerce 의 `serving_tier` 규약과 **다른 스키마**).
- **공통 Publisher**: ASAC-DAG [`common/serving/`](https://github.com/ASAC-DE-bigkk/ASAC-DAG/tree/dev/common/serving)
  · **Validator/CI Gate**: ASAC-DBT [`serving_contract/`](https://github.com/ASAC-DE-bigkk/ASAC-DBT/tree/dev/serving_contract).
- **트레일**(따라가려면): 허브 [#478](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/478)
  ([v1 결정](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/478#issuecomment-5056366122)·
  [v1.1](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/478#issuecomment-5065980055)) ← #477(등록 누락 장애)·
  #476(진입점)·#201(부분 절단 실증). 정본·Publisher·Validator 는 2026-07-27 dev 머지(PR #499·#503·#337).

## 3. #478 과 commerce 의 관계 (중요 — 정합 주의)

- commerce 는 **자체 `serving_tier` 규약**을 쓴다. #478 의 `meta.serving.enabled/…` 를 gold 모델에
  중복 선언하지 않는다(같은 `config.meta.serving` 블록 충돌 + export 가 읽지 않음).
- **⚠ CI 게이트 정합**: ASAC-DBT `serving-contract-gate` 는 `config.meta.serving` 이 있는 **모든** 모델을
  #478 규격으로 검사한다. commerce 의 `serving_tier` 블록(#335)은 #478 필수필드가 없고
  `publication_mode: iceberg/rollup` 이 #478 enum 밖이라 **게이트에 걸릴 수 있다.** 이 정합은
  **commerce 자체 PR(#335 계열)에서 결정**한다 — 게이트 예외 요청 / 네임스페이스 분리(예:
  `meta.commerce_serving`) / #478 로 수렴 중 택1. **결론을 이 문서가 강제하지 않는다.**
- 즉 commerce 는 org 계약을 "따라가는" 게 아니라, **공유 자원(D1·`_catalog`) 정합만** 맞추고 나머지는
  자체 관리한다.

## 4. 이 문서 갱신 규칙

- commerce 서빙 규격은 여기 복붙하지 않는다 — 정본(§1 의 serving-design.md·#335·#494)만 링크한다.
- #494/#335 가 머지되면 §1 표의 PR 상태를 갱신한다.
- #478 정합 결정(§3)이 commerce PR 에서 나면 그 결론을 §3 에 한 줄로 기록한다.

## 관련

- commerce 자체: dbt `docs/DB/gold/serving-design.md` · ASAC-DBT #334/PR#335 · ASAC-DAG #493/PR#494
- org 참고: ASAC-DAG [#478](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/478) · [`docs/contracts/serving-contract-v1.md`](https://github.com/ASAC-DE-bigkk/ASAC-DAG/blob/dev/docs/contracts/serving-contract-v1.md)
- 번들 내: [CLAUDE.md](../CLAUDE.md) §19.2 · [Share.md](../Share.md)
