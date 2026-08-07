# Serving 계약 추적 — commerce 는 org 공통 계약(#478)을 따른다

> **commerce 의 D1 서빙 정본은 ASAC-DAG#478 Serving Contract v1/v1.1 이다.** commerce 는
> 2026-07-22 그 스레드에서 채택에 동의했고, gold 22종이 확정 필드를 선언하고 있다. 그 위에
> commerce 전용 확장 필드를 **같은 `meta.serving` 블록 안에** 얹는다. 이 문서는 ①무엇을 선언하고
> ②누가 소비하며 ③어디를 봐야 하는지를 추적한다.
>
> ⚠️ **이 문서는 2026-08-05 에 전면 정정됐다.** 이전 판은 "commerce 는 자체 규약으로 관리하며
> #478 에 강제 종속되지 않는다"고 적고 있었는데, 코드와 정반대였다. 경위는 §4.

## 1. 선언 — dbt `config.meta.serving`

`ASAC-DBT domains/commerce/models/gold/_commerce_gold__models.yml`, gold 22종. (2026-08-05 실측)

| 구분 | 필드 | 선언 |
|---|---|---|
| **#478 v1 필수** | `enabled` · `product_id` · `contract_version` · `grain` · `primary_key` · `publication_mode` · `zero_policy` | **22/22** |
| **#478 v1.1** | `publication_trigger` 22 · `event_time` 3 · `freshness_slo_minutes` 3 | 조건부 필수 충족 |
| **#478 선택** | `product_question` 22 · `shape` 22 · `partial_policy` 21 | — |
| **계약 v1.10** | `display`(title·summary + 선택 caveat·use_cases) | **22/22** (2026-08-07, #706) |
| **외부 공개** | `external` | 22/22 |
| **commerce 확장** | `serving_tier` · `d1_table` · `d1_display` · `usage_patterns` · `source_evidence` · `quality_coverage` · `public_projection` · `public_primary_key` | — |

실제 값: `publication_mode: snapshot` 22/22(#478 enum `snapshot|upsert|append` 안),
`zero_policy: retain_last_good` 22/22.

**확장은 같은 블록 안에 얹는다 — 별도 네임스페이스(`meta.commerce_serving` 등)를 만들지 않는다.**
#478 이 금지한 것은 *다른 이름의 규약을 병행 선언하는 것*(이중 선언)이지 확장 자체가 아니다.

### commerce 확장 `d1_display` — 한 모델이 여러 D1 제품을 낳을 때

`gold_license_geo_grid` 하나가 `d1_geo_grid_overview`·`d1_geo_grid_detail` 두 제품이 된다.
계약 필드 `display` 는 모델당 하나뿐이라 그대로 두면 **두 제품이 같은 제목으로 화면에 나란히
선다.** 그래서 `usage_patterns[].d1_table` 라우팅과 같은 방식으로 d1_table 별 덮어쓰기를 둔다.

덮어쓰기를 `display` **안**에 못 넣는 이유: 계약 validator 가 display 하위 키를 스펙 밖이면
오타로 잡는다(`display_unknown_field`). 그래서 형제 키다. 1:1 제품은 이 키가 없다.

### commerce 확장 `serving_tier`

원장 규모에 따라 D1 게시 형태를 가른다. #478 필드를 대체하는 게 아니라 **보완**한다.

| tier | 뜻 | D1 |
|---|---|---|
| `d1_direct` | 소형 — 전량 교체 스냅샷 | 게시 |
| `d1_rollup` | 원장 대용량 — export 시 GROUP BY 로 화면 축만 사전 롤업 | 파생만 게시(세부는 Trino 폴백) |
| `iceberg_api` | 원장 초대용량 | **금지** — Trino 직조회 |

## 2. 소비 — dags `commerce_serving_export`

`include/gold/serving_export.py`. gold 완료 Asset(`iceberg://commerce/gold`) 트리거로 도는 **분리 DAG**
(`commerce_load_gold` 와 별개).

- `meta.serving.*` 를 읽어 `_catalog` 15컬럼(`product_id`·`external`·`product_question`·`event_time` 등)을
  채운다 — **선언이 곧 게시 결과**다.
- commerce 소유 `d1_*` 테이블만 DROP+CREATE. 공유 `_catalog`/`_request_log`/`d1_meta` 는
  **upsert(DROP 금지 — transit 규약 승계)**.
- 핸드오프 보조 5종(`d1_catalog_columns`/`_ext`/`d1_usage_patterns`/`d1_catalog_display`/
  `d1_catalog_glossary`)은 자연키 upsert. **commerce 는 공용 `publish_product_meta` 를 쓰지 않고
  자체 `_publish_handoff` 로 게시한다** — 계약에 새 표가 들어와도 이 파일을 함께 고치지 않으면
  한 행도 안 나간다(#706 때 실제로 그랬다. 다른 도메인은 "선언뿐"이지만 commerce 는 아니다).
- 스왑 전 **행수 밴드 게이트**(0행/2배 → `stale`, 직전 게시 유지). 상태 마커는 R2
  `commerce_serve_state/_export_state.json`(silver/bronze state 와 대칭).
- `SERVING_SPEC`(export 쪽 목록) ↔ dbt 선언은 실행 시 대조되어 어긋나면 경보(파이프라인은 진행).
- 보안: `security.http_post`, 토큰 `CLOUDFLARE_API_TOKEN`.

### 외부 공개를 내릴 때

`external: false` **하나만** 바꾼다(ASAC-DBT#434 가 요청하는 방식). gold 테이블·파이프라인은 그대로
두고 export 를 한 번 돌리면 `_catalog.external` 이 0 이 되어 카탈로그에서 빠진다.
**데이터를 지우는 조치가 아니다.**

## 3. org 공통 계약 #478 — 정본 위치

- **규격 정본**: ASAC-DAG [`docs/contracts/serving-contract-v1.md`](https://github.com/ASAC-DE-bigkk/ASAC-DAG/blob/dev/docs/contracts/serving-contract-v1.md)
- **공통 Publisher**: ASAC-DAG [`common/serving/`](https://github.com/ASAC-DE-bigkk/ASAC-DAG/tree/dev/common/serving)
  · **Validator/CI Gate**: ASAC-DBT [`serving_contract/`](https://github.com/ASAC-DE-bigkk/ASAC-DBT/tree/dev/serving_contract)
- **트레일**: 허브 [#478](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/478)
  ([v1 결정](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/478#issuecomment-5056366122) ·
  [v1.1](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/478#issuecomment-5065980055))
  ← #477(등록 누락 장애) · #476(진입점) · #201(부분 절단 실증). 정본·Publisher·Validator 는
  2026-07-27 dev 머지(PR #499·#503·#337). **#478 은 2026-07-30 `COMPLETED` 로 닫혔다.**
- `serving-contract-gate` CI 는 `config.meta.serving` 이 있는 **모든** 모델을 #478 규격으로 검사한다 —
  commerce 는 확정 필드를 갖췄고 `publication_mode` 도 enum 안이므로 **통과 대상**이다.

## 4. 왜 이 문서를 정정했나 (같은 사고 반복 방지)

```
2026-07-22  #478 발행 → commerce "전체 동의합니다"
2026-07-23  gold 22종에 meta.serving 선언                       ← 약속 이행
2026-07-27  같은 내용을 한 번 더 적용 → config.meta.serving 블록 충돌
2026-07-27  중복 철회(맞음) + 이 문서·CLAUDE.md 를 "자체 관리, #478 비종속"으로 재서술(과잉)
2026-07-28  PR#335 머지 · 22종을 #478 확정 필드 규격으로 재작성   ← 코드는 정반대로
2026-07-30  #478 CLOSED / COMPLETED
2026-08-05  문서 정정(이 판)
```

로컬 충돌 범위는 **YAML 한 파일**이었는데 결론은 **"org 계약 비적용"** 까지 갔다. 그리고 그 문서가
9일간 살아 있으면서, #434(외부 공개 정리 요청) 같은 org 차원 조치에서 commerce 만 빠지는 근거로
쓰일 뻔했다.

> **로컬 충돌은 충돌만 되돌린다. 거버넌스 문서를 함께 고치지 않는다.**
> 문서와 코드가 갈릴 때는 **코드를 실측해 문서를 맞춘다** — 반대가 아니다.

이전 판이 근거로 들었던 주장은 모두 무효다.

| 이전 판 주장 | 실측(2026-08-05) |
|---|---|
| "#478 필수필드가 없다" | v1 필수 7종 **22/22** 선언 |
| "`publication_mode: iceberg/rollup` 이 enum 밖" | 실제 값은 `snapshot` **22/22** — enum 안 |
| "export 가 #478 필드를 읽지 않는다" | `serving_export.py` 가 읽어 `_catalog` 를 채움 |

## 5. 이 문서 갱신 규칙

- 규격을 여기 복붙하지 않는다 — 정본(§3 링크, dbt `docs/DB/gold/serving-design.md`)만 링크한다.
- 선언 필드가 바뀌면 §1 표를 **실측으로** 갱신한다(모델 수까지). 선언과 문서가 갈리면 문서가 틀린 것이다.
- #478 이 v2 로 가면 §3 트레일에 링크를 추가하고 §1 정합을 다시 실측한다.

## 관련

- 설계 정본: dbt `domains/commerce/docs/DB/gold/serving-design.md`(tier 분류 · 22 gold → 6 화면 매핑)
- 계약 이력: ASAC-DBT #334 → **PR#335(머지 2026-07-28)** · export ASAC-DAG #493 → PR#494
- 번들 내: [CLAUDE.md](../CLAUDE.md) §19.2 · [Share.md](../Share.md)
