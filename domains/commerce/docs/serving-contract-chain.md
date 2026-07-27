# Serving Contract 추적 (chain) — 도메인 공통 D1 서빙 계약

> **이 문서는 commerce 소유 계약이 아니다.** ASAC-DAG #478에서 확정된 **도메인 공통 Serving
> Contract**(어떤 Gold를 어떤 기준·규약으로 공용 Cloudflare **D1** 데이터 제품으로
> 승격·게시·등록하는가)를, commerce 에이전트가 **필요할 때 빠르게 따라가도록** 이슈 → 결정 →
> 정본 문서 → 구현 → 롤아웃 순으로 엮은 **추적(navigation) 문서**다. 규격 정본은 이 문서가 아니라
> ASAC-DAG `docs/contracts/serving-contract-v1.md`([링크](https://github.com/ASAC-DE-bigkk/ASAC-DAG/blob/dev/docs/contracts/serving-contract-v1.md)) —
> 이 문서는 그 정본으로 가는 길만 안내한다. 문서·구현이 어긋나면 정본이 우선.

## 0. 경계 — commerce 는 지금 이 계약의 대상이 아니다 (읽기 전 확인)

- **commerce gold 는 공용 D1 서빙 대상이 아니다.** commerce gold 는 카탈로그 구동 Python →
  **서빙 Postgres**(`commerce_load_gold`, 06:00 KST)로 적재된다([docs/pipeline/gold/README.md](pipeline/gold/README.md)).
  이 계약이 다루는 건 그와 별개인 **공용 Cloudflare D1**(외부 공개 API·`/catalog`) 계층이다.
- #478 대상 도메인은 **citydata · transit · culture · weather · traffic** 뿐이다. commerce 는 없다.
- 따라서 이 계약은 commerce 가 **미래에 어떤 gold 제품을 공용 D1(외부 공개 또는 내부 Agent용)에
  올리게 될 때** 준수해야 하는 **외부 계약**이다. 그 전까지 이 문서는 **참조·추적용**이며, commerce
  코드를 이 계약에 맞춰 지금 바꿀 필요는 없다. (작업 경계는 CLAUDE.md Working Scope 유지 —
  이 계약을 이유로 번들 밖 파일을 임의 수정하지 않는다.)
- commerce 에도 이미 `commerce_watchdog.py`가 있으나, 그건 commerce Postgres 서빙 감시다.
  이 계약의 §7.4 운영 감시(watchdog)는 **공용 D1** 대상 별개 책임이다. 혼동 금지.

## 1. 따라가는 순서 (5분 브리핑)

빨리 맥락만 잡으려면 이 순서로 3개만 읽으면 된다.

1. **문제가 왜 생겼나** → ASAC-DAG **#477**(닫힘, 장애): transit 골드가 D1에 적재됐는데
   `_catalog` 미등록으로 API 404. 적재 잡은 exit 0 — **조용한 실패**. "서빙 대상" 표시 필드가
   도메인마다 달랐던(`serving_tier`/`external`/`serving_gold_candidate`) 구조적 원인도 여기서 첫 지적.
2. **무엇을 정했나** → ASAC-DAG **#478**(결정 스레드). 아래 두 코멘트만 읽으면 결론이 다 있다:
   - [v1 최종 결정](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/478#issuecomment-5056366122) (2026-07-23)
   - [v1.1 운영 보강](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/478#issuecomment-5065980055) (2026-07-24, 멘토 피드백 반영)
3. **규격이 뭐고 어떻게 쓰나** → **정본 문서** [`docs/contracts/serving-contract-v1.md`](https://github.com/ASAC-DE-bigkk/ASAC-DAG/blob/dev/docs/contracts/serving-contract-v1.md)
   (필드 정의 §3 · Publication 파이프라인 §7 · 예제 §10) + **공통 Publisher** [`common/serving/README.md`](https://github.com/ASAC-DE-bigkk/ASAC-DAG/blob/dev/common/serving/README.md).

더 깊이 들어갈 때만 아래 §2~§5 트레일을 따라간다.

## 2. 결정 트레일 (issue)

| # | repo | 상태 | 무엇 | 이 계약과의 관계 |
|---|---|---|---|---|
| **#478** | ASAC-DAG | OPEN | **도메인 공통 Serving Contract v1** 결정 스레드 | **허브.** v1(#c-5056366122)·v1.1(#c-5065980055) 두 결정이 여기서 확정 |
| #477 | ASAC-DAG | CLOSED | D1 적재됐으나 `_catalog` 미등록 → 404 (조용한 실패) | **근본 원인 이슈.** Publication 원자화(§7)·등록 자기검증(§7.1)의 동기 |
| #476 | ASAC-DAG | OPEN | 공개 API 진입점 통합 — Gateway/Worker 구조·URL·과금·빈데이터 게이트 | **인접·비범위.** `api_path`/필터/limit 은 이쪽 소유(§3.3). 빈데이터 게이트 ④ → `zero_policy`(§5.1)로 수용 |
| #201 | ASAC-DAG | CLOSED | KOPIS 간헐 400 → 300/1,692행만 수집 후 exit 0 (조용한 절단) | **실증.** `partial_policy`(§5.2)의 근거 |

결정 스레드 참여·요지(코멘트 순): masondev1024(제안·정리) → yooseongjin527(culture: 필드 축소·이중선언 금지) →
kang-gyeongmin(`enabled`≠`external` 분리·`product_question` 필수) → **Exisign(전체 동의)** →
ehddnr301(멘토: `refresh`→cron 명확화 = `publication_trigger`) → masondev1024(v1.1 잠정안).

## 3. 정본·구현 트레일 (문서 + 코드) — 전부 `dev` 머지 (2026-07-27)

| 산출물 | repo · 경로 | 이슈 → PR | 핵심 commit |
|---|---|---|---|
| **정본 계약 문서** | ASAC-DAG `docs/contracts/serving-contract-v1.md` | #497·#505 → **PR #499** | `7a6952e3e`(v1) · `42a1ef36f`(v1.1) |
| **공통 D1 Publisher** | ASAC-DAG `common/serving/` (`contract·gate·d1_client·publisher·runtime·dag_factory`) | #498·#505 → **PR #503** | `4a7499238`(초판) · `108c2da38`·`f4332b6b8`(`domains/common`→루트 `common/serving` 이동) |
| **계약 Validator + CI Gate** | ASAC-DBT `serving_contract/`(`validator·schema.yml·cli`) + `.github/workflows/serving-contract-gate.yml` | #336 → **PR #337** | `3f83391e7`(v1) · `7dc797db4`(v1.1 조건부 필수) |

- 공통 Publisher 는 **루트 `common/serving/`** 에 산다 — commerce 가 이미 쓰는 저장 공유 패키지
  `dags/common`(예: `from common.storage import ...`, CLAUDE.md §19)의 형제 서브패키지다.
  로컬 체크아웃에는 서브모듈 포인터가 이 머지 이전이라 아직 없을 수 있다 → **GitHub `dev` 기준**으로 본다.
- 3주체 책임 경계(정본 §2): **dbt YAML = 정적 계약** / **Validator = merge 전 CI 차단** /
  **Export DAG(공통 Publisher) = 실측·게이트·D1 write·`_catalog` upsert·smoke** / **Gateway = API 형태(#476)**.

## 4. 적용·롤아웃 트레일 (도메인이 실제로 붙는 곳)

| # | repo | 상태 | 무엇 |
|---|---|---|---|
| #520 | ASAC-DAG | OPEN | `culture_serving_export` DAG — 외부 gold 7종 D1 게시. **공통 Publisher `build_serving_export_dag` factory 소비자 1호** (스냅샷 7종, A안) |
| #508 → PR #509 | ASAC-DAG | OPEN | citydata 파일기반 관측(record_run→R2) + 품질 집계 + OOM transform 티어 게이트 |
| #516 → PR #517 | ASAC-DAG | OPEN | citydata 관측 digest·서빙 신선도·hourly 게이트 안정성 3건 |
| #324 | ASAC-DBT | OPEN | Traffic D1 서빙 후보 3종 v1 물리 계약 승격 |
| #327·#328·#314 | ASAC-DBT | CLOSED/OPEN | Weather D1 서빙 후보 v1 승격 (#314 = 팀 첫 enforced contract) |
| #318 → PR #319 / #325 → PR #326 | ASAC-DBT | MERGED | culture `meta.display`(전시 문구) / `meta.refresh`(→`publication_trigger`로 수렴) 선례 |
| #331 | ASAC-DBT | MERGED | transit 골드 14종 `serving_tier`·`refresh` meta 부여 (구 규약 → 마이그레이션 대상) |

**패턴(commerce 가 참고할 원형)**: 계약 선언(dbt `meta.serving.*`) 은 도메인 dbt 이슈가,
DAG 배선·라이브 게시 검증은 짝 이슈가 담당 — **#520(DAG) + 짝 ASAC-DBT 계약 이슈**가 그 원형이다.

## 5. 실증 근거 (왜 이 게이트들이 이론이 아닌가)

- **`_catalog` 자기검증(§7.1)** ← #477: transit 4종 적재 정상인데 404. exit 0 조용한 실패.
- **`zero_policy`(§5.1)** ← Weather PR #314 / #476 부록: `contract.enforced`+`replace` 는 재빌드마다
  커밋 2개(빈 테이블→행 삽입), 그 사이 export 가 읽으면 "정상 응답 0행"을 박제. citydata `place_latest`
  실측 노출 2.69%(A-B-A 대조).
- **`partial_policy`(§5.2)** ← ASAC-DAG #201 / culture facility: KOPIS 400 으로 1,692행 중 300행만
  수집 후 exit 0 — 볼륨 계약(`rows < 80% baseline`)만이 절단을 잡음.

## 6. commerce 가 공용 D1 서빙에 참여하게 될 때 (compliance 체크)

> **필드 매핑 정본**: commerce 각 gold 모델이 `meta.serving.*` 에서 어떤 값을 갖는지·어떻게 세팅되는지는
> dbt 번들 [`docs/DB/gold/serving-contract-mapping.md`](https://github.com/ASAC-DE-bigkk/ASAC-DBT/blob/dev/domains/commerce/docs/DB/gold/serving-contract-mapping.md)
> 에 확정돼 있다(22 gold 중 지금 선언 대상은 d1_direct 15개, product_id·grain·primary_key·
> publication_mode·zero_policy·publication_trigger 값 + PK 근거 테스트 요건). **현재 commerce D1
> export 는 미구현이고 CI 게이트에도 걸리지 않는다**(선언 0 = PASS) — 아래는 export 를 짓는 시점의 절차.

그날이 오면(어떤 commerce gold 를 외부 공개 API·`/catalog` 또는 내부 Agent용 D1 제품으로 올릴 때):

1. **먼저 정본을 읽는다** — `docs/contracts/serving-contract-v1.md` §3(필드)·§7(Publication).
2. **자체 Publisher 를 새로 만들지 않는다** — `common/serving`의 `build_serving_export_dag`
   factory 를 소비한다(#520 culture 사례가 원형). D1 write·`_catalog` upsert·검증·smoke·last-good
   유지는 공통 모듈이 강제한다.
3. **계약을 dbt `meta.serving.*` 로 선언**하고 **ASAC-DBT Validator/CI Gate** 를 통과시킨다.
   필수 9필드(`enabled·external·product_id·product_question·grain·primary_key·publication_mode·
   zero_policy·publication_trigger`) + `event_time` 선언 시 `freshness_slo_minutes` 조건부 필수.
   — commerce gold 가 dbt 가 아니라 Python 이라면, 계약 표현/검증을 어디에 둘지(정본 §2 책임 경계)를
   **먼저 결정 이슈로 올린다**. 임의로 commerce 안에 서빙 규약을 재발명하지 않는다.
4. **작업 경계 유지** — 계약은 호스트(ASAC-DAG) 소유다. commerce 는 **소비자**로서 자기 도메인
   스키마·DAG 안에서만 배선하고, 공용 계약 문서·Publisher 를 고쳐야 하면 **결정 이슈(#478 계열)로
   제안**한다(직접 수정 금지 — CLAUDE.md Working Scope).

## 7. 이 문서 갱신 규칙

- 이 문서는 **추적 인덱스**다. 계약 규격을 여기 복붙하지 않는다 — 정본(§1의 3번)만 링크한다.
  규격이 궁금하면 정본을 열고, 이 문서는 "어디를 보라"만 유지한다.
- #478 계열에 **새 결정·롤아웃·commit** 이 생기면 §2~§4 표에 한 줄 추가한다(상태/PR/commit).
- v2(비호환 변경, 정본 §8)가 나오면 §1 의 정본 링크와 §6 필수필드 목록을 갱신한다.
- commerce 가 실제로 D1 서빙에 착수하면 이 문서는 "추적"에서 "commerce 서빙 운영" 문서로 승격될 수
  있다 — 그때 change-log.md 에 구조 변경으로 기록한다.

## 관련

- 정본: ASAC-DAG [`docs/contracts/serving-contract-v1.md`](https://github.com/ASAC-DE-bigkk/ASAC-DAG/blob/dev/docs/contracts/serving-contract-v1.md)
- 공통 Publisher: ASAC-DAG [`common/serving/README.md`](https://github.com/ASAC-DE-bigkk/ASAC-DAG/blob/dev/common/serving/README.md)
- Validator: ASAC-DBT [`serving_contract/`](https://github.com/ASAC-DE-bigkk/ASAC-DBT/tree/dev/serving_contract)
- 허브 이슈: [ASAC-DAG #478](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/478)
- 번들 내: [docs/pipeline/gold/README.md](pipeline/gold/README.md) · [CLAUDE.md](../CLAUDE.md) · [Share.md](../Share.md)
