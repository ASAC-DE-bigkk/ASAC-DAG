# raw 증분화 — UPDATEDT·LASTMODTS 정렬 · 검증키 · diff (feat/58, #193)

매 수집이 전체 데이터를 다시 저장하던 것을, **정렬본 기준으로 전날과 다른 신규 row만** 저장하도록
바꾼다. 코드: [../../../include/bronze/incremental.py](../../../include/bronze/incremental.py) ·
배선: [../../../include/bronze/bronze_tasks.py](../../../include/bronze/bronze_tasks.py) ·
경로: [../../../include/commerce_core/paths.py](../../../include/commerce_core/paths.py).

## 1. 저장 모델 (랜딩 → 증분 → diff 이동, 수집일 태깅)

구 데이터 소실·버전이력 유실을 막으면서 full 은 **한 벌만** 유지한다:

| 계열 | 위치 | 내용 |
|---|---|---|
| **landing(임시)** | `…/run_id=<ts>/_full/<short>.jsonl` | 오늘 정렬 full 의 랜딩 — 비교/이동 **전에 먼저 저장**(중단돼도 수집분 보존). 완료 시 diff 로 **이동**되어 사라짐(잔존 = 그 run 중단의 증거) |
| **save(증분 영구)** | `…/run_id=<ts>/<short>.jsonl` (run 폴더) | 첫 수집=전체, 이후=신규/변경분만. **삭제 안 함**(이력 보존) |
| **diff-target(최신 full)** | `ops/control/state/commerce/diff_target/<short>.<수집일>.jsonl` (+ 같은 이름 `.key`) | 다음 수집의 비교 기준(정렬 full). landing 에서 **이동**해 옴. **파일명 수집일(YYYY-MM-DD)로 완료/중단 구분** — 교체 시 구 날짜 파일 삭제 |

- 첫 수집: landing → save(run 폴더, full)와 diff-target 에 **같은 내용(전체 정렬본)** 반영.
- 이후 매 수집: ① landing 저장 → ② diff-target 과 비교(§3) → ③ **다른 내용만 save 로 증분 저장**
  → ④ 구 날짜 diff 삭제 + landing 을 오늘 수집일 태깅으로 diff 에 **이동**.
  identical(검증키 동일)이어도 ④는 수행 — diff 파일명 날짜 = **최신 완료 수집일**.
- 실패 복구: ① 후 중단 = landing 잔존(수집분 보존) + 구 diff 유지 → 재실행 시 구 diff 와 재비교.
  ④ 도중 중단 = 신·구 날짜 diff 잠시 공존 → 발견(`find_diff_target`)이 최신 날짜를 선택(자가 복구).
- 저장 포맷: **row-NDJSON**(줄당 레코드 1개, UPDATEDT→LASTMODTS desc 정렬). (기존 page-NDJSON 에서 전환.)

## 2. 정렬 (외부 병합 정렬, 스트리밍)

- 정렬키 = **(UPDATEDT desc → LASTMODTS desc → OPNSFTEAMCODE → MGTNO)**. 각 타임스탬프를 14자리
  정수(YYYYMMDDHHMMSS)로 치환해 **내림차순**(최신 먼저). **UPDATEDT 가 없거나 비정형이면(None 케이스,
  #193) LASTMODTS 로 폴백**해 최신순 위치에 둔다(예전엔 UPDATEDT 없으면 0=최하단). 2순위
  LASTMODTS(desc)는 UPDATEDT 동률의 tie-break. 3·4순위는 **업소 식별키 (OPNSFTEAMCODE, MGTNO)**
  — MGTNO 는 발급 자치단체 안에서만 유니크라 **단독 사용 시 다른 구청의 별개 업소가 같은 키로
  충돌**(중복/이력 매핑 오류)하므로 OPNSFTEAMCODE 를 포함한다. → silver 그레인 (dataset,
  opnsfteamcode, mgtno)·정렬 `coalesce(updatedt_ts, lastmodts_ts, epoch) desc, lastmodts desc` 와 일치.
- **v1/v2 정본 해석(#65)**: 기본 데이터셋의 정렬·식별키는 원본 row 에서 뽑되 `schemas.canonical_get` 으로 **v1 정본 키가
  없으면 v2 별칭**(`DATA_UPDT_YMD`·`LAST_MDFCN_YMD`·`OGDP_INST_CD`·`MNG_NO`)을 해석한다. 안 하면
  v2(환경 13종)는 v1 키가 전무해 sort_key 가 **전부 `(0,0,'','')` 로 붕괴** → 정렬이 페이지네이션(무순서)
  으로 무너지고, 같은 데이터도 매 수집이 위치 어긋남으로 **전량 신규(오탐)** 방출(수질오염·대기배출
  매일 수천 건 오탐의 원인이었다).
- **통신판매업 스키마 전환(2026-08-04)**: registry에 `format: v2`, `canonical_format: v1`을 선언한다.
  API에서 받은 v2 25개 키를 검증된 v1 25개 키로 **값 변경 없이 1:1 치환한 다음** 정렬·normalize·
  검증키·diff를 수행한다. source/canonical 형식과 매핑 버전은 completed 마커에 남는다. 필드 누락·추가·
  혼합은 실패시켜 미확인 스키마를 조용히 저장하지 않는다. 이 데이터셋은 timestamp 갱신 보장을 전제로
  한 조기 중단을 끄고 전체 정렬본을 비교하므로, 기적재 행은 다시 저장하지 않고 실제 변경 행만 save한다.
- **전량 RAM 금지** → `external_merge_sort`: 청크를 임시파일로 쓰고 `heapq.merge` 로 병합(스트리밍·바운디드 RAM).
  비교정렬 하한 O(n log n). (정수키라 이론상 radix O(n) 가능하나, 외부 정렬 견고성/단순성으로 병합 채택.)

## 3. 검증키 & diff

- **검증키**(`verification_key`) = 정렬본 row 정규화(JSON key정렬) 문자열들을 순서대로 이어 sha256(순서 민감).
  오늘 키 == diff-target 키 → **동일**(증분 없음, 마커만 — 단 diff 파일명 날짜는 오늘로 롤링).
- **diff**(`diff_new_rows`) = 오늘·전날 둘 다 같은 키로 정렬 → **스트리밍 병합**으로 신규/변경 row만 방출.
  같은 정렬키 위치는 정규화 문자열 **직접 비교**(hot loop 에 해시 안 씀).
- **비교 조기 중단**(`stop_on_aligned_match`): 기본값은 UPDATEDT/LASTMODTS desc 정렬이라 신규/변경 row 는 항상
  위쪽에 온다 → 정렬 프런티어에서 **같은 정보(키+내용)가 처음 일치하는 순간 비교를 중단**(이하 동일 간주).
  전제: 내용이 바뀌면 UPDATEDT **또는 LASTMODTS** 가 갱신된다. 둘 다 그대로면서 내용만 바뀌는
  이상치는 이 모드에서 감지되지 않음 — 파일 단위 동일/상이는 검증키가 판정. `mail_order_sale`은 이
  소스 보장이 공식 계약으로 확인되지 않았으므로 `False`로 실행해 끝까지 비교한다.

## 4. 수집 흐름 (bronze_tasks)

- **수집 완료(status==ok)일 때만** 처리: 수집 페이지 → row 파싱 → registry 선언에 따른 가역 키 정본화
  → `incremental_store`
  (landing 저장 → 이전 diff 발견/다운로드 → 비교(조기 중단) → 증분 업로드 → diff 이동+키 사이드카).
- **중간 중단(status!=ok)은 증분/이동을 수행하지 않는다** — 구 날짜 diff 가 그대로 남아
  파일명 날짜로 "그 API 는 오늘 완료 안 됨"이 식별된다. 마커에
  `verification_key/increment_mode/increment_count/sorted_row_count/diff_target_key` 기록.
- 이전 diff 발견은 `find_diff_target`(`<short>.` 접두 나열 → 최신 날짜 선택, 구형 무날짜도 인식).

## 5. step0 (1회성 시드)

- `seed_diff_target`: 기존 수집물로 diff-target + 검증키를 미리 시드 → 다음 첫 수집이 곧바로 diff 가능.
- **미실행이어도** 첫 수집이 `mode=first` 로 전체를 save+target 생성(self-seed)하므로 필수는 아님.

## 6. 검증

- 단위테스트 **21 통과**([../../../tests/test_incremental.py](../../../tests/test_incremental.py)):
  다중청크 정렬·순서민감 검증키·diff(identical/new-head/changed/deleted)·파일브리지·orchestration
  (first→identical→changed)·step0 시드→identical + **#193**(UPDATEDT None→LASTMODTS 폴백 정렬·
  LASTMODTS 2순위 tie-break·resort 재정렬 멱등).
- **end-to-end 라이브 검증 완료**(실 Seoul API, 격리 프리픽스 `_verify58` → 검증 후 전량 삭제):
  run1=first(row-NDJSON) → run2 동일=identical(증분 미생성) → 변경분=changed(변경/신규만 증분, diff-target 롤링).
  **이력 보존**: 이전 run 증분 유지 + 같은 업장의 원본·변경 두 버전 공존(이력 추적 가능). 실 bronze 무오염.
- **사이드 이펙트**: 기존 page-NDJSON 과 신규 row-NDJSON **형식 혼재**(이력 손실 아님 — 구 run 보존).
  실운영 첫 수집은 `_diff_target` 미존재라 mode=first 전체 저장(자가 시드); step0 사전 시드 시 첫 수집부터 diff.
- **다운스트림**: bronze 가 row-NDJSON 으로 바뀌어, page 파서를 쓰는 소비자(dbt 로더 등)의 row 기준
  조정은 feat/58 밖(별도).

## 7. 정렬키 변경 시 재정렬 마이그레이션 (#193)

정렬키를 바꾸면(예: UPDATEDT→LASTMODTS 폴백 추가 #193, **v1/v2 별칭 해석 추가 #65**) **검증키도
바뀌므로**, 기존 diff-target 을 새 규칙으로 **1회 재정렬**해야 다음 수집의 diff 정렬이 정합한다(안 하면
새 정렬 today ↔ 옛 정렬 prev 가 어긋나 diff 부정확). 배포 순서: **로직 반영 → 재정렬 → 스케줄 재개.**
(#65 는 v2 환경 13종만 정렬키가 바뀌므로 그 종만 `changed=RESORT`, v1 139종은 `ok`(no-op).)

```bash
docker compose exec airflow-scheduler python -m bronze.resort --dry-run   # 대상/변경여부 점검
docker compose exec airflow-scheduler python -m bronze.resort             # 재정렬 적용
```

- `resort_diff_target`([incremental.py](../../../include/bronze/incremental.py)): 각 diff-target 을
  현재 `sort_key` 로 재정렬(외부 병합 정렬) + 검증키 갱신. **내용(row 집합) 보존, 순서/검증키만 변경.**
- **멱등**: 이미 새 규칙으로 정렬돼 있으면 no-op(`changed=False`). dev(`iceberg_dev`/`seoul-dev`)에서
  먼저 돌려 확인 후 prod.
- run 폴더의 과거 증분/ Iceberg bronze 는 **재정렬 불필요**(row 집합 — 순서 무관, silver 가 재정렬).
