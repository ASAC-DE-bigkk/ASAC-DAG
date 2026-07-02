# bronze 증분화 — UPDATEDT 정렬 · 검증키 · diff (feat/58)

매 수집이 전체 데이터를 다시 저장하던 것을, **정렬본 기준으로 전날과 다른 신규 row만** 저장하도록
바꾼다. 코드: [../../../include/bronze/incremental.py](../../../include/bronze/incremental.py) ·
배선: [../../../include/bronze/bronze_tasks.py](../../../include/bronze/bronze_tasks.py) ·
경로: [../../../include/common/paths.py](../../../include/common/paths.py).

## 1. 저장 모델 (랜딩 → 증분 → diff 이동, 수집일 태깅)

구 데이터 소실·버전이력 유실을 막으면서 full 은 **한 벌만** 유지한다:

| 계열 | 위치 | 내용 |
|---|---|---|
| **landing(임시)** | `…/run_id=<ts>/_full/<short>.jsonl` | 오늘 정렬 full 의 랜딩 — 비교/이동 **전에 먼저 저장**(중단돼도 수집분 보존). 완료 시 diff 로 **이동**되어 사라짐(잔존 = 그 run 중단의 증거) |
| **save(증분 영구)** | `…/run_id=<ts>/<short>.jsonl` (run 폴더) | 첫 수집=전체, 이후=신규/변경분만. **삭제 안 함**(이력 보존) |
| **diff-target(최신 full)** | `raw/commerce/_diff_target/<short>.<수집일>.jsonl` (+ 같은 이름 `.key`) | 다음 수집의 비교 기준(정렬 full). landing 에서 **이동**해 옴. **파일명 수집일(YYYY-MM-DD)로 완료/중단 구분** — 교체 시 구 날짜 파일 삭제 |

- 첫 수집: landing → save(run 폴더, full)와 diff-target 에 **같은 내용(전체 정렬본)** 반영.
- 이후 매 수집: ① landing 저장 → ② diff-target 과 비교(§3) → ③ **다른 내용만 save 로 증분 저장**
  → ④ 구 날짜 diff 삭제 + landing 을 오늘 수집일 태깅으로 diff 에 **이동**.
  identical(검증키 동일)이어도 ④는 수행 — diff 파일명 날짜 = **최신 완료 수집일**.
- 실패 복구: ① 후 중단 = landing 잔존(수집분 보존) + 구 diff 유지 → 재실행 시 구 diff 와 재비교.
  ④ 도중 중단 = 신·구 날짜 diff 잠시 공존 → 발견(`find_diff_target`)이 최신 날짜를 선택(자가 복구).
- 저장 포맷: **row-NDJSON**(줄당 레코드 1개, UPDATEDT desc 정렬). (기존 page-NDJSON 에서 전환.)

## 2. 정렬 (외부 병합 정렬, 스트리밍)

- 정렬키 = UPDATEDT(datetime, -1단계 확인상 39종 100% 존재) → **14자리 정수(YYYYMMDDHHMMSS) 내림차순**,
  동률은 MGTNO tie-break(결정적 전순서).
- **전량 RAM 금지** → `external_merge_sort`: 청크를 임시파일로 쓰고 `heapq.merge` 로 병합(스트리밍·바운디드 RAM).
  비교정렬 하한 O(n log n). (정수키라 이론상 radix O(n) 가능하나, 외부 정렬 견고성/단순성으로 병합 채택.)

## 3. 검증키 & diff

- **검증키**(`verification_key`) = 정렬본 row 정규화(JSON key정렬) 문자열들을 순서대로 이어 sha256(순서 민감).
  오늘 키 == diff-target 키 → **동일**(증분 없음, 마커만 — 단 diff 파일명 날짜는 오늘로 롤링).
- **diff**(`diff_new_rows`) = 오늘·전날 둘 다 같은 키로 정렬 → **스트리밍 병합**으로 신규/변경 row만 방출.
  같은 정렬키 위치는 정규화 문자열 **직접 비교**(hot loop 에 해시 안 씀).
- **비교 조기 중단**(`stop_on_aligned_match`): UPDATEDT desc 정렬이라 신규/변경 row 는 항상 위쪽에
  온다 → 정렬 프런티어에서 **같은 정보(키+내용)가 처음 일치하는 순간 비교를 중단**(이하 동일 간주).
  전제: 내용이 바뀌면 UPDATEDT 가 갱신된다(LOCALDATA 계약). UPDATEDT 갱신 없는 내용 변경은 이
  모드에서 감지되지 않음 — 파일 단위 동일/상이는 검증키가 판정.

## 4. 수집 흐름 (bronze_tasks)

- **수집 완료(status==ok)일 때만** 처리: 수집 페이지 → row 파싱 → `incremental_store`
  (landing 저장 → 이전 diff 발견/다운로드 → 비교(조기 중단) → 증분 업로드 → diff 이동+키 사이드카).
- **중간 중단(status!=ok)은 증분/이동을 수행하지 않는다** — 구 날짜 diff 가 그대로 남아
  파일명 날짜로 "그 API 는 오늘 완료 안 됨"이 식별된다. 마커에
  `verification_key/increment_mode/increment_count/sorted_row_count/diff_target_key` 기록.
- 이전 diff 발견은 `find_diff_target`(`<short>.` 접두 나열 → 최신 날짜 선택, 구형 무날짜도 인식).

## 5. step0 (1회성 시드)

- `seed_diff_target`: 기존 수집물로 diff-target + 검증키를 미리 시드 → 다음 첫 수집이 곧바로 diff 가능.
- **미실행이어도** 첫 수집이 `mode=first` 로 전체를 save+target 생성(self-seed)하므로 필수는 아님.

## 6. 검증

- 단위테스트 **14 통과**([../../../tests/test_incremental.py](../../../tests/test_incremental.py)):
  다중청크 정렬·순서민감 검증키·diff(identical/new-head/changed/deleted)·파일브리지·orchestration
  (first→identical→changed)·step0 시드→identical.
- **end-to-end 라이브 검증 완료**(실 Seoul API, 격리 프리픽스 `_verify58` → 검증 후 전량 삭제):
  run1=first(row-NDJSON) → run2 동일=identical(증분 미생성) → 변경분=changed(변경/신규만 증분, diff-target 롤링).
  **이력 보존**: 이전 run 증분 유지 + 같은 업장의 원본·변경 두 버전 공존(이력 추적 가능). 실 bronze 무오염.
- **사이드 이펙트**: 기존 page-NDJSON 과 신규 row-NDJSON **형식 혼재**(이력 손실 아님 — 구 run 보존).
  실운영 첫 수집은 `_diff_target` 미존재라 mode=first 전체 저장(자가 시드); step0 사전 시드 시 첫 수집부터 diff.
- **다운스트림**: bronze 가 row-NDJSON 으로 바뀌어, page 파서를 쓰는 소비자(dbt 로더 등)의 row 기준
  조정은 feat/58 밖(별도).
