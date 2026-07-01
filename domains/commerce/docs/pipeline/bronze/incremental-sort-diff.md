# bronze 증분화 — UPDATEDT 정렬 · 검증키 · diff (feat/58)

매 수집이 전체 데이터를 다시 저장하던 것을, **정렬본 기준으로 전날과 다른 신규 row만** 저장하도록
바꾼다. 코드: [../../../include/bronze/incremental.py](../../../include/bronze/incremental.py) ·
배선: [../../../include/bronze/bronze_tasks.py](../../../include/bronze/bronze_tasks.py) ·
경로: [../../../include/common/paths.py](../../../include/common/paths.py).

## 1. 저장 모델 (save 증분 + diff-target 롤링)

구 데이터 소실·버전이력 유실을 막기 위해 **2계열**로 저장한다:

| 계열 | 위치 | 내용 |
|---|---|---|
| **save(증분 영구)** | `…/run_id=<ts>/<short>.jsonl` (run 폴더) | 첫 수집=전체, 이후=신규/변경분만. **삭제 안 함**(이력 보존) |
| **diff-target(롤링 최신본)** | `bronze/commerce/_diff_target/<short>.jsonl` (+ `.key` 사이드카) | 다음날 비교 기준. 매일 오늘본으로 **교체** |

- 첫 수집: save(run 폴더) 와 diff-target 을 **같은 내용(전체 정렬본)**으로 생성.
- 이후: 오늘 vs diff-target diff → **신규분만 save 로 증분 저장** → diff-target 을 오늘본으로 교체(구본 대체).
- 저장 포맷: **row-NDJSON**(줄당 레코드 1개, UPDATEDT desc 정렬). (기존 page-NDJSON 에서 전환.)

## 2. 정렬 (외부 병합 정렬, 스트리밍)

- 정렬키 = UPDATEDT(datetime, -1단계 확인상 39종 100% 존재) → **14자리 정수(YYYYMMDDHHMMSS) 내림차순**,
  동률은 MGTNO tie-break(결정적 전순서).
- **전량 RAM 금지** → `external_merge_sort`: 청크를 임시파일로 쓰고 `heapq.merge` 로 병합(스트리밍·바운디드 RAM).
  비교정렬 하한 O(n log n). (정수키라 이론상 radix O(n) 가능하나, 외부 정렬 견고성/단순성으로 병합 채택.)

## 3. 검증키 & diff

- **검증키**(`verification_key`) = 정렬본 row 정규화(JSON key정렬) 문자열들을 순서대로 이어 sha256(순서 민감).
  오늘 키 == diff-target 키 → **동일**(증분 없음, 마커만).
- **diff**(`diff_new_rows`) = 오늘·전날 둘 다 같은 키로 정렬 → **스트리밍 병합**으로 신규/변경 row만 방출.
  같은 정렬키 위치는 정규화 문자열 **직접 비교**(hot loop 에 해시 안 씀).

## 4. 수집 흐름 (bronze_tasks)

- **수집 완료(status==ok)일 때만** 처리: 수집 페이지 → row 파싱 → `incremental_store`
  (전날 diff-target 다운로드 → `build_increment` → 증분 업로드 + diff-target 교체 + 키 사이드카).
- **중간 중단(status!=ok)은 저장하지 않는다**(부분 결과 미저장). 마커에
  `verification_key/increment_mode/increment_count/sorted_row_count` 기록.
- raw 페이지는 휘발(메모리) — 영속물은 save 증분 + diff-target 뿐이라 **별도 "수집 파일 삭제" 대상 없음**
  (§4 삭제-맨나중 요구는 본 모델에선 "미저장 + 재검증"으로 갈음).

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
