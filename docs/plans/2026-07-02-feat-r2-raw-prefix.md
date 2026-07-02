# 저장 계약 — R2 원본 경로 `bronze/` → `raw/` 전환

- 상태: 합의됨 (마이그레이션 방식만 결정 대기)
- 작성일: 2026-07-02
- 이슈: [#75](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/75) / 브랜치: `feat/75-r2-raw-prefix` (합의 후)
- 발단: PR #74 리뷰 @yooseongjin527 제안 → #75 인계 → 팀 결정

## 확정 결정 (2026-07-02)

| 항목 | 결정 |
|---|---|
| R2 오브젝트 원본 경로 | **`raw/` 채택** — `bronze/<domain>/...` → `raw/<domain>/...` |
| dag_id stage 표기 | **`bronze` 유지** (PR #74 회신 결론 그대로) |
| Iceberg 테이블명·dbt | **`bronze_*` 유지** — dbt source/모델 무변경 |

용어 정리: **raw = R2 오브젝트 원본(랜딩)** / **bronze = Iceberg 테이블(웨어하우스 원본층)**.
DAG는 raw 랜딩과 bronze 적재를 모두 수행하므로 stage 표기는 bronze 유지가 정합.

## 영향 범위 (조사 완료)

| 도메인 | 경로 생성 지점 | 변경 난이도 | 비고 |
|---|---|---|---|
| traffic | `traffic_ingest/common/runtime.py` — env `ASK_SEOUL_RAW_PREFIX`(기본 `"bronze"`) | **최소** | env 이름이 이미 `RAW_PREFIX`고 **dev 값은 이미 `dev/junghyun/raw`** — 기본값 `"bronze"`→`"raw"` + prod env 정리만 |
| weather | `weather_ingest/common/runtime.py` — 동일 구조 | **최소** | 〃 |
| culture | `culture_ingest/source/config.py` — `LANDING_ROOT = "bronze/culture"` | 낮음 | 상수 1개. append-only 랜딩(과거 경로 조회 없음) |
| population | `ppltn_ingest/source/config.py` — `LANDING_ROOT = "bronze/population"` | 낮음 | 〃 |
| transit | DAG 3개의 `stage="bronze"` 인자 (`r2_landing`) | 낮음 | 리터럴 3곳. append-only |
| commerce | `include/common/paths.py` — `BRONZE_LAYER = "bronze/commerce"` | **높음 — 주의** | 아래 참조 |

**commerce가 유일한 리스크 지점**: 상수는 1개지만, 이 경로를 **읽는** 로직이 있음 —
- `markers.py`가 과거 run 폴더를 스캔해 재수집 대상·동일자 성공분 제외를 판단 (feat/59 계약)
- diff-target(롤링 최신본, feat/58)이 같은 경로 아래에 있어 접두 변경 시 "첫 수집"으로 오인 → 전량 재수집 발생

부수 항목(경로 아님, 결정 필요): culture·population 마커 메타데이터의 `"layer": "bronze"` 필드값을
`"raw"`로 바꿀지 — 기존 저장물과의 스키마 일관성 문제이므로 **유지 권장**(schema_version 올릴 때 일괄).

## 마이그레이션 방식 (도메인 차등 — 권장안)

- **traffic·weather·culture·population·transit (5개)**: **신규부터 `raw/`** — append-only 랜딩이라
  과거 경로를 읽는 코드가 없음. 기존 `bronze/` 객체는 그대로 두고(재처리 가능성 보존) 전환일만 기록.
- **commerce**: 두 안 중 결정 필요 (열린 질문 1)
  - A안: 전환 시 `bronze/commerce/` → `raw/commerce/` **일괄 copy 후 전환** — 마커·diff-target 연속성
    보존, 재수집·증분 로직 무중단. R2 copy 비용/시간 소요
  - B안: **diff-target + 최근 N일 run 폴더만 copy** — 증분·재수집에 실제 필요한 것만 이전. 저비용,
    스크립트 필요
  - (C안: copy 없이 전환 — 전환일 전량 재수집 + 동일자 제외·재수집 계약 일시 공백. 비권장)

## 단계별 계획 (마이그레이션 방식 확정 후)

1. 5개 도메인 접두 변경 (도메인별 커밋) + traffic/weather는 루트 `.env`의 `ASK_SEOUL_*_RAW_PREFIX` 값 정리
2. commerce: 결정된 안으로 copy 스크립트 실행 → `BRONZE_LAYER` → `RAW_LAYER = "raw/commerce"` 전환 (변수명 포함) + change-log 기록
3. 검증: 각 DAG 1회 트리거 → `raw/` 경로 적재 확인, commerce는 재수집·동일자 제외 로직 검증 (기존 테스트 + 실행)
4. 문서 갱신: 각 도메인 docs의 경로 표기, commerce CLAUDE.md §2.3 경로 패턴

## 열어둔 질문

1. **commerce 마이그레이션 A안(일괄 copy) vs B안(diff-target+최근 run만)** — 결정 대기
2. 전환 후 기존 `bronze/` 객체 보존 기간 (무기한 보존 vs 검증 후 정리)
