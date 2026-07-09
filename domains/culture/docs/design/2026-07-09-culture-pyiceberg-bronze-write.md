# culture bronze pyiceberg 직접 write 전환 설계 (#203)

- 이슈: ASAC-DAG#203 `[Feat] culture bronze pyiceberg 직접 write 전환 — 커밋 216→12/일`
- 날짜: 2026-07-09 · 브랜치: `feat/203-culture-pyiceberg-bronze-write`
- 멘토 게이트: **해소** — `Dockerfile.airflow`에 `pyiceberg[s3fs]>=0.11.1,<0.12` 반영(ask-seoul dev/main 양쪽), 실행 중 3개 컨테이너에서 pyiceberg 0.11.1 + pyarrow 24.0.0 import 확인(2026-07-09), 전 도메인 DAG 파싱 무에러.

## 문제

culture `load_bronze`가 26분(run의 87%) — 원인은 **Iceberg 커밋 고정비 × 커밋 횟수**.
Trino `INSERT ... VALUES`는 SQL 텍스트로 데이터를 나르므로 800KB 배치 상한이 구조적이고
(7/8 실측: 쿼리 ~216개 × 평균 7.2초), 배치 1개 = Iceberg 스냅샷 1개다. 부산물인
스냅샷 216/일은 culture_maintenance가 지운 옛 metadata.json 760개의 생산자이기도 하다.

pyiceberg `table.transaction()` 안에서 delete+append 하면 **데이터셋당 커밋 1회**
(12커밋/일) — 26분 → 2~3분 추정, 스토리지 위생 동시 해결.

## 결정 요약

| 결정 | 선택 | 근거 |
|---|---|---|
| 기본 엔진 | **처음부터 pyiceberg** | 이번 사이클에서 dev 라이브 대조 검증까지 끝내고 PR 1개로 전환. 검증 실패 시 PR 자체를 내지 않음 |
| 로더 구조 | **병렬 클래스** `PyicebergBronzeWarehouse` | 기존 `BronzeWarehouse` 무변경 = 롤백 안전 + 테스트 경계 깔끔. 단일 클래스 내부 분기(비대해짐)·weather 헬퍼 공용 승격(공유 인프라 게이트, KMA 특화 분리 선행 필요)은 기각 |
| DDL·count | **Trino 유지** | weather 선례. `ensure_table`(CREATE IF NOT EXISTS)을 그대로 두면 기존 테이블과 타입 드리프트 없음 |
| 엔진 스위치 | DAG 파라미터 `engine`(기본 `"pyiceberg"`, 허용 `"trino"`, 그 외 즉시 실패) + CLI `--engine` | commerce `unit.get("engine")` 패턴. **환경 변수 아님** — 새 env는 카탈로그 연결용 `R2_DATA_CATALOG_*`뿐(기존 존재) |
| 스위치 일몰 | **자정런 7회 연속 성공 후 trino 쓰기 경로 제거** | 후속 정리 이슈로 `BronzeWarehouse.load`(INSERT VALUES/배치 로직)와 `engine` 파라미터 삭제. `TrinoClient`는 DDL·count·maintenance가 계속 쓰므로 존치 — 죽는 건 INSERT 경로뿐 |

## 아키텍처

`load_bronze_from_raw`(코어)·DAG 태스크·CLI·리포트는 **전부 무변경**. 바뀌는 건
`build_warehouse()`가 돌려주는 객체 하나.

```
[DAG/CLI] → load_bronze(target, engine)
              └→ build_warehouse(target, engine)
                   ├─ "pyiceberg" (기본) → PyicebergBronzeWarehouse   ← 신설
                   └─ "trino"    (롤백)  → BronzeWarehouse            ← 기존 무변경
```

`PyicebergBronzeWarehouse`는 기존과 동일한 3메서드 인터페이스:

- `ensure_table(dataset)` — 내부 `TrinoClient`에 위임 (기존 DDL 그대로)
- `count(dataset, ingest_ts=None)` — 내부 `TrinoClient`에 위임
- `load(ds, ctx, records)` — **pyiceberg delete+append** (아래)

기존 테스트의 FakeWarehouse는 인터페이스 동일이라 무변경 통과.

## 카탈로그 연결 (신규 설정)

`common/config.py`에 `build_catalog_settings(target)` 추가 — `build_r2_settings`와
같은 프리픽스 규칙:

- dev → `R2_DEV_DATA_CATALOG_URI/WAREHOUSE/TOKEN` · prod → `R2_DATA_CATALOG_*`
  (양쪽 세트 모두 `.env`에 존재 확인, 2026-07-09)
- s3 자격은 기존 `build_r2_settings` 재사용: `s3.endpoint` / `s3.access-key-id` /
  `s3.secret-access-key` / `s3.region="auto"`
- **`TOKEN`·secret키는 `register_secret`으로 redactor literal 등록** (#144 방어선 —
  pyiceberg 예외 표면 누출 차단)
- 연결(weather 선례): `RestCatalog("culture", uri=…, warehouse=…, token=…, **s3옵션)`
  → `catalog.load_table(f"{schema}.bronze_{dataset}")`
- pyiceberg import는 **함수 안 lazy import** (weather/commerce 선례 — 이미지에
  없어도 DAG 파싱이 깨지지 않음)

## load() 데이터 흐름

```python
ensure_table(ds.name)                      # Trino DDL (멱등, 기존 그대로)
with table.transaction() as txn:           # ← 블록 전체 = Iceberg 커밋 1회
    txn.delete(EqualTo("ingest_ts", ctx.ingest_ts))   # 멱등: 재실행 중복 방지
    for chunk in chunks(records, 50_000):
        txn.append(arrow_table(chunk))     # 같은 txn 안 append n회 = 커밋 1회
```

- **Arrow 스키마 = 기존 테이블 11컬럼 정확 일치**: `record_seq` → `pa.int32()`,
  `collected_at` → `pa.timestamp("us")`(UTC now), 나머지 9개(dataset, source,
  endpoint, record_json, raw_object_key, page_no, load_date, ingest_ts, run_id)
  → `pa.string()`. 값 구성(record_json `ensure_ascii=False` 직렬화, seq 부여,
  키/파일명)은 Trino 경로와 동일 로직.
- 청크 50,000행은 **메모리 안전용**(세종 88MB)이며 커밋 수와 무관.
  800KB SQL 배치 로직은 pyiceberg 경로에 존재하지 않는다.
- 진행 로그: 데이터셋 단위 1줄(`[load] {name}: n행 · x.xs`) — 기존
  `load_bronze_from_raw`의 로그 그대로, 배치 로그는 경로와 함께 소멸.

## 에러 처리

- `CommitFailedException`(낙관적 잠금 — culture_maintenance의 스냅샷 정리와 겹칠
  수 있음): weather 패턴 — `table.refresh()` 후 지수 백오프(1s→2s→4s, cap 30s)
  재시도 3회, 소진 시 예외 전파.
- 그 외 예외는 그대로 전파 → `load_bronze_from_raw`의 데이터셋별 격리 + 말미
  fail loud가 기존처럼 수신. **트랜잭션이라 부분 적재 없음**(커밋 전 실패 =
  테이블 무변화) — Trino 경로의 "배치 도중 실패 시 반쯤 적재" 문제가 사라짐.
- 실패 run 복구 절차 불변: raw 박제 후 `load_bronze`만 clear (operations.md).

## 테스트 (TDD)

`tests/test_warehouse_pyiceberg.py` 신규:

1. **arrow 변환** — 11컬럼 스키마·타입 정확성, 한글/None/대형 record_json 값 보존
2. **FakeCatalog/FakeTable/FakeTxn** — delete 필터가 `EqualTo("ingest_ts", ctx.ingest_ts)`
   인지, append 청크 분할, **트랜잭션(=커밋) 1회** 검증, 빈 records는 no-op
3. **CommitFailed 재시도** — 2회 실패 후 성공 시 refresh 호출·총 3회 시도,
   3회 실패 시 예외 승격
4. **`build_catalog_settings`** — dev/prod 프리픽스 분기, 누락 env 시 명시적 에러,
   `register_secret` 배선(redact 검증 — test_redaction_surfaces 패턴)
5. **`build_warehouse` 디스패치** — pyiceberg/trino/불량값(즉시 실패)

기존 `test_load_bronze.py`(FakeWarehouse 주입)는 무변경 통과가 요구사항.

## 라이브 검증 (dev, PR 전 게이트)

1. 오늘 자정런(Trino 경로) 파티션을 기준값으로, 같은 raw를 **검증용 ingest_ts**로
   pyiceberg 적재
2. Trino로 두 파티션 대조 — **데이터셋별 행수 일치**(이슈 완료 조건) +
   `sum(length(record_json))`·`count(distinct record_seq)` 일치(내용 대조)
3. 소요 시간 실측(26분→2~3분 목표) 기록 + 검증 파티션 삭제
4. `$snapshots` 메타테이블로 **데이터셋당 커밋 1회** 확인

## 문서

- `operations.md` — `engine` 파라미터(기본 pyiceberg)·trino 롤백 절차·일몰 계획
- `architecture.md` — 로더 절 갱신(Trino HTTP → pyiceberg, DDL은 Trino 유지)
- `storage.md` — 적재 코드 참조 갱신
- `change-log.md` — #203 항목
- PR 후 dags 레포 dev 복귀 (컨테이너 워킹트리 마운트)

## 비범위

- weather/commerce와의 pyiceberg 헬퍼 공용화(루트 common 승격) — 3도메인 안정
  가동 후 별도 논의
- trino 쓰기 경로 제거 — 일몰 조건(자정런 7회 연속 성공) 충족 후 후속 이슈
- silver/gold·조회 경로 — 변경 없음(dbt-trino 그대로)
