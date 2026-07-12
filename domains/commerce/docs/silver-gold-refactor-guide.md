# [작업 가이드] silver/gold 구조 판정 + 리팩터 수정 지시

> **다른 세션/PC의 에이전트(Opus)가 이 문서만 읽고 그대로 구현할 수 있게** 쓴 실행 가이드다.
> 읽는 순서: CLAUDE.md → 이 문서. §2 확정 변경을 위→아래 순서로 구현하고, 각 항목의 **수용 기준**을
> 통과시킨 뒤 §7 완료 처리를 수행한다. §4 는 **변경 금지 목록**이다 — 정당성이 확인된 구조이므로
> 이 가이드 범위에서 건드리지 않는다. §6 은 타 도메인 관찰(공유용)로 **이 가이드로 수정 금지**.

## 0. 상태 · 실행 규칙

- 상태: **OPEN(미구현)**. 완료 시 `DONE(YYYY-MM-DD, 커밋 …)` 으로 바꾸고 §7 을 채운다.
- 대상 레포: dbt(ASAC-DBT, `dbt/domains/commerce/`) + dags(ASAC-DAG, `dags/domains/commerce/`).
  **타 도메인(`dbt/domains/{citydata,culture,traffic,weather,transit}`)은 수정 금지**(번들 경계 —
  CLAUDE.md Working Scope).
- 검증 환경: 로컬(py3.9)에선 dbt/DAG 실행 불가 — **컨테이너에서 검증**한다(커맨드는 각 항목에 명시).
  `dbt compile` 은 어댑터 연결이 필요하므로 compose 스택(trino)이 떠 있어야 한다.
- 커밋: 한국어 conventional, 논리 단위별. push/PR 은 사용자 승인 후(CLAUDE.md Commit & PR policy).

## 1. 판정 요약 (1차 구조 검토 결과, 2026-07-12)

코드·문서·타 도메인 비교 검토 결과. 상세 근거는 각 항목에.

| 영역 | 판정 | 근거 요약 |
|---|---|---|
| 레이어 분리(bronze→silver→gold) | **정당** | 실측(고유필드 342 중 41%가 1종 전용) 기반 설계, 문서화됨 |
| silver 의 record_json 보존 | **정당** | Iceberg 테이블(내부 parquet)의 VARCHAR 컬럼 1개 — "JSON 파일 저장" 아님. schema-on-read 의도적 설계 |
| 증분·마커·중단 방어(3계층) | **정당** | raw→bronze(watermark+manifest) / bronze→silver(DONE marker+pre_hook) / silver→gold(collected_at watermark+선삭제) — 코드가 문서 계약 그대로임을 확인 |
| gold 가 dbt 아닌 Python→Postgres | **정당** | 서빙 DB 대상 + bigserial(entity_seq)·인덱스 500+·뷰 320 등 **Postgres 네이티브 기능은 dbt-trino(postgresql 커넥터)로 표현 불가**. 단 lineage 사각(→C5)만 보완 |
| gold detail 78(카탈로그 구동) | **정당(사용자 확정)** | 엄격 클러스터 규칙 + 데이터 근거. 재론 금지 |
| gold 식별자 검증 | **결함(Major)** | 외부 API 유래 필드명이 무검증으로 DDL/JSONPath f-string 에 유입 → **C1** |
| silver history 모델 구조 | **결함(Major, 유지보수)** | 42컬럼 목록 3중 중복(union all 위치 기반 → 재정렬 실수 시 무증상 데이터 오염) → **C2** |
| gold lineage 가시성 | **결함(Major)** | gold 가 dbt 그래프·OpenLineage 양쪽에서 불가시 — silver→gold 에서 전체 lineage 단절 → **C5**(+V1) |
| gold 구현 상태 문서 정합 | **결함(Major, 문서)** | 파이프라인 문서 5곳은 gold "미구현/예정", DB 문서는 "적용 완료" — 실제는 구현·가동 중. 정반대 진술 공존 → **C7** |
| 좌표 변환 인라인 200줄 | 개선(Minor) | 재사용·검증성 — 매크로 추출 → **C3** |
| 마스킹 규칙 5중 중복 | 개선(Minor) | 매크로 공용화 → **C4** |
| taxonomy seed 고아화 | 개선(Minor) | detail_health 제거(#284) 후 소비자 0 → **C6** |

## 2. 확정 변경 (즉시 구현 — 위에서 아래 순서)

### C1. gold 식별자 게이트 (Major — 보안 §20 `assert_identifier` 계약)

- **현재**: [include/gold/measure.py](../include/gold/measure.py) `measure_fields()` 가
  `map_keys(json_parse(record_json))` 로 **외부 API 응답 유래 필드명**을 무검증 수집 →
  `catalog_rules.build_catalog` 가 lowercase 해 payload 컬럼명으로 채택 →
  [include/gold/ddl.py](../include/gold/ddl.py) `create_detail_sql`/`create_detail_index_sql` 의
  DDL f-string, [include/gold/loader.py](../include/gold/loader.py) `load_detail` 의
  `json_extract_scalar(record_json, '$.{c.upper()}')` 와 insert 컬럼 목록에 그대로 삽입된다.
  또 `load_gold` 태스크는 카탈로그를 **DB 테이블에서 재로드**하므로 DB 우회 유입 경로도 있다.
- **위험**: LOCALDATA 필드명은 관행상 `[A-Z0-9_]` 지만 게이트가 없다. 필드명에 인용부호/공백/`$`
  등이 오면 SQL·JSONPath 파손, 최악은 서빙 DB SQL 주입. 번들 보안 규약(§20)은 동적 식별자에
  `assert_identifier()` 를 요구한다.
- **변경**(3중 게이트 — 유입·생성·소비):
  1. `measure.measure_fields()` — 수집 직후 필터 + 품질 알림(§19.1 규격):
     ```python
     from security.dbio import is_identifier
     ...
     out: dict[str, set[str]] = {}
     dropped: dict[str, list[str]] = {}
     for r in rows:
         fields = set(r[1])
         bad = sorted(f for f in fields if not is_identifier(f))
         if bad:
             dropped[r[0]] = bad
         out[r[0]] = {f for f in fields if is_identifier(f)}
     if dropped:
         from security import log_event
         log_event(level="error", task="commerce_load_gold.build_catalog",
                   message="비식별자 필드명 제외(injection guard)", dropped=dropped)
     ```
  2. `ddl.generate_all(details)` 진입부 — 방어선(생성 경계):
     ```python
     from security.dbio import assert_identifier
     for d in details:
         assert_identifier(d["object"], field="gold object")
         for c in d["payload"]:
             assert_identifier(c, field="gold payload column")
     ```
  3. `loader.load_detail()` 진입부 — DB 재로드 우회 경로 차단(소비 경계):
     ```python
     from security.dbio import assert_identifier
     assert_identifier(detail["object"], field="gold object")
     for c in detail["payload"]:
         assert_identifier(c, field="gold payload column")
     for m in detail["members"]:
         assert_identifier(m, field="dataset short")
     ```
- **수용 기준**:
  - 신규 pytest: `dags/domains/commerce/tests/test_gold_ddl.py` 에 케이스 추가 — payload 에
    `"x'); drop table t; --"` 류 악성 이름이 든 detail dict 로 `generate_all`/`create_detail_sql`
    호출 시 `ValueError` 발생; 정상 이름은 통과.
  - `PYTHONPATH=dags/domains/commerce/include python3 -m pytest dags/domains/commerce/tests -q` 전체 통과.
  - `PYTHONPATH=dags/domains/commerce/include python3 -m security` PASS(차단 0).

### C2. silver history 컬럼 목록 단일화 (Major — 무증상 오염 방지)

- **현재**: `dbt/domains/commerce/models/silver/silver_license_history.sql` 에서 42개 컬럼 목록이
  **3곳에 중복**된다 — `projected_new`(L368), `prior_tail`(L416), 최종 select(L509). `prior_tail
  union all projected_new` 는 **위치 기반**이라, 한쪽 목록만 순서를 바꾸면(컬럼 수 동일) 오류 없이
  **열이 어긋난 채 적재**된다. gold 쪽은 이미 `ddl.ENTITY_COLUMNS` 단일 소스 패턴을 쓴다 — silver 만 정합화.
- **변경**: `dbt/domains/commerce/macros/silver_columns.sql` 신설:
  ```jinja
  {#- silver_license_history 산출 컬럼(단일 소스) — projected_new/prior_tail/최종 select 가 공유.
      union all 이 위치 기반이므로 목록이 갈라지면 무증상 오염 → 반드시 여기서만 수정한다. -#}
  {% macro silver_history_column_list() %}
  {{ return([
      'dataset', 'opnsfteamcode', 'mgtno', 'record_json', 'bplcnm',
      'trdstategbn', 'trdstatenm', 'dtlstategbn', 'dtlstatenm',
      'apvpermymd', 'dcbymd', 'sitetel',
      'road_address', 'jibun_address', 'jibun_address_source',
      'road_address_norm', 'jibun_address_norm',
      'gu', 'gu_code', 'legal_dong', 'legal_code', 'admin_dong', 'admin_dong_code',
      'address_key_road', 'address_key_jibun',
      'source_coord_x', 'source_coord_y', 'latitude', 'longitude',
      'content_hash', 'updatedt', 'updatedt_ts', 'updatedt_sort',
      'lastmodts', 'lastmodts_ts', 'lastmodts_sort',
      'observed_date', 'collected_at', 'bronze_run_id', 'dag_run_id',
      'raw_object_key', 'load_date',
  ]) }}
  {% endmacro %}
  ```
  모델 상단에 `{%- set h_cols = silver_history_column_list() -%}` 를 두고 3곳을 치환:
  - `projected_new`: `select {{ h_cols | join(',\n        ') }}, 'new' as _silver_source from keyed`
  - `prior_tail`(incremental 분기 안): `select {{ h_cols | join(',\n        ') }}, 'prior' as _silver_source from ( … ) where rn = 1`
  - 최종: `select {{ h_cols | join(',\n    ') }} from deduped`
  ⚠ 목록 순서는 위(기존 최종 select 순서)와 **완전 동일**해야 한다 — 기존 테이블(append,
  `on_schema_change: fail`)과의 위치 정합.
- **수용 기준**: 컨테이너에서 변경 전/후 `dbt compile` 산출 SQL 이 **공백 외 동일**(컬럼명·순서 일치):
  ```bash
  docker compose run --rm airflow-scheduler /home/airflow/dbt-venv/bin/dbt compile \
    --project-dir /opt/airflow/dbt/domains/commerce --profiles-dir /opt/airflow/dbt/domains/commerce \
    --target dev --select silver_license_history
  # target/compiled/.../silver_license_history.sql 을 변경 전 사본과 diff -w 로 비교
  ```
  이후 `dbt run --select silver_license_history silver_license_current`(증분) + `dbt test` 통과.

### C3. 좌표 변환(EPSG:5174→WGS84) 매크로 추출 (Minor — 재사용·검증성)

- **현재**: history 모델 L232–L343 에 측지 변환 CTE 10개(`geo_mu`…`geo`)가 **수치 리터럴과 함께
  인라인**. 모델이 553줄로 비대하고, 변환 로직을 단독 검증/재사용할 수 없다.
- **변경**: `dbt/domains/commerce/macros/geo_transform.sql` 신설 — CTE 체인을 그대로 이관:
  ```jinja
  {#- 중부원점 TM(EPSG:5174) → WGS84 CTE 체인. 입력 CTE 는 source_coord_x/y 를 가져야 하며,
      출력 CTE `geo` 가 latitude/longitude 를 추가한다. 파라미터·검증: docs/address-and-geo.md -#}
  {% macro tm5174_to_wgs84_ctes(input_cte) %}
  geo_mu as ( … from {{ input_cte }} ), geo_fp as ( … ), geo_t as ( … ), geo_d as ( … ),
  geo_bl as ( … ), geo_ecef as ( … ), geo_xyz as ( … ), geo_pb as ( … ), geo_wgs as ( … ),
  geo as ( … )
  {% endmacro %}
  ```
  (모델 L232–L343 을 **문자 그대로** 옮기고, 첫 CTE 의 `from dong` 만 `from {{ input_cte }}` 로 치환.
  주석 블록도 함께 이동.) 모델에서는 해당 구간을 `{{ tm5174_to_wgs84_ctes('dong') }},` 한 줄로 대체
  (뒤따르는 `keyed` CTE 앞의 콤마 유지).
- **수용 기준**: C2 와 동일한 compile diff(공백 외 동일) + 기존 싱귤러 테스트
  `assert_silver_license_current_geo_landmark.sql` 통과(landmark 좌표 검증이 곧 매크로 회귀 테스트).

### C4. 마스킹 규칙 매크로 공용화 (Minor — 5중 중복 제거)

- **현재**: 동일 술어(`road_address/jibun_address 에 '*' 포함 시 null`)가 5곳 중복 —
  `silver_license_current.sql` L65–L84(legal_dong/legal_code/admin_dong/admin_dong_code 4개 CASE) +
  `silver_license_history.sql` dong_token CTE L141–L145.
- **변경**: `dbt/domains/commerce/macros/masking.sql` 신설:
  ```jinja
  {#- 마스킹 주소(도로명/지번에 '*') 행은 동 단위 값을 null 처리 — §19.1 masked_address 규칙. -#}
  {% macro null_if_masked_address(expr) -%}
  case
      when coalesce(road_address, '') like '%*%' or coalesce(jibun_address, '') like '%*%'
          then null
      else {{ expr }}
  end
  {%- endmacro %}
  ```
  치환: current 4곳 → `{{ null_if_masked_address('legal_dong') }} as legal_dong` 등;
  history dong_token → `{{ null_if_masked_address("nullif(regexp_extract(coalesce(jibun_address_norm, ''), '서울(?:특별시|시)?\\s*[가-힣]+?구\\s*([가-힣]+\\d*(?:동|가))', 1), '')") }} as dong_raw`.
  ⚠ regexp 문자열 안 백슬래시는 원문 그대로 보존(이스케이프 이중화 주의 — compile diff 로 확인).
- **수용 기준**: compile diff 공백 외 동일 + `dbt test`(masked 관련 테스트 포함) 통과.

### C5. gold 를 dbt exposure 로 등록 (Major — lineage 사각 해소 1단계)

- **현재**: gold(Python→serving Postgres)는 dbt 그래프·문서 어디에도 없다. silver 모델이 "누가
  소비하는지" dbt 수준에서 불가시 → 전체 lineage 가 silver 에서 끊긴다. culture 도메인은 이미
  exposures 로 소비자를 문서화하는 선례가 있다(`dbt/domains/culture/models/exposures.yml`).
- **변경**: `dbt/domains/commerce/models/exposures.yml` 신설(culture 형식 준용):
  ```yaml
  version: 2

  exposures:
    - name: commerce_gold_serving
      label: commerce gold 서빙 DB (serving-postgres)
      type: application
      maturity: high
      owner:
        name: data-eng
      description: >
        commerce_load_gold DAG(ASAC-DAG) 가 silver 를 읽어 서빙 Postgres 의 commerce_* 객체
        (core entity/history · detail 78 · dim 3 · view 320)로 증분 적재한다. 카탈로그 구동 —
        규칙: dags/domains/commerce/include/gold/ · 명세: docs/DB/gold/.
      depends_on:
        - ref('silver_license_history')
        - ref('silver_license_current')
  ```
- **수용 기준**: 컨테이너 `dbt parse`(commerce 프로젝트) 오류 없음. `dbt ls --select exposure:*` 에
  `commerce_gold_serving` 표시. Cosmos DAG 렌더에는 영향 없음(RenderConfig select 는 모델 2개 유지 —
  `dags/domains/commerce/commerce_load_silver.py` 수정 불필요).

### C6. cleanup 러북 보강 — taxonomy seed 고아화 반영

- **현재**: `commerce_dataset_taxonomy` seed 의 소비자는 `silver_license_detail_health` 뿐(전수 grep
  확인 — gold 의 dim_dataset 는 dags 레지스트리에서 직접 파생). detail_health 제거(#284) 후 seed 는
  고아가 되는데 러북([cleanup-detail-health.md](cleanup-detail-health.md))에 없다.
- **변경**: `cleanup-detail-health.md` §3(코드 정리)에 체크 항목 추가:
  ```markdown
  - [ ] **고아 seed 삭제**: `dbt/domains/commerce/seeds/commerce_dataset_taxonomy.csv` +
        `models/schema.yml` 의 `seeds:` 섹션(commerce_dataset_taxonomy 블록) 삭제 — 소비자가
        detail_health 뿐이었음(gold dim_dataset 는 dags 레지스트리에서 직접 파생, seed 미사용).
        삭제 전 재확인: `grep -rn commerce_dataset_taxonomy dbt dags | grep -v logs/` 가
        detail_health·schema.yml 외 무결과인지.
  - [ ] **문서 정리 추가 대상**: `dags/domains/commerce/docs/pipeline/data-model.md`(L27·L173·L176),
        `docs/pipeline/silver/README.md`(L19·L22), `docs/pipeline/raw/api-field-coverage.md`(L67),
        `docs/architecture/storage.md`(L34) 의 detail_health 언급 갱신.
  ```
- **수용 기준**: 러북에 두 항목 반영(이 가이드 구현 시점에는 문서 수정만 — 실삭제는 #284 실행 시).

### C7. 문서 정합 복구 (Major — gold 구현 상태에 대해 문서가 정반대 진술)

- **현재(핵심 모순)**: DB 문서(`dbt/domains/commerce/docs/DB/gold/*`)는 gold **적용 완료**(85 테이블,
  전량 적재 8.68M행 실측)로 기술하는데, **파이프라인 문서 5곳은 "미구현/예정"** 으로 남아 있다 —
  gold 구현 이전에 쓰인 채 미갱신. 이 문서들을 읽는 에이전트/팀원이 gold 부재로 오판한다.
  - `dags/domains/commerce/docs/pipeline/data-model.md` (L30·L167·L178·L193 부근 "gold 미구현/예정")
  - `dags/domains/commerce/docs/pipeline/gold/README.md` ("⚠️ 미구현(계획). models/gold/ 는 아직 없다")
  - `dags/domains/commerce/docs/pipeline/README.md` (L21 "gold ⚠️ 미구현(계획)")
  - `dags/domains/commerce/docs/pipeline/silver-gold-load-plan.md` (L5 "gold 는 여전히 미구현")
  - `dbt/domains/commerce/docs/beginner-guide.md` (§7 L193–195 "gold … 지금은 미구현")
- **추가 드리프트 4건**:
  1. `dbt/.../docs/DB/gold/views.md` L18 — 뷰 320개 생성 메커니즘을 "gold-catalog 를 **dbt seed** 로
     올리고 jinja 루프"로 기술 — **실제는 Python**(`include/gold/ddl.py` `view_domain_sql`/`view_api_sql`,
     loader 가 CREATE OR REPLACE). dbt seed 방식은 폐기된 구상.
  2. `dbt/.../docs/DB/gold/partitioning-indexing-plan.md` §2 — 인덱스 "6개 적용"으로 기술 — **실제
     17개(entity/history) + detail 자동 401개**(change-log #53, `ddl.create_index_sql`/
     `create_detail_index_sql`). `tables.md` §3.2 가 최신 정본.
  3. `dbt/.../docs/DB/gold/tables.md` §3 — `commerce_dim_dataset` 소스를 "seed(taxonomy) + registry +
     카탈로그"로 기술 — **실제 코드는 registry + 카탈로그만** 사용(`loader.load_dims`), seed 미사용
     (→ C6 의 고아 판정 근거).
  4. `dbt/.../docs/timestamps-and-nulls.md` L57 — "미정(후속 결정 필요): DCBYMD 등 날짜 형변환" —
     **이미 해소**(gold `ddl._DATE` + `loader._to_date` 가 opened_at/closed_at 을 date 규격화).
- **변경**: 각 지점을 현재 상태로 **한두 줄 정정 + 정본 포인터**(`docs/DB/gold/` · change-log)로 갱신.
  장문 재작성 금지 — 서술을 실제와 일치시키는 최소 수정. 이력 성격 문서(change-log, 계획 이력 명시
  구간)는 "당시 계획" 표기를 남기되 현재 상태 한 줄을 덧붙인다.
- **수용 기준**: `grep -rn "미구현" dags/domains/commerce/docs/pipeline dbt/domains/commerce/docs/beginner-guide.md`
  에서 gold 를 미구현으로 주장하는 잔존 0건(이력 인용 제외). views.md 에서 "dbt seed" 메커니즘 서술
  제거 확인. partitioning-indexing-plan.md 에 최신 포인터 존재.

## 3. 검증 동반 변경 (런타임 확인 필요 — 컨테이너·Marquez 기동 후)

### V1. gold DAG 에 OpenLineage inlets/outlets (silver→gold 엣지를 Marquez 에)

- **목적**: C5 는 dbt 문서 수준. Marquez 그래프에서 silver(iceberg)→gold(postgres) 엣지를 실제로
  이으려면 `commerce_load_gold.load_gold` 태스크가 OL 이벤트에 입출력 dataset 을 실어야 한다.
- **왜 "검증 동반"인가**: OL 의 dataset 이름은 **네임스페이스·이름 규격이 방출자와 정확히 일치**해야
  stitch 된다. Cosmos(dbt) 가 방출하는 silver dataset 의 실제 네임스페이스/이름 표기는 런타임에
  확인해야 한다(예상: namespace `trino://trino:8080`, name `iceberg_dev.commerce.silver_license_history`).
- **절차**:
  1. Marquez 기동(`docker compose --profile lineage up -d`) 후 `commerce_load_silver` 1회 실행.
  2. `curl -s http://127.0.0.1:5000/api/v1/namespaces` 및 UI(3000)에서 **silver dataset 의
     namespace/name 표기를 실측**.
  3. `commerce_load_gold.py` 의 `load_gold` 태스크에 실측 표기와 일치하는 inlets/outlets 를 부여
     (Airflow 3 Asset URI 사용; outlets 는 `postgres://serving-postgres:5432/serving.public.commerce_business_entity` 형
     — 역시 OL provider 의 postgres 표기 규격을 2번에서 함께 실측해 맞춘다).
  4. gold 1회 실행 → Marquez 에서 silver→gold 엣지 확인. 안 이어지면 표기 불일치 — 2번 재실측.
- **수용 기준**: Marquez UI 에서 `silver_license_history/current → commerce_*` 엣지 가시화.
  실패해도 파이프라인 무영향(OL fail-open) — 이 항목만 미완으로 남기고 §7 에 기록 가능.

## 4. 변경 금지 (정당성 확인 완료 — 재론하지 말 것)

1. **마커·증분·청크 체계**(silver pre_hook/DONE marker/cold-start 시드, gold watermark/선삭제):
   3계층 일관 구현 확인. Cosmos 제약과의 결합은 [cosmos.md](cosmos.md) §3 에 이미 정리.
2. **silver 의 record_json 보존**: 실측(고유필드 41%가 1종 전용) 근거의 schema-on-read 설계.
   "silver 에서 전부 컬럼화"로 바꾸지 않는다.
3. **gold detail 78 + 뷰 320 카탈로그 구동**: 사용자 확정(엄격 클러스터 규칙). 자동 명명 금지
   가드(NAME_BY_MEMBER ValueError)도 의도된 거버넌스다.
4. **gold 의 Python 구현**: dbt 이관 금지 — 서빙 Postgres 의 인덱스/bigserial/뷰 계약은 dbt-trino 로
   불가. lineage 는 C5/V1 로 보완한다.
5. **`commerce_entity_key` 영구 보존**: 재적재/초기화에서 절대 삭제 금지(ddl.py docstring).
6. **타 도메인 파일**: 이 가이드로 수정 금지(§6 은 공유·합의용 관찰).

## 5. 구현 순서·검증 일괄

권장 순서: C1(보안) → C2 → C3 → C4(dbt 리팩터, compile diff 는 항목별로) → C5 → C6 → C7(문서) → (선택) V1.

전체 검증(컨테이너):
```bash
# dbt: parse + compile diff(항목별) + run/test
docker compose run --rm airflow-scheduler /home/airflow/dbt-venv/bin/dbt parse \
  --project-dir /opt/airflow/dbt/domains/commerce --profiles-dir /opt/airflow/dbt/domains/commerce --target dev
# pytest + 보안 게이트(로컬 가능)
PYTHONPATH=dags/domains/commerce/include python3 -m pytest dags/domains/commerce/tests -q
PYTHONPATH=dags/domains/commerce/include python3 -m security
# DAG import
docker compose run --rm airflow-scheduler airflow dags list-import-errors
```

## 6. 범위 외 관찰 — 타 도메인 (공유·합의용, 이 가이드로 수정 금지)

2026-07-12 전 도메인 dbt 실측(citydata·culture·traffic·weather·transit·elt_smoke·asac_axes) 요약.
수정은 각 도메인 담당 몫 — 팀 합의 안건으로만 공유한다.

1. **gold 이원화(용어)**: 타 도메인 gold 는 **전부 Iceberg 분석 집계**, commerce 만 서빙 Postgres
   마트 — 프로젝트에서 "gold" 가 두 의미로 쓰인다. 파이프라인은 양쪽 다 정상이나, 상위 문서에 용어
   정의(분석 gold vs 서빙 마트)를 합의해 명시할 것을 권장.
2. **행정동 축 3원화**: ① asac_axes(dim_admin_dong/boundary/crosswalk seed — citydata·culture·
   traffic·transit 소비) ② commerce 자체 MOIS ref(`bronze_ref_admin_dong`, 전국·매일 전량 교체)
   ③ weather 자체 seed(`weather_place_grid_mapping.csv`). asac_axes 의 `dim_beop_admin_link` 는
   commerce 를 겨냥해 설계됐지만 **commerce 는 미소비**(자체 ref 가 더 신선·전국 범위). 조인이 코드
   (admin_dong_code) 기준이라 즉시 위험은 낮으나, 행정동 개편 시점에 정본 단일화 합의 필요.
3. **명명·스키마 관례 분열**: transit 만 `slv_` 접두(나머지 `silver_`) + 스키마가 도메인명이 아닌
   `ops_smoke`/`dev_local`(elt_smoke 와 공유); traffic·weather 는 bronze 를 `ask_seoul` 별도 스키마로
   분리(나머지는 동일 스키마 + 접두 방식); citydata 만 profiles `catalog:` 키 + `generate_schema_name`
   오버라이드.
4. **재료화 관례 분열**: culture 는 silver 12 + gold 6 **전부 full-rebuild table**(증분 없음 — 데이터
   증가 시 비용·시간 리스크), weather silver 2종도 full rebuild. 증분 전략도 citydata `delete+insert`
   vs traffic·weather·transit `merge` 로 갈린다. citydata schema.yml 서술("incremental(merge)")과
   실제 config(delete+insert) 불일치(문서 드리프트).
5. **테스트 편중**: 싱귤러 테스트 weather 24·traffic 12·transit 12 vs citydata 2·elt_smoke 2.
   culture 는 warn 등급 제네릭 위주 + `int_culture_activity_days`(ephemeral) 무테스트.
6. **exposures 선례**: culture 만 보유(`cross_domain_join_contract` — 크로스도메인 조인 계약 문서화).
   C5 가 이 선례를 따른다. 타 도메인도 소비자 명시에 유용.
7. **commerce 결합**: 어떤 도메인도 commerce 테이블을 소비하지 않음(현재 결합 0 — 크로스도메인 소스는
   citydata→culture·traffic 뿐).

## 7. 완료 처리

- [ ] 각 항목 수용 기준 통과 기록(명령·결과 요약)
- [ ] `change-log.md` 항목 추가(구조 변경 이력 규칙)
- [ ] 이 문서 §0 상태 `DONE` + CLAUDE.md Pending 목록에서 상태 갱신
- [ ] V1 미완이면 사유와 함께 명시
