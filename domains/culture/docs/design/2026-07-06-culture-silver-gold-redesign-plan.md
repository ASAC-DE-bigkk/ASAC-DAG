# culture silver/gold 재설계 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** bronze 12테이블에서 #48 공통축 표준(공간 5컬럼·KST 시간·계보 사전)을 canonical로 탑재한 silver 9모델 + gold 3마트를 dbt로 재구축한다.

**Architecture:** 전 모델 `table`(full rebuild) 물질화. 파싱·dedup 몸통과 행정동 할당(`asac_axes` 패키지 의존)을 CTE로 격리. dedup 정렬은 `load_date desc, ingest_ts desc, raw_object_key desc`(관측 역전 방지). 설계 문서: [2026-07-06-culture-silver-gold-redesign.md](2026-07-06-culture-silver-gold-redesign.md)

**Tech Stack:** dbt-trino + Trino + Iceberg(R2 Data Catalog, dev=`iceberg_dev`) · 공용 패키지 `asac_axes`(ASAC-DBT#49) · Airflow `culture_transform` DAG(현재 pause)

## Global Constraints

- 구현 레포·작업 디렉토리: `C:\Users\Dell3571\ask-seoul\sample\dbt` (= ASAC-DBT clone, base 브랜치 `dev`). 파일 경로는 모두 이 레포 기준 `domains/culture/...`
- 문서 커밋만 ASAC-DAG 레포(`C:\Users\Dell3571\ask-seoul\sample\dags`) — Task 20에서만
- **선행 조건: ASAC-DBT PR #49 머지** (Task 1에서 게이트) — 미머지면 전체 중단
- dbt 실행은 컨테이너 경유. 모든 dbt 명령은 아래 `$DBT` 패턴 (Git Bash에서 실행):
  ```bash
  DBT="docker exec -e DBT_PROFILES_DIR=/opt/airflow/dbt/domains/culture -w /opt/airflow/dbt/domains/culture elt-infra-airflow-scheduler-1 /home/airflow/dbt-venv/bin/dbt"
  MSYS_NO_PATHCONV=1 $DBT <args> --target dev --no-use-colors
  ```
  애드혹 조회는 `$DBT show --inline "<SQL>"` (조회 전용)
- 보안: API 키·웹훅 값 출력/커밋 금지. 쓰기는 dev 카탈로그(`iceberg_dev`)·culture 스키마만. prod·공용 인프라 변경 금지(멘토 게이트)
- 커밋 메시지 끝: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>` / PR 본문 끝: `🤖 Generated with [Claude Code](https://claude.com/claude-code)`
- 이슈는 org `.github/ISSUE_TEMPLATE` 템플릿 준수 (#150 등록 때와 동일 절차)
- bronze 컬럼 계약(전 테이블 공통): `dataset, source, endpoint, record_json, raw_object_key, page_no, load_date(varchar), ingest_ts(varchar 'yyyyMMddTHHmmssZ' UTC), run_id, collected_at(timestamp(6) UTC)`
- `asac_axes` 인터페이스(실물 확인 완료): `seoul_lonlat(lon_raw, lat_raw)` → `... as longitude, ... as latitude` / `utc_to_kst(ts_expr)` → +9h / `admin_dong_contains(wkt_col, lon, lat)` → ST_Contains 술어 / 제네릭 테스트 `in_seoul_bbox: {kind: lon|lat}`·`axis_coverage: {min_ratio: N}` / seed `seoul_admin_dong_crosswalk(admin_dong_code, gu_code, stat_dong_code, stat_gu_code, gu, admin_dong, latitude, longitude, snapshot_ref)`·`seoul_admin_dong_boundary(sigungu, sigungu_code, dong, dong_code, boundary_wkt, admin_dong_code, gu_code)`

---

### Task 1: 사전 게이트 — #49 확인 · dev 동기화 · 이슈 · 브랜치

**Files:** 없음 (git/gh 준비 작업)

**Interfaces:**
- Produces: 환경변수 `$ISSUE`(이슈 번호), 브랜치 `feat/$ISSUE-culture-silver-redesign`

- [ ] **Step 1: #49 머지 확인 (게이트)**

```bash
gh pr view 49 --repo ASAC-DE-bigkk/ASAC-DBT --json state --jq .state
```
Expected: `MERGED`. 아니면 **여기서 중단**하고 사용자에게 보고.

- [ ] **Step 2: dev 동기화**

```bash
cd /c/Users/Dell3571/ask-seoul/sample/dbt && git checkout dev && git pull origin dev
ls packages/asac_axes/dbt_project.yml
```
Expected: pull 성공, `packages/asac_axes/dbt_project.yml` 존재.

- [ ] **Step 3: 이슈 등록**

org 템플릿(기능 제안)에 맞춰 등록 — 제목 `[Feat] culture silver/gold 재설계 — #48 공통축 canonical 적용`, 본문에 설계 문서 요지(9모델+gold3, 표준 컬럼, dedup 정렬키, NUM 게이트)와 `Ref #48` 포함:

```bash
gh issue create --repo ASAC-DE-bigkk/ASAC-DBT --title "[Feat] culture silver/gold 재설계 — #48 공통축 canonical 적용" --body-file <(작성한 본문)
```
Expected: 이슈 URL 반환 → `ISSUE=<번호>` 기록.

- [ ] **Step 4: 브랜치 생성**

```bash
git checkout -b feat/$ISSUE-culture-silver-redesign
```

### Task 2: B 게이트 — space `NUM` 안정성 실측

**Files:** 없음 (조회·기록만)

**Interfaces:**
- Produces: **결정값 `SPACE_KEY_MODE`** = `num`(안정) 또는 `hash`(불안정) — Task 8이 소비

- [ ] **Step 1: NUM→FAC_NAME 일관성 조회**

```bash
MSYS_NO_PATHCONV=1 $DBT show --inline "
select count(*) as unstable_nums from (
    select json_extract_scalar(record_json, '\$.NUM') as num,
           count(distinct json_extract_scalar(record_json, '\$.FAC_NAME')) as names
    from iceberg_dev.culture.bronze_seoul_cultural_space
    group by json_extract_scalar(record_json, '\$.NUM')
) t where names > 1" --target dev
```
Expected: `unstable_nums` 1행. `0` → `SPACE_KEY_MODE=num` / `>0` → `SPACE_KEY_MODE=hash`.

- [ ] **Step 2: 결과를 이슈에 코멘트로 기록** (`gh issue comment $ISSUE ...` — 수치와 결정 명시)

### Task 3: 구조 리셋 + 패키지 배선

**Files:**
- Delete: `domains/culture/models/silver/*.sql`(9개) · `domains/culture/models/gold/*.sql`(3개) · `domains/culture/macros/seoul_lonlat.sql` · `domains/culture/tests/assert_gold_grain_unique.sql` · `domains/culture/tests/assert_reservation_grain_unique.sql` · `domains/culture/tests/assert_boxoffice_grain_unique.sql` · `domains/culture/models/schema.yml`
- Create: `domains/culture/packages.yml`

**Interfaces:**
- Produces: `asac_axes` 매크로·seed를 `{{ ref('seoul_admin_dong_crosswalk') }}`·`{{ ref('seoul_admin_dong_boundary') }}`·`{{ asac_axes.* }}`로 사용 가능

- [ ] **Step 1: 구모델 삭제** (`git rm` 사용 — sources.yml·seeds·profiles·dbt_project.yml은 유지)

- [ ] **Step 2: packages.yml 작성**

```yaml
packages:
  - local: ../../packages/asac_axes
```

- [ ] **Step 3: deps + seed**

```bash
MSYS_NO_PATHCONV=1 $DBT deps --target dev
MSYS_NO_PATHCONV=1 $DBT seed --target dev
```
Expected: deps 성공, seed 4종(패키지 3 + sema_branch_gu) 적재 성공.

- [ ] **Step 4: 패키지 seed 조회 확인**

```bash
MSYS_NO_PATHCONV=1 $DBT show --inline "select count(*) as n from iceberg_dev.culture.seoul_admin_dong_crosswalk" --target dev
```
Expected: n=420.

- [ ] **Step 5: Commit** — `refactor(culture): 구 silver/gold 제거 + asac_axes 패키지 배선 (#$ISSUE)`

### Task 4: 공통 매크로 `culture_axes.sql`

**Files:**
- Create: `domains/culture/macros/culture_axes.sql`

**Interfaces:**
- Produces (전 모델이 소비):
  - `culture_lineage(source_system)` — 컬럼 6개 방출: `source_system, dag_run_id, raw_object_key, collected_at(KST), ingested_at(KST), load_date`
  - `culture_dedup_order()` — `load_date desc, ingest_ts desc, raw_object_key desc`
  - `culture_dong_map(src)` — 서브쿼리 방출: `(longitude, latitude, admin_dong_code, admin_dong, coord_gu_code)` — src의 고유 좌표별 행정동 (다중 폴리곤 매치는 코드 최소값으로 결정적)

- [ ] **Step 1: 매크로 작성**

```sql
{#
  culture_axes.sql — culture silver 공통 조각 3종.
  - lineage: #48 컬럼 사전(계보) — 모든 _at 은 KST.
  - dedup 정렬: 관측일(load_date) 우선 — proxy/백필의 ingest_ts 역전이
    최신 관측을 가리지 않게(설계 §3-A). raw_object_key 는 결정적 tie-breaker.
  - dong_map: 좌표 → 행정동 point-in-polygon. 고유 좌표만 연산(비용),
    다중 매치는 admin_dong_code 최소값(결정적).
#}

{% macro culture_lineage(source_system) -%}
    '{{ source_system }}' as source_system,
    run_id as dag_run_id,
    raw_object_key,
    {{ asac_axes.utc_to_kst('collected_at') }} as collected_at,
    {{ asac_axes.utc_to_kst("try(cast(date_parse(ingest_ts, '%Y%m%dT%H%i%sZ') as timestamp(6)))") }} as ingested_at,
    load_date
{%- endmacro %}

{% macro culture_dedup_order() -%}
load_date desc, ingest_ts desc, raw_object_key desc
{%- endmacro %}

{% macro culture_dong_map(src) -%}
(
    select
        c.longitude,
        c.latitude,
        min(b.admin_dong_code)               as admin_dong_code,
        min_by(b.dong, b.admin_dong_code)    as admin_dong,
        min_by(b.gu_code, b.admin_dong_code) as coord_gu_code
    from (
        select distinct longitude, latitude
        from {{ src }}
        where longitude is not null and latitude is not null
    ) c
    join {{ ref('seoul_admin_dong_boundary') }} b
      on {{ asac_axes.admin_dong_contains('b.boundary_wkt', 'c.longitude', 'c.latitude') }}
    group by c.longitude, c.latitude
)
{%- endmacro %}
```

- [ ] **Step 2: 파스 확인** — `MSYS_NO_PATHCONV=1 $DBT parse --target dev` Expected: 에러 0.

- [ ] **Step 3: Commit** — `feat(culture): 공통 매크로 — lineage/dedup정렬/dong_map (#$ISSUE)`

### Task 5: seed 확장 — sema 좌표 + sejong 위치

**Files:**
- Create: `domains/culture/seeds/sema_branch_location.csv` (기존 sema_branch_gu.csv 대체)
- Create: `domains/culture/seeds/sejong_location.csv`
- Delete: `domains/culture/seeds/sema_branch_gu.csv`
- Modify: `domains/culture/dbt_project.yml` (seeds column_types)

**Interfaces:**
- Produces: `ref('sema_branch_location')` — `keyword, gu, priority(int), latitude(double|null), longitude(double|null)` / `ref('sejong_location')` — `venue, gu, latitude, longitude`
- 원칙: **seed엔 좌표만, 행정동 코드는 항상 모델에서 파생** (개편 시 자동 재부여)

- [ ] **Step 1: sema_branch_location.csv 작성** — 기존 29행 유지(`location_key`→`gu` rename), SeMA 실분관(priority 10) 10행에만 좌표. ⚠️ 좌표는 근사값 — Step 4 검증이 게이트:

```csv
keyword,gu,priority,latitude,longitude
서소문본관,중구,10,37.5641,126.9738
북서울,노원구,10,37.6295,127.0667
난지,마포구,10,37.5661,126.8788
남서울,관악구,10,37.4765,126.9817
SeMA 창고,은평구,10,37.6109,126.9345
벙커,영등포구,10,37.5254,126.9276
사진미술관,도봉구,10,37.6540,127.0479
미술아카이브,종로구,10,37.6115,126.9670
서서울,금천구,10,37.4525,126.9019
백남준,종로구,10,37.5726,127.0155
경희궁,종로구,8,,
충무아트,중구,8,,
금나래,금천구,8,,
갤러리관악,관악구,8,,
예송,송파구,8,,
인재개발원,서초구,8,,
서울숲,성동구,8,,
역삼,강남구,6,,
상암,마포구,6,,
본관,중구,5,,
금천구,금천구,5,,
관악,관악구,5,,
동작,동작구,5,,
영등포,영등포구,5,,
송파,송파구,5,,
용산,용산구,5,,
도봉,도봉구,5,,
중랑,중랑구,5,,
성북,성북구,5,,
```

- [ ] **Step 2: sejong_location.csv 작성**

```csv
venue,gu,latitude,longitude
세종문화회관,종로구,37.5725,126.9757
```

- [ ] **Step 3: dbt_project.yml에 seed 타입 추가** (`models:` 블록 아래 append)

```yaml
seeds:
  culture:
    sema_branch_location:
      +column_types: {priority: integer, latitude: double, longitude: double}
    sejong_location:
      +column_types: {latitude: double, longitude: double}
```

- [ ] **Step 4: seed 적재 + 좌표→구 검증 (게이트)**

```bash
MSYS_NO_PATHCONV=1 $DBT seed --target dev
MSYS_NO_PATHCONV=1 $DBT show --inline "
select s.keyword, s.gu as seed_gu, b.sigungu as coord_gu
from iceberg_dev.culture.sema_branch_location s
join iceberg_dev.culture.seoul_admin_dong_boundary b
  on ST_Contains(ST_GeometryFromText(b.boundary_wkt), ST_Point(s.longitude, s.latitude))
where s.latitude is not null and s.gu <> b.sigungu
union all
select j.venue, j.gu, b.sigungu
from iceberg_dev.culture.sejong_location j
join iceberg_dev.culture.seoul_admin_dong_boundary b
  on ST_Contains(ST_GeometryFromText(b.boundary_wkt), ST_Point(j.longitude, j.latitude))
where j.gu <> b.sigungu" --target dev
```
Expected: **0행**. 불일치 행이 나오면 해당 분관 공식 주소로 좌표 재조사 후 CSV 수정·재실행 (seed의 gu가 정답 라벨).

- [ ] **Step 5: 구 seed 테이블 정리** — 파일 삭제 후 warehouse의 `sema_branch_gu` 테이블 DROP:

```bash
cat > "$SCRATCH/drop_old_seed.py" <<'EOF'
import trino
conn = trino.dbapi.connect(host="trino", port=8080, user="airflow", catalog="iceberg_dev", schema="culture")
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS sema_branch_gu")
cur.fetchall(); print("dropped: sema_branch_gu")
EOF
docker cp "$SCRATCH/drop_old_seed.py" elt-infra-airflow-scheduler-1:/tmp/drop_old_seed.py
MSYS_NO_PATHCONV=1 docker exec elt-infra-airflow-scheduler-1 python /tmp/drop_old_seed.py
```
Expected: `dropped: sema_branch_gu`.

- [ ] **Step 6: Commit** — `feat(culture): seed 확장 — 분관 좌표(sema_branch_location)·sejong_location (#$ISSUE)`

### Task 6: sources.yml — detail 2종 추가

**Files:**
- Modify: `domains/culture/models/sources.yml` (tables 목록 끝에 append)

**Interfaces:**
- Produces: `source('culture_bronze', 'bronze_kopis_facility_detail')` · `source('culture_bronze', 'bronze_kopis_performance_detail')` — Task 7·9가 소비

- [ ] **Step 1: 테이블 2개 추가**

```yaml
      - name: bronze_kopis_facility_detail
        description: KOPIS 공연시설상세(prfplc/{mt10id}) 원본 — 좌표(la=위도, lo=경도)·주소(adres). 시설 좌표의 유일 원천
        freshness:   # 좌표 차원(느린 변화) → 목록과 동일 완화
          warn_after: {count: 8, period: day}
          error_after: {count: 14, period: day}
        columns:
          - name: record_json
            description: 원본 레코드(JSON) — mt10id·adres·la·lo 등
      - name: bronze_kopis_performance_detail
        description: KOPIS 공연상세(pblprfr/{mt20id}) 원본 — mt10id(공연시설 ID) 보유, 공연→시설 정밀 조인의 원천
        columns:
          - name: record_json
            description: 원본 레코드(JSON) — mt20id·mt10id·prfstate 등
```

- [ ] **Step 2: 확인** — `MSYS_NO_PATHCONV=1 $DBT parse --target dev` Expected: 에러 0.

- [ ] **Step 3: Commit** — `feat(culture): sources에 KOPIS detail 2종 선언 (#$ISSUE)`

### Task 7: silver_culture_facility (dim — 좌표 허브)

**Files:**
- Create: `domains/culture/models/silver/silver_culture_facility.sql`
- Create: `domains/culture/models/schema.yml` (신규 — 이 태스크에서 파일 생성, 이후 태스크는 append)

**Interfaces:**
- Consumes: Task 4 매크로, Task 6 sources
- Produces: `ref('silver_culture_facility')` — `facility_id, facility_name, sido, address, longitude, latitude, gu, gu_code, admin_dong, admin_dong_code, source_system, dag_run_id, raw_object_key, collected_at, ingested_at, load_date` (Task 9·10이 공간축 소스로 조인)

- [ ] **Step 1: 모델 작성**

```sql
-- silver: KOPIS 공연시설 dim — 목록(prfplc) + 상세(la/lo·adres) 병합. KOPIS 계열 좌표 허브.
-- 상세는 max_detail 캡으로 부분 수집(설계 §5) → left join fail-open, 좌표 없는 시설도 구 레벨 유지.
-- SCD2 는 의도적 보류(설계 §3-C) — bronze 가 전 이력 박제, v1 은 최신본 dim.

with list_bronze as (
    select
        json_extract_scalar(record_json, '$.mt10id')  as facility_id,
        json_extract_scalar(record_json, '$.fcltynm') as facility_name,
        json_extract_scalar(record_json, '$.sidonm')  as sido,
        json_extract_scalar(record_json, '$.gugunnm') as gu_raw,
        ingest_ts,
        {{ culture_lineage('kopis') }}
    from {{ source('culture_bronze', 'bronze_kopis_facility') }}
),

list_latest as (
    select * from (
        select
            facility_id,
            nullif(trim(facility_name), '') as facility_name,
            nullif(trim(sido), '')          as sido,
            nullif(trim(gu_raw), '')        as gu,
            source_system, dag_run_id, raw_object_key, collected_at, ingested_at, load_date,
            row_number() over (partition by facility_id order by {{ culture_dedup_order() }}) as rn
        from list_bronze
        where facility_id is not null
    ) where rn = 1
),

detail_bronze as (
    select
        json_extract_scalar(record_json, '$.mt10id') as facility_id,
        json_extract_scalar(record_json, '$.adres')  as address_raw,
        json_extract_scalar(record_json, '$.la')     as lat_raw,   -- la = 위도
        json_extract_scalar(record_json, '$.lo')     as lon_raw,   -- lo = 경도
        ingest_ts, load_date, raw_object_key
    from {{ source('culture_bronze', 'bronze_kopis_facility_detail') }}
),

detail_latest as (
    select * from (
        select
            facility_id,
            nullif(trim(address_raw), '') as address,
            {{ asac_axes.seoul_lonlat('lon_raw', 'lat_raw') }},
            row_number() over (partition by facility_id order by {{ culture_dedup_order() }}) as rn
        from detail_bronze
        where facility_id is not null
    ) where rn = 1
),

joined as (
    select
        l.facility_id, l.facility_name, l.sido, l.gu,
        d.address, d.longitude, d.latitude,
        l.source_system, l.dag_run_id, l.raw_object_key, l.collected_at, l.ingested_at, l.load_date
    from list_latest l
    left join detail_latest d on d.facility_id = l.facility_id
),

dong_map as {{ culture_dong_map('joined') }},

gu_codes as (select distinct gu, gu_code from {{ ref('seoul_admin_dong_crosswalk') }})

select
    j.facility_id,
    j.facility_name,
    j.sido,
    j.address,
    j.longitude,
    j.latitude,
    j.gu,
    coalesce(g.gu_code, d.coord_gu_code) as gu_code,
    d.admin_dong,
    d.admin_dong_code,
    j.source_system, j.dag_run_id, j.raw_object_key, j.collected_at, j.ingested_at, j.load_date
from joined j
left join dong_map d on j.longitude = d.longitude and j.latitude = d.latitude
left join gu_codes g on g.gu = j.gu
```

- [ ] **Step 2: schema.yml 신규 작성** (파일 생성)

```yaml
version: 2

models:
  - name: silver_culture_facility
    description: KOPIS 공연시설 dim — 목록+상세(좌표) 병합. 좌표는 detail 수집분만(부분 커버리지 — 설계 §5).
    columns:
      - name: facility_id
        tests: [not_null, unique]
      - name: load_date
        tests: [not_null]
      - name: ingested_at
        tests: [not_null]
      - name: longitude
        tests:
          - in_seoul_bbox: {kind: lon}
      - name: latitude
        tests:
          - in_seoul_bbox: {kind: lat}
      - name: gu_code
        tests:
          - axis_coverage:
              min_ratio: 0.9
              config: {severity: warn}
      - name: admin_dong_code
        tests:
          - axis_coverage:
              min_ratio: 0.05   # detail 캡(~12%) 반영 초기값 — Task 18에서 실측 후 확정
              config: {severity: warn}
```

- [ ] **Step 3: 빌드** — `MSYS_NO_PATHCONV=1 $DBT run --select silver_culture_facility --target dev` Expected: PASS 1.

- [ ] **Step 4: 테스트** — `MSYS_NO_PATHCONV=1 $DBT test --select silver_culture_facility --target dev` Expected: 전건 pass(warn 허용).

- [ ] **Step 5: 정합 조회** — 행수(목록 dedup 후)·좌표 보유 수 확인:

```bash
MSYS_NO_PATHCONV=1 $DBT show --inline "select count(*) rows, count(latitude) with_coords, count(admin_dong_code) with_dong from iceberg_dev.culture.silver_culture_facility" --target dev
```
Expected: rows ≈ 1,600~1,700 / with_coords ≈ 200(detail 캡) / with_dong ≤ with_coords.

- [ ] **Step 6: Commit** — `feat(culture): silver_culture_facility — detail 좌표 병합 dim (#$ISSUE)`

### Task 8: silver_culture_space (dim)

**Files:**
- Create: `domains/culture/models/silver/silver_culture_space.sql`
- Modify: `domains/culture/models/schema.yml` (append)

**Interfaces:**
- Consumes: Task 2의 `SPACE_KEY_MODE`
- Produces: `ref('silver_culture_space')` — `space_key, facility_name, gu, address, subject_code, longitude, latitude, gu_code, admin_dong, admin_dong_code, (계보 6종)`

- [ ] **Step 1: 모델 작성** — ⚠️ 좌표 축 스왑: `X_COORD`=위도, `Y_COORD`=경도. `space_key`는 Task 2 결정에 따라 두 형태 중 하나 사용:

`SPACE_KEY_MODE=num`일 때: `json_extract_scalar(record_json, '$.NUM') as space_key,`
`SPACE_KEY_MODE=hash`일 때(아래 SQL 기본): `to_hex(md5(to_utf8(concat_ws('|', coalesce(fac_name,''), coalesce(addr,''))))) as space_key`

```sql
-- silver: 서울 문화공간 dim. ⚠️ 좌표 축 스왑: X_COORD=위도, Y_COORD=경도.
-- space_key: NUM 안정성 실측(Task 2) 결과에 따라 NUM 또는 (FAC_NAME|ADDR) 해시 —
-- 팀 전 도메인이 원천 안정 ID 사용, 순번형 키 선례 없음(설계 §3-B).

with bronze as (
    select
        json_extract_scalar(record_json, '$.NUM')      as num_raw,
        json_extract_scalar(record_json, '$.FAC_NAME') as fac_name,
        json_extract_scalar(record_json, '$.GNGU')     as gu_raw,
        json_extract_scalar(record_json, '$.ADDR')     as addr,
        json_extract_scalar(record_json, '$.SUBJCODE') as subject_code,
        json_extract_scalar(record_json, '$.X_COORD')  as lat_raw,   -- 위도
        json_extract_scalar(record_json, '$.Y_COORD')  as lon_raw,   -- 경도
        ingest_ts,
        {{ culture_lineage('seoul') }}
    from {{ source('culture_bronze', 'bronze_seoul_cultural_space') }}
),

typed as (
    select
        -- SPACE_KEY_MODE 에 따라 아래 한 줄 선택 (Task 2 결정 — 기본: hash)
        to_hex(md5(to_utf8(concat_ws('|', coalesce(nullif(trim(fac_name), ''), ''), coalesce(nullif(trim(addr), ''), ''))))) as space_key,
        nullif(trim(fac_name), '')     as facility_name,
        nullif(trim(gu_raw), '')       as gu,
        nullif(trim(addr), '')         as address,
        nullif(trim(subject_code), '') as subject_code,
        {{ asac_axes.seoul_lonlat('lon_raw', 'lat_raw') }},
        ingest_ts, source_system, dag_run_id, raw_object_key, collected_at, ingested_at, load_date
    from bronze
    where nullif(trim(fac_name), '') is not null
),

latest as (
    select * from (
        select *, row_number() over (partition by space_key order by {{ culture_dedup_order() }}) as rn
        from typed
    ) where rn = 1
),

dong_map as {{ culture_dong_map('latest') }},

gu_codes as (select distinct gu, gu_code from {{ ref('seoul_admin_dong_crosswalk') }})

select
    l.space_key, l.facility_name, l.gu, l.address, l.subject_code,
    l.longitude, l.latitude,
    coalesce(g.gu_code, d.coord_gu_code) as gu_code,
    d.admin_dong, d.admin_dong_code,
    l.source_system, l.dag_run_id, l.raw_object_key, l.collected_at, l.ingested_at, l.load_date
from latest l
left join dong_map d on l.longitude = d.longitude and l.latitude = d.latitude
left join gu_codes g on g.gu = l.gu
```

- [ ] **Step 2: schema.yml append**

```yaml
  - name: silver_culture_space
    description: 서울 문화공간 dim (좌표 축 스왑 처리, space_key = Task 2 결정 키)
    columns:
      - name: space_key
        tests: [not_null, unique]
      - name: load_date
        tests: [not_null]
      - name: ingested_at
        tests: [not_null]
      - name: longitude
        tests:
          - in_seoul_bbox: {kind: lon}
      - name: latitude
        tests:
          - in_seoul_bbox: {kind: lat}
      - name: admin_dong_code
        tests:
          - axis_coverage:
              min_ratio: 0.5
              config: {severity: warn}
```

- [ ] **Step 3~4: run + test** (`--select silver_culture_space`) Expected: run PASS, test pass. `unique` 실패 시 = 해시 충돌(동명·동주소) — FAC_NAME별 건수 조회로 원인 확인 후 키에 `subject_code` 추가.

- [ ] **Step 5: 정합 조회** — rows ≈ 1,000~1,100 (bronze 최신 load_date 행수와 비교).

- [ ] **Step 6: Commit** — `feat(culture): silver_culture_space (#$ISSUE)`

### Task 9: silver_culture_performance (기간 fact — detail 정밀 조인)

**Files:**
- Create: `domains/culture/models/silver/silver_culture_performance.sql`
- Modify: `domains/culture/models/schema.yml` (append)

**Interfaces:**
- Consumes: `ref('silver_culture_facility')` (Task 7 산출 컬럼)
- Produces: `ref('silver_culture_performance')` — `performance_id, performance_name, genre, state, venue_name, facility_id, facility_match('detail_id'|'name'|null), event_start_date, event_end_date, event_at, longitude, latitude, gu, gu_code, admin_dong, admin_dong_code, (계보 6종)` — Task 16·17이 소비

- [ ] **Step 1: 모델 작성**

```sql
-- silver: KOPIS 공연 기간 fact. 시설 축은 detail(mt10id) 정밀 조인 + 이름 매칭 폴백 —
-- 기존 이름 단독 매칭의 동명 시설 임의 선택을 해소(설계 §2). 공간축은 facility dim 경유.

with list_bronze as (
    select
        json_extract_scalar(record_json, '$.mt20id')    as performance_id,
        json_extract_scalar(record_json, '$.prfnm')     as performance_name,
        json_extract_scalar(record_json, '$.genrenm')   as genre,
        json_extract_scalar(record_json, '$.fcltynm')   as venue_name,
        json_extract_scalar(record_json, '$.prfstate')  as state,
        json_extract_scalar(record_json, '$.prfpdfrom') as start_raw,
        json_extract_scalar(record_json, '$.prfpdto')   as end_raw,
        ingest_ts,
        {{ culture_lineage('kopis') }}
    from {{ source('culture_bronze', 'bronze_kopis_performance') }}
),

list_latest as (
    select * from (
        select
            performance_id,
            nullif(trim(performance_name), '') as performance_name,
            nullif(trim(genre), '')            as genre,
            nullif(trim(venue_name), '')       as venue_name,
            nullif(trim(state), '')            as state,
            try(cast(date_parse(trim(start_raw), '%Y.%m.%d') as date)) as event_start_date,
            try(cast(date_parse(trim(end_raw), '%Y.%m.%d') as date))   as event_end_date,
            source_system, dag_run_id, raw_object_key, collected_at, ingested_at, load_date,
            row_number() over (partition by performance_id order by {{ culture_dedup_order() }}) as rn
        from list_bronze
        where performance_id is not null
    ) where rn = 1
),

detail_latest as (
    select * from (
        select
            json_extract_scalar(record_json, '$.mt20id') as performance_id,
            json_extract_scalar(record_json, '$.mt10id') as facility_id,
            row_number() over (
                partition by json_extract_scalar(record_json, '$.mt20id')
                order by {{ culture_dedup_order() }}
            ) as rn
        from {{ source('culture_bronze', 'bronze_kopis_performance_detail') }}
        where json_extract_scalar(record_json, '$.mt10id') is not null
    ) where rn = 1
),

fac as (
    select facility_id, facility_name, longitude, latitude, gu, gu_code, admin_dong, admin_dong_code
    from {{ ref('silver_culture_facility') }}
),

fac_by_name as (
    select facility_name, min(facility_id) as facility_id
    from fac
    where facility_name is not null
    group by facility_name
),

resolved as (
    select
        l.*,
        coalesce(d.facility_id, n.facility_id) as facility_id,
        case when d.facility_id is not null then 'detail_id'
             when n.facility_id is not null then 'name' end as facility_match
    from list_latest l
    left join detail_latest d on d.performance_id = l.performance_id
    left join fac_by_name n on n.facility_name = l.venue_name
)

select
    r.performance_id, r.performance_name, r.genre, r.state, r.venue_name,
    r.facility_id, r.facility_match,
    r.event_start_date, r.event_end_date,
    cast(r.event_start_date as timestamp(6)) as event_at,
    f.longitude, f.latitude, f.gu, f.gu_code, f.admin_dong, f.admin_dong_code,
    r.source_system, r.dag_run_id, r.raw_object_key, r.collected_at, r.ingested_at, r.load_date
from resolved r
left join fac f on f.facility_id = r.facility_id
```

- [ ] **Step 2: schema.yml append**

```yaml
  - name: silver_culture_performance
    description: KOPIS 공연 기간 fact — detail(mt10id) 정밀 시설 조인, 공간축은 facility 경유
    columns:
      - name: performance_id
        tests: [not_null, unique]
      - name: load_date
        tests: [not_null]
      - name: ingested_at
        tests: [not_null]
      - name: facility_match
        tests:
          - accepted_values:
              values: ["detail_id", "name"]
      - name: gu_code
        tests:
          - axis_coverage:
              min_ratio: 0.5
              config: {severity: warn}
```

- [ ] **Step 3~4: run + test** Expected: pass.

- [ ] **Step 5: 정합 조회** — rows ≈ 1,200 / `facility_match='detail_id'` 비율 확인(detail 캡만큼):

```bash
MSYS_NO_PATHCONV=1 $DBT show --inline "select facility_match, count(*) n from iceberg_dev.culture.silver_culture_performance group by 1" --target dev
```

- [ ] **Step 6: Commit** — `feat(culture): silver_culture_performance — detail 정밀 시설 조인 (#$ISSUE)`

### Task 10: silver_culture_festival (기간 fact)

**Files:**
- Create: `domains/culture/models/silver/silver_culture_festival.sql`
- Modify: `domains/culture/models/schema.yml` (append)

**Interfaces:**
- Consumes: `ref('silver_culture_facility')`
- Produces: `ref('silver_culture_festival')` — `festival_id, festival_name, genre, venue_name, facility_id, facility_match('name'|null), event_start_date, event_end_date, event_at, longitude, latitude, gu, gu_code, admin_dong, admin_dong_code, (계보 6종)`

- [ ] **Step 1: 모델 작성** (performance와 동형 — detail 없음, 이름 매칭만)

```sql
-- silver: KOPIS 축제 기간 fact. detail 미수집 → 시설은 이름 매칭만(미매칭 NULL 허용, 매칭률 테스트 감시).

with bronze as (
    select
        json_extract_scalar(record_json, '$.mt20id')    as festival_id,
        json_extract_scalar(record_json, '$.prfnm')     as festival_name,
        json_extract_scalar(record_json, '$.genrenm')   as genre,
        json_extract_scalar(record_json, '$.fcltynm')   as venue_name,
        json_extract_scalar(record_json, '$.prfpdfrom') as start_raw,
        json_extract_scalar(record_json, '$.prfpdto')   as end_raw,
        ingest_ts,
        {{ culture_lineage('kopis') }}
    from {{ source('culture_bronze', 'bronze_kopis_festival') }}
),

latest as (
    select * from (
        select
            festival_id,
            nullif(trim(festival_name), '') as festival_name,
            nullif(trim(genre), '')         as genre,
            nullif(trim(venue_name), '')    as venue_name,
            try(cast(date_parse(trim(start_raw), '%Y.%m.%d') as date)) as event_start_date,
            try(cast(date_parse(trim(end_raw), '%Y.%m.%d') as date))   as event_end_date,
            source_system, dag_run_id, raw_object_key, collected_at, ingested_at, load_date,
            row_number() over (partition by festival_id order by {{ culture_dedup_order() }}) as rn
        from bronze
        where festival_id is not null
    ) where rn = 1
),

fac as (
    select facility_id, facility_name, longitude, latitude, gu, gu_code, admin_dong, admin_dong_code
    from {{ ref('silver_culture_facility') }}
),

fac_by_name as (
    select facility_name, min(facility_id) as facility_id
    from fac
    where facility_name is not null
    group by facility_name
)

select
    l.festival_id, l.festival_name, l.genre, l.venue_name,
    n.facility_id,
    case when n.facility_id is not null then 'name' end as facility_match,
    l.event_start_date, l.event_end_date,
    cast(l.event_start_date as timestamp(6)) as event_at,
    f.longitude, f.latitude, f.gu, f.gu_code, f.admin_dong, f.admin_dong_code,
    l.source_system, l.dag_run_id, l.raw_object_key, l.collected_at, l.ingested_at, l.load_date
from latest l
left join fac_by_name n on n.facility_name = l.venue_name
left join fac f on f.facility_id = n.facility_id
```

- [ ] **Step 2: schema.yml append**

```yaml
  - name: silver_culture_festival
    description: KOPIS 축제 기간 fact — 시설 이름 매칭(미매칭 NULL)
    columns:
      - name: festival_id
        tests: [not_null, unique]
      - name: load_date
        tests: [not_null]
      - name: ingested_at
        tests: [not_null]
      - name: gu_code
        tests:
          - axis_coverage:
              min_ratio: 0.3
              config: {severity: warn}
```

- [ ] **Step 3~5: run + test + 정합(rows ≈ 160)** — Expected: pass.
- [ ] **Step 6: Commit** — `feat(culture): silver_culture_festival (#$ISSUE)`

### Task 11: silver_culture_event (기간 fact — 자체 좌표)

**Files:**
- Create: `domains/culture/models/silver/silver_culture_event.sql`
- Modify: `domains/culture/models/schema.yml` (append)

**Interfaces:**
- Produces: `ref('silver_culture_event')` — `event_key, event_title, place, category, is_free, event_start_date, event_end_date, event_at, longitude, latitude, gu, gu_code, admin_dong, admin_dong_code, (계보 6종)`

- [ ] **Step 1: 모델 작성**

```sql
-- silver: 서울 문화행사 기간 fact. 자연키 부재 → event_key = md5(제목|시작일|장소) —
-- 제목 수정 시 분열은 알려진 한계(설계 §3-G). 좌표: LOT=경도, LAT=위도.
-- dedup 은 load_date 우선(§3-A) — 7/1 proxy(ingest_ts=7/6 수동)가 이후 관측을 못 가림.

with bronze as (
    select
        json_extract_scalar(record_json, '$.TITLE')    as title_raw,
        json_extract_scalar(record_json, '$.GUNAME')   as gu_raw,
        json_extract_scalar(record_json, '$.PLACE')    as place_raw,
        json_extract_scalar(record_json, '$.CODENAME') as category,
        json_extract_scalar(record_json, '$.IS_FREE')  as is_free,
        json_extract_scalar(record_json, '$.STRTDATE') as start_raw,
        json_extract_scalar(record_json, '$.END_DATE') as end_raw,
        json_extract_scalar(record_json, '$.LOT')      as lon_raw,   -- 경도
        json_extract_scalar(record_json, '$.LAT')      as lat_raw,   -- 위도
        ingest_ts,
        {{ culture_lineage('seoul') }}
    from {{ source('culture_bronze', 'bronze_seoul_cultural_event') }}
),

typed as (
    select
        nullif(trim(title_raw), '') as event_title,
        nullif(trim(gu_raw), '')    as gu,
        nullif(trim(place_raw), '') as place,
        nullif(trim(category), '')  as category,
        nullif(trim(is_free), '')   as is_free,
        try(cast(substr(start_raw, 1, 10) as date)) as event_start_date,
        try(cast(substr(end_raw, 1, 10) as date))   as event_end_date,
        {{ asac_axes.seoul_lonlat('lon_raw', 'lat_raw') }},
        ingest_ts, source_system, dag_run_id, raw_object_key, collected_at, ingested_at, load_date
    from bronze
    where nullif(trim(title_raw), '') is not null
),

keyed as (
    select
        to_hex(md5(to_utf8(concat_ws('|',
            coalesce(event_title, ''),
            coalesce(cast(event_start_date as varchar), ''),
            coalesce(place, ''))))) as event_key,
        typed.*
    from typed
),

latest as (
    select * from (
        select *, row_number() over (partition by event_key order by {{ culture_dedup_order() }}) as rn
        from keyed
    ) where rn = 1
),

dong_map as {{ culture_dong_map('latest') }},

gu_codes as (select distinct gu, gu_code from {{ ref('seoul_admin_dong_crosswalk') }})

select
    l.event_key, l.event_title, l.place, l.category, l.is_free,
    l.event_start_date, l.event_end_date,
    cast(l.event_start_date as timestamp(6)) as event_at,
    l.longitude, l.latitude, l.gu,
    coalesce(g.gu_code, d.coord_gu_code) as gu_code,
    d.admin_dong, d.admin_dong_code,
    l.source_system, l.dag_run_id, l.raw_object_key, l.collected_at, l.ingested_at, l.load_date
from latest l
left join dong_map d on l.longitude = d.longitude and l.latitude = d.latitude
left join gu_codes g on g.gu = l.gu
```

- [ ] **Step 2: schema.yml append**

```yaml
  - name: silver_culture_event
    description: 서울 문화행사 기간 fact. 기간 질의는 event_start_date <= D and D <= event_end_date (event_at 단독 필터 = 시작일 기준)
    columns:
      - name: event_key
        tests: [not_null, unique]
      - name: load_date
        tests: [not_null]
      - name: ingested_at
        tests: [not_null]
      - name: longitude
        tests:
          - in_seoul_bbox: {kind: lon}
      - name: latitude
        tests:
          - in_seoul_bbox: {kind: lat}
      - name: gu_code
        tests:
          - axis_coverage:
              min_ratio: 0.9
              config: {severity: warn}
      - name: admin_dong_code
        tests:
          - axis_coverage:
              min_ratio: 0.7
              config: {severity: warn}
```

- [ ] **Step 3~4: run + test** Expected: pass. run이 수 분 걸릴 수 있음(고유 좌표 × 426 폴리곤 — dong_map이 distinct로 억제).

- [ ] **Step 5: 정합 + proxy 역전 검증 (설계 A의 실증)**

```bash
MSYS_NO_PATHCONV=1 $DBT show --inline "select count(*) rows from iceberg_dev.culture.silver_culture_event" --target dev
MSYS_NO_PATHCONV=1 $DBT show --inline "
select load_date, count(*) n
from iceberg_dev.culture.silver_culture_event
group by load_date order by load_date" --target dev
```
Expected: rows ≈ 19,000~20,000(고유 엔티티). **load_date 분포에서 최신 load_date가 지배적**이어야 함 — 7/1(proxy)이 지배적이면 정렬키 역전이 남아있는 것(실패, 모델 수정).

- [ ] **Step 6: Commit** — `feat(culture): silver_culture_event — load_date 우선 dedup (#$ISSUE)`

### Task 12: silver_culture_exhibition (기간 fact — seed 위치)

**Files:**
- Create: `domains/culture/models/silver/silver_culture_exhibition.sql`
- Modify: `domains/culture/models/schema.yml` (append)

**Interfaces:**
- Consumes: `ref('sema_branch_location')` (Task 5)
- Produces: `ref('silver_culture_exhibition')` — `exhibition_id, title, venue_name, event_start_date, event_end_date, event_at, longitude, latitude, gu, gu_code, admin_dong, admin_dong_code, (계보 6종)`

- [ ] **Step 1: 모델 작성**

```sql
-- silver: 시립미술관 전시 기간 fact. 위치는 sema_branch_location seed 키워드 매칭 —
-- 분관(priority 10)만 좌표 보유 → 동 레벨은 분관 전시만, 그 외 구 레벨(설계 §4).

with bronze as (
    select
        json_extract_scalar(record_json, '$.DP_EX_NO') as exhibition_id,
        json_extract_scalar(record_json, '$.DP_NAME')  as title_raw,
        json_extract_scalar(record_json, '$.DP_PLACE') as venue_raw,
        json_extract_scalar(record_json, '$.DP_START') as start_raw,
        json_extract_scalar(record_json, '$.DP_END')   as end_raw,
        ingest_ts,
        {{ culture_lineage('seoul') }}
    from {{ source('culture_bronze', 'bronze_seoul_sema_exhibition') }}
),

latest as (
    select * from (
        select
            exhibition_id,
            nullif(trim(title_raw), '') as title,
            nullif(trim(venue_raw), '') as venue_name,
            try(cast(substr(start_raw, 1, 10) as date)) as event_start_date,
            try(cast(substr(end_raw, 1, 10) as date))   as event_end_date,
            source_system, dag_run_id, raw_object_key, collected_at, ingested_at, load_date,
            row_number() over (partition by exhibition_id order by {{ culture_dedup_order() }}) as rn
        from bronze
        where exhibition_id is not null
    ) where rn = 1
),

matched as (
    select
        l.exhibition_id,
        s.gu, s.latitude, s.longitude,
        row_number() over (
            partition by l.exhibition_id
            order by s.priority desc, s.keyword
        ) as pr
    from latest l
    join {{ ref('sema_branch_location') }} s on strpos(l.venue_name, s.keyword) > 0
),

best as (select exhibition_id, gu, latitude, longitude from matched where pr = 1),

placed as (
    select
        l.exhibition_id, l.title, l.venue_name,
        l.event_start_date, l.event_end_date,
        b.gu, b.latitude, b.longitude,
        l.source_system, l.dag_run_id, l.raw_object_key, l.collected_at, l.ingested_at, l.load_date
    from latest l
    left join best b on b.exhibition_id = l.exhibition_id
),

dong_map as {{ culture_dong_map('placed') }},

gu_codes as (select distinct gu, gu_code from {{ ref('seoul_admin_dong_crosswalk') }})

select
    p.exhibition_id, p.title, p.venue_name,
    p.event_start_date, p.event_end_date,
    cast(p.event_start_date as timestamp(6)) as event_at,
    p.longitude, p.latitude, p.gu,
    coalesce(g.gu_code, d.coord_gu_code) as gu_code,
    d.admin_dong, d.admin_dong_code,
    p.source_system, p.dag_run_id, p.raw_object_key, p.collected_at, p.ingested_at, p.load_date
from placed p
left join dong_map d on p.longitude = d.longitude and p.latitude = d.latitude
left join gu_codes g on g.gu = p.gu
```

- [ ] **Step 2: schema.yml append**

```yaml
  - name: silver_culture_exhibition
    description: 시립미술관 전시 기간 fact — 위치는 분관 seed 매칭
    columns:
      - name: exhibition_id
        tests: [not_null, unique]
      - name: load_date
        tests: [not_null]
      - name: ingested_at
        tests: [not_null]
      - name: gu_code
        tests:
          - axis_coverage:
              min_ratio: 0.5
              config: {severity: warn}
```

- [ ] **Step 3~5: run + test + 정합(rows ≈ 870)** — Expected: pass.
- [ ] **Step 6: Commit** — `feat(culture): silver_culture_exhibition (#$ISSUE)`

### Task 13: silver_culture_sejong (기간 fact — 상수 위치)

**Files:**
- Create: `domains/culture/models/silver/silver_culture_sejong.sql`
- Modify: `domains/culture/models/schema.yml` (append)

**Interfaces:**
- Consumes: `ref('sejong_location')` (Task 5)
- Produces: `ref('silver_culture_sejong')` — `sejong_id, title, genre, venue_name, event_start_date, event_end_date, event_at, longitude, latitude, gu, gu_code, admin_dong, admin_dong_code, (계보 6종)`

- [ ] **Step 1: 모델 작성**

```sql
-- silver: 세종문화회관 공연/전시 기간 fact. 단일 시설 → 위치는 sejong_location seed 상수(cross join 1행).

with bronze as (
    select
        json_extract_scalar(record_json, '$.PERFORM_IDX') as sejong_id,
        json_extract_scalar(record_json, '$.TITLE')       as title_raw,
        json_extract_scalar(record_json, '$.GENRE_NAME')  as genre,
        json_extract_scalar(record_json, '$.PLACE_LIST')  as venue_raw,
        json_extract_scalar(record_json, '$.START_DATE')  as start_raw,
        json_extract_scalar(record_json, '$.END_DATE')    as end_raw,
        ingest_ts,
        {{ culture_lineage('seoul') }}
    from {{ source('culture_bronze', 'bronze_seoul_sejong') }}
),

latest as (
    select * from (
        select
            sejong_id,
            nullif(trim(title_raw), '') as title,
            nullif(trim(genre), '')     as genre,
            nullif(trim(venue_raw), '') as venue_name,
            try(cast(date_parse(start_raw, '%Y%m%d') as date)) as event_start_date,
            try(cast(date_parse(end_raw, '%Y%m%d') as date))   as event_end_date,
            source_system, dag_run_id, raw_object_key, collected_at, ingested_at, load_date,
            row_number() over (partition by sejong_id order by {{ culture_dedup_order() }}) as rn
        from bronze
        where sejong_id is not null
    ) where rn = 1
),

placed as (
    select l.*, s.gu, s.latitude, s.longitude
    from latest l
    cross join {{ ref('sejong_location') }} s
),

dong_map as {{ culture_dong_map('placed') }},

gu_codes as (select distinct gu, gu_code from {{ ref('seoul_admin_dong_crosswalk') }})

select
    p.sejong_id, p.title, p.genre, p.venue_name,
    p.event_start_date, p.event_end_date,
    cast(p.event_start_date as timestamp(6)) as event_at,
    p.longitude, p.latitude, p.gu,
    coalesce(g.gu_code, d.coord_gu_code) as gu_code,
    d.admin_dong, d.admin_dong_code,
    p.source_system, p.dag_run_id, p.raw_object_key, p.collected_at, p.ingested_at, p.load_date
from placed p
left join dong_map d on p.longitude = d.longitude and p.latitude = d.latitude
left join gu_codes g on g.gu = p.gu
```

- [ ] **Step 2: schema.yml append**

```yaml
  - name: silver_culture_sejong
    description: 세종문화회관 기간 fact — 위치 상수(seed)
    columns:
      - name: sejong_id
        tests: [not_null, unique]
      - name: load_date
        tests: [not_null]
      - name: ingested_at
        tests: [not_null]
      - name: gu_code
        tests: [not_null]   # 상수 위치 — 항상 채워져야 함
```

- [ ] **Step 3~5: run + test + 정합(rows ≈ 16,000~17,000 고유)** — Expected: pass.
- [ ] **Step 6: Commit** — `feat(culture): silver_culture_sejong (#$ISSUE)`

### Task 14: silver_culture_reservation (일 스냅샷 fact)

**Files:**
- Create: `domains/culture/models/silver/silver_culture_reservation.sql`
- Modify: `domains/culture/models/schema.yml` (append)
- Create: `domains/culture/tests/assert_reservation_grain_unique.sql`

**Interfaces:**
- Produces: `ref('silver_culture_reservation')` — `service_id, reservation_type('culture'|'sport'), service_name, status, category, place, pay_type, event_at, longitude, latitude, gu, gu_code, admin_dong, admin_dong_code, (계보 6종)` — 그레인 (service_id, load_date)

- [ ] **Step 1: 모델 작성**

```sql
-- silver: 공공예약 일 스냅샷 fact (culture+sport union). 그레인 = (service_id, load_date) —
-- 그날의 서비스 상태. 같은 날 재적재/페이지 겹침(7/1 실증 44행)은 dedup 이 흡수.

with unioned as (
    select 'culture' as reservation_type, record_json, ingest_ts,
           {{ culture_lineage('seoul') }}
    from {{ source('culture_bronze', 'bronze_seoul_culture_reservation') }}
    union all
    select 'sport' as reservation_type, record_json, ingest_ts,
           {{ culture_lineage('seoul') }}
    from {{ source('culture_bronze', 'bronze_seoul_sports_reservation') }}
),

typed as (
    select
        reservation_type,
        json_extract_scalar(record_json, '$.SVCID') as service_id,
        nullif(trim(json_extract_scalar(record_json, '$.SVCNM')), '')      as service_name,
        nullif(trim(json_extract_scalar(record_json, '$.AREANM')), '')     as gu,
        nullif(trim(json_extract_scalar(record_json, '$.SVCSTATNM')), '')  as status,
        nullif(trim(json_extract_scalar(record_json, '$.MINCLASSNM')), '') as category,
        nullif(trim(json_extract_scalar(record_json, '$.PLACENM')), '')    as place,
        nullif(trim(json_extract_scalar(record_json, '$.PAYATNM')), '')    as pay_type,
        {{ asac_axes.seoul_lonlat("json_extract_scalar(record_json, '$.X')", "json_extract_scalar(record_json, '$.Y')") }},
        ingest_ts, source_system, dag_run_id, raw_object_key, collected_at, ingested_at, load_date
    from unioned
    where json_extract_scalar(record_json, '$.SVCID') is not null
),

latest as (
    select * from (
        select *, row_number() over (
            partition by service_id, load_date
            order by {{ culture_dedup_order() }}
        ) as rn
        from typed
    ) where rn = 1
),

dong_map as {{ culture_dong_map('latest') }},

gu_codes as (select distinct gu, gu_code from {{ ref('seoul_admin_dong_crosswalk') }})

select
    l.service_id, l.reservation_type, l.service_name, l.status, l.category, l.place, l.pay_type,
    cast(try(cast(l.load_date as date)) as timestamp(6)) as event_at,   -- 스냅샷 대표 시각
    l.longitude, l.latitude, l.gu,
    coalesce(g.gu_code, d.coord_gu_code) as gu_code,
    d.admin_dong, d.admin_dong_code,
    l.source_system, l.dag_run_id, l.raw_object_key, l.collected_at, l.ingested_at, l.load_date
from latest l
left join dong_map d on l.longitude = d.longitude and l.latitude = d.latitude
left join gu_codes g on g.gu = l.gu
```

- [ ] **Step 2: 그레인 singular 테스트 작성** (`tests/assert_reservation_grain_unique.sql`)

```sql
-- 스냅샷 그레인 (service_id, load_date) 유일성 단언. 중복 행이 있으면 실패(>0행).
select service_id, load_date, count(*) as n
from {{ ref('silver_culture_reservation') }}
group by service_id, load_date
having count(*) > 1
```

- [ ] **Step 3: schema.yml append**

```yaml
  - name: silver_culture_reservation
    description: 공공예약 일 스냅샷 fact — 그레인 (service_id, load_date)
    columns:
      - name: service_id
        tests: [not_null]
      - name: load_date
        tests: [not_null]
      - name: ingested_at
        tests: [not_null]
      - name: reservation_type
        tests:
          - accepted_values:
              values: ["culture", "sport"]
      - name: longitude
        tests:
          - in_seoul_bbox: {kind: lon}
      - name: latitude
        tests:
          - in_seoul_bbox: {kind: lat}
      - name: gu_code
        tests:
          - axis_coverage:
              min_ratio: 0.9
              config: {severity: warn}
```

- [ ] **Step 4~5: run + test** Expected: pass — 특히 grain 테스트가 7/1 중복 44행 흡수를 증명.

- [ ] **Step 6: 정합 조회** — `select load_date, count(*) from ... group by 1 order by 1` → 7/1~최신 일별 스냅샷 유지(기간형과 달리 날짜별 행 보존) 확인.

- [ ] **Step 7: Commit** — `feat(culture): silver_culture_reservation — 스냅샷 그레인 (#$ISSUE)`

### Task 15: silver_culture_boxoffice (일 스냅샷 fact — 공간축 면제)

**Files:**
- Create: `domains/culture/models/silver/silver_culture_boxoffice.sql`
- Modify: `domains/culture/models/schema.yml` (append)
- Create: `domains/culture/tests/assert_boxoffice_grain_unique.sql`

**Interfaces:**
- Produces: `ref('silver_culture_boxoffice')` — `rank_no, performance_id, performance_name, genre, venue_name, area, event_start_date, event_end_date, event_at, perf_count, seat_count, (계보 6종)` — 그레인 (load_date, rank_no)

- [ ] **Step 1: 모델 작성**

```sql
-- silver: KOPIS 예매상황판 일 스냅샷 fact (top50 랭킹). 그레인 = (load_date, rank_no).
-- 공간축 면제(area=시도뿐 — 설계 §2). 랭킹 대상 기간은 event_start/end 로 보존.

with bronze as (
    select
        json_extract_scalar(record_json, '$.rnum')      as rank_raw,
        json_extract_scalar(record_json, '$.mt20id')    as performance_id,
        json_extract_scalar(record_json, '$.prfnm')     as performance_name,
        json_extract_scalar(record_json, '$.cate')      as genre,
        json_extract_scalar(record_json, '$.prfplcnm')  as venue_name,
        json_extract_scalar(record_json, '$.area')      as area,
        json_extract_scalar(record_json, '$.prfpd')     as period_raw,
        json_extract_scalar(record_json, '$.prfdtcnt')  as perf_count_raw,
        json_extract_scalar(record_json, '$.seatcnt')   as seat_count_raw,
        ingest_ts,
        {{ culture_lineage('kopis') }}
    from {{ source('culture_bronze', 'bronze_kopis_boxoffice') }}
),

typed as (
    select
        try(cast(rank_raw as integer))                   as rank_no,
        performance_id,
        nullif(trim(performance_name), '')               as performance_name,
        nullif(trim(genre), '')                          as genre,
        nullif(trim(venue_name), '')                     as venue_name,
        nullif(trim(area), '')                           as area,
        try(cast(date_parse(trim(split_part(period_raw, '~', 1)), '%Y.%m.%d') as date)) as event_start_date,
        try(cast(date_parse(trim(split_part(period_raw, '~', 2)), '%Y.%m.%d') as date)) as event_end_date,
        try(cast(replace(perf_count_raw, ',', '') as integer)) as perf_count,
        try(cast(replace(seat_count_raw, ',', '') as integer)) as seat_count,
        ingest_ts, source_system, dag_run_id, raw_object_key, collected_at, ingested_at, load_date
    from bronze
    where try(cast(rank_raw as integer)) is not null
)

select
    rank_no, performance_id, performance_name, genre, venue_name, area,
    event_start_date, event_end_date,
    cast(try(cast(load_date as date)) as timestamp(6)) as event_at,
    perf_count, seat_count,
    source_system, dag_run_id, raw_object_key, collected_at, ingested_at, load_date
from (
    select *, row_number() over (
        partition by load_date, rank_no
        order by {{ culture_dedup_order() }}
    ) as rn
    from typed
) where rn = 1
```

- [ ] **Step 2: 그레인 테스트** (`tests/assert_boxoffice_grain_unique.sql`)

```sql
-- 랭킹 그레인 (load_date, rank_no) 유일성 단언.
select load_date, rank_no, count(*) as n
from {{ ref('silver_culture_boxoffice') }}
group by load_date, rank_no
having count(*) > 1
```

- [ ] **Step 3: schema.yml append**

```yaml
  - name: silver_culture_boxoffice
    description: KOPIS 예매상황판 일 스냅샷 — 그레인 (load_date, rank_no), 공간축 면제
    columns:
      - name: rank_no
        tests: [not_null]
      - name: load_date
        tests: [not_null]
      - name: ingested_at
        tests: [not_null]
```

- [ ] **Step 4~5: run + test + 정합(일별 ≈50행 × 날짜 수)** — Expected: pass.
- [ ] **Step 6: Commit** — `feat(culture): silver_culture_boxoffice (#$ISSUE)`

### Task 16: 오배정 실측 테스트 (경계 정밀도 — #48 코멘트 약속)

**Files:**
- Create: `domains/culture/tests/assert_gu_label_vs_coord_mismatch.sql`

**Interfaces:**
- Consumes: 좌표 보유 silver 4종 (facility·space·event·reservation)

- [ ] **Step 1: 테스트 작성** — severity warn: 실패시키지 않고 불일치 목록·규모만 상시 계측

```sql
-- 원본 구명(정답 라벨) vs 좌표 유래 구(admin_dong_code 앞 5자리) 불일치 실측.
-- 단순화 경계의 오배정률 데이터 축적(#48 코멘트) — 정밀 경계 교체 이슈의 근거·벤치마크.
-- warn: 경계 오배정은 v1 수용, 데이터만 쌓는다.
{{ config(severity = 'warn') }}

with cw as (select distinct gu_code, gu from {{ ref('seoul_admin_dong_crosswalk') }}),

unioned as (
    select 'silver_culture_event' as model, event_key as row_key, gu, admin_dong_code
    from {{ ref('silver_culture_event') }}
    union all
    select 'silver_culture_facility', facility_id, gu, admin_dong_code
    from {{ ref('silver_culture_facility') }}
    union all
    select 'silver_culture_space', space_key, gu, admin_dong_code
    from {{ ref('silver_culture_space') }}
    union all
    select 'silver_culture_reservation', service_id || '|' || load_date, gu, admin_dong_code
    from {{ ref('silver_culture_reservation') }}
)

select u.model, u.row_key, u.gu as label_gu, c.gu as coord_gu
from unioned u
join cw c on c.gu_code = substr(u.admin_dong_code, 1, 5)
where u.gu is not null
  and u.admin_dong_code is not null
  and u.gu <> c.gu
```

- [ ] **Step 2: 실행** — `MSYS_NO_PATHCONV=1 $DBT test --select assert_gu_label_vs_coord_mismatch --target dev` Expected: WARN N행(0이어도 정상). N과 대표 사례를 이슈에 코멘트로 기록.

- [ ] **Step 3: Commit** — `test(culture): 구 라벨 vs 좌표 유래 구 오배정 실측 (#$ISSUE)`

### Task 17: gold 3마트 복원 (gu_code 축)

**Files:**
- Create: `domains/culture/models/gold/gold_culture_location_daily.sql`
- Create: `domains/culture/models/gold/gold_culture_reservation_daily.sql`
- Create: `domains/culture/models/gold/gold_culture_boxoffice_daily.sql`
- Create: `domains/culture/tests/assert_gold_grain_unique.sql`
- Modify: `domains/culture/models/schema.yml` (append)

**Interfaces:**
- Consumes: silver 9종 전부

- [ ] **Step 1: gold_culture_location_daily** (기간형 5종 union → 일자 전개, 그레인 gu_code × event_date)

```sql
-- gold: culture 활동(공연·행사·축제·전시·세종)을 gu_code × 일자로 집계 — #48 코드 축.
-- 기간 → 일자 전개(cross join unnest sequence). 그레인: gu_code × event_date.

with raw_activities as (
    select gu_code, gu, cast(performance_id as varchar) as activity_id, 'performance' as activity_type, event_start_date, event_end_date
    from {{ ref('silver_culture_performance') }}
    union all
    select gu_code, gu, event_key, 'event', event_start_date, event_end_date
    from {{ ref('silver_culture_event') }}
    union all
    select gu_code, gu, cast(festival_id as varchar), 'festival', event_start_date, event_end_date
    from {{ ref('silver_culture_festival') }}
    union all
    select gu_code, gu, cast(exhibition_id as varchar), 'exhibition', event_start_date, event_end_date
    from {{ ref('silver_culture_exhibition') }}
    union all
    select gu_code, gu, cast(sejong_id as varchar), 'sejong', event_start_date, event_end_date
    from {{ ref('silver_culture_sejong') }}
),

activities as (
    select * from raw_activities
    where gu_code is not null
      and event_start_date is not null
      and event_end_date is not null
      and event_end_date >= event_start_date
      and date_diff('day', event_start_date, event_end_date) <= 400
),

expanded as (
    select a.gu_code, a.gu, a.activity_id, a.activity_type, d.activity_date
    from activities a
    cross join unnest(sequence(a.event_start_date, a.event_end_date, interval '1' day)) as d(activity_date)
)

select
    gu_code,
    max(gu) as gu,
    activity_date as event_date,
    count(distinct activity_id) as activities_count,
    count(distinct case when activity_type = 'performance' then activity_id end) as performances_count,
    count(distinct case when activity_type = 'event'       then activity_id end) as events_count,
    count(distinct case when activity_type = 'festival'    then activity_id end) as festivals_count,
    count(distinct case when activity_type = 'exhibition'  then activity_id end) as exhibitions_count,
    count(distinct case when activity_type = 'sejong'      then activity_id end) as sejong_count
from expanded
group by gu_code, activity_date
```

- [ ] **Step 2: gold_culture_reservation_daily**

```sql
-- gold: gu_code × 스냅샷일 공공예약 가용 현황. 가용률 = 접수중/전체.

with svc as (
    select * from {{ ref('silver_culture_reservation') }}
    where gu_code is not null
)

select
    gu_code,
    max(gu) as gu,
    load_date as snapshot_date,
    count(*)                                                  as total_services,
    count(case when reservation_type = 'culture' then 1 end) as culture_services,
    count(case when reservation_type = 'sport' then 1 end)   as sport_services,
    count(case when status = '접수중' then 1 end)            as open_services,
    round(1.0 * count(case when status = '접수중' then 1 end) / nullif(count(*), 0), 3) as availability_rate
from svc
group by gu_code, load_date
```

- [ ] **Step 3: gold_culture_boxoffice_daily** (facility 경유 gu_code — 구판의 낮은 매칭률을 detail 정밀 조인으로 개선)

```sql
-- gold: 예매상황판 랭킹 스냅샷. 공간은 performance(→facility) 경유 best-effort.

with box as (select * from {{ ref('silver_culture_boxoffice') }}),

perf_axis as (
    select performance_id, max(gu_code) as gu_code, max(gu) as gu
    from {{ ref('silver_culture_performance') }}
    where performance_id is not null
    group by performance_id
)

select
    b.load_date as snapshot_date,
    b.rank_no,
    b.performance_id,
    b.performance_name,
    b.genre,
    b.venue_name,
    b.event_start_date,
    b.event_end_date,
    b.perf_count,
    b.seat_count,
    p.gu_code,
    p.gu
from box b
left join perf_axis p on p.performance_id = b.performance_id
```

- [ ] **Step 4: gold 그레인 테스트** (`tests/assert_gold_grain_unique.sql`)

```sql
-- gold 그레인(gu_code × event_date) 유일성 단언.
select gu_code, event_date, count(*) as n
from {{ ref('gold_culture_location_daily') }}
group by gu_code, event_date
having count(*) > 1
```

- [ ] **Step 5: schema.yml append**

```yaml
  - name: gold_culture_location_daily
    description: gu_code × 일자 문화활동 집계 (기간 겹침 전개)
    columns:
      - name: gu_code
        tests: [not_null]
      - name: event_date
        tests: [not_null]
  - name: gold_culture_reservation_daily
    description: gu_code × 스냅샷일 예약 가용 현황
    columns:
      - name: gu_code
        tests: [not_null]
  - name: gold_culture_boxoffice_daily
    description: 랭킹 스냅샷 (snapshot_date × rank_no)
    columns:
      - name: rank_no
        tests: [not_null]
```

- [ ] **Step 6~7: run + test** (`--select gold_culture_location_daily gold_culture_reservation_daily gold_culture_boxoffice_daily` + 테스트) Expected: pass.
- [ ] **Step 8: Commit** — `feat(culture): gold 3마트 복원 — gu_code 축 (#$ISSUE)`

### Task 18: 전체 빌드 + 실측 확정 (재개 게이트 ②③ 충족)

**Files:**
- Modify: `domains/culture/models/schema.yml` (axis_coverage min_ratio 실측 반영)

- [ ] **Step 1: 클린 전체 빌드**

```bash
MSYS_NO_PATHCONV=1 $DBT build --target dev
```
Expected: seed 5 + model 12 + test 전건 성공(warn 허용). ERROR 0.

- [ ] **Step 2: 행수 매트릭스 리포트** — 12 bronze → 9 silver + 3 gold 정합:

```bash
MSYS_NO_PATHCONV=1 $DBT show --inline "
select 'facility' m, count(*) n from iceberg_dev.culture.silver_culture_facility
union all select 'space', count(*) from iceberg_dev.culture.silver_culture_space
union all select 'performance', count(*) from iceberg_dev.culture.silver_culture_performance
union all select 'festival', count(*) from iceberg_dev.culture.silver_culture_festival
union all select 'event', count(*) from iceberg_dev.culture.silver_culture_event
union all select 'exhibition', count(*) from iceberg_dev.culture.silver_culture_exhibition
union all select 'sejong', count(*) from iceberg_dev.culture.silver_culture_sejong
union all select 'reservation', count(*) from iceberg_dev.culture.silver_culture_reservation
union all select 'boxoffice', count(*) from iceberg_dev.culture.silver_culture_boxoffice
union all select 'gold_location', count(*) from iceberg_dev.culture.gold_culture_location_daily
union all select 'gold_reservation', count(*) from iceberg_dev.culture.gold_culture_reservation_daily
union all select 'gold_boxoffice', count(*) from iceberg_dev.culture.gold_culture_boxoffice_daily" --target dev
```

- [ ] **Step 3: axis_coverage 실측 → 임계 확정** — 모델별 gu_code/admin_dong_code 커버리지 조회, schema.yml의 min_ratio를 "실측 − 여유 5%p"로 갱신(초기 추정치 대체), `$DBT test` 재실행 전건 pass 확인.

- [ ] **Step 4: 결과(행수 매트릭스·커버리지·오배정 N)를 이슈에 코멘트로 기록**

- [ ] **Step 5: Commit** — `chore(culture): axis_coverage 임계 실측 확정 (#$ISSUE)`

### Task 19: PR 생성 (ASAC-DBT)

- [ ] **Step 1: push + PR**

```bash
git push -u origin feat/$ISSUE-culture-silver-redesign
gh pr create --repo ASAC-DE-bigkk/ASAC-DBT --base dev \
  --title "[Feat] culture silver/gold 재설계 — #48 공통축 canonical 적용" \
  --body-file <(PR 본문)
```
PR 본문에 포함: `Closes #$ISSUE` · 설계 요지(9모델+gold3, dedup 정렬키 근거, NUM 게이트 결과, detail 좌표 커버리지 한계) · 행수 매트릭스 · 검증 방법 · `🤖 Generated with [Claude Code](https://claude.com/claude-code)`

Expected: PR URL. **머지는 사용자(리뷰 후) — 자동 머지 금지.**

### Task 20: ASAC-DAG 사이드 정리 (문서·주석)

**Files (레포: `C:\Users\Dell3571\ask-seoul\sample\dags`):**
- Modify: `domains/culture/culture_ingest/source/datasets.py` (load_pattern 주석)
- Modify: `domains/culture/culture_transform.py` (docstring의 seed 명 갱신)
- Add: `domains/culture/docs/design/2026-07-06-culture-silver-gold-redesign.md` + 본 계획서 (이미 작성됨 — 커밋만)

- [ ] **Step 1: datasets.py 주석 갱신 (설계 §3-C)** — `load_pattern` dataclass 주석 뒤에 한 줄 추가:

```python
    # 아래 줄을 기존 주석에 덧붙임 (라인 21 부근, load_pattern 설명):
    # ※ scd2_dim 은 설계 의도 — silver v1 은 최신본 dim 으로 보류(bronze 가 이력 박제, 소급 구축 가능).
```

- [ ] **Step 2: culture_transform.py docstring** — `* seed — sema_branch_gu(...)` 줄을 `* seed — sema_branch_location(분관 좌표)·sejong_location + asac_axes 패키지 seed(행정동 크로스워크·경계)`로 교체.

- [ ] **Step 3: 브랜치·커밋·PR** — dev 기반 새 브랜치 `docs/culture-silver-redesign-notes`, 문서 2건 + 수정 2건 커밋, ASAC-DAG에 소형 PR (`Ref ASAC-DBT#$ISSUE`).

### Task 21: (머지 후 운영) 재개 게이트 ④ — culture_transform 재개

> 이 태스크는 **ASAC-DBT PR 머지 후** 사용자와 함께 실행. 코드 변경 없음.

- [ ] **Step 1**: `sample/dbt` dev pull (머지 반영) — 컨테이너 마운트 자동 갱신
- [ ] **Step 2**: unpause — `MSYS_NO_PATHCONV=1 docker exec elt-infra-airflow-scheduler-1 airflow dags unpause culture_transform`
- [ ] **Step 3**: 다음 자정런에서 bronze Asset → transform 자동 기동 확인, dbt build 성공·행수 리포트 확인
- [ ] **Step 4**: `__dbt_tmp` 고아·metadata 증식 대응 = [ASAC-DAG#157](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/157) `culture_maintenance` DAG 착수 (#155 maintain + #156 storage_cleanup 패턴, 주 1회) — 이 시점(`culture_transform` 재가동 = `__dbt_tmp` 재발 시작)이 #157의 착수 트리거. scratchpad `gc_orphans.py` 루틴은 폐기

---

## Self-Review 결과

- **Spec coverage**: 설계 §1(구조)=T3~T17 / §2(컬럼 계약)=T4·각 모델 / §3(그레인·dedup·A·B·C·G)=T2·T4·T11 Step5·T20 / §4(패키지·seed)=T3·T5 / §5(테스트 3층·freshness(기존 sources.yml 유지)·정량 리포트(기존 transform DAG 체인 유지)·타깃(dev 고정)·재개 게이트)=T16·T18·T21. 설계 E(source freshness)는 **이미 구현돼 있음**(sources.yml + transform DAG 체인) — 신규 작업 없음, T6이 detail 2종에 완화 freshness만 추가.
- **Placeholder scan**: 조건 분기(T2→T8 키 선택)는 양쪽 코드 완비. 좌표 근사값은 T5 Step4 검증 게이트로 커버.
- **Type consistency**: `culture_lineage` 방출 6컬럼 ↔ 전 모델 최종 select 일치 / `culture_dong_map` 방출 5컬럼(`coord_gu_code` 포함) ↔ 소비부 일치 / facility 산출 컬럼 ↔ performance·festival 소비 일치 확인.
