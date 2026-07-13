# Cosmos 적용 — commerce silver dbt 오케스트레이션

`commerce_load_silver` 의 dbt 변환을 **astronomer-cosmos** 로 오케스트레이션한다. 이 문서는
채택 형태와 함께, **Cosmos 가 기존 구조를 그대로 따라가지 못한(대응 불가) 영역** — 그 영역이 생긴
이유와 Cosmos 로 대처가 불가능했던 이유, 그리고 우리가 택한 대처 — 를 단일 소스로 정리한다.

관련 코드: [../commerce_load_silver.py](../commerce_load_silver.py) ·
[../include/silver/chunked_run.py](../include/silver/chunked_run.py) · 이력 [../change-log.md](../change-log.md) #54.
dbt 프로젝트(별도 레포): `dbt/domains/commerce/`.

---

## 1. 요약 (무엇을·왜)

- **무엇**: 기존 silver dbt 실행을 단일 `BashOperator`(dbt run) + 별도 `dbt test` 조합에서,
  Cosmos `DbtTaskGroup`(**모델당 run+test 태스크**)로 바꿨다. 목적은 모델단위 관측성·테스트·
  lineage(→ §6 cross-domain) 확보다.
- **범위**: commerce 의 dbt 는 **silver 전용**(`silver_license_history`, `silver_license_current`,
  `silver_license_detail_health`). gold 는 dbt 가 아니라 Python→serving-Postgres(`commerce_load_gold`)
  라 Cosmos 범위 밖. 현재 DAG 가 선택하는 모델은 `SILVER_SELECT = [history, current]` 2개(§3.5).
- **핵심 제약**: "전체 재빌드까지 Cosmos" 는 **불가능**하다 — 이유는 §3.1/§3.2. 전량 빌드는
  cold-start 시드 가드(§4)가 청크로 처리하고, Cosmos 는 **증분 run+test** 를 담당한다.

## 2. 채택 구성 (형태)

| 항목 | 선택 | 비고 |
|---|---|---|
| 배치 형태 | 기존 `commerce_load_silver` DAG **안에 `DbtTaskGroup` 임베드** | DAG·스케줄 1개 유지(분리 안 함). enrich/marker/notify 오케스트레이션 보존 |
| 실행 모드 | `ExecutionMode.LOCAL` + `dbt_executable_path=/home/airflow/dbt-venv/bin/dbt` | dbt 는 기존 **별도 venv** 유지. airflow env 엔 Cosmos 만 설치(dbt 미설치) |
| 프로필 | `ProfileConfig(profiles_yml_filepath=…/profiles.yml)` | 기존 `profiles.yml`(Trino, env_var) **재사용** — Cosmos 가 새로 만들지 않음 |
| 모델 선택 | `RenderConfig(select=SILVER_SELECT)` | history, current |
| 테스트 | `TestBehavior.AFTER_EACH` | 모델별 run 직후 test 태스크 렌더(기존 단일 `dbt test` 대체) |
| 그래프 로드 | `LoadMode.DBT_LS` | DAG 파싱 시 venv dbt 로 `dbt ls`(§3.4) |
| deps | `install_deps=False` | commerce 는 `packages.yml` 없음 → `dbt deps` 불필요 |

배선:

```text
[enrich_admin_dong_ref, enrich_fill_jibun, ensure_silver_marker]
   → seed_silver_if_empty → dbt_silver(Cosmos: 모델당 run+test)
   → notify_masked_address_summary → mark_silver_done → report_silver
```

호스트 이미지: 루트 `Dockerfile.airflow` 의 airflow env 에 `astronomer-cosmos>=1.8,<2` 추가(사용자
승인). dbt-core/dbt-trino 는 venv 에 그대로(airflow env 에 넣지 않는다).

---

## 3. Cosmos 가 기존 구조를 따라가지 못한 영역 (비대응)

> 각 항목: **무엇** / **왜 생겼나** / **왜 Cosmos 로 대처 불가** / **우리의 대처**.

### 3.1 dataset 청크 전량 빌드 (가장 큰 비대응)

- **무엇**: 전체 재빌드(테이블 drop 후 최초/`--full-refresh`)를 `chunked_run` 이 **dataset 를 행수
  기준 배치로 나눠** `dbt run --select … --vars include_datasets=<batch>` 로 순차 실행한다.
  (`silver_license_history` 의 window 연산이 전 행을 한 번에 올리면 Trino 노드 메모리를 초과 —
  `EXCEEDED_LOCAL_MEMORY_LIMIT`. 배치로 쪼개야 각 배치가 메모리에 든다.)
- **왜 생겼나**: 배치 계획이 **런타임 값**에 의존한다 — bronze 를 Trino 로 실측해
  (`dataset_row_counts()`) dataset별 행수를 구하고 greedy-pack 으로 배치를 만든 뒤, 배치 수만큼
  `dbt run` 을 **반복**한다. 즉 "실행 시점에 계산된 N개 배치를 루프"하는 구조다.
- **왜 Cosmos 로 대처 불가**: Cosmos 는 **DAG 파싱 시점에 정적으로** dbt 노드 그래프를 읽어
  **모델당 1개의 Airflow 태스크**를 만든다. 파싱 시점엔 런타임 행수·배치 수를 알 수 없고, 하나의
  모델 태스크를 "런타임에 계산된 배치만큼 여러 번, 매번 다른 `--vars` 로" 실행하도록 만들 수단이
  없다(동적 fan-out 부재). 배치 수를 파싱시 상수로 못 박으면 재빌드 때마다 실측 분포와 어긋난다.
- **우리의 대처**: 전량 빌드는 **Cosmos 밖**에서 처리한다 — cold-start 시드 가드
  (`seed_silver_if_empty`, §4)가 기존 `run_silver_chunked` 를 호출해 청크로 안전하게 빌드한다.
  Cosmos 는 빌드된 뒤의 **증분**만 맡는다.

### 3.2 마커 기반 증분 + `delete_unmarked` pre_hook 의 결합 (전량 빌드를 Cosmos 가 소유하면 안 되는 이유)

- **무엇**: `silver_license_history` 는 (a) pre_hook `delete_unmarked_silver_history_runs()` 로 매
  증분 실행 시작에 **마킹 안 된 이력 행을 삭제**하고, (b) 증분 술어
  `silver_unmarked_publishable_predicate` 로 **DONE 마커 없는 publishable run 만** 처리한다. 마커는
  `dbt test` 통과 후 `mark_silver_done` 이 기록한다(불변식: **테스트 통과 후에만 마킹**).
  (매크로: `dbt/domains/commerce/macros/silver_markers.sql` · `…/exclusions.sql`.)
- **왜 생겼나**: 중단/실패 재시도 안전성 때문이다 — 적재는 됐지만 테스트 전에 죽은 run 을 다음
  실행이 자동 정리(delete)하고 다시 처리하도록 설계했다. pre_hook 의 삭제 범위는 `include_datasets`
  로 **스코프**되어(청크 배치끼리 격리) 청크 재빌드가 안전하다. **단, `include_datasets` 가 비면
  삭제는 전역이다.**
- **왜 Cosmos 로 대처 불가**: Cosmos 증분 run 은 `--vars include_datasets` 없이
  `dbt run --select history current` 를 돌린다. 만약 **마킹되지 않은 전량**(예: preseed 로 통짜
  빌드한 상태) 위에서 이걸 돌리면 → pre_hook 이 `include_datasets` 없이 **전역**으로 그 전량을
  삭제하고 → 증분 술어가 마커 없는 **전 run 을 비청크 단일 run 으로 재처리** → §3.1 의 OOM 이 정확히
  재발한다. 즉 "preseed(빌드) 후 Cosmos 증분" 하이브리드는 **안전하지 않다.**
- **우리의 대처**: 시드 가드가 청크 빌드 후 **테스트→마킹까지 끝낸다**(§4). 마킹이 끝나면 Cosmos
  증분의 pre_hook 은 지울 대상이 없고 증분 술어도 처리할 run 이 없어 **no-op** 가 된다. 불변식
  (테스트 후 마킹)도 시드 안에서 그대로 지켜진다.

### 3.3 태스크 사이 파이썬 훅(notify) 삽입 순서

- **무엇**: 기존 배선은 `dbt run → notify_masked_address_summary → dbt test` 로, notify 가 **run 과
  test 사이**에 있었다.
- **왜 생겼나**: notify 는 silver_current 를 Trino 로 집계해 마스킹 주소 스킵 건수를 warning 으로
  알린다 — 원래 run 직후(테스트 전)에 끼워 두었다.
- **왜 Cosmos 로 대처 불가**: `TestBehavior.AFTER_EACH` 는 각 모델의 run→test 를 **한 쌍으로 묶어**
  태스크그룹 안에서 렌더한다. 특정 모델의 run 과 test **사이**에 외부 파이썬 태스크를 끼워 넣는
  훅이 없다(그룹 내부 토폴로지는 Cosmos 소유).
- **우리의 대처**: notify 를 **그룹 뒤**로 옮겼다(`dbt_silver → notify_masked_address_summary`).
  집계는 silver 가 만들어져 있으면 되고, 오히려 test 통과 후 집계라 더 안전하다 — 기능 동일.

### 3.4 DAG 파싱 시점의 dbt 실행 의존 (`LoadMode.DBT_LS`)

- **무엇**: Cosmos 는 기본적으로 DAG **파싱 시** `dbt ls` 를 실행해 모델 그래프를 만든다.
- **왜 생겼나**: 모델당 태스크를 만들려면 파싱 시점에 dbt 노드 목록이 필요하다.
- **왜(부분) 대처 한계**: 기존 `BashOperator` 방식은 파싱 시 dbt 를 전혀 부르지 않았는데, Cosmos 는
  파싱마다(dag-processor) venv dbt 로 `dbt ls` 를 부른다 — 프로필/프로젝트가 파싱 시 해석 가능해야
  하고, dbt 실행 실패가 곧 DAG 파싱 실패가 될 수 있다. (현 compose 는 scheduler 가 trino 헬스체크
  의존이라 파싱 시 Trino 가 떠 있어 대체로 문제없다.)
- **우리의 대처(현재/견고화)**: 현재는 `DBT_LS` 로 둔다(commerce 는 `packages.yml` 없음 →
  `install_deps=False` 로 단순). 견고화가 필요하면 **`manifest.json` 선생성 후 `LoadMode.DBT_MANIFEST`**
  로 바꿔 파싱 시 dbt 서브프로세스를 없앤다(빌드 단계에서 `dbt parse` → 산출 manifest 를 Cosmos 가
  읽음). 이건 선택적 하드닝으로 남긴다.

### 3.5 경계 사례 — **데이터 누락이 아님을 명시**

이 둘은 "Cosmos 가 못 따라간 영역"이 아니라 현행 스코프를 그대로 보존한 결과이며, **신규 데이터
처리 누락을 만들지 않는다**(검증 결과 아래).

- **`silver_license_detail_health` 미선택 = 누락 아님**:
  - 원본 DAG 도 `SILVER_SELECT` 가 history·current 2개뿐이었고 detail_health 는 **한 번도 선택된 적
    없다**(git 이력 확인, `-S detail_health` 0건). 즉 Cosmos 전환이 새 누락을 만든 게 아니다(스코프 동일).
  - 실제 "비공통/상세(detail) 필드" 처리는 **gold** 가 `silver_license_current.record_json` 에서
    수행한다(`include/gold/loader.py` 의 `json_extract_scalar(record_json, …)` → API별 detail 테이블).
    이 경로는 current 가 빌드되는 한 유지되고(그리고 current 는 Cosmos 가 빌드) 데이터는 누락되지 않는다.
  - `silver_license_detail_health` 자체는 문서상 **재설계 대상(파킹) 모델**이다(current 기반이라
    비공통 값의 과거 버전 소실 이슈 — `dbt/domains/commerce/docs/silver-noncommon-catalogs.md`).
    gold 카탈로그로 이관 예정이라 현행 파이프라인에서 비활성이다.
  - 원한다면 `SILVER_SELECT` 에 한 줄로 추가하면 Cosmos 가 ref 순서(`current → detail_health`)로
    자동 빌드한다 — 단 재설계 이슈가 있는 모델이라 **별도 제품 결정**으로 둔다.
- **수동 `--full-refresh` = 신규 데이터 누락 아님**:
  - 매일 증분은 **마킹 없는 publishable run 전부**를 처리하고 test 후 마킹한다 → 신규 수집분은
    빠짐없이 반영된다. cold-start(빈 테이블)는 시드가 전량 빌드한다.
  - `--full-refresh` 는 "이미 처리된 데이터를 로직 변경 등으로 **재계산**"하는 의도적 운영 작업이지
    신규 데이터를 수집하는 경로가 아니다. 따라서 이를 수동으로 두는 것은 데이터 누락과 무관하며,
    전량 재계산이 필요할 때만 청크 절차(`rebuild-and-ops.md §6`)로 돌린다(§3.1 과 동일 이유).

---

## 4. cold-start 시드 계약 (`seed_silver_if_empty`)

Cosmos 가 청크 전량 빌드를 못 하므로(§3.1), **빈 silver 최초 빌드만** 이 태스크가 담당한다.

- **동작**: `silver_license_history` 행수 > 0 이면 **no-op**(평상시). 0이면(=cold start):
  1. `run_silver_chunked(...)` — dataset 청크 전량 빌드(OOM 회피).
  2. `run_dbt_test(...)` — `dbt test --select history current`(실패 시 예외 → 3 진행 안 함).
  3. `mark_silver_runs_done()` — DONE 마킹.
- **왜 안전한가**: 3까지 끝내면 downstream Cosmos 증분은 삭제·처리 대상이 없어 no-op(§3.2). 불변식
  (테스트 후 마킹)도 지켜진다. cold-start 날엔 Cosmos 가 test 를 한 번 더 수행(중복이나 무해).
- **경계**: 청크 빌드가 됐는데 **테스트에서 실패**하면 이력 행이 마킹 안 된 채 남는다. 다음 실행은
  "행수 > 0" 이라 시드를 건너뛰고 Cosmos 증분이 마킹 없는 전량을 재처리 → OOM 위험. 이는 **기존
  단일-태스크 방식에도 있던 동일 경계**이며 복구는 `--full-refresh` 청크 재빌드다(`rebuild-and-ops.md
  §6`). 시드의 cold-start 판정은 기존과 동일하게 "행수 0" 을 쓴다(신규 위험 도입 없음).

---

## 5. 운영 절차

- **첫 배포(빈 silver)**: DAG 는 생성 시 paused(`DAGS_ARE_PAUSED_AT_CREATION=true`). 이미지 리빌드
  후 unpause 하면 첫 run 의 `seed_silver_if_empty` 가 청크로 전량 빌드·검증·마킹하고, 같은 run 의
  `dbt_silver` 는 no-op 로 통과한다. (원한다면 시드만 먼저 수동 트리거해도 됨.)
- **평상시(증분)**: `seed` no-op → `dbt_silver` 가 마커 없는 신규 publishable run 만 증분 처리 →
  `mark_silver_done` 이 마킹. 소량이라 OOM 무관.
- **전량 재빌드/단위 제외·복원**: `dbt vars(exclude_*)` 수정 + 청크 절차. 상세는 dbt 레포
  `dbt/domains/commerce/docs/rebuild-and-ops.md`.

---

## 6. cross-domain lineage 방향 (구조 유지 + Cosmos 활용)

도메인별 **독립 dbt 프로젝트**(`dbt/domains/<도메인>/`) 구조라 dbt `ref()` 가 프로젝트 경계를 못 넘고,
도메인 간 참조는 `source()` 로 우회한다(예: citydata gold 가 `source('traffic',
'silver_seoul_traffic_incident')`). 그 결과 **dbt 네이티브 그래프는 도메인 경계에서 끊긴다**(source =
그래프 말단). 단, 그 `source()` 의 물리 relation(`iceberg[_dev].<schema>.<table>`)은 **생산 도메인
모델의 산출물과 정확히 일치**한다(확인됨) — 파이프라인은 정상이고 끊기는 건 그래프 표현뿐이다.

- **전체 lineage 확보 방향(채택·배선 완료)**: **OpenLineage → Marquez**. Cosmos 는 dbt run
  태스크마다 OpenLineage 이벤트(입력/출력 = 물리 relation)를 방출하므로, 이를 백엔드가 물리명으로
  **stitch** 하면 도메인 경계를 넘는 통합 lineage 가 된다(traffic 모델 출력 dataset == citydata 입력
  dataset → 엣지 형성). dbt+Cosmos 장점을 그대로 살리며 dbt 코드/구조 변경 0 으로 전체 그래프를 만든다.
- **구성(배선 완료 — #55)**:
  - Airflow 이미지: `astronomer-cosmos[openlineage]` + `apache-airflow-providers-openlineage`
    (`Dockerfile.airflow`).
  - 방출 대상: `AIRFLOW__OPENLINEAGE__TRANSPORT`(→ `http://marquez-api:5000`) +
    `AIRFLOW__OPENLINEAGE__NAMESPACE=commerce-elt`(`docker-compose.yml` airflow-common).
  - 백엔드: `docker-compose.yml` 의 **`lineage` 프로파일** 서비스 3종 — `marquez-db`(postgres)·
    `marquez-api`(수집 API, 127.0.0.1:5000)·`marquez-web`(UI, **http://127.0.0.1:3000**).
  - **격리**: 프로파일이라 `docker compose up`(core)엔 안 뜬다. marquez 미기동 시 OL 방출은
    **fail-open**(경고 로그만, 태스크 실패 아님) → 파이프라인 안전.
- **기동·확인**:
  ```bash
  docker compose --profile lineage up -d          # marquez 3종 기동(최초 flyway 마이그레이션)
  curl -s http://127.0.0.1:5000/api/v1/namespaces  # API 헬스(namespaces 응답)
  # 이후 commerce_load_silver 실행 → 브라우저 http://127.0.0.1:3000 에서 lineage 확인
  ```
- **로컬 검증 한계(주의)**: Marquez 이미지 버전(`0.50.0`)·OL provider 버전은 canonical 기준으로
  고정했고 이 저장소에서 런타임 검증은 못 했다(도커 미기동). 최초 기동 시 이미지 버전/마이그레이션이
  어긋나면 태그·env 를 조정한다(core 스택엔 영향 없음 — 프로파일 격리).
  → 2026-07-12/13 실검증 완료: silver(Cosmos)→gold(Asset inlets/outlets) 엣지 Marquez 그래프 확인.
- **트러블슈팅 — "Python 버전 때문에 Marquez 가 안 된다"(오인 주의)**: Marquez 3종은 **Java/Node
  컨테이너**라 호스트·이미지 Python 과 무관하다. 이렇게 보이는 실제 원인 2가지와 해법:
  1. **구 `docker-compose`(v1 — Python 기반, pip 설치)**: `profiles`/신문법 파싱 실패가 Python
     스택트레이스로 나타난다. **Docker Compose v2(Go) 필요** — `docker compose version` 확인,
     v1(`docker-compose` 1.x)이면 제거하고 하이픈 없는 `docker compose …` 를 쓴다.
  2. **Airflow 이미지 build-arg `PYTHON_VERSION` 이 지원 밖**: Airflow 3.2.2 베이스 태그는
     **3.10~3.13 만 존재**(3.9 는 404 → FROM 단계 정체불명 실패). canonical=**3.11**.
     Dockerfile 에 fail-fast 가드가 있어 범위 밖이면 명확한 메시지로 즉시 실패한다.
  - **Python 버전 변경(상향·하향) 시 전-프로젝트 영향 검증 절차**: 이미지는 전 도메인 공유 —
    ① 대상 태그 존재 확인(`apache/airflow:3.2.2-pythonX.Y`) ② rebuild 후
    `airflow dags list-import-errors` **전 도메인 0건** ③ commerce `pytest`/`python -m security`
    ④ Marquez 기동 + DAG 1회 실행으로 OL 방출 확인. **하향(≤3.9)은 태그 부재로 불가.**
- **전체 도메인 통합 뷰(리니지 공유 전략 — 2026-07-13 확정)**: commerce 가 파일럿이다.
  **dbt 프로젝트 폴더 통합은 불필요** — OL/Marquez 는 **물리 테이블명**(namespace `trino://trino:8080`
  + `catalog.schema.table`)으로 stitch 하므로 프로젝트 구조와 무관하다(실증: commerce 에서 방출자가
  다른 Cosmos(silver)와 Airflow Asset(gold)이 물리명만으로 자동 연결됨). 타 도메인이 `source()` 로
  읽는 크로스도메인 테이블도 같은 물리명 → 자동 엣지. Marquez 버전 상향도 전제조건 아님(0.50 충분).
  **필요한 것 = 각 도메인의 OL 방출 배선** 두 가지 경로:
  1. **Cosmos 전환**(commerce 방식) — 모델당 태스크·테스트 렌더까지 원하면.
  2. **`dbt-ol` 래퍼**(경량, BashOperator 유지) — 이미지 dbt venv(py3.11)에 `openlineage-dbt` 동봉
     (Dockerfile). 기존 `dbt run …` 명령을 `dbt-ol run …` 으로 바꾸고 env 만 주면 끝:
     ```bash
     OPENLINEAGE_URL=http://marquez-api:5000 OPENLINEAGE_NAMESPACE=<domain>-elt \
       /home/airflow/dbt-venv/bin/dbt-ol run --project-dir … --profiles-dir … --target dev
     ```
     ⚠ 호스트 파이썬으로 dbt-ol 을 돌리지 말 것(버전 편차) — **이미지 venv 사용이 정본**.
- **보류**: dbt Mesh(cross-project `ref`)는 네이티브로 그래프를 잇지만 프로젝트별 public 계약·
  `dependencies.yml`·버전관리가 붙는 구조 변경이라, 현재는 손대지 않는다.

## 7. 검증 절차 (이미지 리빌드 후 — 로컬은 py3.9+미설치라 불가)

```bash
# 1) 이미지 리빌드(cosmos 설치)
docker compose build airflow-scheduler

# 2) DAG import/파싱(모델당 태스크 렌더 확인)
docker compose run --rm airflow-scheduler airflow dags list-import-errors
docker compose run --rm airflow-scheduler airflow tasks list commerce_load_silver

# 3) dbt 연결·모델 확인(venv dbt, commerce 프로젝트)
docker compose run --rm airflow-scheduler /home/airflow/dbt-venv/bin/dbt ls \
  --project-dir /opt/airflow/dbt/domains/commerce \
  --profiles-dir /opt/airflow/dbt/domains/commerce --target dev

# 4) 보안 게이트(코드 변경 후 상시)
PYTHONPATH=dags/domains/commerce/include python -m security
```

로컬(개발 머신)에서는 airflow/cosmos 미설치·py3.9 라 DAG 파싱·`dbt run` 실측이 불가하다. 위 절차는
컨테이너(이미지)에서 수행한다. 로컬은 보안 게이트 + 순수 pytest(`include/` 로직)까지만.

## 8. 참조

- DAG: [../commerce_load_silver.py](../commerce_load_silver.py) · 청크/시드: [../include/silver/chunked_run.py](../include/silver/chunked_run.py)
- 규약: [../CLAUDE.md](../CLAUDE.md) · 이력: [../change-log.md](../change-log.md) #54
- dbt(별도 레포): `dbt/domains/commerce/` — `profiles.yml` · `macros/silver_markers.sql` · `macros/exclusions.sql` · `docs/rebuild-and-ops.md`
