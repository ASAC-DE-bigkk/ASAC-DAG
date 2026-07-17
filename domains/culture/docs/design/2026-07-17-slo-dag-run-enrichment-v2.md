# SLO dag_run enrichment v2 — PostgresHook + Connection (#411)

> 설계 승인: 2026-07-17. 구현 계획은 본 문서 §7. 실행 = executing-plans(inline).

## 1. 배경

#257 v1 은 Airflow 3 태스크 격리(#303: ORM `create_session()` 금지 +
`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` env 차단)로 `load_dag_runs` 가
**graceful-skip** → `bronze_culture_dag_runs` 0행.

- `silver_culture_dag_run` 0행 → `gold_culture_slo_daily` 의
  transform_runs / maintenance_ran / ingest_duration_min 이 기본값
- 7/17 03:11 freshness 사고(DBT#238): 빈 테이블에 freshness 감시 → NULL error →
  야간 transform 실패. "v1 스킵과 freshness 감시의 자기모순" 실증 —
  freshness 임시 비활성(`freshness: null`) 상태

## 2. 목표 / 비목표

**목표**: 태스크 허용 경로(PostgresHook + 정의된 Connection)로 dag_run 적재 재개.
**비목표**: SQL·행 빌드·멱등 쓰기 변경(v1 그대로), REST API 전환(과투자),
compose 인프라 수정(멘토 게이트 회피 — CLI 등록 채택).

## 3. 변경 (접근안 B 채택)

`culture_ingest/slo/io.py::load_dag_runs` 의 **접속 획득 한 조각만** 교체:

```python
METADB_CONN_ID = "airflow_metadb"  # 도메인 중립 — 메타DB 는 팀 공용(§6.2 _shared 승격 대비)

# v1: engine = create_engine(os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", ""))
# v2:
from airflow.providers.postgres.hooks.postgres import PostgresHook
engine = PostgresHook(postgres_conn_id=METADB_CONN_ID).get_sqlalchemy_engine()
```

- `text()/bindparam(expanding)` SQL·14일 윈도우 delete+insert 무변경 (검증된 경로)
- **try/except graceful-skip 유지** — Connection 미등록·스택 재구축 직후에도
  culture_slo 가 안 죽는 안전망 (7/17 사고의 교훈)
- provider 6.7 에 `get_sqlalchemy_engine()` 부재 시 폴백:
  `create_engine(hook.get_uri())` (동일 효과)

## 4. Connection 등록 (CLI 1회, 운영 절차)

컨테이너 **안에서** 기존 env 를 재사용 — 접속 문자열이 채팅·레포·런북 어디에도
노출되지 않는다:

```bash
docker exec elt-infra-airflow-scheduler-1 bash -c \
  'airflow connections add airflow_metadb \
     --conn-uri "${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN/postgresql+psycopg2/postgres}"'
```

스택 재구축(메타DB 볼륨 소실) 시 재등록 필요 → culture 런북에 절차 기록
(`docs/runbook.md` 또는 change-log). 공유 인프라(compose) 무수정.

## 5. 배포 순서 게이트 — freshness 재활성은 적재 확인 후

v2 머지 직후에도 첫 적재 전까지 테이블은 0행. freshness 를 같은 날 켜면
03:05 transform 이 또 NULL error 로 죽는다. 순서 강제:

1. **PR-1 (ASAC-DAG, 이 브랜치)**: io.py v2 + 런북 + Connection 등록 +
   라이브 적재 검증(수동 트리거 — 첫 실행이라 메타DB 전체 이력 백필)
2. **적재 확인 후 PR-2 (ASAC-DBT)**: #238 되돌림 + `loaded_at_field` 를
   `from_iso8601_timestamp(start_at)` 표현식으로 교정
   (start_at 은 KST ISO varchar — 원래 정의(raw varchar)는 dbt freshness 가
   나이 계산을 못 하는 잠재 버그였다. #238 분석의 후속 수정)
3. 다음 야간 03:05 transform 에서 freshness 17소스 PASS 확인

## 6. 검증 / AC

- 호스트 pytest: 기존 test_slo_loader 6건 유지(순수 로직 무변경) +
  conn_id 상수·Hook 배선 테스트 1건 신규
- 컨테이너 라이브(순서대로):
  1. Connection 등록 → culture_slo 수동 트리거
  2. `bronze_culture_dag_runs` 행수 > 0 (전체 이력 백필 — 6/30 이후 40+ 런 기대)
  3. `silver_culture_slo`/`dbt build tag:slo` 후 `silver_culture_dag_run` 채워짐
  4. `gold_culture_slo_daily` 의 transform_runs / maintenance_ran /
     ingest_duration_min 실측값 전환
  5. (음성 경로) Connection 삭제 상태에서 load_dag_runs 가 스킵 로그 + return 0
- PR-2 후: `dbt source freshness` 17소스 PASS · exit 0

## 7. 구현 계획 (bite-sized)

- [ ] T1: io.py — METADB_CONN_ID 상수 + Hook 엔진 교체 (graceful-skip 유지)
- [ ] T2: 배선 테스트 — io 모듈이 METADB_CONN_ID="airflow_metadb" 를 노출하고
      load_dag_runs 가 Hook 경로를 쓰는지 (호스트, airflow 미설치 환경이라
      import-guard 스타일)
- [ ] T3: 호스트 전체 pytest (culture 도메인)
- [ ] T4: change-log + 런북(재등록 절차) 문서
- [ ] T5: Connection 등록(CLI) + detached 라이브 검증 (AC 2~5) + dev 복귀
- [ ] T6: PR-1 생성 → (머지·적재 후) PR-2: DBT freshness 재활성
