# cleanup — v2(환경 13종) 오탐 누적 데이터 삭제 + 재수집 (#66)

**상태: dev DONE(2026-07-14) · prod OPEN(환경 존재 시)**

## 배경 (왜)

#65 이전 bronze 증분 diff([include/bronze/incremental.py](../include/bronze/incremental.py))가
v2 별칭 키(`DATA_UPDT_YMD`·`OGDP_INST_CD`·`MNG_NO`…)를 해석하지 못해 v2 정렬키가 전부
`(0,0,'','')` 로 붕괴 → **매 수집 전량이 신규로 오탐**되어 raw 증분·bronze Iceberg 에 날마다
전체 스냅샷이 중복 누적됐다(dev 실측: bronze 65,289행 vs 고유 ~36,125 — air_pollution 4.2배).
silver/gold 는 content_hash 인접중복 dedup 이 대부분 흡수(진짜 버전 이력 23행뿐)했으나,
사용자 결정(2026-07-14)으로 **전 계층 v2 삭제 + raw 재수집**으로 클린 슬레이트를 만들었다.

## 절차 (환경별 체크리스트)

전제: #65 수정 코드가 반영된 상태(./dags 마운트면 즉시 유효). 대상 환경의 컨테이너에서 실행.

1. **DAG pause**: `commerce_collect_raw` · `commerce_recollect_raw` · `commerce_load_bronze` ·
   `commerce_load_silver` · `commerce_load_gold` · `commerce_collect_watchdog`
   (실행 중 run 없음 확인: `airflow dags list-runs <id> --state running`)
2. **실측(dry-run)** — 삭제 전 계수 확인(레이어별 v2 행수·파일수):
   ```bash
   docker compose exec airflow-scheduler \
     python /opt/airflow/dags/domains/commerce/scripts/purge_v2_environment.py
   ```
3. **삭제**: 같은 명령 + `--apply` → 재실행(dry-run)으로 **전 계층 0 확인**(멱등).
   - 삭제 범위: raw(증분/마커/diff-target) · bronze state(워터마크 v2 엔트리·pending·receipts) ·
     bronze Iceberg(license+manifest) · silver(history/current/marker + R2 스냅샷 동기화) ·
     gold(detail 13 + entity/history). **보존**: `commerce_entity_key`(entity_seq 안정) ·
     gold 마커(재수집분이 워터마크 초과라 증분 창 자동 포착) · dim(다음 run 전량 재생성).
4. **재수집 라인**(순서대로, 각 성공 확인 후 다음):
   ```bash
   airflow dags unpause commerce_collect_raw   && airflow dags trigger commerce_collect_raw
   airflow dags unpause commerce_load_bronze   && airflow dags trigger commerce_load_bronze
   airflow dags unpause commerce_load_silver   && airflow dags trigger commerce_load_silver
   airflow dags unpause commerce_load_gold     && airflow dags trigger commerce_load_gold
   ```
   - collect 는 **당일 completed 제외 규칙(feat/59)** 때문에 v1 이 오늘 이미 수집됐다면 v2 만 수집한다.
   - 첫 v2 수집은 `mode=first`(diff-target 자가 시드) → **전량이 '신규'로 리포트되는 것이 정상**(1회).
   - diff-target 이 새 정렬 기준으로 재생성되므로 `bronze.resort` 마이그레이션은 **불필요**해진다.
5. **원상복구**: watchdog 등 pause 했던 DAG unpause(recollect 는 기존 pause 상태였다면 유지).
6. **검증**: purge dry-run 재실행으로 재적재 계수 확인(bronze=manifest 1 run/dataset,
   silver history≈current≈bronze), 다음날 collect 리포트에서 v2 증분이 0(identical)
   또는 소량(실변경)인지 확인 — 이것이 #65 수정의 최종 검증이다.

## dev 실행 기록 (2026-07-14)

| 계층 | 삭제 전 | 삭제 후 재수집 |
|---|---:|---:|
| raw | 132 파일(증분 25·마커 81·target 26) | 52 파일(증분 13·마커 13·target 26) |
| bronze | 65,289행 · manifest 25 runs | 36,125행 · manifest 13 runs(1/dataset) |
| silver history/current | 36,140 / 36,117 | (재적재 — change-log #66 참조) |
| gold entity/history/detail | 36,117 / 36,140 / 13객체 | (재적재 — 동일) |

- 손실 수용: silver 진짜 버전 이력 23행(수일치) — 사용자 결정에 포함.
- prod: 환경 구축 시 이 체크리스트 그대로 실행(상태가 다르므로 반드시 실측부터).
