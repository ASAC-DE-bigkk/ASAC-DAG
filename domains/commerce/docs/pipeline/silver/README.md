# docs/pipeline/silver — 정규화·보강(silver) 레이어

bronze `record_json` 을 파싱해 **152종 공통 스키마**로 정규화하고, 중복제거·버저닝·주소/좌표
보강을 적용하는 단계. dbt(Trino) 로 구현한다. 이전 단계는 [../bronze/](../bronze/README.md),
전체 계보/테이블 정의는 [../data-model.md](../data-model.md).

- 모델: [`dbt/domains/commerce/models/silver/`](../../../../../../dbt/domains/commerce/models/silver/)
- 보강 코드: [../../../include/silver/enrich_tasks.py](../../../include/silver/enrich_tasks.py) · 마커 `silver_markers.py`
- DAG: `commerce_load_silver`(05:00 KST) —
  `[enrich_admin_dong_ref, enrich_fill_jibun, ensure_silver_marker] → dbt_run_silver →
  notify_masked_address_summary → dbt_test_silver → mark_silver_done → report_silver`

## 모델

| 모델 | materialization | grain | 설명 |
|---|---|---|---|
| `silver_license_history` | incremental **append** | 행 `(dataset, opnsfteamcode, mgtno, collected_at, content_hash)` | 파싱+보강+인접중복 제거. **전 버전 보존**(값 변경돼도 과거행 유지, A→B→A 원복 보존) |
| `silver_license_current` | incremental **delete+insert** | `(dataset, opnsfteamcode, mgtno)` | history 버전정렬 **최신 1행** + 마스킹 주소 동단위 null 처리 |
| `silver_license_detail_health` | incremental **delete+insert** | `(dataset, opnsfteamcode, mgtno)` | **보건 대분류** 업종별 상세 컬럼(record_json 에서 추출) |

> 스케줄 DAG 는 `silver_license_history silver_license_current` **2개만** 빌드한다.
> `silver_license_detail_health` 는 전체 `dbt run` / `--full-refresh` 로만 갱신(대분류 상세는 별도 주기).

## v1/v2 통합 — 152종이 여기서 합쳐진다

`silver_license_history` 의 `parsed` CTE 가 **`lf(v1, v2)` 매크로**로 v1 우선·없으면 v2 필드를 꺼내
canonical 컬럼으로 정규화한다. 이 CTE 이후 **139(v1)+13(v2)=152 가 공통 스키마**. record_json 은
그대로 실려 다녀 비공통(업종별) 필드는 보존. 자세히: [../data-model.md §2](../data-model.md).

```sql
{{ lf('MGTNO','MNG_NO') }} as mgtno,  {{ lf('TRDSTATEGBN','SALS_STTS_CD') }} as trdstategbn,
{{ lf('X','XCRD') }} as source_coord_x,  {{ lf('Y','YCRD') }} as source_coord_y, ...
```

## 보강(값 추가)

- **행정동/자치구**: `bronze_ref_admin_dong`(**서울만**) 조인 → `gu`/`gu_code`/법정·행정동 명·코드 파생.
- **지번 보강**: 도로명만 있는 행은 Juso 래더 조회 캐시(`bronze_address_enrichment`, `status='filled'`)로 지번 채움.
- **좌표**: 원천 `X/Y`(EPSG:5174 중부원점 TM) → **WGS84 위경도 순수 수식 변환**(서울 bbox 게이트). 원본 X/Y 보존.
- **타임존**: `collected_at` 은 bronze UTC → **+9h KST**. 전 timestamp KST 일원화.

## 증분·재개 (marker)

- **발행 게이트**: bronze `manifest.status='SUCCESS' AND is_publishable` 인 `(dataset, bronze_run_id)` 만 읽음.
- **DONE 마커**: `silver_load_run_marker` 에 dbt test 통과분 기록 → 다음 실행은 미마커 run 만 처리.
- **중단 재개**: `pre_hook` 이 미마커(미완성) run 을 history 에서 선삭제 후 재삽입 → 부분 적재/중복 방지.
- 강제 bronze 재적재/`exclude_*` 변경 시: `dbt run --full-refresh --select silver_license_history+`.

재개 표준 전체: [../PROJECT.md §3](../../PROJECT.md).
