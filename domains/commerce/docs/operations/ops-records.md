# 운영 기록 — 무엇이 언제 생기고, 언제 어디로 실리는가

파이프라인이 돌 때마다 남는 기록이 **어디에 · 언제 · 어떤 모양으로** 생기고, 그게 **언제 조회
DB 로 실리는지**를 한 곳에 정리한다. 규약 정본은 ASK-Seoul#78, 구현 근거는 `change-log.md`
§87·§88.

## 1. 한눈에

```text
[생성]                              [저장소 = 원본]                    [조회 DB = 사본]
                                    R2 버킷 seoul                      Cloudflare D1
commerce 태스크 9개 DAG
  ├ 성공/실패 콜백 ────────────▶ ops/runs/commerce/observed_date=…    ─┐
  │   (관문: common.ops.contract)      /dag_id=…/event_id=….json        │
  │                                                                     │
  └ 태스크 텍스트 로그 ──▶ 컨테이너 볼륨 ──▶ ops/logs/commerce/         │
        (Airflow 가 기록)      (일시)   01:30   observed_date=…/…tar.gz  │
                                                                        │  매일 01:30
타 도메인 기록기 4벌                                                     │  common_ops_d1_load
  ├ run_sink        ──────────▶ ops/runs/…        (citydata)          ─┤  (3시간마다)
  ├ runmetrics      ──────────▶ ops/metrics/…     (transit·weather·traffic)
  ├ errors.sink     ──────────▶ ops/errors/…      (전 도메인)          ─┤
  └ product_observability ────▶ ops/product-events, product-health     ─┘
                                                                        ▼
                                                        _ops_run_event      (기록 1건)
                                                        _ops_daily_metric   (날짜×도메인×단계)
                                                        _ops_pipeline_state (DAG 현재 상태)
                                                        _ops_pipeline_expectation (기대 주기)
```

**원본은 저장소, 조회 DB 는 사본이다.** 저장소 폴더는 합치지 않고 조회 단계에서 합친다
(ASK-Seoul#78 §8) — 그래서 기존 폴더를 하나도 건드리지 않는다.

## 2. 생성 — 무엇이 언제 생기나

### 2-a. 실행 기록 (`ops/runs/commerce/`) — 태스크가 끝나는 즉시

commerce 9개 DAG 의 **모든 태스크**가 성공·실패할 때 콜백이 한 건씩 쓴다. 배선은
`default_args` 한 줄(`ops_default_args(Layer.X)`)이고, 실제 기록은 공용 관문이 만든다.

```text
ops/runs/commerce/observed_date=<KST 날짜>/dag_id=<dag>/event_id=<sha256>.json
```

- 한 건 = **Airflow 태스크 1회 시도**. 재시도는 별개의 기록이다(`try_number` 가 다르면 다른 건).
- 담기는 것: 단계(raw/bronze/silver/gold/d1) · 상태 · 시작·종료·소요 · 행 수와 **그 행 수를
  어디서 얻었는지** · 최종 시도 여부 · 환경.
- **행 수를 못 쟀으면 비워 둔다.** 0 으로 적지 않는다 — 그러면 "빈 실행(성공했는데 0건)"과
  구분이 사라진다.
- **기록 저장이 실패해도 태스크는 계속 간다.** 관측 때문에 파이프라인이 멈추면 안 된다.
  다만 규약 위반(필수 항목 누락 등)은 `ERROR` 로 남긴다 — 배선이 잘못됐다는 뜻이라서.

### 2-b. 태스크 텍스트 로그 (`ops/logs/commerce/`) — 매일 01:30

Airflow 가 컨테이너 볼륨에 남기는 원문 로그다. 볼륨에 쌓이는 것을 막기 위해 매일 한 번
run 단위 `tar.gz` 로 묶어 올리고 로컬에서 지운다.

```text
ops/logs/commerce/observed_date=<run 시작일 KST>/<dag_id>/<run_id>.tar.gz
```

- 대상은 **종결(success·failed) run 만**. 실행 중이거나 메타DB에 없는 run 의 로그는 **지우지 않는다.**
- **업로드가 검증된 뒤에만** 로컬을 지운다(무손실).
- 이미 올라간 run 은 다시 올리지 않는다. 전환 전 경로(`load_date=`)에 있는 것도 함께 확인한다.
- 이 파일은 조회 DB 의 **행이 되지 않는다.** "기록 1건"이 아니라 텍스트 압축본이라서,
  같은 `(dag_id, run_id)` 실행 기록에 `log_bundle_key` **포인터**로 붙는다.

### 2-c. 타 도메인 기록 — 각 도메인이 자기 시점에

`ops/metrics` · `ops/errors` · `ops/reports` · `ops/recovery` · `ops/product-events` ·
`ops/product-health`. 기록기가 4벌이라 경로 축과 날짜 칸 이름이 서로 다르다. **적재기가 현재
살아 있는 배치를 전부 읽으므로**, 각 도메인이 경로를 정리하기 전에도 그대로 실린다.

### 2-d. 적재 대상이 아닌 것

`ops/control/**` 과 `ops/receipts/` 는 **상태 계열**이다. 로그가 아니라 다음 실행의 동작을
바꾸는 값이라, 적재도 자동 삭제도 하지 않는다.

## 3. 적재 — 언제 어떻게 실리나

운영 기록 관련 DAG 는 **둘**이고, 성격이 달라 분리돼 있다.

| DAG | 하는 일 | 주기 | 어디서 |
|---|---|---|---|
| `common_ops_logship` | 그 인스턴스의 **모든 도메인** 태스크 로그 → `ops/logs/<domain>/` | 매일 02:30 | **인스턴스마다** |
| `common_ops_d1_load` | R2 **전 도메인** 기록 → 조회 DB | **3시간마다** | 어디서든(겹쳐도 무해) |

**왜 나눴나** — 로그 이관은 **그 인스턴스의 로컬 디스크 파일**을 다루므로 남의 인스턴스 로그를
읽을 수 없다. 여러 Airflow 가 같은 저장소를 공유하는 구성이라, 로그는 인스턴스마다 치워야 한다.
반면 조회 DB 적재는 R2 만 읽으므로 **어디서 돌든 결과가 같다.**

**왜 겹쳐 돌아도 되나** — 중복 판정이 2단이고 적재가 자연키 갱신이라, 두 인스턴스가 동시에 돌아도
결과가 같다. 낭비되는 것은 목록 조회 몇 번뿐이다.

`common_ops_d1_load` 의 순서:

1. **훑는다** — 기본 최근 **3일** 구간(`OPS_D1_LOOKBACK_DAYS`). 도메인 범위는 기본 전체
   (`OPS_D1_DOMAINS` 로 좁힐 수 있다).
2. **이미 넣은 파일은 읽지 않고 건너뛴다** — 조회 DB 에 그 오브젝트 키가 있는지로 판정한다.
   파일을 옮기거나 표식을 남기지 않는다.
3. 남은 것만 읽어 기록 형식 한 벌로 접고, `event_id` 로 한 번 더 거른 뒤 넣는다.
4. 건드린 날짜의 **일별 집계를 다시 계산**한다.
5. DAG 별 현재 상태와 **등록된 도메인의 기대 주기**를 갱신한다.

### 왜 3시간마다 / 왜 3일 창인가

**주기** — 하루 1회면 기록이 조회 DB 에 보이기까지 최대 하루가 걸리고, 그 사이 파이프라인이 죽어도
화면은 조용하다(`C-9`). 이미 넣은 것은 파일을 열지도 않고 건너뛰므로 자주 돌아도 비용이 거의
없다 — 새로 생긴 것이 없으면 목록 조회와 질의 한 번으로 끝난다.

**창** — 하루 걸러 실패해도 다음 실행이 따라잡도록 여유를 둔 값이다.

### 한 번에 처리하는 양

새로 읽는 오브젝트 상한은 **50,000건**이다(운영 실측: 관측 계열 전체 36,536건 · 최근 일별
8,000~10,000건대). 상한에 걸리면 **잘린 건수를 결과에 남기고** 다음 실행이 이어받는다.

### 오래된 구간을 넣어야 할 때 — 백필

정기 실행은 최근 며칠만 훑는다. 그보다 오래된 구간이 필요하면 **하루씩 끊어 도는 스크립트**를 쓴다.

```bash
# 무엇이 들어갈지만 본다(기본 — 조회 DB 무변경)
python .../scripts/backfill_ops_records.py --since 2026-07-01 --until 2026-07-31
# 실제 적재
python .../scripts/backfill_ops_records.py --since 2026-07-01 --until 2026-07-31 --apply
```

**정기 실행의 창을 크게 잡는 방식은 쓰지 않는다.** 적재기는 주어진 창을 **전부 읽은 뒤에 한 번에
쓰기** 때문에, 창이 크면 진행이 안 보이고 중단 시 통째로 버려진다(실측: 3일 창 22,589건에서
30분 넘게 0행). 스크립트는 날짜마다 확정하므로 중단해도 끝난 날짜는 남고, 다시 돌리면 이미
넣은 것은 **파일을 열지도 않고** 건너뛴다.

> **과거분은 넣지 않기로 했다(2026-08-02 결정).** 관문 이전에 쓰인 기록에는 단계(`layer`) 정보가
> 없어 **날짜×도메인×단계 집계에 들어가지 않고 상세 조회만 된다** — 그 값이 낮다는 판단이다.
> 조회 DB 는 **2026-08-02 이후분부터** 쌓인다. 위 스크립트는 **나중에 시스템·저장소를 이관할 때
> 옮겨 온 기록 파일을 넣을 수 있게** 남겨 둔 경로다. 근거: `change-log.md` §92 `decision:`.

## 4. 주기 한눈에

| DAG / 작업 | 주기(KST) | 남기는 기록 |
|---|---|---|
| `commerce_collect_raw` | 일 1회 00:00 | 태스크마다 `ops/runs` 1건 |
| `commerce_recollect_raw` | 6시간 | 〃 |
| **`common_ops_logship`** (공용) | **일 1회 02:30** | 전 도메인 로그 번들 업로드·로컬 정리 |
| **`common_ops_d1_load`** (공용) | **3시간마다** | 전 도메인 기록 → 조회 DB |
| `commerce_load_bronze` | 일 1회 04:00 | 〃 |
| `commerce_load_silver` | 일 1회 05:00 | 〃 |
| `commerce_load_gold` | 일 1회 06:00 | 〃 |
| `commerce_serving_export` | gold 완료 신호(상류 이벤트) | 〃 |
| `commerce_collect_watchdog` | 하루 4회 08·12·16·20시 | 〃 |
| `commerce_load_gold_refresh` | 수동 전용 | 〃 (감시 대상 제외) |

기대 주기의 **정본은 각 DAG 파일의 `schedule=` 선언**이고, 조회 DB 의
`_ops_pipeline_expectation` 은 사본이다. 스케줄을 바꾸면
`include/commerce_core/ops_expectations.py` 표를 같은 커밋에서 고친다 — 테스트가 양방향으로
대조하므로 한쪽만 고치면 막힌다.

## 5. 보관 기간

| 카테고리 | 보관 |
|---|---|
| `runs` · `metrics` · `product-events` · `product-health` | 400일 |
| `errors` · `reports` · `recovery` | 180일 |
| `logs` | 30일 |
| `control` · `receipts` (상태 계열) | **만료 금지** |
| 조회 DB `_ops_run_event` | 180일 |
| 조회 DB `_ops_daily_metric` | 영구 |

자동 삭제 규칙은 `ops/` 루트가 아니라 **반드시 카테고리 단위로** 건다. 상태 계열에 걸면
다음 실행의 동작을 바꾸는 값이 사라진다.

## 6. 같은 것을 두 번 넣지 않는 방법

**파일을 옮기지도, 표식을 만들지도 않는다.** "어디까지 넣었는지"는 경로가 아니라 **조회 DB 에
그 기록이 있는지 없는지**로 판단한다(ASK-Seoul#78 `C-6`).

- 1차: 이 구간에서 **이미 적재한 오브젝트 키**를 먼저 받아 와 읽기 전에 거른다.
- 2차: 남은 것만 읽어 기록마다 붙는 식별자(`event_id`)로 한 번 더 거른다.
- 조회 DB 의 기본키가 그 식별자라, 설령 두 번 들어가도 한 행으로 접힌다.
- 전환 기간에 옛 경로·새 경로 양쪽으로 쓰인 중복도 여기서 하나가 된다.

적재기는 저장소를 **읽기만 한다**(쓰기·삭제·이동 없음). 테스트가 이를 강제한다.

## 7. 실패하면 어떻게 되나

| 상황 | 동작 |
|---|---|
| 기록 저장 실패(R2 장애 등) | 태스크는 계속 간다. 경고만 남는다 |
| 규약 위반(필수 항목 누락 등) | 그 자리에서 거부하고 `ERROR` 로 남긴다 — 배선을 고쳐야 한다 |
| 로그 업로드 실패 | 그 run 로컬 로그를 **지우지 않는다**. 다음 날 다시 시도 |
| 조회 DB 적재 실패 | 다음 실행이 못 넣은 것만 이어서 넣는다(이미 넣은 것은 건너뜀) |
| 파일 형식을 못 읽음 | 그 파일만 건너뛰고 **건수와 사유를 결과에 남긴다** |
| 상한 초과 | 잘린 건수를 결과에 남기고 다음 실행이 이어받는다 |

**말하지 않은 누락이 없게** 한 것이 요점이다. 조용히 자르면 "전부 봤다"로 읽힌다.

## 8. 관측 공백을 "이상 없음"으로 읽지 않기

점검이 하루 1회면 그 사이 누락은 최대 하루 안 보인다. 그래서 `_ops_pipeline_state` 에
**완전 / 부분 / 미확인**을 둔다. 아직 점검이 지나지 않은 최근 구간은 `unverified` 이고,
이건 "정상"이라는 뜻이 **아니다**. 알림은 "기록 없음"을 곧바로 장애로 부르지 않고 **기대 주기
초과와 함께** 판정한다.

## 9. 확인하는 법

```sql
-- 어제 도메인별·단계별 실행 결과
SELECT domain, layer, event_count, success_count, failed_count, rows_unknown_count
FROM _ops_daily_metric WHERE observed_date_kst = '2026-08-01' ORDER BY domain, layer;

-- 멈춘 파이프라인 후보(기대 주기와 함께 본다)
SELECT s.dag_id, s.last_observed_at, s.observation_state, e.expected_interval, e.max_delay_minutes
FROM _ops_pipeline_state s JOIN _ops_pipeline_expectation e USING (dag_id)
WHERE e.monitored = 1 ORDER BY s.last_observed_at;

-- 특정 실행의 원문 로그 위치
SELECT dag_id, run_id, status, log_bundle_key
FROM _ops_run_event WHERE dag_id = 'commerce_load_bronze' ORDER BY observed_at DESC LIMIT 5;
```

저장소와 조회 DB 가 맞는지 대조하려면 `common/ops/ingest.py` 의 `reconcile()` 을 쓴다 —
경로 날짜별로 **저장소 파일 수와 DB 보유 건수**를 나란히 보여주고 부족한 것만 짚는다.

## 10. 관련 문서

- 환경·버킷 현황: [environments.md](../configuration/environments.md)
- 저장 경로 규칙: [storage.md](../architecture/storage.md)
- 기록을 남기는 규약(코드에서 강제): `CLAUDE.md` §19.3
- 규약 정본: ASK-Seoul#78 · 구현 이슈 ASAC-DAG#647
