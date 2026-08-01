# 재수집 파이프라인 · 알림 인터페이스 · API별 진행 가시성

bronze 저장 구조(run_id 폴더 + 마커)를 기반으로 한 운영 보조 3종.

## 1. 재수집 파이프라인 (`commerce_recollect_raw`)

매 실행 전체 수집하는 `commerce_collect_raw` 와 별개로, **미완료(incomplete/미시도)인 API만
주기적으로 다시 수집**하는 안전망 DAG.

- 스케줄: `0 */6 * * *`(6시간마다). 수동 트리거도 가능. `max_active_runs=1`.
- 흐름: `find_incomplete_targets` → `ingest_one.expand` → `finalize_run`. (bronze 전용 — silver 분리)
- **대상 선정**([../../include/bronze/markers.py](../../include/bronze/markers.py)):
  1. `latest_run_id` — `raw/commerce/` 아래 가장 최근 `run_id` 폴더(사전식=시간순).
  2. `incomplete_targets` — 수집 대상 중 그 run 에서 **`<short>.completed` 마커가 없는** API
     (= incomplete 이거나 미시도). 이력이 없으면(첫 실행) 전체.
- **당일 수집 대상이 없으면 수집 진행 안 함**: 대상이 빈 리스트면 `ingest_one` 이 0개로 매핑돼
  아무것도 호출하지 않고, `finalize_run` 이 `_RUN` 마커도 쓰지 않아 **run 폴더가 생기지 않는다**.
  단 **완료 알림은 보낸다**(0종 리포트 — 아래 §1-b). 마커를 안 쓰는 것과 알림을 안 보내는 것은 별개다.
- 재수집분은 **새 `run_id` 폴더**에 그 API들만 적재(기존 run 폴더는 불변).

### 1-a. 재수집 규칙 (feat/59)

- **동일자 수동 재실행 → 성공분 제외**: daily 를 같은 KST 날짜에 (수동) 다시 실행하면, 그날 이미
  `completed` 인 API 는 다시 수집하지 않는다(`plan_excluding_same_day_completed`).
- **KST 일자변경 가드**: recollect 는 최근 run 의 incomplete 중 **run 의 KST 날짜가 오늘과 같은** 것만
  대상으로 한다(`recollect_targets_same_day`). 한국시간 기준 일자가 바뀌면(최근 run 이 이전 날) 그 정보는
  **다른 일자**라 재수집하지 않는다 — 새 일자 수집이 처리.
- **한 파일 관리(option 2)**: 재수집이 성공하면, **같은 KST 일자**의 이전 실패 run 파편(파일·incomplete
  마커)을 `cleanup_incomplete` 로 삭제해 API당 하나로 유지한다(다른 일자는 건드리지 않음).

```bash
# 수동 재수집(미완료만)
docker compose exec airflow-scheduler airflow dags trigger commerce_recollect_raw
```

### 1-b. 0종 실행 완료 알림 (run report)

`finalize_run` 은 **대상이 0종이어도 DAG 완료 리포트를 1건 보낸다**(정책: [../PROJECT.md](../PROJECT.md) §2).
0종은 정상 경로(동일자 성공분 제외)지만, 알림이 없으면 수신자는 "오늘 이미 다 받았다"와 "수집이
멈췄다"를 구분할 수 없다 — 둘 다 **메시지 없음**으로 관측되기 때문이다.

- 본문은 `신규 0건 · 대상 0종` 요약 + **사유 섹션**:
  - collect — `동일 KST 일자에 이미 완료 152/152종` + **그 일을 끝낸 run_id**(`완료 run: ...`).
    다른 Airflow 인스턴스가 같은 마커 존을 쓰는 경우도 이 run_id 로 드러난다.
  - recollect — `최근 run 에 미완료 API 없음`(또는 KST 일자변경 가드).
- **동일자 완료로 설명되지 않는 잔여**(예: `check_api_key` 게이트 실패로 수집 자체가 안 돈 경우)는
  `scope_shorts` 로 넘겨 **⛔ 미수집 + 경고색**으로 뜬다 → 상류 스킵이 초록 0종에 묻히지 않는다.
- run 마커(`_RUN.*`)는 **여전히 쓰지 않는다** — 수집이 없었으므로 run 폴더도 만들지 않는다.
  즉 "마커 생략"과 "알림 생략"은 분리돼 있다.
- 구현: [`commerce_raw.py`](../../commerce_raw.py) `_no_target_reason` / `_send_report`,
  근거 조회 [`bronze/markers.py`](../../include/bronze/markers.py) `same_day_completed_summary`,
  문구 [`commerce_core/run_report.py`](../../include/commerce_core/run_report.py) `no_target_section`.
  근거 조회가 실패해도 알림은 나간다(사유만 축약).

> daily 가 전체를 매번 받으므로 엄밀히는 재수집도 daily 가 흡수한다. recollect 는 **부분 실패를
> 더 자주(6h) 메우는 경량 보강**이다(전체가 아니라 미완료분만).

## 2. 알림 인터페이스 (예외 → 알림) — *아직 비활성*

예외 발생 시 **로그 내용을 알림 메시지로 보낼 수 있는 인터페이스**. 구현체만 두고 **실제 전송은
하지 않는다**(기본 `NoopNotifier`). 파이프라인에도 **아직 와이어링하지 않음** — 인터페이스 제공만.
코드: [../../include/commerce_core/notify.py](../../include/commerce_core/notify.py).

```python
from commerce_core.notify import Notifier, set_notifier, notify_exception

# 1) 채널 구현(예: 웹훅) — 운영에서 주입
class WebhookNotifier(Notifier):
    def send(self, *, subject, message, level="error", context=None):
        import requests; requests.post(URL, json={"subject": subject, "text": message}, timeout=10)

set_notifier(WebhookNotifier())   # 기본 NoopNotifier 대신 주입(앱/DAG 부트스트랩에서)

# 2) 태스크에서 예외를 알림으로(향후 ingest_one 등에 추가)
try:
    ...수집...
except Exception as exc:
    notify_exception(exc, where="ingest_one:general_restaurant",
                     context={"run_id": "...", "short": "general_restaurant"})
    raise
```

- 기본 `NoopNotifier` 는 **전송하지 않고 로그만** 남긴다 → 지금 호출해도 안전(무동작).
- `notify_exception` 은 트레이스백을 메시지에 담되, **시크릿(SEOUL_API_KEY_COMM/R2 토큰)은 넣지
  않는다**(CLAUDE.md §2.5). 전송 실패는 삼켜서 본 파이프라인을 막지 않는다.
- 와이어링 시점: ingest/silver 태스크의 `except` 에 `notify_exception(...)` 추가 + 부트스트랩에서
  `set_notifier(...)`. (이번엔 의도적으로 미적용.)

## 3. API별 진행 가시성 (Airflow Grid/Graph)

`ingest_one` 은 Dynamic Task Mapping 으로 **API당 1개 태스크 인스턴스**이고,
`map_index_template="{{ short }}"` 로 각 인스턴스를 **API(short) 이름으로 라벨링**한다
([../../commerce_raw.py](../../commerce_raw.py)).

→ Airflow **Grid/Graph** 에서 한 실행의 내부 job 을 **API별로** 성공/실패/실행중/대기로 확인:

| 보이는 것 | 의미 |
|---|---|
| `ingest_one[clinic]` 초록 | clinic 수집 성공 |
| `ingest_one[general_restaurant]` 빨강 | 해당 API 실패(→ incomplete 마커, 다음/recollect 재수집) |
| `ingest_one[lodging]` 회색/대기 | 아직 실행 전(대기) |

- 실측 확인: 매핑 인스턴스의 `rendered_map_index` 가 `clinic`·`pharmacy`·`hospital` … 로 표시됨.
- 라벨은 태스크 실행 시 확정된다(대기 중 인스턴스는 잠시 정수 index 로 보일 수 있음).
- 실행 결과 요약은 run 폴더의 마커에도 남는다(`_markers/<short>.completed|.incomplete`,
  `_RUN.*` metrics) — UI 와 별개로 스토리지에서도 job 결과를 추적할 수 있다([../architecture/storage.md](../architecture/storage.md)).

> 참고: silver 태스크는 호스트 이미지에 `pandas`/`pyarrow` 가 없으면 실패한다(번들 밖 의존성 —
> [../../requirements.txt](../../requirements.txt)). bronze(ingest) 가시성/수집과는 독립적이다.
