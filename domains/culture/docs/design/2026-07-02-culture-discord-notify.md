# 설계: culture bronze 적재 완료 Discord 일일 리포트 알림

- 상태: 설계 확정 대기 (리뷰 중)
- 관련 이슈: [#68](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/68)
- 작성일: 2026-07-02
- 도메인: 문화(culture) / 영향 레이어: DAG·Orchestration, Bronze

## 1. 목적

`culture_bronze_ingest`(@daily)는 매 자정 12개 데이터셋을 Bronze에 적재한다.
현재 성공/실패는 **Airflow UI로만** 확인 가능해, 자정에 조용히 실패해도 능동적으로
알 방법이 없다. 배치 완료 시 Discord 채널로 **데이터셋별 세분화 일일 리포트**를
보내 운영 가시성을 확보한다.

commerce의 예외 알림(`notify_exception`, noop, 미와이어링)과 달리, 이 설계는
**적재 완료 요약 알림**을 지향한다(성공/실패 모두 자정 1회).

## 2. 확정된 결정

| 항목 | 결정 |
|---|---|
| 알림 성격 | 적재 **완료 요약** 알림 (예외 알림 아님) |
| 범위 | **성공 + 실패 모두** — 매 완료 시 1건, PASS/FAIL 구분 |
| 전송 위치 | `report` 태스크 끝 (run_report 완성 직후) |
| 채널 | Discord Incoming Webhook |
| 활성화 | env `CULTURE_DISCORD_WEBHOOK_URL` 있을 때만. **없으면 no-op** |
| cadence | @daily = 하루 1건 (스팸 아님) |
| 실패 처리 | best-effort — 전송 실패는 삼켜 본 파이프라인을 막지 않음 |

## 3. 구조

신규 모듈 `culture_ingest/common/notify.py` (commerce 인터페이스 모양 차용):

```
Notifier (ABC)          .send(payload: dict) -> None
NoopNotifier            로그만 (URL 없을 때 기본)
DiscordWebhookNotifier  requests.post(url, json=payload, timeout); 실패 삼킴
build_report_payload(report) -> dict   # 순수 함수: run_report -> Discord 임베드 JSON
notifier_from_env(env=os.environ) -> Notifier   # URL 있으면 Discord, 없으면 Noop
```

- `build_report_payload`는 **네트워크 없는 순수 함수** → 포맷 로직을 단위 테스트로 검증.
- `report` 태스크: `notifier_from_env().send(build_report_payload(report))`.
- 관심사 분리: 포맷(build_report_payload) / 전송(DiscordWebhookNotifier) / 선택(notifier_from_env).

## 4. 데이터 출처

대부분 기존 `run_report`(= `build_run_report`)에 이미 있음:

- `coverage{expected,landed,skipped,failed,coverage_pct}`, `total_rows`,
  `total_iceberg_rows`, `freshness{max_age_hours}`, `violations`, `failed_datasets`,
  `slo_passed`, `datasets`(= 데이터셋별 `summary()`), `load_date`, `ingest_ts`, `run_id`.
- 한국어 서비스명: `datasets.py`의 `Dataset.title` → `build_report_payload`에서
  `BY_NAME[name].title` 조회 (파이프라인 변경 없음).

**신규 캡처 필요 (per-dataset 타이밍):**

- `DatasetResult`에 `duration_sec: float`, `finished_ts: str`(UTC ISO) 추가.
- `ingest_dataset`에서 데이터셋 시작/종료 시각 기록 → `summary()`에 노출.
- run 레벨: 시작 = `ctx.ingest_ts`(UTC), 완료 = `max(finished_ts)`.
  둘 다 KST로 변환해 표시(소요 = 완료 − 시작).

## 5. 메시지 설계

Discord 임베드 1건. PASS면 초록 색띠, FAIL이면 빨강.

```
📊 culture bronze 적재 리포트 · 2026-07-02 (KST)          [색띠: green/red] SLO PASS/FAIL
수집 00:00:07 → 완료 00:01:58 KST · 소요 1m51s
커버리지 12/12 · landed 12 · skipped 0 · failed 0 · rows 5,635 · Iceberg 5,635 · freshness 0.3h
──────────────────────────────────────────────
 st    rows  pages  done      sec   dataset
 ok   1,204     13  00:00:12   4.1  공연목록 · kopis_performance
 ...
 WARN    18      1  00:00:03   0.4  시립미술관 전시 · seoul_sema_exhibition
 FAIL     0      0  --         --   세종문화회관 공연/전시 · seoul_sejong
──────────────────────────────────────────────
❌ FAIL 1 — 세종문화회관(seoul_sejong): HTTP 500
⚠️ WARN 1 — 시립미술관 전시(seoul_sema_exhibition): 완전성 미달 rows 18 < min 20
⚠️ Iceberg 불일치 1 — 공연상세(kopis_performance_detail): raw 1,180 ≠ iceberg 1,150
run_id=scheduled__2026-07-01T15:00 · @daily
```

- **표는 코드블록(monospace)**. dataset 슬러그가 전부 ASCII라 숫자열 정렬 유지.
  한국어명 + slug는 **맨 끝 ragged 열**(한글 double-width가 앞 칸을 안 깨뜨림).
- 정상일 땐 색띠 초록 + `SLO PASS`, 하단 이슈 줄 생략, 표 전부 `ok`.

### 상태 판정 규칙 (표 `st`)

| 상태 | 조건 |
|---|---|
| `FAIL` | `error != ""` 이고 `"skipped"` 미포함 |
| `skip` | `error`에 `"skipped"` 포함 (예: detail off) |
| `WARN` | landed인데 `checks.passed == False`(계약 위반) **또는** Iceberg 불일치(`rows != iceberg_rows`) |
| `ok` | landed + 계약 통과 + Iceberg 일치 |

### Payload 형태

```json
{ "embeds": [ {
  "title": "culture bronze 적재 리포트 · <load_date> (KST)",
  "color": 3066993,            // green(PASS) / 15158332 red(FAIL)
  "description": "<요약 2줄>\n```\n<표>\n```\n<이슈 요약줄>",
  "footer": { "text": "run_id=<...> · @daily" }
} ] }
```

### Discord 한계 대응

- 임베드 description ≤ 4096자. 현재 12행은 여유.
- 향후 데이터셋이 크게 늘어 초과 시: FAIL/WARN 우선 + `+N more`로 절단하고
  절단 사실을 `log()`(무음 절단 금지).

## 6. 설정 · 시크릿

- env: `CULTURE_DISCORD_WEBHOOK_URL` (culture 전용 네임스페이스).
- **없으면 `NoopNotifier`** → 코드는 안전하게 dev 선머지 가능.
- 공유 `sample/.env`에 실제 URL 추가 = **멘토 게이트**(별도, 코드 머지 후).
- 시크릿 원칙: 웹훅 URL은 **메시지·로그에 절대 미포함**. 표의 `error` 문자열은
  이미 상태코드/건수 수준(키 없음)이며 N자로 잘라 넣음.

## 7. 실패 동작 (best-effort)

- `DiscordWebhookNotifier.send`의 예외는 잡아서 로그만 남기고 삼킴 →
  알림 실패가 `report` 태스크(및 배치)를 실패시키지 않음.
- 타임아웃 명시(예: 10s).

## 8. 테스트

- `build_report_payload`: PASS 케이스(전부 ok) / FAIL 케이스(WARN+FAIL+Iceberg 불일치)
  → 색상, 제목, 표에 데이터셋행·한국어명 포함, 이슈 요약줄 존재 검증.
- `notifier_from_env`: URL 없음 → Noop, 있음 → Discord 분기.
- `DiscordWebhookNotifier.send`: `requests.post` mock 호출 검증 + 예외 시 삼킴 검증.
- 컨테이너: `report` 태스크가 Noop 경로에서 정상 동작(전송 없이 통과) 확인.

## 9. 범위 밖 (YAGNI)

- 예약 데이터 실시간(분 단위) 수집 — 필요 시 별도 DAG(이 설계와 무관).
- silver/gold 알림, Slack/Email 등 다른 채널 — 인터페이스로 확장 가능하나 이번 범위 아님.
- commerce의 `notify_exception`(예외 알림) 통합 — 별개 관심사.

## 10. 머지 순서

1. 본 PR(코드 + 설계 문서): URL 없으면 no-op이라 **기능 off 상태로 안전 머지**.
2. (멘토 게이트) 공유 `sample/.env`에 `CULTURE_DISCORD_WEBHOOK_URL` 추가 → 실제 발송 on.
