# 신규 패턴 저작 브리핑 (에이전트 공용 — 전부 읽고 따를 것)

커머스 외 도메인(citydata·culture·transit·traffic_weather)의 gold→D1 서빙 제품에 대해
**검증 가능한 신규 질의 패턴**(usage_patterns)을 저작한다. 소비자는 AI(MCP)·API. 목표는 각
제품으로 실질 답 가능한 질문 공간을, 기존 패턴이 못 덮는 데까지 넓히는 것.

## 게이트웨이 실행 계약 (실측 확정)

1. `pattern_id` 는 `^[a-z0-9_]{1,64}$` (하이픈 금지).
2. 실행 전 `--`/`/* */` 주석 제거 → `SELECT`/`WITH` 단일문만.
3. `:name` 은 등장 순서대로 `?` prepared bind. **모든 파라미터 필수**(기본값 없음), 문자열/숫자만.
4. 행수 제한은 `LIMIT :n`(또는 `:limit`/`:top_n`) — 이 세 이름만 상한 5000 clamp.
5. 식별자(컬럼/테이블/방향) bind 불가 — 구조 가변은 아래 관용구로.
6. 같은 D1 안 **이 도메인 소유 gold 테이블끼리** JOIN 가능(교차 테이블 패턴 허용).

## ★ 테이블 명명 (커머스와 다름 — 중요)

- 이 도메인들의 D1 테이블명 = **dbt 모델명 그대로** = `gold_<domain>_<name>`
  (예: `gold_culture_activity_by_dong`, `gold_citydata_ppltn_daily`). 커머스의 `d1_*` 아님.
- 패턴 SQL 의 `FROM`/`JOIN` 은 **반드시 이 gold_ 테이블명**을 쓴다(자기 도메인 소유만).
- 정적 감사기가 자기 도메인 gold_ 테이블 밖 참조(내부표·타도메인)를 게시에서 막는다.

## 안전 조합 관용구 (적극 활용)

- 차원 스위치: `CASE :dim WHEN 'gu' THEN gu ELSE area_name END AS dim_value` (GROUP BY 반복)
- 정렬 방향 스위치: `ORDER BY CASE WHEN :dir='asc' THEN m END ASC, CASE WHEN :dir='desc' THEN m END DESC`
- 센티널(전체): `AND (:gu = 'ALL' OR gu = :gu)`
- 임계값: `HAVING SUM(x) >= :min_x` / 기간 창: `WHERE event_date BETWEEN :from AND :to`
- **배열 IN(실 D1 검증)**: `WHERE area_name IN (SELECT value FROM json_each(:areas))`,
  `:areas='["성수동","여의도"]'`(JSON 배열 문자열). 다중 선택에 쓴다.

관용구 사용 시 출력 `composition_idiom` 에 어느 관용구인지 기입.

## 표기 규약

- `pattern_id`: `^[a-z0-9_]{1,64}$`. 같은 테이블의 기존 id 와 충돌 금지(current.json 대조).
- SQL 첫 줄은 예시값 주석. 이 도메인들은 **따옴표 없는 값**을 관례로 쓴다:
  `-- :area=성수동, :date=2026-07-30, :n=10`. (문자열/날짜는 bare, 숫자도 bare.)
- 본문에 `값 /* :name */`(값 박힘) **금지** — 실제 `:name` 바인딩만.
- `requires` 어휘: select_columns, sort, aggregate, group_by, filter_range, having,
  subquery, window, join(교차일 때), filter_set(다중값 IN), filter_null.

## 품질 기준

- **실질 질문**일 것(그 도메인 소비자·AI 가 실제로 물을 법). 기존 패턴과 **의미 중복 금지**
  (current.json 을 읽고 — 단 고정 패턴을 관용구로 일반화한 상위호환은 가치 있음).
- 예시값은 **메타 덤프 실측값에서만**(distinct_values·numeric_ranges·sample_rows). 코드값 날조 금지.
- 파라미터 있는 패턴은 예시값 조합 **최소 2개**, 첫 조합(=예시 주석 값)은 **0행 초과**.
- `insight_sample_ko` 는 실제 반환 first_row 수치로. 레플리카는 표본이니 수치는 실측 그대로.
- 개수 채우기용 패딩 금지. 제품이 얇으면 적게 내는 게 맞다(보통 제품당 4~10개).

## 테스트 (로컬 레플리카 — 무제한)

후보를 JSON 으로 만들어:
`python <authoring>/local_run.py <authoring>/<domain>/replica.sqlite <cands.json>`
실패·0행이면 고쳐 재시도(무제한). 레플리카는 실데이터 표본(대형표는 부분) — 0행 초과 판정과
문법·컬럼 검증이 목적. 실 D1 최종 검증은 이후 별도.

## 표현 불가 아이디어 (버리지 말 것)

게이트웨이로 안 되는 아이디어는 `not_expressible` 에: 무슨 질문(idea_ko)·왜 막히는지
(why_blocked_ko)·무엇 열리면 되는지(what_would_enable_ko). 마켓플레이스 역제안(#192) 재료.

## 출력 (제품/테이블 단위)

`<authoring>/<domain>/candidates/<table>.json`:
```
{"table":"gold_<domain>_<name>","product_id":"<domain>_<name>","new_patterns":[
  {"pattern_id":"...","question_ko":"...","axes":"...","sql":"-- :a=..\nSELECT ...",
   "requires":[...],"combos":[{...},{...}],"rows_by_combo":[n,n],
   "insight_sample_ko":"실측 수치","composition_idiom":null}],
 "not_expressible":[{"idea_ko":"","why_blocked_ko":"","what_would_enable_ko":""}]}
```
저장소 파일 수정 금지 — 산출물은 candidates 파일 + StructuredOutput 요약뿐.
