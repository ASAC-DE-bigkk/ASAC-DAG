# TOPIS 빈 응답 1회 재시도 설계

## 목적

TOPIS AccInfo가 HTTP 200과 빈 본문을 일시적으로 반환할 때 같은 page를 한 번만
다시 요청하고, 두 번째 응답도 비어 있으면 기존처럼 task를 실패시킨다.

## 변경하지 않을 경계

- `INFO-000`이 아닌 business error는 재시도하지 않는다.
- 비어 있지 않은 malformed XML은 재시도하지 않는다.
- 실패한 빈 본문은 raw object나 checkpoint page로 기록하지 않는다.
- 기존 incomplete full-snapshot 재수집 정책과 Airflow retry 정책은 바꾸지 않는다.

## 설계

`parse_seoul_acc_info_response`는 빈/공백 본문을
`TrafficSourceEmptyResponseError`로 구분한다. `TrafficLanding.collect`의 page fetch
경계는 이 예외에만 같은 `(start_index, end_index)`를 즉시 한 번 더 요청한다.
두 번째 파싱도 같은 예외를 내면 그대로 전파한다.

## 검증

- `empty -> valid`: 같은 page를 두 번 요청하고 유효 응답만 적재한다.
- `empty -> empty`: 두 번 요청한 뒤 `TrafficSourceEmptyResponseError`로 실패한다.
- `malformed -> valid`: 첫 malformed XML에서 실패하고 두 번째 응답은 요청하지 않는다.

