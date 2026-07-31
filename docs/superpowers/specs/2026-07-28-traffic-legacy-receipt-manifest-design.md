# Traffic legacy receipt manifest 복구 설계

## 목적

P0 raw manifest gate 배포 전에 생성된 Traffic Incident pending receipt가 Bronze 전체를 차단하지 않도록 복구한다. manifest가 없는 raw를 Bronze가 읽는 것은 허용하지 않는다.

## 범위와 경계

- 대상은 `traffic-snapshot-receipts`의 legacy `LANDED` pending receipt다.
- receipt 문서와 기존 raw object는 수정하거나 삭제하지 않는다.
- 외부 TOPIS API를 다시 호출하지 않는다.
- Weather, Traffic Flow, P1 ops/control 경로 전환은 이번 변경 범위가 아니다.

## 설계

materializer는 `manifest_key`가 없는 receipt에 한해 기존 `raw_object_keys`를 `TrafficLanding.replay(...)`로 재검증한다. replay는 R2의 기존 객체만 읽어 page 범위, object 존재, receipt의 `raw_hash`와 재조회 payload hash 일치를 확인하고, 성공한 경우 원래 snapshot run identity에 표준 `_manifest.json`을 마지막으로 기록한다.

복구가 성공하면 그 실행의 메모리상 `raw_result`에만 `manifest_key`를 보강해 기존 Bronze loader로 넘긴다. append-only `LandedSnapshot` 및 pending receipt의 저장 문서는 변경하지 않다. 복구가 실패하면 validation error로 task를 실패시켜 receipt를 pending으로 보존한다.

## 검증

- legacy receipt의 유효 raw object가 manifest를 재발행하고 Bronze loader에 전달된다.
- manifest가 존재하는 새 receipt는 replay를 수행하지 않는다.
- replay가 실패하면 Bronze loader를 호출하지 않고 pending receipt도 삭제되지 않는다.
- Traffic unit suite, DAG import, dev 재배포 후 기존 pending receipt의 자연 재시도를 확인한다.
