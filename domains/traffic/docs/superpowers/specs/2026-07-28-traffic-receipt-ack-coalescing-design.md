# Traffic receipt acknowledgement coalescing fence 설계

- 상태: 확정
- 기준 브랜치: `origin/dev`의 `9ddd11d`
- 이슈: 사용자 지시에 따라 별도 이슈를 만들지 않고 PR로 추적

## 문제

`traffic_incident_bronze`는 pending receipt를 한 번 읽어 배치로 처리한다. 성공한
materializer task의 Airflow asset callback은 `MATERIALIZED` receipt를 검증한 뒤
pending marker를 삭제한다. 다음 Bronze task가 callback 완료 전의 pending 목록을
이미 읽었다면, callback 이후에도 그 오래된 목록을 계속 순회한다.

기존 `verified-skip`은 Bronze 행 수·raw hash·audit evidence가 정확히 일치하는
receipt의 Iceberg load를 생략해 데이터 중복을 막는다. 그러나 이 경로도 manifest
`STARTED`/`SUCCESS`와 `MATERIALIZED` receipt 확인을 반복한다. 따라서 5분 수집
주기에서 stale receipt 여러 개가 겹치면 Trino 제어-plane MERGE만으로 한 task가
15분 이상 걸려 최신 snapshot과 downstream asset이 밀린다.

## 목표와 비목표

목표는 이미 acknowledgement된, 검증 완료 receipt가 현재 task의 오래된 배치
목록에만 남아 있는 경우 어떠한 materialization side effect도 만들지 않는 것이다.

이번 변경은 다음을 하지 않는다.

- R2 raw, Bronze/Silver/Gold 데이터 계약 또는 table schema 변경
- Airflow asset acknowledgement 순서 변경
- `pending` 상태인 verified receipt의 asset 재발행 복구 제거
- `trino_traffic_heavy` pool slot 증설 또는 병렬 writer 도입
- ASAC-DBT Gold incrementalization 및 장기 control-plane migration

## 계약과 알고리즘

`ReceiptQueue`에 `is_pending(snapshot_run_id) -> bool`을 추가한다.
`TrafficSnapshotReceipts` 구현은 해당 snapshot의 pending marker를 엄격히 읽는다.
`FileNotFound` 또는 R2의 명시적 missing-object code만 false이며, 권한·전송·5xx·문서
오류는 예외로 전파한다. 이 조회는 상태를 변경하지 않는다.

materializer는 preflight 결과가 **정확히 검증된** receipt에만, `manifest.start`보다
앞서 아래 fence를 적용한다.

```text
initial pending batch -> exact Bronze/audit preflight
for each receipt:
  if receipt is exact-preverified and pending marker no longer exists:
    skip with no manifest, no MATERIALIZED receipt, no asset metadata
  otherwise:
    retain the existing materialize or verified-skip recovery path
```

검증되지 않은 receipt는 기존처럼 load/verify를 시도한다. 이 구분은 Trino/R2
오류를 "이미 처리됨"으로 오인해 조용히 유실시키지 않는다.

또한 `MATERIALIZED`는 존재하지만 pending marker가 남은 receipt는 fence를 통과한다.
이는 이전 worker가 Iceberg commit 뒤 Airflow asset publish 또는 success callback 전에
중단된 경우, 다음 run이 정확한 evidence를 바탕으로 asset을 재발행하고 acknowledgement
될 수 있게 하는 at-least-once 복구 계약이다.

## 정합성·성능 근거

- fence가 false인 receipt는 성공 callback이 이미 pending marker를 삭제했다는 뜻이다.
  해당 snapshot의 asset event는 Airflow가 수락했고 pending receipt 계약도 종료됐다.
- fence가 true인 receipt는 종료되지 않았으므로 기존 경로를 보존한다.
- per-receipt R2 receipt check는 stale receipt마다 발생하던 manifest의 Trino
  `STARTED`/`SUCCESS` MERGE와 receipt write보다 작고, raw/Iceberg 쓰기를 추가하지
  않는다.
- `max_active_runs=1`과 단일 Traffic Trino writer lane은 유지한다. 이 변경은
  concurrency를 키우지 않고 오래된 배치만 coalesce한다.

## 검증 시나리오

1. pending marker가 남아 있는 verified receipt는 load 없이 manifest publish,
   materialized receipt, asset metadata를 계속 만든다.
2. preflight 뒤 pending marker가 사라진 verified receipt는 loader, verifier,
   manifest, receipt write, asset metadata 어느 것도 호출하지 않는다.
3. 검증되지 않은 receipt는 marker 상태와 무관하게 기존 load/verify 실패-표면화
   계약을 유지한다.
4. receipt storage는 landed -> materialized -> acknowledgement 이후에만
   `is_pending`이 false가 되는지 단위 테스트로 고정한다.
5. Traffic 단위 테스트, compile, DAG import 후 dev 재배포에서 다음 scheduled
   Traffic asset cycle의 Bronze/Flow/Transform/Gold 성공과 freshness를 확인한다.

## Rollout과 후속

변경은 `domains/traffic/**`만 포함한 DAG PR로 `dev`에 병합한다. 병합 후 clean
runtime checkout에서 최신 `origin/dev` DAG ref로만 재배포한다. 실행 중인 current
Traffic run을 pause하거나 직접 trigger하지 않고, 정상 scheduler cycle로 효과를
확인한다.

후속 단계는 15분 end-to-end SLO를 보장하기 위한 Gold full rebuild 최소화,
desired-watermark coordinator, 독립 durable control state의 설계·분리 구현이다.
