# 새 asset-triggered DAG 배포 — pause 해제 런북

Issue: [#479](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/479)

## 배경

`schedule=[Asset(...)]`로 만든 새 DAG를 `is_paused_upon_creation=True`로 배포하는 패턴은
의도적인 안전장치다([#413](https://github.com/ASAC-DE-bigkk/ASAC-DAG/pull/413) 참고) —
머지 직후 리뷰 전에 자동 실행되는 것을 막는다. 하지만 이 pause는 **영구가 아니라
"머지 후 사람이 확인하고 풀어야 하는" 임시 상태**다.

`weather_w2_canonical_transform`이 2026-07-17 이 패턴으로 배포된 뒤 unpause 단계가
빠져 **5일간 DAG run이 0건**이었고, 그 DAG가 유일하게 만드는
`gold_weather_forecast_by_admin_dong`도 같은 기간 정체됐다(자세한 경위는
[LessonRun.md](../../../../../LessonRun.md) 2026-07-22 항목 참고). Airflow UI에서 pause
상태를 직접 보지 않는 한 증상이 겉으로 드러나지 않는다는 점이 발견을 늦췄다.

## 체크리스트

`is_paused_upon_creation=True`로 새 DAG를 배포하는 PR에는 다음을 지킨다.

1. DAG 정의부에 `is_paused_upon_creation=True`를 쓸 때, 바로 위나 옆에 이 문서를
   가리키는 주석을 남긴다(예시는 `domains/weather/weather_w2_canonical_transform.py`
   참고).
2. PR 본문 "후속 작업"에 unpause 담당자와 예정 시점을 명시한다.
3. 머지·배포 후 아래 명령으로 실제로 unpause한다.

   ```bash
   airflow dags unpause <dag_id>
   ```

4. unpause 직후 asset 이벤트 backlog 유무를 반드시 확인한다 — 이 DAG가 구독하는
   asset이 이미 오래 존재했다면(예: 기존 Bronze asset을 새로 구독하는 DAG), 그동안
   쌓인 미소비 이벤트가 **첫 실행에 한꺼번에** 묶여 들어올 수 있고, 그중 스키마가
   오래된 이벤트가 섞여 있으면 첫 실행이 예상치 못하게 실패할 수 있다.

   ```bash
   airflow dags list-runs <dag_id> -o table
   ```

   `No data found`가 나오면 아직 asset이 한 번도 트리거되지 않은 것이고, 실행됐는데
   실패했다면 로그에서 실패 task를 먼저 확인한다.
5. 위 확인이 끝나기 전까지는 이 PR을 "머지 완료"가 아니라 "배포는 됐지만 아직
   운영에 반영 안 됨"으로 취급한다.

## 왜 자동 감지가 아니라 체크리스트인가

이 저장소의 아키텍처 테스트(`test_*_architecture.py`)는 소스 파일을 정적으로
검사하는 방식이라 살아있는 Airflow 인스턴스의 pause 상태나 run 이력은 볼 수 없다.
실행 중인 DAG의 pause 상태를 주기적으로 감시하는 자동화는 별도 설계가 필요하며
이 문서의 범위 밖이다.
