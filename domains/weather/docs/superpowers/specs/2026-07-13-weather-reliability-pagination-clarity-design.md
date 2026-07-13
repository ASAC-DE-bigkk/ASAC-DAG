# 기상청 신뢰성 리포트 페이지네이션 지표 명확화 설계

## 목표

Discord 리포트에서 커버리지 grid slot, 실제 raw API page, 추가 페이지 수를 구분해 `640/640`과 `800`의 관계를 즉시 이해하게 한다.

## 결정

- Bronze 적재·pagination·PASS/FAIL 판단은 변경하지 않는다.
- `grid_slot_count`는 "최소 1 page를 확보한 격자-발표시각 slot"으로 표기한다.
- `raw_object_count`는 "실제 저장·적재한 API raw page"로 표기한다.
- `additional_raw_page_count = max(0, raw_object_count - grid_slot_count)`를 계산해 `page 2+`가 몇 개인지 별도 표기한다.
- raw page 수가 grid slot보다 작으면 기존 `raw_pages_ok=False`로 FAIL을 유지한다.

## Discord 출력

```text
Bronze grid slots(커버리지): 640/640개
Bronze raw API pages(실제 응답): 800개
추가 pagination pages(page 2+): 160개
```

## 검증

- 640 slot, 800 raw page 입력에서 추가 160과 설명 문구를 검증한다.
- page 추가가 없는 640/640 입력에서 추가 0을 검증한다.
- 기존 freshness·coverage·Discord payload 테스트가 유지된다.
