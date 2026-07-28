"""commerce gold 레이어 Airflow Asset URI (번들 자립 — 공유 common.assets 미변경).

`commerce_load_gold` 가 dbt_gold **성공** 직후 이 Asset 을 발행하면, 분리 DAG
`commerce_serving_export` 가 스케줄 트리거(`schedule=[Asset(GOLD_READY_ASSET)]`)로 자동
기동한다. gold **빌드 라인**(집계)과 **D1 서빙 export** 를 분리하되(사용자 확정), gold 완료
이벤트로 묶어 "덜 끝난 gold 를 서빙하는" 경합 없이 신선도를 유지한다(citydata bronze→transform
Asset 배선과 동일 규약, #274).

번들 경계(CLAUDE.md §19): 상수를 공유 `dags/common/assets.py` 가 아니라 이 번들 안에 둬
commerce 자립성을 지킨다. URI 포맷은 공유 규약(`iceberg://<domain>/<layer>`)을 따른다.
"""
from __future__ import annotations

GOLD_READY_ASSET = "iceberg://commerce/gold"
