# 서울 TOPIS TrafficInfo 소통정보

`traffic_flow_bronze.py`는 OA-13291의 서울 TOPIS `TrafficInfo` XML endpoint
(`/xml/TrafficInfo/1/1/{link_id}/`)를 호출한다.

- 원문 응답은 R2 raw에 XML로 저장한다.
- Bronze table은 `bronze_seoul_traffic_flow`다.
- 핵심 원천 필드는 `link_id`, `prcs_spd`, `prcs_trv_time`다.
- `link_id`는 기본적으로 최신 publishable 돌발정보 Bronze snapshot에서
  해석하며, 테스트 시 `dag_run.conf.link_ids` 또는
  `SEOUL_TRAFFIC_FLOW_LINK_IDS`로 고정할 수 있다.
- API key는 `SEOUL_OPEN_API_KEY`를 사용하고,
  `SEOUL_API_KEY_TRIC`은 하위 호환 fallback으로만 허용한다.

API key 값 자체는 request metadata, raw 경로, 로그에 저장하지 않는다.
