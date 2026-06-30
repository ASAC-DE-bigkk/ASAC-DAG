"""서울 교통 실시간 수집 로직 패키지 (transit 도메인 동봉).

DAG 는 얇게 — 실제 호출/파싱/적재는 이 패키지가 담당한다.
가공 최소화: 원본 행을 보존하고(envelope.raw) source/시각/좌표만 덧붙인다.
  {source, ts_collected, ts_source, lat, lon, raw:{...원본 그대로...}}

소스 무관 공통(api·records·r2_landing·config)과 소스별 수집(subway, 후속: parking/road)
으로 나뉜다. 주차·버스 확장 = 소스 파일 추가로 끝나도록 공통부를 분리해 둔다.
"""
