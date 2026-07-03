"""commerce_core — commerce 카테고리 공유 모듈(설정/스토리지어댑터/스키마/경로/해시/레지스트리).

구 `include/common` 에서 개명(#109) — top-level `common` 이 dags/common(공통 상위 패키지)과
충돌해 단일 프로세스 DagBag 로드를 깨뜨리는 문제 해소. 범용 스토리지 구현은 `common.storage`
로 승격됐고, 나머지 모듈은 무삭제 보존. bronze·silver 가 함께 쓴다.
"""
