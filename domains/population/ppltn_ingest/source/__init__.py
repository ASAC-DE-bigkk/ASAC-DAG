"""서울 citydata_ppltn(실시간 인구혼잡도) 소스 계층 (population 전용).

URL 생성·성공 판정·장소 목록 등 source-specific 로직을 담는다(이슈 #16: source
로직은 도메인에 남긴다). ``common``의 R2/Trino/HTTP helper를 조합해 오케스트레이션.
"""
