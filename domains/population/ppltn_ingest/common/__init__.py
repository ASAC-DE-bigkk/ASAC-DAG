"""도메인 무관 공통 helper 계층.

이슈 #16의 "얇은 common" 방향을 따른다 -- 틀리면 위험한 반복 로직(환경/시크릿/
R2/HTTP/Trino 적재 규칙)만 모으고, source-specific 로직(URL 생성, 성공 판정,
파싱, DDL 의미)은 ``source``와 도메인 파일에 남긴다.
"""
