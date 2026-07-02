# 적용 기술 목록 및 해설 (techniques)

이 플러그인이 대응하는 **취약점 클래스**와 **적용한 방어 기술**, 그리고 각 기술의 **원리·근거·
한계**를 정리한다. 기준 가이드라인(2026-07 확인):

- **OWASP Top 10:2025**(A01 Broken Access Control[SSRF 흡수]·A02 Security Misconfiguration·
  A03 **Software Supply Chain Failures**[신설]·A04 Cryptographic Failures·A05 Injection·
  A09 Security Logging & Alerting Failures·A10 **Mishandling of Exceptional Conditions**[신설])
- **CWE Top 25 2025**(SQLi #2 · Path Traversal CWE-22 #6 · OS Command CWE-78 #9 ·
  Code Injection CWE-94 #10 · Deserialization CWE-502 #15 · SSRF CWE-918 #22 ·
  **CWE-770 Allocation w/o Limits #25**[신설])
- **ASVS 5.0.0**(2025) · **OWASP Password Storage / SSRF Prevention / Session Mgmt Cheat Sheets**
- **Python 권고**: PEP 706(tarfile filter) · CVE-2021-42574(Trojan Source) ·
  CVE-2024-0450(zipfile overlap) · CVE-2024-4032(ipaddress 오분류)

전 기술 **stdlib only**(외부 의존성 0) 구현 — 이식성과 감사 용이성이 목적.

---

## A. 시크릿 마스킹(redaction) — CWE-532/312/201/209

**문제**: API 키·R2 자격증명·DB 비밀번호가 로그·stdout·예외 트레이스백·저장 마커·알림으로
샌다. 데이터 파이프라인에서 서울 OpenAPI 는 인증키를 URL 경로에 박아, 네트워크 예외 하나가
키를 평문으로 영구 저장할 수 있다.

**기술**: 2중 마스킹(defense in depth).
1. **literal redaction** — env 변수 중 이름이 시크릿 규칙(`KEY|SECRET|TOKEN|PASSWORD|
   CREDENTIAL|ACCESS_KEY|AUTH|…`, 단 `*_URL/_ENDPOINT/_PATH` 제외)인 것의 **실제 값**을 수집해
   텍스트 어디서든 치환. 가장 정확(키가 URL 경로에 박혀도 잡음). 짧은 값(<6자)·`${...}` 참조·
   placeholder 는 오탐 방지로 제외.
2. **structural redaction** — 값을 몰라도 형태로: 서울 OpenAPI URL 경로 키, `Authorization`/
   `Bearer`, AWS 액세스 키(`AKIA…`), `secret=`·`token=`·`api_key=` 류 할당/쿼리, **URL userinfo**
   (`scheme://<user>:<pass>@` 및 `scheme://<token>@` 토큰 단독).

**적용 지점**: `install_security()` 가 로그 필터·stdout 프록시·excepthook 에 자동 연결. 저장·
전송 지점은 호출측 `redact()` 로 2차 방어. `scrub_exception()` 은 예외 **타입을 보존**하며 args
체인(`__cause__`/`__context__`)까지 마스킹 → 재전파해도 누출 없음.

**한계**: 출력/저장 시점 방어이며 write 1회 단위. 시크릿을 변수로 다루는 것 자체는 정상 —
키를 파일명/경로/파티션에 쓰지 않는 설계 원칙은 별도로 유지.

---

## B. 로그 인젝션 무력화(CWE-117) + 관측성 유지

**문제**: 외부 입력에 개행/CR 을 넣어 **위조 로그 라인**을 만들거나, ANSI ESC 로 로그를 tail
하는 사람의 터미널을 공격(CWE-93/150). 그러나 보안 때문에 로그 분석을 못 하게 되면 실패다.

**기술**: `sanitize_log_value()` — CR/LF/탭은 가시 이스케이프(`\n`/`\r`/`\t`), 나머지 제어문자
(C0/C1/DEL/ESC)와 U+2028/2029 는 `\uXXXX` 로 치환, 길이 절단. `log_event()`/`log_exception()`
은 **마스킹된 단일 라인 JSON**(`ts/level/event/where` + 자유 필드)으로 남겨 jq/Loki/ELK 가
그대로 파싱·집계 → **값은 남기고 시크릿·제어문자만 제거**한다. 로그 필터의 제어문자 무력화는
`neutralize_controls` opt-in(기본 off — 기존 로그 바이트 불변).

**핵심 수정(검증 반영)**: 로그 필터가 **포맷 문자열**(`"secret=%s"`)을 마스킹하면 `%s`
변환지정자가 소비돼 포매팅이 깨지고 로그 라인이 유실된다 → 인자가 있을 때는 **먼저 렌더한 뒤**
마스킹하고 args 를 비운다. 또 같은 필터가 로거+핸들러 양쪽에 달려 레코드가 두 번 처리되면
백슬래시가 중복 이스케이프되므로 **1회 처리 센티넬**로 방지.

---

## C. SSRF 방어(CWE-918 / OWASP A01:2025)

**문제**: 사용자가 준 URL 로 서버가 사설망·클라우드 메타데이터(IMDS `169.254.169.254`)에
요청하도록 유도.

**기술**: `assert_url_allowed()` — urllib.parse + ipaddress(stdlib).
- 스킴 화이트리스트(`http`/`https`만 → `file`/`ftp`/`gopher` 차단), userinfo 포함 URL 거부.
- **명시적 CIDR 차단 리스트**(IPv4/IPv6): loopback·사설·링크로컬·CGNAT·IMDS(AWS/GCP/Alibaba)·
  NAT64·v4-mapped IPv6. `is_private`/`is_global` 에 의존하지 않는다 — **CVE-2024-4032** 로
  구버전 파이썬이 오분류하기 때문.
- **호스트명 DNS 해석**(`resolve_dns=True`, `http_request(url_check=True)` 의 기본) — 이게
  없으면 `metadata.google.internal`·십진/8진/16진 IP 표기가 그대로 통과한다(검증 반영 수정).

**한계**: 검증 후 재요청이 재해석되는 **DNS 리바인딩(TOCTOU)** 은 stdlib 래퍼로 못 막는다 →
`url_check=True` 는 `allow_redirects=False` 를 기본 적용하고, 완전 방어가 필요하면 검증된 IP
고정(커스텀 트랜스포트)을 권한다.

---

## D. 전송/자원 보호 — TLS·timeout·응답 상한 (CWE-295/400/770)

- **TLS 검증 강제**: `netio` 가 `verify=False` 를 `InsecureRequestBlocked` 로 차단. 정적 점검은
  `verify=False`·`ssl._create_unverified_context`·`ssl.CERT_NONE`·`check_hostname=False`·
  `urllib3.disable_warnings` 전 변형을 탐지.
- **timeout 주입**: 미지정 시 기본 30초(env `SECURITY_HTTP_TIMEOUT`) — 무한 대기 자원 고갈 차단.
- **응답 크기 상한**(`max_response_bytes`): Content-Length 는 **힌트로만** 쓰고 실제 스트리밍
  바이트를 카운트(헤더는 거짓말할 수 있음). `stream=False` 를 줘도 강제로 스트리밍(검증 반영).

---

## E. 경로 주입 / 아카이브 안전 추출 (CWE-22 #6)

- **입력 경계**: `assert_iso_date`/`assert_safe_segment` 가 `../`·절대경로·구분자·제어문자를
  경계에서 거부. `safe_key`/`safe_join` 은 조립 시 각 컴포넌트를 검증하고 `safe_join` 은
  realpath 가 root 밖이면 `ValueError`.
- **아카이브**(`safe_extract_zip`/`safe_extract_tar`): 멤버명 검증(절대/`..`/드라이브) +
  **realpath+commonpath 봉쇄 증명** + 엔트리수/총량/파일당/압축비 상한. tar 는 PEP 706
  `filter='data'`(3.9.17+) 우선, zip 은 필터가 없어 수동. 헤더 선언 크기를 신뢰하지 않고
  **실제 스트리밍 바이트**로 재검증하며(CVE-2024-0450 overlap 폭탄 대비), tar 는 `getmembers()`
  로 전체를 물질화하지 않고 `next()` 로 한 멤버씩 열람 + 압축 입력 상한으로 압축폭탄을 막는다
  (검증 반영). 심링크/하드링크/디바이스 멤버는 거부.

---

## F. 주입/역직렬화 정적 점검 (CWE-89/78/94/502)

- **SQL 주입**(`no_sql_injection`): `execute()/executemany()` 에 f-string 보간·`%` 포맷·
  `.format()`·`+` 연결로 조립한 SQL 을 직접 넘기는지 탐지. 파라미터 바인딩(`execute(sql, params)`)
  은 통과. 내부 인용부호·삼중따옴표 SQL 도 잡도록 개선(검증 반영).
- **위험 호출**(`no_dangerous_calls`): `eval`·`exec`·`os.system`·`pickle`·`marshal`·`os.popen`·
  `shell=True`. `shell=True` 는 중첩 괄호/멀티라인에서도 라인 단위로 탐지(검증 반영).
- **역직렬화**(`safe_yaml_load`): `yaml.load` 를 Safe/Full/Base 로더 없이(위치·키워드 인자
  무관) 쓰거나 `unsafe_load` 사용 시 탐지(검증 반영).

의도적 예외는 `# security: allow-sql` 표식으로 통과.

---

## G. 자격증명 하드코딩 / 공급망 (CWE-798/522, A03:2025)

- **커밋 자격증명 원문**(`no_credential_material`, CRITICAL): PEM 개인키 블록, 벤더 토큰
  접두(`ghp_`/`xox…`/`AIza`/`glpat-`/`sk-`), URL 에 박힌 비밀번호. 예외 판정을 **매치 문자열
  근방**에만 적용해, 진짜 시크릿이 주석/`example` 과 한 줄을 공유해도 놓치지 않는다(검증 반영).
- **하드코딩 시크릿/.env 위생**(`no_hardcoded_secrets`/`env_gitignored`/`env_example_clean`).
- **공급망 위생**(`requirements_hygiene`, advisory): 버전 무제한 deps·비TLS 인덱스/VCS 소스.
  환경 마커(`; python_version>=…`)의 연산자를 버전 고정으로 오인하지 않는다(검증 반영).

---

## H. 암호 기본기 (ASVS V6/V11, Password Storage Cheat Sheet)

- **CSPRNG 토큰**: `secrets.token_urlsafe`(기본 256비트) — `random` 모듈 금지(`no_insecure_random`
  이 시크릿 문맥의 `random.*` 사용을 탐지).
- **비밀번호 저장**: PBKDF2-HMAC-SHA256 **600,000회**(치트시트 권고), salt 16바이트 랜덤,
  자기서술 인코딩(`pbkdf2_sha256$iters$salt$hash`)으로 반복수 상향 시 `needs_rehash()` 투명
  업그레이드. **NFKC 정규화**(NIST 800-63B)로 호환 유니코드 동치 처리. 손상 저장값·반복수≤0
  은 예외 없이 깨끗한 거부(로그인 DoS 방지, 검증 반영).
- **상수시간 비교**: `hmac.compare_digest` — 타이밍 부채널(CWE-208) 차단.
- **약한 해시 탐지**(`no_weak_hash`, advisory): 보안 용도 md5/sha1(대문자 다이제스트명 포함,
  검증 반영). 비보안 용도는 `usedforsecurity=False` 로 예외.

Argon2id 가 1순위 권고 — 이 모듈은 의존성 0 이 필요한 환경의 **의도된 stdlib/FIPS 폴백**.

---

## I. 소스 무결성 / 설정 / 전송 (advisory)

- **Trojan Source**(`no_trojan_source`, CVE-2021-42574): bidi 제어·zero-width 문자로 코드 로직을
  위장하는 공격. 공격 문자를 `chr()` 로 조립해 탐지(점검 코드 자체에 공격 문자가 없어 자기매칭
  면역). 의도적 RTL 은 `# security: allow-bidi` 로 예외.
- **취약 파일 조작**(`no_insecure_file_ops`): `tempfile.mktemp`(레이스)·world-writable chmod.
- **웹 설정**(`no_web_misconfig`, advisory): `debug=True`·`0.0.0.0` 바인드·CORS 와일드카드 —
  파이프라인엔 없지만 FastAPI 서비스 이식 대비.
- **XML/평문 http**(advisory): XXE/billion-laughs 노출 지점, 허용 목록 외 평문 http 엔드포인트.

---

## J. 자기매칭 회피(정적 점검의 메타 원칙)

정적 점검은 번들 전체(점검 코드·테스트·문서 포함)를 스캔하므로, **점검 패턴 자체가 점검에
걸리면 안 된다**. 규칙: 패턴 리터럴은 정규식 이스케이프(`\.`)나 조각 결합(`"htt" + "p://"`,
`chr(0x202E)`)으로 작성하고, 위반 샘플 테스트는 tmp 파일에 런타임 기록하거나 조각 결합한다.
이는 "보안 도구가 자기 자신을 오탐하지 않는다"는 실용적 계약이며, 이 원칙 위반이 검증에서
자기매칭 결함으로 여러 건 잡혀 전부 반영됐다.

---

## K. 단일 포인트 검증

모든 정적(18종)+런타임(4종) 점검이 `run_security_verification()` 한 곳으로 모이고,
`python -m security`(CLI, exit code)·`pytest test_security.py`(CI)·`assert_secure()`(코드)가
같은 함수를 호출한다. 차단 = CRITICAL/HIGH 미통과. 새 점검은 `audit.py` 에 `check_*(root)
-> Finding` 추가 후 `STATIC_CHECKS` 등록만 하면 자동 포함된다.

> 이 문서의 모든 "검증 반영" 표기는 feat/96 2차 확장에서 다중 에이전트 적대적 검증으로
> 확인된 16개 결함(SSRF 호스트명 우회·로그 포맷문자열 훼손·tar 압축폭탄·정적 점검 우회 등)을
> 수정해 반영했다는 뜻이다. 상세 근거·CWE 매핑은 [security.md](security.md) 위협 모델 참조.
