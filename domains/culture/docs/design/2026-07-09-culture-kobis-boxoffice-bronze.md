# KOBIS 일별 박스오피스 bronze 수집 설계 (#197)

**이슈**: [ASAC-DAG#197](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/197)
**작성일**: 2026-07-09 · **브랜치**: `feat/197-culture-kobis-boxoffice-bronze` (dev 기준)
**범위**: bronze만. silver(일×순위 그레인)·gold(서울 쏠림 파생)는 후속 PR.

## 목표

영화진흥위원회(KOBIS) 오픈API 일별 박스오피스를 **전국 1벌 + 서울 한정 1벌**(일 2요청)로
culture bronze에 편입한다. "전국 집계를 서울 소비 온도로" 프록시 가정의 부정확성을,
KOBIS가 지원하는 상영지역 필터(`wideAreaCd`)로 보정해 진짜 서울 영화소비 시계열 축을 신설한다.

## 소스 사실(라이브 검증 2026-07-09)

- **엔드포인트**: `http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json`
- **인증**: 쿼리파라미터 `key=<KOBIS_SERVICE_KEY>`(32자 alnum). `QueryKey("key", ...)`로
  params 병합 → URL 문자열에 키 미노출(#144 기조 유지).
- **파라미터**: `targetDt`(YYYYMMDD, 필수) · `wideAreaCd`(선택, 상영지역 코드). **서울 = `0105001`**.
- **응답**: UTF-8 JSON. `boxOfficeResult.dailyBoxOfficeList[]` = **일별 top10 고정**(정확히 10건).
  각 item 18필드: `rnum, rank, rankInten, rankOldAndNew, movieCd, movieNm, openDt, salesAmt,
  salesShare, salesInten, salesChange, salesAcc, audiCnt, audiInten, audiChange, audiAcc,
  scrnCnt, showCnt`.
- **지역 필터 실효 확인**: 같은 targetDt(20260707)에서 전국 1위(audiCnt 44,464)와 서울 1위
  (10,930)가 **서로 다른 영화** — 서울 한정 랭킹이 실제로 다르게 반환됨(프록시 아님).
- **에러 형태**: 잘못된 요청은 `{"faultInfo": {...}}` 반환(정상은 `boxOfficeResult`만).
- **오버슛 없음**: 단일 GET, 페이징 없음. targetDt로 과거 임의 일자 조회 = **완전 백필 가능**(비소멸).

## 아키텍처 — "소스 추가" 패턴 (seoul→kcisa에 이은 3번째)

KOBIS는 **JSON**(KOPIS XML과 다름) + **단일 GET 스냅샷**(kopis_boxoffice 계열). 기존 seam에
`kobis` 소스를 최소 침습으로 추가한다.

### 신규 데이터셋 2벌

| name | wideAreaCd | 제목 |
|---|---|---|
| `kobis_boxoffice_nation` | (없음) | KOBIS 일별 박스오피스 — 전국 |
| `kobis_boxoffice_seoul` | `0105001` | KOBIS 일별 박스오피스 — 서울 |

공통 필드: `source="kobis"`, `kind="kobis_boxoffice"`, `endpoint="searchDailyBoxOfficeList"`,
`load_pattern="snapshot_append"`, `row_tag="item"`(row_tag는 JSON이라 파싱에 미사용, 관례상 명시),
`key_fields=("movieCd", "movieNm")`, `min_rows=5`, `volume_drop_threshold=0.5`.

- **kopis_boxoffice와 이름·의미 충돌 없음**: KOPIS=공연 예매상황판 / KOBIS=영화 관객수.
  서로 다른 도메인 축이라 dedup·중복 우려 없음. bronze 테이블도 별개(`bronze_kobis_boxoffice_*`).
- **min_rows=5**: 실제 top10 고정이나 공휴일·데이터 지연 여지로 하한은 5(0건/절단 감지, 정상은 여유 통과).
- **volume 0.5**: 고정 10이라 kopis_boxoffice(고정 50, 0.5)와 동일 기조.

### targetDt = load_date − 1일

DAG는 03:00 KST(#201) 실행, `load_date`=당일(KST). KOBIS는 **전일 확정** 박스오피스를 제공하므로
`targetDt = date(load_date) − 1일`. ingest의 `kobis_boxoffice` 분기가 `landing.ctx.load_date`에서
계산한다(KCISA의 base_params처럼 정적이지 않고 실행일 파생). 백필·수동런은 명시 date로 재현.

### KobisClient (신규, clients.py)

```python
KOBIS_BASE = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice"

class KobisError(RuntimeError): ...

class KobisClient:
    def __init__(self, service_key, timeout=30, core=None):
        self.service_key = service_key
        self.core = core or HttpCore(source="kobis", timeout=timeout)

    def daily_boxoffice(self, target_dt, wide_area_cd=None) -> Page:
        """일별 박스오피스 단일 GET. wide_area_cd=None 이면 전국."""
        params = {"targetDt": target_dt}
        if wide_area_cd:
            params["wideAreaCd"] = wide_area_cd
        auth = QueryKey("key", self.service_key)
        resp = self.core.get(f"{KOBIS_BASE}/searchDailyBoxOfficeList.json",
                             params=params, auth=auth)
        body = resp.content
        data = json.loads(body.decode("utf-8", "ignore"))
        if "faultInfo" in data:
            raise KobisError(redact(f"KOBIS fault for {target_dt}: {data['faultInfo']}"))
        rows = data.get("boxOfficeResult", {}).get("dailyBoxOfficeList", [])
        return Page(index=1, body=body, row_count=len(rows), ext="json")
```

- 재시도는 core(429/5xx/연결)에 위임. KOBIS는 자정 400 도메인 이슈가 없어 KOPIS식 400 재시도 불필요.
- faultInfo 메시지는 redact 통과(키 유출 방지).

### parse_records — kobis 분기 (records.py)

```python
if source == "kobis":
    payload = json.loads(body.decode("utf-8", "ignore"))
    lst = payload.get("boxOfficeResult", {}).get("dailyBoxOfficeList", [])
    return [r for r in lst if isinstance(r, dict)]
```

KCISA는 XML `<item>`이라 kopis 분기 재사용이었지만, KOBIS는 JSON이고 배열 경로가
서울(`.row`)과 달라(`boxOfficeResult.dailyBoxOfficeList`) **전용 분기**가 맞다.

### 배선

- **config.py**: `KOBIS_KEY_ENV = "KOBIS_SERVICE_KEY"`, `SourceKeys.kobis` 필드, `source_keys`가
  읽고, `missing_keys`가 검사(필수화).
- **ingest.py**: `Clients.kobis` 필드 · `build_clients`에서 `register_secret(keys.kobis)` +
  `KobisClient(keys.kobis)` 생성 · `kobis_boxoffice` 분기(전국·서울 각각 단일 GET, page-0001.json 1장).
- **datasets.py**: `KOBIS_DATASETS = [...]`(2벌) · `ALL_DATASETS = KOPIS + SEOUL + KOBIS`.

### 데이터 흐름

```
DAG plan (03:00 KST, load_date=D)
  → ingest_one("kobis_boxoffice_nation")  targetDt=D-1, wideAreaCd 없음
  → ingest_one("kobis_boxoffice_seoul")   targetDt=D-1, wideAreaCd=0105001
     각각 KobisClient.daily_boxoffice → page-0001.json(top10) R2 raw 박제 + _manifest
  → load_bronze  parse_records("kobis", ...) → bronze_kobis_boxoffice_{nation,seoul}
```

## 계약·검증

- **min_rows=5 / volume 0.5**: 기존 evaluate_landing 계약이 그대로 적용(신규 코드 없음).
- **drift(key_fields)**: `movieCd`·`movieNm`이 관측 스키마에 있는지.
- **freshness**: 기본 30h(일배치).

## 테스트 (TDD)

- `test_kobis_client.py`: daily_boxoffice parse(전국·서울) · faultInfo→KobisError+키 마스킹 ·
  wideAreaCd 유무에 따른 params(스텁 Transport 경계, session mock 금지 #152).
- `test_kobis_dataset.py`: config 키(source_keys/missing_keys) · dataset 2벌 등록 ·
  ingest `kobis_boxoffice` 분기 디스패치(`res.error == ""` 그물, #196 params 버그 교훈) ·
  targetDt=load_date−1 계산 검증.
- `test_redaction_surfaces.py`: FAKE_KOBIS 상수 + build_clients에 `KOBIS_SERVICE_KEY` setenv +
  마스킹 단언 + fixture teardown.
- `test_ingest_timing.py`: `Clients(kopis=, seoul=None, kobis=None)` 위치인자 보정.
- parse_records kobis 분기 단위 테스트.

## 라이브 검증(dev)

전국·서울 각 트리거 → `iceberg_dev.culture.bronze_kobis_boxoffice_nation` /
`_seoul` 각 10행, movieCd/audiCnt 내장, 서울≠전국 랭킹 확인, 실키 로그 미노출.

## 후속(범위 밖)

silver `silver_culture_boxoffice`(일×순위, region 컬럼 = bronze 테이블 유래) · gold 서울/전국
관객 비중 파생 · schema.yml grain unique 계약.

## 브랜치·머지

dev에서 독립 분기(사용자 결정). #225(KCISA)와 config/clients/ingest/records seam 공유 →
union 충돌 예상(#204/#210 선례처럼 union 해소). 머지 순서 자유.
