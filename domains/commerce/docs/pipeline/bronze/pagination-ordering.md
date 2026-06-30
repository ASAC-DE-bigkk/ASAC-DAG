# bronze — 서울 OpenAPI 페이지네이션 정렬 분석

> 분석일 2026-06-30 · 대상: LOCALDATA 39종 중 **service_name 해석된 12종** · 사용 키:
> `sample`(환경에 실키 부재) · base `http://openapi.seoul.go.kr:8088`
> 관련: [../common_info.md](../common_info.md) §4-1(페이지네이션·완전성) · [../../CLAUDE.md](../../../CLAUDE.md) §2(bronze 원본 보존)

## 결론 (TL;DR)

서울 LOCALDATA OpenAPI 의 `START_INDEX`/`END_INDEX` 페이징은 **위치 기반(positional)이고
동일 스냅샷 안에서 안정적**이다. 그러나 **정렬 기준이 되는 컬럼이 없다.**

- 응답 행에는 `RNUM`/순번/시퀀스 같은 **명시적 정렬 필드가 없다**.
- 반환 순서는 `MGTNO`·`OPNSFTEAMCODE`·주소(`RDNWHLADDR`)·인허가일(`APVPERMYMD`)·
  수정시점(`LASTMODTS`)·갱신일(`UPDATEDT`) **어느 값으로도 정렬돼 있지 않다.**
- 즉 정렬은 **API/DB 의 내부 기본 순서**(사실상 입력·물리 순서)이며, 사용자가 `ORDER BY`
  를 지정할 수 없다. 페이징은 그 기본 순서를 위치로 잘라 가져오는 방식이다.

→ 정렬의 "기준 값"은 **없음**. 페이징은 "정렬된 컬럼"이 아니라 **위치(offset)** 로 동작한다.

## 검증 방법 (재현)

`sample` 키는 **한 번에 최대 5건**만 허용한다(`END>5` 또는 `START>5` 시 `ERROR-335`).
그래서 (a) 1~5행 범위에서 윈도우를 옮겨 가며 **위치 안정성**을, (b) 12종 전체에서 5행의
**컬럼 단조성(정렬 여부)** 을 확인했다.

```bash
# 1) 단일 호출 — 5건
curl -s "http://openapi.seoul.go.kr:8088/sample/json/LOCALDATA_072404/1/5/"

# 2) 위치 안정성 — (2,5) 가 (1,5) 의 2~5행과 같은지
curl -s "http://openapi.seoul.go.kr:8088/sample/json/LOCALDATA_072404/2/5/"

# 3) 5건 초과 — ERROR-335
curl -s "http://openapi.seoul.go.kr:8088/sample/json/LOCALDATA_072404/1/10/"
```

## 증거 1 — 페이징은 위치 기반이고 안정적

같은 데이터셋에서 윈도우만 옮기면 **같은 행이 같은 위치**에 온다(예: `general_restaurant`).

| 요청 | 반환 행(MGTNO 앞부분) |
|---|---|
| `1/5` | `…07985`, `…01852`, `…00202`, `…02104`, `…09890` |
| `2/5` | `…01852`, `…00202`, `…02104`, `…09890` |
| `3/5` | `…00202`, `…02104`, `…09890` |
| `5/5` | `…09890` |

`(2,5) == (1,5)[1:]`, `(5,5) == [5번째 행]` 이 **4개 데이터셋(072404·072218·072210·072202)
모두에서 True** 였다 → 행의 위치는 호출마다 흔들리지 않는다(스냅샷 기준).

## 증거 2 — 어떤 컬럼으로도 정렬돼 있지 않다

`general_restaurant`(LOCALDATA_072404) 1~5행의 후보 정렬 컬럼:

| 행 | MGTNO | OPNSFTEAMCODE | APVPERMYMD | LASTMODTS | UPDATEDT |
|---|---|---|---|---|---|
| 1 | 3020000-101-2001-07985 | 3020000 | 2001-05-23 | 2007-02-02 | 2026-01-14 |
| 2 | 3160000-101-1996-01852 | 3160000 | 1996-05-13 | 2002-01-18 | 2026-05-04 |
| 3 | 3000000-101-2007-00202 | 3000000 | 2007-08-27 | 2022-10-04 | 2026-01-14 |
| 4 | 3030000-101-1995-02104 | 3030000 | 1995-04-14 | 2025-07-31 | 2025-12-15 |
| 5 | 3060000-101-2001-09890 | 3060000 | 2001-04-17 | 2001-05-28 | 2026-05-10 |

→ 오름/내림 어느 쪽으로도 단조가 아니다. `MGTNO`/`OPNSFTEAMCODE` 의 자치단체코드 접두가
뒤섞여 있어 **관리번호 정렬도 아니다**(다른 11종도 동일 — 예: `bakery` 는 `…00017, …00018,
3200000-…00016, …00019` 로 접두가 끼어든다).

### 12종 단조성 집계 (작은 표본 주의)

5행이라는 작은 표본에선 저카디널리티 컬럼이 **우연히** 정렬돼 보인다. 집계 결과 어떤 컬럼도
12종 전부에서 일관되지 않았다:

| 컬럼 | 양상 | 빈도 | 비고 |
|---|---|---|---|
| `TRDSTATEGBN` | asc | 8/12 | 영업상태 **2값 코드** — 5행 우연 정렬(노이즈) |
| `APVPERMYMD` | desc | 6/12 | 표본이 최근 건에 치우쳐 보이는 착시 |
| `RDNWHLADDR`/`RDNPOSTNO`/`DCBYMD`/… | 혼재 | ≤3/12 | 일관성 없음 |

일관된 정렬 컬럼이 **존재하지 않음** = 정렬 기준 값 없음(내부 기본 순서).

## 의미 — bronze 수집에 미치는 영향

안정적인 **정렬 키가 없다**는 점은 전수 순회(`SEOUL_MAX_PAGES` 미설정=무제한)에 다음 함의를 준다:

- 장시간 다중 페이지 순회 도중 원본 테이블에 **삽입/삭제가 일어나면 행 위치가 밀려서**
  페이지 경계에서 **누락·중복**이 생길 수 있다(정렬 키가 있으면 키 기반 커서로 방지 가능하나
  이 API 는 불가).
- 현재 파이프라인 완화책: 순회 후 **완전성 점검**(`rows_total == list_total_count`)으로
  부분 수집을 잡아 `partial` 로 남기고 **다음 실행에서 재수집**한다([../common_info.md](../common_info.md) §4-1).
  다만 이는 **건수** 보장이지 행 단위 일치 보장은 아니다.
- 권장: **bronze 는 원본 페이지를 그대로 보존**(정렬과 무관, CLAUDE.md §2.2)하고,
  **다운스트림(silver)에서 `MGTNO` 로 중복 제거**한다. `MGTNO` 가 인허가 단위 식별자이므로
  페이지 경계 중복/순서 흔들림을 흡수할 수 있다.

## 한계 & 실키 재검증 레시피

- `sample` 키는 **5건 제한**이라 **실제 페이지 경계(예: 1000↔1001)** 의 정렬 연속성은 확인하지
  못했다. 또한 sample 5행이 큐레이션 표본일 가능성이 있어, "전체 결과셋의 1~5위"라고 단정하긴
  어렵다. 확정된 사실은 **(1) 위치 기반·안정 페이징, (2) 표본 행이 어떤 컬럼으로도 정렬돼
  있지 않음** 두 가지다.
- **실키(SEOUL_OPENAPI_KEY) 확보 시** 아래로 페이지 경계까지 재검증할 것:

```bash
# .env.commerce 에 SEOUL_OPENAPI_KEY 설정 후 (configuration.md 참고)
PYTHONPATH=dags/domains/commerce/include python - <<'PY'
import json, urllib.request, os
from common.env import load_commerce_env; load_commerce_env()
KEY=os.environ["SEOUL_OPENAPI_KEY"]; BASE="http://openapi.seoul.go.kr:8088"; SVC="LOCALDATA_072404"
def page(s,e):
    raw=urllib.request.urlopen(f"{BASE}/{KEY}/json/{SVC}/{s}/{e}/",timeout=60).read()
    b=json.loads(raw); b=b[list(b)[0]]; return [r["MGTNO"] for r in b.get("row",[])]
a=page(1,1000); b=page(1001,2000)
print("dup across page boundary:", len(set(a)&set(b)))          # 0 이어야 정상
print("same window stable:", page(1,1000)==a)                    # 두 번 호출 동일?
# 정렬 컬럼 추정: 전체 1페이지에서 단조 컬럼 탐색
PY
```

— 페이지 간 `MGTNO` 교집합이 0이고 동일 윈도우 재호출이 동일하면 "위치 기반·스냅샷 안정",
교집합이 생기면 "수집 중 변동(누락/중복) 발생" 신호다.
