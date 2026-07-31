"""두 culture 데이터 소스용 HTTP 클라이언트.

두 클라이언트 모두 원본 bytes를 *받아오기만* 한다 -- 업무 필드는 파싱하지 않는다.
파싱은 후속 bronze->silver dbt 레이어의 몫이다. 여기서 하는 응답 들여다보기는
페이징을 돌리고 매니페스트에 행 수를 기록하는 데 필요한 최소한이 전부다.

전송 계층은 공용 `common.http`(#78)를 합성으로 소비한다(#152):
- KOPIS  : HttpCore + QueryKey("service", key) — 키가 URL 문자열에 아예 안 들어가
  로그/HttpProblemError 표면에 키 노출 표면 자체가 없다(#144 강화).
- 서울   : SeoulOpenApiClient(PathKey) — URL 규약·키 치환은 공용 소관.
재시도 분담: 429/5xx = core(backoff+jitter+Retry-After) / **400 1회 재시도만 도메인**
(자정 rate-limit 400 은 KOPIS 도메인 지식(#146) — core 는 400 을 정당하게 비재시도).
페이징·probe-beyond-end(#147)·오버슛=목록 끝(#84)·행 카운트는 도메인 소관 유지.
"""

from __future__ import annotations

import json
import logging
import random
import re
import time

# security 가 dags 루트를 sys.path 에 보장하는 진입점(#144) — 루트 기준 패키지인
# common.* 보다 반드시 먼저 import 해야 단독 스크립트/host pytest 문맥이 안 깨진다.
from culture_ingest.common.security import redact

from common.http.auth import QueryKey  # noqa: E402
from common.http.core import HttpCore  # noqa: E402
from common.http.errors import HttpProblemError  # noqa: E402
from common.http.seoul import SOURCE as SEOUL_SOURCE, SeoulOpenApiClient  # noqa: E402
from culture_ingest.common.http import Page  # noqa: E402

log = logging.getLogger(__name__)

KOPIS_BASE = "http://www.kopis.or.kr/openApi/restful"
KOBIS_BASE = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice"

# 서울 API는 한 번 요청 윈도우를 최대 1000행으로 제한한다.
SEOUL_WINDOW = 1000


def _count_tag(body: bytes, tag: str) -> int:
    """XML body 안의 여는 태그 <tag> 개수 = 행 수(KOPIS <db>·KCISA <item>·예매 <boxof> 공통)."""
    return body.decode("utf-8", "ignore").count(f"<{tag}>")


def extract_ids(body: bytes, id_field: str) -> list[str]:
    """XML body 에서 ``<id_field>`` 값 목록을 뽑는다 — 상세 크롤용 id 수집(#363).

    ``KopisClient.list_ids``(API 목록)와 ingest 의 ``_ids_from_landed_list``(랜딩
    raw 재사용, #146)가 같은 추출 정의를 공유한다 — 두 경로의 id 집합이 어긋나면
    detail 폴백이 다른 대상을 크롤하게 되므로 정의는 한 곳에만 둔다.
    """
    return re.findall(rf"<{id_field}>(.*?)</{id_field}>", body.decode("utf-8", "ignore"))


def _paginate(fetch_page, count_body, rows, max_pages):
    """목록 페이징 공용 골격. page=1부터 fetch_page(page)로 body를 받아 Page를 내보내고,
    빈 페이지(count 0)·마지막 페이지(count<rows)·max_pages 도달·fetch_page가 None을
    반환(소스가 '끝'으로 신호한 오버슛)하면 멈춘다. 소스별 파라미터명과 오버슛 처리
    (KOPIS 자정 400=끝 vs KCISA 빈 item=끝)는 fetch_page 클로저가 흡수한다.
    """
    page = 1
    while True:
        if max_pages is not None and page > max_pages:
            return
        body = fetch_page(page)
        if body is None:
            return
        count = count_body(body)
        if count == 0:
            return
        yield Page(index=page, body=body, row_count=count, ext="xml")
        if count < rows:
            return
        page += 1


class KopisError(RuntimeError):
    """KOPIS 응답이 에러를 담고 있을 때 발생."""


class SeoulError(RuntimeError):
    """서울 열린데이터 응답 코드가 정상이 아닐 때 발생."""


class KobisError(RuntimeError):
    """KOBIS 응답이 faultInfo(에러)를 담고 있을 때 발생."""


class KopisClient:
    """KOPIS 공연예술통합전산망 open API (XML)."""

    def __init__(self, service_key: str, timeout: int = 30, retry_delay_sec: float = 2.0,
                 core: HttpCore | None = None):
        self.service_key = service_key
        self.retry_delay_sec = retry_delay_sec  # 400 1회 재시도 전 대기(+jitter)
        self.core = core or HttpCore(source="kopis", timeout=timeout)

    def _get(self, path: str, params: dict) -> bytes:
        """단일 GET. 400 은 **1회 백오프 재시도**로 '일시(자정 rate-limit)'와
        '지속(진짜 범위 밖/오류)'을 구분한다(#146) — 자정 rate-limit 400 이 #84 오버슛
        처리에 '목록 끝'으로 오인돼 목록이 1페이지에서 절단된 실증(7/4·7/5) 대응.
        지속 400 은 그대로 전파되어 기존 의미(오버슛=끝, 1페이지=오류)를 유지한다.
        429/5xx·연결 오류는 core 가 backoff 재시도 후 HttpProblemError 로 던진다(#152).
        """
        auth = QueryKey("service", self.service_key)
        for attempt in (1, 2):
            try:
                resp = self.core.get(f"{KOPIS_BASE}/{path}", params=params, auth=auth)
            except HttpProblemError as exc:
                if attempt == 1 and exc.status == 400:
                    if self.retry_delay_sec:
                        time.sleep(self.retry_delay_sec + random.uniform(0, 0.5))
                    continue
                raise
            body = resp.content
            text = body[:600].decode("utf-8", "ignore")
            if "<errmsg>" in text or "<returncode>" in text:
                raise KopisError(redact(f"KOPIS error for {path}: {text}"))
            return body
        raise AssertionError("unreachable")  # pragma: no cover

    @staticmethod
    def _count(body: bytes) -> int:
        return _count_tag(body, "db")  # 페이지 안의 <db> 개수 = 행 수

    def _list_still_alive(self, path: str, base_params: dict, rows: int) -> bool:
        """'목록 끝'과 '서버가 지금 우리를 거절함'을 가르는 확인 요청(#201).

        **이미 성공했던 1페이지를 다시 물어본다.** 살아 있으면 방금 400 은 범위 문제
        (= 진짜 목록 끝)이고, 1페이지까지 같이 죽으면 서버 쪽 거절이라 목록 끝이 아니다.
        확인은 목록당 최대 1회이므로 비용은 런당 KOPIS 목록 수만큼이다.
        """
        try:
            self._get(path, {**base_params, "cpage": 1, "rows": rows})
            return True
        except (HttpProblemError, KopisError):
            return False

    def list_pages(self, path: str, base_params: dict, rows: int, max_pages: int | None):
        """KOPIS 목록 엔드포인트를 페이징하며 :class:`Page`를 하나씩 내보낸다.

        한 페이지가 ``rows``보다 적게 오면(마지막 페이지) 또는 ``max_pages``에
        도달하면 멈춘다. 총 행수가 ``rows``의 정확한 배수면 마지막 페이지가 꽉 차
        다음 페이지를 조회하게 되는데, KOPIS는 범위 밖 페이지에 HTTP 400을 준다 —
        이 오버슛 400은 '목록 끝'으로 처리한다(#84). 1페이지의 400은 진짜 오류.

        단 **끝으로 읽기 전에 확인 요청을 한 번 더 넣는다**(#201). 밤 시간대 동시
        호출에서 오는 400 은 페이지네이션 도중에도 오는데, 그걸 끝으로 읽으면 부분
        데이터가 완전한 결과로 반환된다 — 7/29 실측이 그 형태였다(1,261 행 중 500 행을
        정상 완결로 반환, 볼륨 계약 #147 하나가 잡았다). 즉시 400(rows=0)은 오히려
        안전하다. 위험한 건 조용한 절단 쪽이고, 그건 baseline 이 있는 데이터셋에서만
        걸린다.
        """
        def fetch_page(page: int) -> bytes | None:
            try:
                return self._get(path, {**base_params, "cpage": page, "rows": rows})
            except HttpProblemError as exc:
                if page > 1 and exc.status == 400:
                    if self._list_still_alive(path, base_params, rows):
                        log.info("[kopis] %s cpage=%d 오버슛 400 — 목록 끝으로 종료", path, page)
                        return None  # 오버슛 400 = 목록 끝(#84)
                    log.warning(
                        "[kopis] %s cpage=%d 400 — 확인 요청(cpage=1)도 400. 목록 끝이 아니라 "
                        "서버 거절로 판단해 전파한다(#201, 조용한 절단 방지)", path, page)
                raise
        yield from _paginate(fetch_page, self._count, rows, max_pages)

    def detail(self, path: str, identifier: str) -> Page:
        body = self._get(f"{path}/{identifier}", {})
        return Page(index=1, body=body, row_count=self._count(body), ext="xml")

    def fetch_once(self, path: str, params: dict, row_tag: str) -> Page:
        """단일 GET(페이징 없음). 예매상황판(boxoffice) 전용 -- 기간 랭킹을
        <boxof> 아래로 한 번에 주고 cpage/rows를 무시한다.
        """
        body = self._get(path, params)
        return Page(index=1, body=body, row_count=_count_tag(body, row_tag), ext="xml")

    def list_ids(self, path: str, base_params: dict, id_field: str, limit: int | None) -> list[str]:
        """목록 엔드포인트에서 최대 ``limit``개의 id를 수집한다(상세 크롤용).

        ``limit=None`` = 전체(#466 missing 모드 폴백 — cap 은 안티조인 이후 적용).
        """
        ids: list[str] = []
        for page in self.list_pages(path, base_params, rows=100, max_pages=None):
            ids.extend(extract_ids(page.body, id_field))
            if limit is not None and len(ids) >= limit:
                break
        return ids if limit is None else ids[:limit]


class SeoulClient:
    """서울 열린데이터광장 open API (JSON)."""

    def __init__(self, api_key: str, timeout: int = 30, core: HttpCore | None = None):
        self.api_key = api_key
        self._api = SeoulOpenApiClient(
            core or HttpCore(source=SEOUL_SOURCE, timeout=timeout), api_key)

    def _get_window(self, service: str, start: int, end: int) -> tuple[bytes, dict]:
        # 서울 키는 URL 경로에 박히지만(#144) HttpProblemError 는 생성 시 자체 redact.
        body = self._api.fetch_bytes(service, start, end).content
        payload = json.loads(body.decode("utf-8", "ignore"))
        if service in payload:
            result = payload[service].get("RESULT", {})
        else:
            result = payload.get("RESULT", {})
        code = result.get("CODE", "")
        # INFO-000 = 정상, INFO-200 = 데이터 없음(정상 종료로 간주).
        if code not in ("INFO-000", "INFO-200"):
            raise SeoulError(redact(f"Seoul error for {service}: {result}"))
        return body, payload.get(service, {})

    def list_pages(self, service: str, max_rows: int | None):
        """서울 서비스를 1000행 윈도우 단위로 소진할 때까지 :class:`Page`로 내보낸다.

        ``max_rows``가 주어지면 **첫 윈도우부터** 그 상한을 지킨다 — 샘플/드라이런이
        1000행을 통째로 받지 않게 한다. (``list_total_count``는 윈도우 크기와 무관하게
        전체 건수를 주므로, 첫 윈도우를 줄여도 남은 페이징 계산엔 영향이 없다.)

        ``list_total_count``는 **신뢰하지 않는다**(#147): 서버가 간헐적으로 축소된
        총량을 INFO-000(정상)으로 반환하는 truncation 이 실측됐다(7/1 event 3925/19377,
        -80% 조용한 누락). 주장된 총량까지 소진한 뒤 그 **너머 창을 1회 더 요청**해
        (probe-beyond-end) 행이 오면 거짓말로 판정, 빈 창이 나올 때까지 계속 페이징한다.
        정직한 총량일 때 비용은 데이터셋당 INFO-200 요청 1회다.
        """
        # 첫 윈도우도 max_rows를 존중(없으면 1000). 응답이 전체 건수도 알려준다.
        first_end = SEOUL_WINDOW if max_rows is None else min(SEOUL_WINDOW, max_rows)
        body, container = self._get_window(service, 1, first_end)
        total = int(container.get("list_total_count", 0))
        rows = container.get("row", []) or []
        if not rows:
            return
        yield Page(index=1, body=body, row_count=len(rows), ext="json")

        # 남은 행을 1000개씩 윈도우를 밀어가며 가져온다(max_rows 있으면 거기까지).
        target = total if max_rows is None else min(total, max_rows)
        start = first_end + 1
        while start <= target:
            end = min(start + SEOUL_WINDOW - 1, target)
            body, container = self._get_window(service, start, end)
            rows = container.get("row", []) or []
            if not rows:
                return
            yield Page(index=start, body=body, row_count=len(rows), ext="json")
            start = end + 1

        if max_rows is not None:
            return  # 캡이 걸린 실행은 의도된 절단 — probe 생략

        # probe-beyond-end: total 주장 너머를 빈 창(INFO-200)이 나올 때까지 소진.
        probed = 0
        while True:
            end = start + SEOUL_WINDOW - 1
            body, container = self._get_window(service, start, end)
            rows = container.get("row", []) or []
            if not rows:
                if probed:
                    log.warning(
                        "[seoul] %s: list_total_count=%d 축소 반환 — probe 로 %d행 추가 회수(#147)",
                        service, total, probed,
                    )
                return
            probed += len(rows)
            yield Page(index=start, body=body, row_count=len(rows), ext="json")
            start = end + 1


class KobisClient:
    """KOBIS 영화진흥위원회 오픈API — 일별 박스오피스 (JSON, 단일 GET)."""

    def __init__(self, service_key: str, timeout: int = 30, core: HttpCore | None = None):
        self.service_key = service_key
        self.core = core or HttpCore(source="kobis", timeout=timeout)

    def daily_boxoffice(self, target_dt: str, wide_area_cd: str | None = None) -> Page:
        """일별 박스오피스 단일 GET(page-0001.json).

        ``target_dt`` = YYYYMMDD(전일). ``wide_area_cd`` 가 있으면 상영지역 한정
        (서울 = "0105001"), 없으면 전국. 키는 ``QueryKey("key")`` 로 params 에
        병합돼 URL 문자열엔 안 들어간다(#144). 페이징 없음 — 응답은 top10 배열.
        429/5xx·연결 오류는 core 가 backoff 재시도 후 HttpProblemError 로 던진다(#152);
        KOBIS 는 KOPIS 식 자정 400 이슈가 없어 도메인 400 재시도는 두지 않는다.
        """
        params: dict = {"targetDt": target_dt}
        if wide_area_cd:
            params["wideAreaCd"] = wide_area_cd
        auth = QueryKey("key", self.service_key)
        resp = self.core.get(
            f"{KOBIS_BASE}/searchDailyBoxOfficeList.json", params=params, auth=auth)
        body = resp.content
        data = json.loads(body.decode("utf-8", "ignore"))
        if "faultInfo" in data:
            raise KobisError(redact(f"KOBIS fault for {target_dt}: {data['faultInfo']}"))
        rows = (data.get("boxOfficeResult") or {}).get("dailyBoxOfficeList") or []
        return Page(index=1, body=body, row_count=len(rows), ext="json")


KCISA_BASE = "https://apis.data.go.kr/B553457/cultureinfo"
KCISA_ROWS = 200  # area2 페이지 크기(단일 진실원 — ingest 디스패치·매니페스트 공유)


class KcisaError(RuntimeError):
    """KCISA 응답이 데이터가 아닌 에러 봉투(cmmMsgHeader/returnReasonCode)를 담을 때 발생."""


class KcisaClient:
    """KCISA 한눈에보는문화정보 open API (data.go.kr B553457, XML).

    KOPIS·서울과 다른 세 번째 소스. 인증키는 ``serviceKey`` 쿼리(QueryKey)라 URL
    문자열에서 사라져 노출 표면이 없다(#144). 오버슛이 400 인 KOPIS 와 달리 status
    200·빈 item 으로 오므로 '빈 페이지 = 끝'으로 페이징한다.
    """

    def __init__(self, service_key: str, timeout: int = 30, core: HttpCore | None = None):
        self.service_key = service_key
        self.core = core or HttpCore(source="kcisa", timeout=timeout)

    def _get(self, path: str, params: dict) -> bytes:
        auth = QueryKey("serviceKey", self.service_key)
        resp = self.core.get(f"{KCISA_BASE}/{path}", params=params, auth=auth)
        body = resp.content
        head = body[:400].decode("utf-8", "ignore")
        # data.go.kr 인증/한도 오류는 데이터가 아닌 에러 봉투로 온다(키는 응답에 없음).
        if "<returnReasonCode>" in head or "<cmmMsgHeader>" in head:
            raise KcisaError(redact(f"KCISA error for {path}: {head}"))
        return body

    @staticmethod
    def _count(body: bytes) -> int:
        return _count_tag(body, "item")

    def list_pages(self, path: str, base_params: dict, rows: int, max_pages: int | None):
        """area2 를 PageNo 증가로 페이징. 빈 페이지(item 0) 또는 rows 미만이면 종료."""
        def fetch_page(page: int) -> bytes:
            return self._get(path, {**base_params, "PageNo": page, "numOfrows": rows})
        yield from _paginate(fetch_page, self._count, rows, max_pages)
