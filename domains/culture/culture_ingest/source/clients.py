"""두 culture 데이터 소스용 HTTP 클라이언트.

두 클라이언트 모두 원본 bytes를 *받아오기만* 한다 -- 업무 필드는 파싱하지 않는다.
파싱은 후속 bronze->silver dbt 레이어의 몫이다. 여기서 하는 응답 들여다보기는
페이징을 돌리고 매니페스트에 행 수를 기록하는 데 필요한 최소한이 전부다.
"""

from __future__ import annotations

import json
import logging
import random
import re
import time

import requests

from culture_ingest.common.http import Page, build_session
from culture_ingest.common.security import redact, scrub_exception

log = logging.getLogger(__name__)

KOPIS_BASE = "http://www.kopis.or.kr/openApi/restful"
SEOUL_BASE = "http://openapi.seoul.go.kr:8088"

# KOPIS 목록 페이지는 XML <dbs><db>...</db></dbs> 형태 -- 페이지당 <db> 개수를 센다.
_KOPIS_DB_RE = re.compile(r"<db>")
# 서울 API는 한 번 요청 윈도우를 최대 1000행으로 제한한다.
SEOUL_WINDOW = 1000


class KopisError(RuntimeError):
    """KOPIS 응답이 에러를 담고 있을 때 발생."""


class SeoulError(RuntimeError):
    """서울 열린데이터 응답 코드가 정상이 아닐 때 발생."""


class KopisClient:
    """KOPIS 공연예술통합전산망 open API (XML)."""

    def __init__(self, service_key: str, timeout: int = 30, retry_delay_sec: float = 2.0):
        self.service_key = service_key
        self.timeout = timeout
        self.retry_delay_sec = retry_delay_sec  # 400/429 1회 재시도 전 대기(+jitter)
        self.session = build_session()

    def _get(self, path: str, params: dict) -> bytes:
        """단일 GET. 400/429 는 **1회 백오프 재시도**로 '일시(자정 rate-limit)'와
        '지속(진짜 범위 밖/오류)'을 구분한다(#146) — 자정 rate-limit 400 이 #84 오버슛
        처리에 '목록 끝'으로 오인돼 목록이 1페이지에서 절단된 실증(7/4·7/5) 대응.
        지속 400 은 그대로 전파되어 기존 의미(오버슛=끝, 1페이지=오류)를 유지한다.
        """
        params = {"service": self.service_key, **params}
        for attempt in (1, 2):
            resp = self.session.get(f"{KOPIS_BASE}/{path}", params=params, timeout=self.timeout)
            try:
                resp.raise_for_status()
            except requests.HTTPError as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if attempt == 1 and status in (400, 429):
                    if self.retry_delay_sec:
                        time.sleep(self.retry_delay_sec + random.uniform(0, 0.5))
                    continue
                # HTTPError 메시지엔 `service=<키>` 가 박힌 URL 이 들어간다(#144) —
                # 예외가 어디로 전파되든 키가 남지 않게 args 를 여기서 마스킹.
                raise scrub_exception(exc)
            body = resp.content
            text = body[:600].decode("utf-8", "ignore")
            if "<errmsg>" in text or "<returncode>" in text:
                raise KopisError(redact(f"KOPIS error for {path}: {text}"))
            return body
        raise AssertionError("unreachable")  # pragma: no cover

    @staticmethod
    def _count(body: bytes) -> int:
        # 페이지 안의 <db> 개수 = 행 수.
        return len(_KOPIS_DB_RE.findall(body.decode("utf-8", "ignore")))

    def list_pages(self, path: str, base_params: dict, rows: int, max_pages: int | None):
        """KOPIS 목록 엔드포인트를 페이징하며 :class:`Page`를 하나씩 내보낸다.

        한 페이지가 ``rows``보다 적게 오면(마지막 페이지) 또는 ``max_pages``에
        도달하면 멈춘다. 총 행수가 ``rows``의 정확한 배수면 마지막 페이지가 꽉 차
        다음 페이지를 조회하게 되는데, KOPIS는 범위 밖 페이지에 HTTP 400을 준다 —
        이 오버슛 400은 '목록 끝'으로 처리한다(#84). 1페이지의 400은 진짜 오류.
        """
        page = 1
        while True:
            if max_pages is not None and page > max_pages:
                return
            params = {**base_params, "cpage": page, "rows": rows}
            try:
                body = self._get(path, params)
            except requests.HTTPError as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if page > 1 and status == 400:
                    log.info("[kopis] %s cpage=%d 오버슛 400 — 목록 끝으로 종료", path, page)
                    return
                raise
            count = self._count(body)
            if count == 0:
                return
            yield Page(index=page, body=body, row_count=count, ext="xml")
            if count < rows:
                return
            page += 1

    def detail(self, path: str, identifier: str) -> Page:
        body = self._get(f"{path}/{identifier}", {})
        return Page(index=1, body=body, row_count=self._count(body), ext="xml")

    def fetch_once(self, path: str, params: dict, row_tag: str) -> Page:
        """단일 GET(페이징 없음). 예매상황판(boxoffice) 전용 -- 기간 랭킹을
        <boxof> 아래로 한 번에 주고 cpage/rows를 무시한다.
        """
        body = self._get(path, params)
        count = len(re.findall(rf"<{row_tag}>", body.decode("utf-8", "ignore")))
        return Page(index=1, body=body, row_count=count, ext="xml")

    def list_ids(self, path: str, base_params: dict, id_field: str, limit: int) -> list[str]:
        """목록 엔드포인트에서 최대 ``limit``개의 id를 수집한다(상세 크롤용)."""
        id_re = re.compile(rf"<{id_field}>(.*?)</{id_field}>")
        ids: list[str] = []
        for page in self.list_pages(path, base_params, rows=100, max_pages=None):
            ids.extend(id_re.findall(page.body.decode("utf-8", "ignore")))
            if len(ids) >= limit:
                break
        return ids[:limit]


class SeoulClient:
    """서울 열린데이터광장 open API (JSON)."""

    def __init__(self, api_key: str, timeout: int = 30):
        self.api_key = api_key
        self.timeout = timeout
        self.session = build_session()

    def _get_window(self, service: str, start: int, end: int) -> tuple[bytes, dict]:
        url = f"{SEOUL_BASE}/{self.api_key}/json/{service}/{start}/{end}/"
        resp = self.session.get(url, timeout=self.timeout)
        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            # 서울 키는 URL 경로에 박힌다 — HTTPError 메시지 전파 전 마스킹(#144).
            raise scrub_exception(exc)
        body = resp.content
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
