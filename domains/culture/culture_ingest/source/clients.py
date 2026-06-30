"""두 culture 데이터 소스용 HTTP 클라이언트.

두 클라이언트 모두 원본 bytes를 *받아오기만* 한다 -- 업무 필드는 파싱하지 않는다.
파싱은 후속 bronze->silver dbt 레이어의 몫이다. 여기서 하는 응답 들여다보기는
페이징을 돌리고 매니페스트에 행 수를 기록하는 데 필요한 최소한이 전부다.
"""

from __future__ import annotations

import json
import re

from culture_ingest.common.http import Page, build_session

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

    def __init__(self, service_key: str, timeout: int = 30):
        self.service_key = service_key
        self.timeout = timeout
        self.session = build_session()

    def _get(self, path: str, params: dict) -> bytes:
        # 모든 요청에 인증키(service)를 붙이고, 응답 앞부분에 에러 태그가 있으면 예외.
        params = {"service": self.service_key, **params}
        resp = self.session.get(f"{KOPIS_BASE}/{path}", params=params, timeout=self.timeout)
        resp.raise_for_status()
        body = resp.content
        text = body[:600].decode("utf-8", "ignore")
        if "<errmsg>" in text or "<returncode>" in text:
            raise KopisError(f"KOPIS error for {path}: {text}")
        return body

    @staticmethod
    def _count(body: bytes) -> int:
        # 페이지 안의 <db> 개수 = 행 수.
        return len(_KOPIS_DB_RE.findall(body.decode("utf-8", "ignore")))

    def list_pages(self, path: str, base_params: dict, rows: int, max_pages: int | None):
        """KOPIS 목록 엔드포인트를 페이징하며 :class:`Page`를 하나씩 내보낸다.

        한 페이지가 ``rows``보다 적게 오면(마지막 페이지) 또는 ``max_pages``에
        도달하면 멈춘다.
        """
        page = 1
        while True:
            if max_pages is not None and page > max_pages:
                return
            params = {**base_params, "cpage": page, "rows": rows}
            body = self._get(path, params)
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
        resp.raise_for_status()
        body = resp.content
        payload = json.loads(body.decode("utf-8", "ignore"))
        if service in payload:
            result = payload[service].get("RESULT", {})
        else:
            result = payload.get("RESULT", {})
        code = result.get("CODE", "")
        # INFO-000 = 정상, INFO-200 = 데이터 없음(정상 종료로 간주).
        if code not in ("INFO-000", "INFO-200"):
            raise SeoulError(f"Seoul error for {service}: {result}")
        return body, payload.get(service, {})

    def list_pages(self, service: str, max_rows: int | None):
        """서울 서비스를 1000행 윈도우 단위로 소진할 때까지 :class:`Page`로 내보낸다."""
        # 첫 윈도우 응답이 전체 건수(list_total_count)도 알려준다.
        body, container = self._get_window(service, 1, SEOUL_WINDOW)
        total = int(container.get("list_total_count", 0))
        rows = container.get("row", []) or []
        if not rows:
            return
        yield Page(index=1, body=body, row_count=len(rows), ext="json")

        # 남은 행을 1000개씩 윈도우를 밀어가며 가져온다(max_rows 있으면 거기까지).
        target = total if max_rows is None else min(total, max_rows)
        start = SEOUL_WINDOW + 1
        while start <= target:
            end = min(start + SEOUL_WINDOW - 1, target)
            body, container = self._get_window(service, start, end)
            rows = container.get("row", []) or []
            if not rows:
                return
            yield Page(index=start, body=body, row_count=len(rows), ext="json")
            start = end + 1
