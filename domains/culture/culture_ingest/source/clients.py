"""HTTP clients for the two culture data sources.

Both clients only *fetch* raw bytes -- they do not parse business fields. Parsing
belongs to the downstream bronze->silver dbt layer. The only inspection done here
is the minimum needed to drive pagination and to record row counts in the manifest.
"""

from __future__ import annotations

import json
import re

from culture_ingest.common.http import Page, build_session

KOPIS_BASE = "http://www.kopis.or.kr/openApi/restful"
SEOUL_BASE = "http://openapi.seoul.go.kr:8088"

# KOPIS list pages are XML <dbs><db>...</db></dbs>; count <db> entries per page.
_KOPIS_DB_RE = re.compile(r"<db>")
# Seoul caps a single request window at 1000 rows.
SEOUL_WINDOW = 1000


class KopisError(RuntimeError):
    pass


class SeoulError(RuntimeError):
    pass


class KopisClient:
    """KOPIS open API (XML)."""

    def __init__(self, service_key: str, timeout: int = 30):
        self.service_key = service_key
        self.timeout = timeout
        self.session = build_session()

    def _get(self, path: str, params: dict) -> bytes:
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
        return len(_KOPIS_DB_RE.findall(body.decode("utf-8", "ignore")))

    def list_pages(self, path: str, base_params: dict, rows: int, max_pages: int | None):
        """Yield :class:`Page` objects paging a KOPIS list endpoint.

        Stops when a page returns fewer than ``rows`` items (last page) or when
        ``max_pages`` is reached.
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
        """Single GET (no pagination). Used for boxoffice, which returns a fixed
        period ranking under <boxof> and ignores cpage/rows.
        """
        body = self._get(path, params)
        count = len(re.findall(rf"<{row_tag}>", body.decode("utf-8", "ignore")))
        return Page(index=1, body=body, row_count=count, ext="xml")

    def list_ids(self, path: str, base_params: dict, id_field: str, limit: int) -> list[str]:
        """Collect up to ``limit`` ids from a list endpoint (for detail crawls)."""
        id_re = re.compile(rf"<{id_field}>(.*?)</{id_field}>")
        ids: list[str] = []
        for page in self.list_pages(path, base_params, rows=100, max_pages=None):
            ids.extend(id_re.findall(page.body.decode("utf-8", "ignore")))
            if len(ids) >= limit:
                break
        return ids[:limit]


class SeoulClient:
    """Seoul Open Data Plaza open API (JSON)."""

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
        # INFO-000 = ok, INFO-200 = no rows (acceptable terminal state).
        if code not in ("INFO-000", "INFO-200"):
            raise SeoulError(f"Seoul error for {service}: {result}")
        return body, payload.get(service, {})

    def list_pages(self, service: str, max_rows: int | None):
        """Yield :class:`Page` windows of a Seoul service until exhausted."""
        # First window also tells us the total count.
        body, container = self._get_window(service, 1, SEOUL_WINDOW)
        total = int(container.get("list_total_count", 0))
        rows = container.get("row", []) or []
        if not rows:
            return
        yield Page(index=1, body=body, row_count=len(rows), ext="json")

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
