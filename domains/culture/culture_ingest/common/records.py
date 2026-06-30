"""원본 페이지(bytes)를 레코드(dict) 단위로 파싱.

bronze Iceberg 적재와 드리프트 점검이 공통으로 쓰는 "원본 → 레코드" 변환.
업무 타입 변환은 하지 않는다(그건 silver의 몫) — 태그/키를 그대로 dict로 풀 뿐.
파싱 실패는 빈 리스트로 흘려보낸다(원본은 이미 R2에 보존되어 있으므로 점검/적재가
파싱 때문에 깨지지 않게).
"""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET


def parse_records(source: str, body: bytes, row_tag: str, endpoint: str) -> list[dict]:
    """페이지 1장에서 레코드 dict 목록을 뽑는다.

    * KOPIS(XML): ``<row_tag>`` 요소마다 ``{자식태그: 텍스트}`` dict.
    * 서울(JSON): 서비스 컨테이너의 ``row`` 배열(이미 dict 목록).
    """
    try:
        if source == "kopis":
            root = ET.fromstring(body)
            out: list[dict] = []
            for elem in root.iter(row_tag):
                out.append({child.tag: (child.text or "").strip() for child in elem})
            return out
        # 서울 (JSON)
        payload = json.loads(body.decode("utf-8", "ignore"))
        container = payload.get(endpoint) if isinstance(payload, dict) else None
        if not isinstance(container, dict):
            # 서비스명 키를 못 찾으면 "row"를 품은 dict를 탐색
            for value in (payload.values() if isinstance(payload, dict) else []):
                if isinstance(value, dict) and "row" in value:
                    container = value
                    break
        rows = (container or {}).get("row") or []
        return [r for r in rows if isinstance(r, dict)]
    except Exception:
        return []
