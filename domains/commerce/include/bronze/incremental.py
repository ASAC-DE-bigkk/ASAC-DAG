"""bronze 증분 — UPDATEDT desc 외부 병합 정렬 · 정규화 해시 검증키 · 스트리밍 diff.

목적: 매 수집이 전체를 다시 저장하지 않도록, **정렬본 기준으로 전날과 다른 신규 row만** 저장.
전량 RAM 금지 → **외부 병합 정렬**(청크를 임시파일로 쓰고 heapq 병합, 스트리밍). 비교정렬 하한
O(n log n). (UPDATEDT는 -1단계 확인상 39종 100% datetime → 14자리 정수키로 치환.)

계약:
- row = dict(파싱된 인허가 레코드). 정렬키 = (UPDATEDT 정수 내림차순, MGTNO) — 결정적 전순서.
- 검증키(verification key) = 정렬본 row 정규화(JSON, key정렬) 문자열을 순서대로 이어 sha256(순서 민감).
- diff = 오늘 정렬본 − 전날 정렬본. 둘 다 같은 키로 정렬 → **스트리밍 병합**으로 신규/변경 row만 방출.
  같은 정렬키 위치에서는 정규화 문자열 **직접 비교**(해시 불필요 — hot loop 경량).

silver/serving 파서 영향은 이 모듈 밖(별도 대응).
"""
from __future__ import annotations

import hashlib
import heapq
import json
import os
import re
import tempfile
from typing import Iterable, Iterator

_NON_DIGIT = re.compile(r"\D")


def updatedt_num(row: dict) -> int:
    """UPDATEDT(datetime 문자열) → YYYYMMDDHHMMSS 정수. 없으면 0(가장 오래된 것으로 취급)."""
    digits = _NON_DIGIT.sub("", (row.get("UPDATEDT") or "").strip())[:14]
    return int(digits) if digits else 0


def sort_key(row: dict) -> tuple[int, str]:
    """내림차순 정렬키: UPDATEDT 정수를 음수화(desc) + MGTNO(동률 tie-break, 결정적)."""
    return (-updatedt_num(row), row.get("MGTNO") or "")


def normalize(row: dict) -> str:
    """정규화 표현(내용 동일성 판정 기준). key 정렬 JSON — 필드 순서 무관하게 같으면 같은 문자열."""
    return json.dumps(row, ensure_ascii=False, sort_keys=True)


def row_hash(row: dict) -> str:
    return hashlib.sha256(normalize(row).encode("utf-8")).hexdigest()


# ── 외부 병합 정렬(스트리밍, 바운디드 RAM) ──────────────────────────────────────
def external_merge_sort(rows: Iterable[dict], *, tmp_dir: str,
                        chunk_rows: int = 100_000) -> Iterator[dict]:
    """rows 를 sort_key 오름차순(=UPDATEDT desc)으로 정렬해 스트리밍 반환.

    chunk_rows 단위로 메모리에서 정렬해 임시 JSONL 로 쓰고, heapq.merge 로 병합(스트리밍).
    RAM 사용은 한 청크 크기로 제한된다. 임시파일은 반드시 정리.
    """
    chunk_paths: list[str] = []
    buf: list[dict] = []

    def _flush() -> None:
        buf.sort(key=sort_key)
        fd, path = tempfile.mkstemp(dir=tmp_dir, suffix=".sortrun.jsonl")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            for r in buf:
                f.write(json.dumps(r, ensure_ascii=False))
                f.write("\n")
        chunk_paths.append(path)
        buf.clear()

    for r in rows:
        buf.append(r)
        if len(buf) >= chunk_rows:
            _flush()
    if buf:
        _flush()

    files = [open(p, encoding="utf-8") for p in chunk_paths]
    try:
        def _read(f):
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)
        for r in heapq.merge(*(_read(f) for f in files), key=sort_key):
            yield r
    finally:
        for f in files:
            f.close()
        for p in chunk_paths:
            try:
                os.remove(p)
            except OSError:
                pass


# ── 검증키(파일 전체 해시, 순서 민감) ────────────────────────────────────────────
def verification_key(sorted_rows: Iterable[dict]) -> tuple[str, int]:
    """정렬본 전체의 검증키(hex) + row 수. 같은 정렬본이면 같은 키(내용 동일성 판정)."""
    h = hashlib.sha256()
    count = 0
    for r in sorted_rows:
        h.update(normalize(r).encode("utf-8"))
        h.update(b"\n")
        count += 1
    return h.hexdigest(), count


# ── 스트리밍 diff (오늘 − 전날) ─────────────────────────────────────────────────
class _Peek:
    """1칸 미리보기 가능한 이터레이터 래퍼."""
    _SENTINEL = object()

    def __init__(self, it: Iterator[dict]):
        self._it = iter(it)
        self._peek = next(self._it, self._SENTINEL)

    @property
    def has(self) -> bool:
        return self._peek is not self._SENTINEL

    @property
    def peek(self) -> dict:
        return self._peek  # type: ignore[return-value]

    def next(self) -> dict:
        cur = self._peek
        self._peek = next(self._it, self._SENTINEL)
        return cur  # type: ignore[return-value]


def diff_new_rows(today_sorted: Iterable[dict], prev_sorted: Iterable[dict]) -> Iterator[dict]:
    """오늘 정렬본에서 **전날 정렬본에 없던 신규/변경 row만** 방출(정렬 병합, 스트리밍).

    둘 다 (UPDATEDT desc, MGTNO) 정렬이라:
      - today 키 < prev 키(더 최신) → 오늘에만 있는 신규 → 방출
      - 키 동일 → 정규화 문자열 직접 비교: 같으면 미변경(건너뜀), 다르면 변경분 → 방출
      - today 키 > prev 키 → 전날에만 있던 행(삭제/이동) → 건너뜀
    전날본이 소진되면 남은 오늘 행은 모두 신규.
    """
    t = _Peek(iter(today_sorted))
    p = _Peek(iter(prev_sorted))
    while t.has:
        if not p.has:
            yield t.next()
            continue
        tk, pk = sort_key(t.peek), sort_key(p.peek)
        if tk < pk:
            yield t.next()
        elif tk > pk:
            p.next()
        else:  # 같은 정렬키 위치
            if normalize(t.peek) == normalize(p.peek):
                t.next(); p.next()          # 미변경
            else:
                yield t.next(); p.next()    # 같은 키·다른 내용 → 변경분
