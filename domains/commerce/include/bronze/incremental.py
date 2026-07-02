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


# ── 파일 브리지(저장 모델: save 증분 + diff-target 롤링) ─────────────────────────
def sort_rows_to_file(rows: Iterable[dict], *, dest_path: str, tmp_dir: str,
                      chunk_rows: int = 100_000) -> tuple[str, int]:
    """rows 를 외부 병합 정렬해 dest_path(row-NDJSON)로 기록. (검증키, row수) 반환."""
    h = hashlib.sha256()
    n = 0
    with open(dest_path, "w", encoding="utf-8") as f:
        for r in external_merge_sort(rows, tmp_dir=tmp_dir, chunk_rows=chunk_rows):
            f.write(json.dumps(r, ensure_ascii=False))
            f.write("\n")
            h.update(normalize(r).encode("utf-8"))
            h.update(b"\n")
            n += 1
    return h.hexdigest(), n


def read_rows(path: str) -> Iterator[dict]:
    """row-NDJSON 파일 스트리밍 읽기."""
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def build_increment(today_rows: Iterable[dict], *, tmp_dir: str, today_sorted_path: str,
                    increment_path: str, prev_target_path: str | None = None,
                    prev_key: str | None = None, chunk_rows: int = 100_000) -> dict:
    """하루치 증분 산출(파일 기반, 스트리밍).

    - 오늘 수집 rows 를 정렬해 `today_sorted_path`(= 새 diff-target 후보)로 기록 + 검증키 계산.
    - 첫 수집(prev 없음): 증분 = 전체(save=diff-target 동일 내용) → increment_path 에 전체 기록.
    - 전날 target 과 검증키 동일: 증분 없음(마커만) → increment_path 미기록.
    - 상이: `diff_new_rows(today, prev)` 로 신규/변경분만 increment_path 에 기록.
    새 diff-target = today_sorted_path (호출측이 교체 업로드). 반환: mode/key/count/increment_count/identical.
    """
    today_key, today_count = sort_rows_to_file(today_rows, dest_path=today_sorted_path,
                                               tmp_dir=tmp_dir, chunk_rows=chunk_rows)
    if prev_target_path is None:                         # 첫 수집 — 전체를 증분(save)로
        inc = 0
        with open(increment_path, "w", encoding="utf-8") as out:
            for r in read_rows(today_sorted_path):
                out.write(json.dumps(r, ensure_ascii=False)); out.write("\n"); inc += 1
        return {"mode": "first", "key": today_key, "count": today_count,
                "increment_count": inc, "identical": False}
    if prev_key is not None and today_key == prev_key:    # 전날과 동일 — 증분 없음
        return {"mode": "identical", "key": today_key, "count": today_count,
                "increment_count": 0, "identical": True}
    inc = 0                                               # 상이 — 신규/변경분만
    with open(increment_path, "w", encoding="utf-8") as out:
        for r in diff_new_rows(read_rows(today_sorted_path), read_rows(prev_target_path)):
            out.write(json.dumps(r, ensure_ascii=False)); out.write("\n"); inc += 1
    return {"mode": "changed", "key": today_key, "count": today_count,
            "increment_count": inc, "identical": False}


# ── 스토리지 orchestration(save 증분 + diff-target 롤링 교체) ────────────────────
def incremental_store(storage, *, increment_key: str, target_key: str, target_key_file: str,
                      rows: Iterable[dict], tmp_dir: str) -> dict:
    """오늘 rows 를 증분 저장 모델로 스토리지에 반영. storage 는 exists/read_bytes/write_bytes 만 필요.

    - 전날 diff-target(있으면) 다운로드 + 검증키 사이드카 로드 → 비교.
    - build_increment 로 mode/증분 산출.
    - 상이/첫수집: 증분(증분_count>0) → increment_key 업로드, **diff-target 을 오늘본으로 교체**(+키 사이드카).
    - 동일: 아무 것도 안 씀(마커만은 호출측). raw 원본은 애초에 저장 안 함(수집 페이지는 휘발).
    반환: {mode, key(검증키), count, increment_count, identical, increment_key(실제 기록시 키 or None)}.
    """
    prev_target_path = None
    prev_key = None
    if storage.exists(target_key):
        prev_target_path = os.path.join(tmp_dir, "prev_target.jsonl")
        with open(prev_target_path, "wb") as f:
            f.write(storage.read_bytes(target_key))
        if storage.exists(target_key_file):
            prev_key = storage.read_bytes(target_key_file).decode("utf-8").strip()

    today_sorted_path = os.path.join(tmp_dir, "today_sorted.jsonl")
    increment_path = os.path.join(tmp_dir, "increment.jsonl")
    res = build_increment(rows, tmp_dir=tmp_dir, today_sorted_path=today_sorted_path,
                          increment_path=increment_path, prev_target_path=prev_target_path,
                          prev_key=prev_key)

    written_increment = None
    if not res["identical"]:
        if res["increment_count"] > 0:
            with open(increment_path, "rb") as f:
                storage.write_bytes(increment_key, f.read())
            written_increment = increment_key
        with open(today_sorted_path, "rb") as f:                 # diff-target 롤링 교체
            storage.write_bytes(target_key, f.read())
        storage.write_bytes(target_key_file, res["key"].encode("utf-8"))
    res["increment_key"] = written_increment
    return res


def seed_diff_target(storage, *, target_key: str, target_key_file: str,
                     rows: Iterable[dict], tmp_dir: str) -> dict:
    """step0(1회성): 기존 수집물로 diff-target(정렬 전체본) + 검증키 사이드카를 시드한다.

    증분 파일은 만들지 않는다 — 이후 첫 수집이 이 target 과 비교되어 변경분만 저장되게 하는 기준.
    """
    sorted_path = os.path.join(tmp_dir, "seed_sorted.jsonl")
    key, count = sort_rows_to_file(rows, dest_path=sorted_path, tmp_dir=tmp_dir)
    with open(sorted_path, "rb") as f:
        storage.write_bytes(target_key, f.read())
    storage.write_bytes(target_key_file, key.encode("utf-8"))
    return {"key": key, "count": count}
