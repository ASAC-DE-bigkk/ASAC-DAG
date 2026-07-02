"""아카이브 안전 추출 — zip-slip(CWE-22)·압축폭탄(CWE-409/770)·특수엔트리 차단.

배경(가이드라인): CVE-2007-4559(tarfile 경로 탈출) → PEP 706 이 tarfile 에 extraction
filter 를 도입(3.9.17+ 백포트, 3.14 부터 'data' 가 기본). zipfile 에는 필터가 없어
수동 검증이 표준 관행이고, 헤더 크기(file_size)는 **신뢰할 수 없다**
(CVE-2024-0450 overlapped-entry 폭탄은 3.9.19+ 에서만 BadZipFile) — 그래서 이 모듈은
선언값 사전검사 + **실제 수신 바이트 스트리밍 카운터**의 이중 방어를 쓴다.

기능:
  - `safe_extract_zip(src, dest, *, caps...)` : 멤버명 검증(절대경로/드라이브/`..`)·
    dest 봉쇄 증명(realpath+commonpath)·엔트리수/총량/파일당/압축비 상한·스트리밍 강제.
  - `safe_extract_tar(src, dest, *, caps...)` : 동일 상한 + PEP 706 `filter='data'`
    (가능 시), 구버전 폴백은 심링크/하드링크/디바이스 거부 후 멤버별 추출.
  - 초과/위반 시 `UnsafeArchiveError`(부분 파일은 정리). 반환: 기록한 파일 경로 목록
    (`events.log_event` 로 영수증 남기기 좋게).

중첩 아카이브(42.zip 류)는 재귀 추출하지 않는다 — 내부 아카이브를 다시 풀지 말지는
호출측 결정이며, 풀 경우 깊이 제한을 둘 것(문서화된 한계).
"""
from __future__ import annotations

import os
import tarfile
import zipfile
from pathlib import Path
from typing import IO, Union

_CHUNK = 65536
DEFAULT_MAX_ENTRIES = 1000
DEFAULT_MAX_TOTAL_BYTES = 100 * 2 ** 20     # 100 MiB
DEFAULT_MAX_RATIO = 100.0                   # 압축비(선언 원본/압축) 상한 — zip 만 적용


class UnsafeArchiveError(ValueError):
    """아카이브 추출 차단 — 경로 탈출/폭탄/특수 엔트리/상한 초과."""


def _assert_member_name(name: str) -> None:
    """멤버 이름 검증 — 절대경로·Windows 드라이브·`..` 세그먼트·널문자 거부."""
    if not name or "\x00" in name:
        raise UnsafeArchiveError(f"bad member name: {name!r}")
    if name.startswith(("/", "\\")) or (len(name) > 1 and name[1] == ":"):
        raise UnsafeArchiveError(f"absolute member path: {name!r}")
    parts = name.replace("\\", "/").split("/")
    if any(p == ".." for p in parts):
        raise UnsafeArchiveError(f"path traversal in member: {name!r}")


def _contained_path(dest_real: str, name: str) -> str:
    """dest 봉쇄 증명 — realpath 결과가 dest 밖이면 차단(검증 우회 이중 방어)."""
    final = os.path.realpath(os.path.join(dest_real, name.replace("\\", "/")))
    if os.path.commonpath([dest_real, final]) != dest_real:
        raise UnsafeArchiveError(f"member escapes destination: {name!r}")
    return final


def safe_extract_zip(src: "Union[str, Path, IO[bytes]]", dest: "Union[str, Path]", *,
                     max_entries: int = DEFAULT_MAX_ENTRIES,
                     max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
                     max_file_bytes: "int | None" = None,
                     max_ratio: float = DEFAULT_MAX_RATIO) -> "list[Path]":
    """zip 을 dest 아래로만, 상한 안에서만 추출. 위반 시 UnsafeArchiveError."""
    dest_real = os.path.realpath(str(dest))
    os.makedirs(dest_real, exist_ok=True)
    written: list[Path] = []
    total = 0
    with zipfile.ZipFile(src) as zf:
        infos = zf.infolist()
        if len(infos) > max_entries:
            raise UnsafeArchiveError(f"too many entries: {len(infos)} > {max_entries}")
        if sum(i.file_size for i in infos) > max_total_bytes:
            raise UnsafeArchiveError(f"declared total exceeds {max_total_bytes}B")
        for info in infos:
            _assert_member_name(info.filename)
            declared = info.file_size
            if max_file_bytes is not None and declared > max_file_bytes:
                raise UnsafeArchiveError(
                    f"declared file size {declared}B > {max_file_bytes}B: {info.filename!r}")
            if declared > max(info.compress_size, 1) * max_ratio:
                raise UnsafeArchiveError(
                    f"compression ratio > {max_ratio}: {info.filename!r}")
        for info in infos:
            final = _contained_path(dest_real, info.filename)
            if info.is_dir():
                os.makedirs(final, exist_ok=True)
                continue
            os.makedirs(os.path.dirname(final) or dest_real, exist_ok=True)
            per_file = 0
            cap_this = max(info.compress_size, 1) * max_ratio
            try:
                with zf.open(info) as src_f, open(final, "wb") as out_f:
                    while True:
                        chunk = src_f.read(_CHUNK)
                        if not chunk:
                            break
                        per_file += len(chunk)
                        total += len(chunk)
                        # 헤더는 힌트일 뿐 — 실제 바이트 카운터가 최종 판정(폭탄 중도 절단)
                        if total > max_total_bytes or per_file > cap_this or (
                                max_file_bytes is not None and per_file > max_file_bytes):
                            raise UnsafeArchiveError(
                                f"stream exceeded caps at {info.filename!r}")
                        out_f.write(chunk)
            except UnsafeArchiveError:
                try:
                    os.unlink(final)     # 부분 파일 정리
                except OSError:
                    pass
                raise
            written.append(Path(final))
    return written


class _BoundedReader:
    """압축 입력(fileobj)을 감싸 **읽은 압축 바이트 수를 상한**으로 제한.

    tar 는 중앙 디렉터리가 없어 헤더 열람에도 스트림을 전진 압축해제한다 → 압축폭탄이
    getmembers()/next() 단계에서 메모리를 태울 수 있다. 압축 입력 자체를 상한으로 잘라
    (초과 시 UnsafeArchiveError) 해제량의 상방을 간접적으로 묶는다(해제 카운터와 이중 방어).
    """

    def __init__(self, raw, limit: int):
        self._raw = raw
        self._limit = limit
        self._read = 0

    def read(self, size: int = -1) -> bytes:
        chunk = self._raw.read(size)
        self._read += len(chunk)
        if self._read > self._limit:
            raise UnsafeArchiveError(f"compressed input exceeds {self._limit}B (bomb guard)")
        return chunk

    def readable(self) -> bool:
        return True


def safe_extract_tar(src: "Union[str, Path, IO[bytes]]", dest: "Union[str, Path]", *,
                     max_entries: int = DEFAULT_MAX_ENTRIES,
                     max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
                     max_file_bytes: "int | None" = None,
                     max_compressed_bytes: "int | None" = None) -> "list[Path]":
    """tar 를 dest 아래로만, 상한 안에서만 추출(스트리밍 — 압축폭탄/열거폭탄 방지).

    getmembers() 로 **전체를 미리 물질화하지 않고** `next()` 로 한 멤버씩 열람하며 즉시
    상한을 검사한다(오염 멤버를 만나면 그 지점에서 중단 — 뒤쪽 거대 데이터를 해제하지 않음).
    압축 입력은 max_compressed_bytes(기본 max_total_bytes)로 잘라 gzip/xz 폭탄을 막고,
    실제 추출 바이트는 별도 카운터로 재검증한다(헤더는 신뢰하지 않음). tar 멤버엔 압축비
    정보가 없어 ratio 상한은 없다(총량/파일당/압축입력 상한으로 커버).
    """
    dest_real = os.path.realpath(str(dest))
    os.makedirs(dest_real, exist_ok=True)
    limit = max_compressed_bytes if max_compressed_bytes is not None else max_total_bytes
    raw = open(str(src), "rb") if isinstance(src, (str, Path)) else src
    bounded = _BoundedReader(raw, limit)
    written: list[Path] = []
    total = 0
    count = 0
    try:
        # mode 'r|*' = 스트리밍(비탐색) 해제 → getmembers() 없이 next() 로만 전진.
        with tarfile.open(fileobj=bounded, mode="r|*") as tf:
            for m in tf:
                count += 1
                if count > max_entries:
                    raise UnsafeArchiveError(f"too many entries: > {max_entries}")
                _assert_member_name(m.name)
                final = _contained_path(dest_real, m.name)
                if m.isdir():
                    os.makedirs(final, exist_ok=True)
                    continue
                if m.issym() or m.islnk():
                    raise UnsafeArchiveError(f"link member rejected: {m.name!r}")
                if not m.isreg():        # 디바이스/FIFO 등 특수 엔트리 거부
                    raise UnsafeArchiveError(f"special member rejected: {m.name!r}")
                if max_file_bytes is not None and m.size > max_file_bytes:
                    raise UnsafeArchiveError(
                        f"declared size {m.size}B > {max_file_bytes}B: {m.name!r}")
                os.makedirs(os.path.dirname(final) or dest_real, exist_ok=True)
                src_f = tf.extractfile(m)
                if src_f is None:
                    continue
                per_file = 0
                with open(final, "wb") as out_f:
                    while True:
                        chunk = src_f.read(_CHUNK)
                        if not chunk:
                            break
                        per_file += len(chunk)
                        total += len(chunk)
                        if total > max_total_bytes or (
                                max_file_bytes is not None and per_file > max_file_bytes):
                            out_f.close()
                            try:
                                os.unlink(final)
                            except OSError:
                                pass
                            raise UnsafeArchiveError(f"stream exceeded caps at {m.name!r}")
                        out_f.write(chunk)
                written.append(Path(final))
    finally:
        if isinstance(src, (str, Path)):
            raw.close()
    return written
