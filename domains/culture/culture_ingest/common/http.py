"""Shared HTTP helpers for source clients."""

from __future__ import annotations

from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def build_session(total_retries: int = 3, backoff: float = 0.5) -> requests.Session:
    """A requests Session with retry/backoff on transient HTTP errors."""
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


@dataclass
class Page:
    """One fetched page/window of raw response bytes (source-agnostic)."""

    index: int  # 1-based page (KOPIS cpage) or window start (Seoul)
    body: bytes
    row_count: int
    ext: str  # "xml" | "json"
