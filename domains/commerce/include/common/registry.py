"""데이터셋 레지스트리 — config/dataset_registry.yaml 로더(단일 진실 공급원).

service_name 이 채워진 것만 수집 대상. 미해석(null)은 자동 제외.
경로는 COMMERCE_REGISTRY_PATH 로 override 가능(기본: dags/domains/commerce/config/dataset_registry.yaml).
"""
from __future__ import annotations

import functools
import os
from pathlib import Path

import yaml

from common.schemas import Dataset

# dags/domains/commerce/include/common/registry.py → parents[2] == dags/domains/commerce/
_DEFAULT_PATH = Path(__file__).resolve().parents[2] / "config" / "dataset_registry.yaml"


def _registry_path() -> Path:
    return Path(os.getenv("COMMERCE_REGISTRY_PATH", str(_DEFAULT_PATH)))


@functools.lru_cache(maxsize=1)
def all_datasets() -> tuple[Dataset, ...]:
    doc = yaml.safe_load(_registry_path().read_text(encoding="utf-8")) or {}
    out = []
    for r in doc.get("datasets", []):
        out.append(Dataset(
            oa_id=r["oa_id"], name_ko=r["name_ko"], short=r["short"],
            category=r["category"], schedule=r["schedule"],
            service_name=r.get("service_name") or None,
        ))
    return tuple(out)


def by_short(short: str) -> Dataset:
    for d in all_datasets():
        if d.short == short:
            return d
    raise KeyError(f"unknown dataset short: {short!r}")


def enabled_for_schedule(schedule: str) -> list[Dataset]:
    """수집 대상 = 해당 schedule + service_name 확인된 것."""
    return [d for d in all_datasets() if d.schedule == schedule and d.service_name]


def pending_for_schedule(schedule: str) -> list[Dataset]:
    """service_name 미확인(수집 제외) — 로그/문서용."""
    return [d for d in all_datasets() if d.schedule == schedule and not d.service_name]
