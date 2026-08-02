"""Real runtime seams for the common publisher (Trino source, D1 client, smoke).

Domain-neutral: builds a Trino connection and the Cloudflare D1 client from the
compose environment (same env var names citydata already uses), so no domain module
is imported. Network/driver imports are lazy — this file is never imported by the
publisher unit tests, only by the DAG at runtime.

Secrets come only from the environment (``CLOUDFLARE_API_TOKEN``) and are never
logged. Account/DB ids are non-secret identifiers passed via ``SERVING_*`` env keys.
"""

from __future__ import annotations

import os
from typing import Any

from common.serving.d1_client import Column, HttpD1Client
from common.serving.contract import ServingContract
from common.serving.publisher import ReadPlan

APPEND_LOOKBACK_HOURS = 2
APPEND_LOOKBACK_DAYS = 2


# ── Trino source ──────────────────────────────────────────────────────────────────

class TrinoSourceReader:
    """Read publishable rows from the domain's Iceberg Gold via a Trino cursor."""

    def __init__(self, cursor: Any, catalog: str, schema: str) -> None:
        self._cursor = cursor
        self._catalog = catalog
        self._schema = schema

    def _relation(self, model_name: str) -> str:
        return f"{self._catalog}.{self._schema}.{model_name}"

    def _columns(self, model_name: str) -> list[Column]:
        self._cursor.execute(f"SHOW COLUMNS FROM {self._relation(model_name)}")
        return [(row[0], row[1]) for row in self._cursor.fetchall()]

    def _select(self, sql: str) -> list[dict[str, Any]]:
        self._cursor.execute(sql)
        colnames = [d[0] for d in self._cursor.description]
        return [dict(zip(colnames, row)) for row in self._cursor.fetchall()]

    def read(self, contract: ServingContract, last_good_max: Any | None) -> ReadPlan:
        columns = self._columns(contract.model_name)
        relation = self._relation(contract.model_name)

        if contract.publication_mode != "append" or not contract.event_time:
            rows = self._select(f"SELECT * FROM {relation}")
            return ReadPlan(columns=columns, rows=rows)

        # append: re-load only the recent window (idempotent) + any new rows.
        column_type = dict(columns).get(contract.event_time, "")
        if last_good_max is None:
            rows = self._select(f"SELECT * FROM {relation}")  # first run: full backfill
            return ReadPlan(columns=columns, rows=rows)

        import pendulum

        base = pendulum.parse(str(last_good_max).replace(" ", "T"))
        if column_type.startswith("date"):
            cutoff = base.subtract(days=APPEND_LOOKBACK_DAYS).format("YYYY-MM-DD")
            literal = f"date '{cutoff}'"
        else:
            cutoff = base.subtract(hours=APPEND_LOOKBACK_HOURS).format("YYYY-MM-DD HH:00:00")
            literal = f"timestamp '{cutoff}'"
        rows = self._select(f'SELECT * FROM {relation} WHERE "{contract.event_time}" >= {literal}')
        return ReadPlan(columns=columns, rows=rows, delete_column=contract.event_time, delete_literal=f"'{cutoff}'")


def _trino_settings(target: str, schema: str) -> dict[str, Any]:
    dev = target != "prod"
    # 카탈로그 키 이름은 배포 환경을 담지 않는다 — canonical ``TRINO_ICEBERG_CATALOG`` 하나이고
    # 값이 배포를 따라간다(호스트 컴포즈도 이 값으로 카탈로그 파일 이름을 짓는다).
    # 미설정 시 기본만 타깃에 따라 다르다.
    catalog = os.environ.get("TRINO_ICEBERG_CATALOG") or ("iceberg_dev" if dev else "iceberg")
    return {
        "host": os.environ.get("TRINO_HOST", "trino"),
        "port": int(os.environ.get("TRINO_PORT", "8080")),
        "user": os.environ.get("TRINO_USER", "airflow"),
        "http_scheme": os.environ.get("TRINO_HTTP_SCHEME", "http"),
        "catalog": catalog,
        "schema": schema,
    }


def build_trino_source_reader(target: str, schema: str) -> TrinoSourceReader:
    import trino.dbapi

    settings = _trino_settings(target, schema)
    conn = trino.dbapi.connect(
        host=settings["host"], port=settings["port"], user=settings["user"],
        catalog=settings["catalog"], http_scheme=settings["http_scheme"],
    )
    return TrinoSourceReader(conn.cursor(), settings["catalog"], settings["schema"])


# ── Cloudflare D1 client ──────────────────────────────────────────────────────────

def build_d1_client_from_env() -> HttpD1Client:
    token = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    account = os.environ.get("SERVING_CLOUDFLARE_ACCOUNT_ID", "")
    database = os.environ.get("SERVING_D1_DATABASE_ID", "")
    if not token:
        raise RuntimeError("CLOUDFLARE_API_TOKEN 미설정 — compose airflow env 에 전달 필요 (D1 Edit)")
    if not account or not database:
        raise RuntimeError("SERVING_CLOUDFLARE_ACCOUNT_ID / SERVING_D1_DATABASE_ID 미설정 — .env 에 추가 필요")
    api_url = f"https://api.cloudflare.com/client/v4/accounts/{account}/d1/database/{database}/query"
    return HttpD1Client(api_url, token)


# ── API smoke test ────────────────────────────────────────────────────────────────

class HttpSmokeTester:
    """Hit the public serving API for one representative row to prove reachability."""

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")

    def check(self, model_name: str) -> str:
        if not self._base_url:
            return "not_evaluated"
        import requests

        try:
            resp = requests.get(f"{self._base_url}/data/{model_name}", params={"limit": 1}, timeout=30)
        except Exception:  # noqa: BLE001 -- unreachable API is a smoke failure
            return "failed"
        return "passed" if resp.status_code == 200 else "failed"


def build_smoke_tester_from_env() -> HttpSmokeTester:
    return HttpSmokeTester(os.environ.get("SERVING_API_BASE_URL", ""))
