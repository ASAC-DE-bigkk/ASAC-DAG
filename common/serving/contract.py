"""Load Serving Contract v1 declarations from a dbt manifest.

Common Contract Load step: the publisher trusts contracts that already passed the
ASAC-DBT validator, and only reads the fields it needs to drive publication.
Keeping this pure (manifest.json in, dataclasses out) lets the publisher run under
tests without Trino/Airflow.
"""

from __future__ import annotations

import json
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class ServingContract:
    """The publication-relevant subset of one model's ``meta.serving`` block."""

    product_id: str
    model_name: str  # physical D1 table name
    enabled: bool
    external: bool
    publication_mode: str
    zero_policy: str
    primary_key: tuple[str, ...]
    upsert_strategy: str | None = None
    partial_min_ratio: float | None = None
    reliability: dict[str, Any] | None = None
    event_time: str | None = None
    description: str = ""
    product_question: str = ""
    tests: tuple[str, ...] = ()
    public_gold: dict[str, Any] | None = None
    mcp_projection: dict[str, Any] | None = None
    public_projection: tuple[str, ...] | None = None
    projection_schema_version: str | None = None
    projection_schema_hash: str | None = None
    # ── 핸드오프 메타(#638) — d1_catalog_columns/ext·d1_usage_patterns 게시 원천 ──
    grain: str | None = None
    serving_tier: str | None = None      # 물리 게시 확장 — 미선언 도메인은 None(#638 §2.2)
    rollup_rule: str | None = None       # 물리 게시 확장(commerce d1_rollup) — 동상
    column_descriptions: dict[str, str] | None = None  # manifest node.columns description
    usage_patterns: tuple[dict[str, Any], ...] = ()


def _merged_meta(node: dict[str, Any]) -> dict[str, Any]:
    top = node.get("meta") or {}
    config_meta = (node.get("config") or {}).get("meta") or {}
    return {**(top if isinstance(top, dict) else {}), **(config_meta if isinstance(config_meta, dict) else {})}


def _gates_by_model(manifest: dict[str, Any]) -> dict[str, list[str]]:
    """Model unique_id -> sorted test labels (for the _catalog `tests` column)."""
    gates: dict[str, set[str]] = {}
    for node in (manifest.get("nodes") or {}).values():
        if node.get("resource_type") != "test" or not node.get("attached_node"):
            continue
        meta = node.get("test_metadata") or {}
        label = meta.get("name") or node.get("name", "test")
        column = (meta.get("kwargs") or {}).get("column_name")
        gates.setdefault(node["attached_node"], set()).add(f"{label}({column})" if column else label)
    return {uid: sorted(names) for uid, names in gates.items()}


def _column_identity_meta(column: dict[str, Any]) -> dict[str, Any]:
    config_meta = ((column.get("config") or {}).get("meta") or {}) if isinstance(column, dict) else {}
    meta = config_meta if isinstance(config_meta, dict) else {}
    return {
        "data_type": str(column.get("data_type", "")).strip().lower(),
        "nullable": meta.get("nullable"),
        "semantic_role": meta.get("semantic_role"),
        "unit": meta.get("unit"),
    }


def _projection_schema_hash(
    schema_version: str,
    projection_columns: tuple[str, ...],
    manifest_columns: dict[str, Any],
) -> str:
    payload = {
        "schema_version": schema_version,
        "columns": [
            {
                "name": column_name,
                **_column_identity_meta(manifest_columns[column_name]),
            }
            for column_name in projection_columns
        ],
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _load_public_projection(
    *,
    product_id: str,
    serving: dict[str, Any],
    node: dict[str, Any],
    require_public_projection: bool,
) -> tuple[tuple[str, ...] | None, str | None, str | None]:
    projection = serving.get("public_projection")
    if projection is None:
        if require_public_projection:
            raise ValueError(f"{product_id}: public_projection required")
        return None, None, None
    if not isinstance(projection, dict):
        raise ValueError(f"{product_id}: public_projection must be object")
    if set(projection) != {"schema_version", "columns"}:
        raise ValueError(f"{product_id}: public_projection must contain exactly schema_version and columns")
    schema_version = projection.get("schema_version")
    if not isinstance(schema_version, str) or not SEMVER_RE.fullmatch(schema_version):
        raise ValueError(f"{product_id}: public_projection schema_version must be semver")
    raw_columns = projection.get("columns")
    if not isinstance(raw_columns, list) or not raw_columns:
        raise ValueError(f"{product_id}: public_projection columns must be a non-empty list")

    seen: set[str] = set()
    columns: list[str] = []
    for column in raw_columns:
        if not isinstance(column, str) or not IDENTIFIER_RE.fullmatch(column):
            raise ValueError(f"{product_id}: public_projection column must be physical identifier")
        if column in seen:
            raise ValueError(f"{product_id}: public_projection duplicate column {column}")
        seen.add(column)
        columns.append(column)

    manifest_columns = node.get("columns") if isinstance(node.get("columns"), dict) else {}
    for column in columns:
        if column not in manifest_columns:
            raise ValueError(f"{product_id}: public_projection unknown column {column}")
        identity = _column_identity_meta(manifest_columns[column])
        missing = [
            key
            for key, value in identity.items()
            if value in ("", None) or (key == "nullable" and not isinstance(value, bool))
        ]
        if missing:
            raise ValueError(f"{product_id}: public_projection identity metadata missing for {column}: {','.join(missing)}")

    public_columns = tuple(columns)
    return public_columns, schema_version, _projection_schema_hash(schema_version, public_columns, manifest_columns)


def load_contracts(
    manifest_path: str | Path,
    product_ids: Iterable[str] | None = None,
    *,
    enabled_only: bool = True,
    require_public_projection: bool = False,
) -> list[ServingContract]:
    """Parse a dbt manifest into ``ServingContract`` records.

    ``product_ids`` (from the thin domain DAG) restricts the set; ``None`` loads all
    models declaring a serving contract. ``enabled_only`` drops ``enabled: false``.
    """
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    wanted = set(product_ids) if product_ids is not None else None
    gates = _gates_by_model(manifest)

    contracts: list[ServingContract] = []
    for uid, node in (manifest.get("nodes") or {}).items():
        if node.get("resource_type") != "model":
            continue
        metadata = _merged_meta(node)
        serving = metadata.get("serving")
        if not isinstance(serving, dict) or not serving:
            continue
        product_id = serving.get("product_id")
        if not product_id:
            continue
        if wanted is not None and product_id not in wanted:
            continue
        if enabled_only and not bool(serving.get("enabled", False)):
            continue
        partial = serving.get("partial_policy") or {}
        public_projection, projection_schema_version, projection_schema_hash = _load_public_projection(
            product_id=str(product_id),
            serving=serving,
            node=node,
            require_public_projection=require_public_projection,
        )
        contracts.append(
            ServingContract(
                product_id=str(product_id),
                model_name=str(node.get("name", "")),
                enabled=bool(serving.get("enabled", False)),
                external=bool(serving.get("external", False)),
                publication_mode=str(serving.get("publication_mode", "")),
                zero_policy=str(serving.get("zero_policy", "retain_last_good")),
                primary_key=tuple(serving.get("primary_key") or ()),
                upsert_strategy=serving.get("upsert_strategy"),
                partial_min_ratio=partial.get("min_publish_ratio") if isinstance(partial, dict) else None,
                reliability=serving.get("reliability") if isinstance(serving.get("reliability"), dict) else None,
                  event_time=serving.get("event_time"),
                  description=str(node.get("description", "")),
                  product_question=str(serving.get("product_question", "")),
                  tests=tuple(gates.get(uid, [])),
                  public_gold=(
                      dict(metadata["public_gold"])
                      if isinstance(metadata.get("public_gold"), dict)
                      else None
                  ),
                  mcp_projection=(
                      dict(serving["mcp_projection"])
                      if isinstance(serving.get("mcp_projection"), dict)
                      else None
                  ),
                  public_projection=public_projection,
                  projection_schema_version=projection_schema_version,
                  projection_schema_hash=projection_schema_hash,
                  grain=serving.get("grain"),
                  # commerce 로컬 키(serving_tier/d1_rollup)와 #638 §1 스펙 키(tier/rollup_rule) 겸용
                  serving_tier=serving.get("serving_tier") or serving.get("tier"),
                  rollup_rule=serving.get("d1_rollup") or serving.get("rollup_rule"),
                  column_descriptions=(
                      {
                          str(column): str(spec.get("description") or "").strip()
                          for column, spec in node["columns"].items()
                      }
                      if isinstance(node.get("columns"), dict) and node["columns"]
                      else None
                  ),
                  usage_patterns=tuple(
                      pattern
                      for pattern in (serving.get("usage_patterns") or ())
                      if isinstance(pattern, dict)
                  ),
              )
        )
    contracts.sort(key=lambda c: c.model_name)
    return contracts


def load_domain_contracts(
    manifest_path: str | Path,
    domain: str,
    product_ids: Iterable[str],
    *,
    require_public_projection: bool = False,
) -> list[ServingContract]:
    """Load one wrapper's complete enabled domain contract set.

    A domain exporter must not silently publish a subset of its enabled dbt
    contracts. Model names remain the dbt-owned domain boundary, so no D1 table
    or product list is duplicated in the DAG factory.
    """
    requested = list(product_ids)
    duplicates = sorted({product_id for product_id in requested if requested.count(product_id) > 1})
    if duplicates:
        raise ValueError(f"{domain}: duplicate product_ids={','.join(duplicates)}")

    prefix = f"gold_{domain}_"
    enabled_domain_contracts = [
        contract
        for contract in load_contracts(
            manifest_path,
            require_public_projection=require_public_projection,
        )
        if contract.model_name.startswith(prefix)
    ]
    enabled_ids = {contract.product_id for contract in enabled_domain_contracts}
    requested_ids = set(requested)
    missing = sorted(enabled_ids - requested_ids)
    unexpected = sorted(requested_ids - enabled_ids)
    if missing or unexpected:
        detail = " ".join(
            part
            for part in (
                f"missing={','.join(missing)}" if missing else "",
                f"unexpected={','.join(unexpected)}" if unexpected else "",
            )
            if part
        )
        raise ValueError(f"{domain}: enabled serving exact-set mismatch {detail}")

    return enabled_domain_contracts
