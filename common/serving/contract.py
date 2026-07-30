"""Load Serving Contract v1 declarations from a dbt manifest.

Common Contract Load step: the publisher trusts contracts that already passed the
ASAC-DBT validator, and only reads the fields it needs to drive publication.
Keeping this pure (manifest.json in, dataclasses out) lets the publisher run under
tests without Trino/Airflow.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


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


def load_contracts(
    manifest_path: str | Path,
    product_ids: Iterable[str] | None = None,
    *,
    enabled_only: bool = True,
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
              )
        )
    contracts.sort(key=lambda c: c.model_name)
    return contracts


def load_domain_contracts(
    manifest_path: str | Path,
    domain: str,
    product_ids: Iterable[str],
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
        for contract in load_contracts(manifest_path)
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
