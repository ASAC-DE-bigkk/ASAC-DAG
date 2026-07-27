from collections.abc import Mapping
from datetime import datetime
from zoneinfo import ZoneInfo

from traffic_ingest.acc_info import SOURCE_ID, request_params_json
from traffic_ingest.common.runtime import (
    create_schema_if_needed,
    sql_int,
    sql_string,
    sql_timestamp,
    trino_cursor,
)
from traffic_ingest.errors import TrafficCompletenessError, TrafficSourceSchemaError


BRONZE_TABLE = "bronze_seoul_traffic_incident"
REQUEST_AUDIT_TABLE = "bronze_seoul_traffic_incident_request_audit"
KST = ZoneInfo("Asia/Seoul")


def ensure_seoul_traffic_bronze_schema(cursor, qualified_table: str) -> None:
    for column_name, column_type in (
        ("request_params_json", "varchar"),
        ("load_date", "varchar"),
    ):
        cursor.execute(
            f"ALTER TABLE {qualified_table} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        )


def create_seoul_traffic_bronze_table(cursor, catalog: str, schema: str) -> str:
    qualified_schema = f"{catalog}.{schema}"
    qualified_table = f"{qualified_schema}.{BRONZE_TABLE}"
    create_schema_if_needed(cursor, qualified_schema)
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            request_id varchar,
            source_id varchar,
            request_params_json varchar,
            start_index integer,
            end_index integer,
            acc_id varchar,
            occr_date varchar,
            occr_time varchar,
            exp_clr_date varchar,
            exp_clr_time varchar,
            acc_type varchar,
            acc_dtype varchar,
            link_id varchar,
            grs80tm_x varchar,
            grs80tm_y varchar,
            acc_info varchar,
            acc_road_code varchar,
            raw_object_key varchar,
            payload_hash varchar,
            http_status integer,
            result_code varchar,
            result_msg varchar,
            list_total_count integer,
            row_count integer,
            collected_at timestamp(6),
            load_date varchar,
            dag_run_id varchar
        )
        WITH (
            format = 'PARQUET'
        )
        """
    )
    ensure_seoul_traffic_bronze_schema(cursor, qualified_table)
    create_seoul_traffic_request_audit_table(cursor, qualified_schema)
    return qualified_table


def create_seoul_traffic_request_audit_table(cursor, qualified_schema: str) -> str:
    qualified_table = f"{qualified_schema}.{REQUEST_AUDIT_TABLE}"
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            request_id varchar,
            source_id varchar,
            request_params_json varchar,
            start_index integer,
            end_index integer,
            raw_object_key varchar,
            payload_hash varchar,
            http_status integer,
            result_code varchar,
            result_msg varchar,
            list_total_count integer,
            row_count integer,
            collected_at timestamp(6),
            load_date varchar,
            dag_run_id varchar
        )
        WITH (
            format = 'PARQUET'
        )
        """
    )
    return qualified_table


def request_audit_table_for(qualified_bronze_table: str) -> str:
    qualified_schema = qualified_bronze_table.rsplit(".", 1)[0]
    return f"{qualified_schema}.{REQUEST_AUDIT_TABLE}"


def metadata_int(metadata: dict, key: str) -> int:
    value = metadata.get(key)
    if value is None or value == "":
        return 0
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise TrafficSourceSchemaError(
            f"Traffic metadata field must be an integer: {key}"
        ) from exc


def validate_seoul_traffic_row_count(rows: list[dict], metadata: dict) -> None:
    list_total_count = metadata_int(metadata, "list_total_count")
    if list_total_count > 0 and not rows:
        raise TrafficCompletenessError(
            "Seoul traffic bronze validation failed: "
            f"list_total_count={list_total_count}, parsed row_count=0"
        )


def insert_seoul_traffic_request_audit(
    cursor,
    qualified_bronze_table: str,
    rows: list[dict],
    metadata: dict,
    request_id: str,
    start_index: int,
    end_index: int,
    raw_object_key: str,
    raw_hash: str,
    http_status: int,
    collected_at: datetime,
    dag_run_id: str,
) -> None:
    qualified_audit_table = request_audit_table_for(qualified_bronze_table)
    load_date = collected_at.astimezone(KST).strftime("%Y-%m-%d")
    request_params = request_params_json(start_index, end_index)
    cursor.execute(
        f"""
        DELETE FROM {qualified_audit_table}
        WHERE source_id = {sql_string(SOURCE_ID)}
            AND dag_run_id = {sql_string(dag_run_id)}
            AND start_index = {sql_int(start_index)}
            AND end_index = {sql_int(end_index)}
        """
    )
    cursor.execute(
        f"""
        INSERT INTO {qualified_audit_table} (
            request_id,
            source_id,
            request_params_json,
            start_index,
            end_index,
            raw_object_key,
            payload_hash,
            http_status,
            result_code,
            result_msg,
            list_total_count,
            row_count,
            collected_at,
            load_date,
            dag_run_id
        )
        VALUES (
            {sql_string(request_id)},
            {sql_string(SOURCE_ID)},
            {sql_string(request_params)},
            {sql_int(start_index)},
            {sql_int(end_index)},
            {sql_string(raw_object_key)},
            {sql_string(raw_hash)},
            {sql_int(http_status)},
            {sql_string(metadata.get("result_code"))},
            {sql_string(metadata.get("result_msg"))},
            {sql_int(metadata.get("list_total_count"))},
            {sql_int(len(rows))},
            {sql_timestamp(collected_at)},
            {sql_string(load_date)},
            {sql_string(dag_run_id)}
        )
        """
    )


def insert_seoul_traffic_bronze_rows(
    cursor,
    qualified_table: str,
    rows: list[dict],
    metadata: dict,
    request_id: str,
    start_index: int,
    end_index: int,
    raw_object_key: str,
    raw_hash: str,
    http_status: int,
    collected_at: datetime,
    dag_run_id: str,
) -> int:
    validate_seoul_traffic_row_count(rows, metadata)
    insert_seoul_traffic_request_audit(
        cursor=cursor,
        qualified_bronze_table=qualified_table,
        rows=rows,
        metadata=metadata,
        request_id=request_id,
        start_index=start_index,
        end_index=end_index,
        raw_object_key=raw_object_key,
        raw_hash=raw_hash,
        http_status=http_status,
        collected_at=collected_at,
        dag_run_id=dag_run_id,
    )

    cursor.execute(
        f"""
        DELETE FROM {qualified_table}
        WHERE source_id = {sql_string(SOURCE_ID)}
            AND dag_run_id = {sql_string(dag_run_id)}
            AND start_index = {sql_int(start_index)}
            AND end_index = {sql_int(end_index)}
        """
    )

    if not rows:
        print(
            "Seoul traffic API returned no incident rows; raw XML was preserved without bronze rows."
        )
        return 0

    load_date = collected_at.astimezone(KST).strftime("%Y-%m-%d")
    request_params = request_params_json(start_index, end_index)
    values = []
    for row in rows:
        values.append(
            "("
            f"{sql_string(request_id)}, "
            f"{sql_string(SOURCE_ID)}, "
            f"{sql_string(request_params)}, "
            f"{sql_int(start_index)}, "
            f"{sql_int(end_index)}, "
            f"{sql_string(row.get('acc_id'))}, "
            f"{sql_string(row.get('occr_date'))}, "
            f"{sql_string(row.get('occr_time'))}, "
            f"{sql_string(row.get('exp_clr_date'))}, "
            f"{sql_string(row.get('exp_clr_time'))}, "
            f"{sql_string(row.get('acc_type'))}, "
            f"{sql_string(row.get('acc_dtype'))}, "
            f"{sql_string(row.get('link_id'))}, "
            f"{sql_string(row.get('grs80tm_x'))}, "
            f"{sql_string(row.get('grs80tm_y'))}, "
            f"{sql_string(row.get('acc_info'))}, "
            f"{sql_string(row.get('acc_road_code'))}, "
            f"{sql_string(raw_object_key)}, "
            f"{sql_string(raw_hash)}, "
            f"{sql_int(http_status)}, "
            f"{sql_string(metadata.get('result_code'))}, "
            f"{sql_string(metadata.get('result_msg'))}, "
            f"{sql_int(metadata.get('list_total_count'))}, "
            f"{sql_int(metadata.get('row_count'))}, "
            f"{sql_timestamp(collected_at)}, "
            f"{sql_string(load_date)}, "
            f"{sql_string(dag_run_id)}"
            ")"
        )

    cursor.execute(
        f"""
        INSERT INTO {qualified_table} (
            request_id,
            source_id,
            request_params_json,
            start_index,
            end_index,
            acc_id,
            occr_date,
            occr_time,
            exp_clr_date,
            exp_clr_time,
            acc_type,
            acc_dtype,
            link_id,
            grs80tm_x,
            grs80tm_y,
            acc_info,
            acc_road_code,
            raw_object_key,
            payload_hash,
            http_status,
            result_code,
            result_msg,
            list_total_count,
            row_count,
            collected_at,
            load_date,
            dag_run_id
        )
        VALUES {", ".join(values)}
        """
    )
    return len(rows)


def verify_seoul_traffic_bronze_runtime(
    raw_object_key: str | None = None,
    raw_object_keys: list[str] | None = None,
    dag_run_id: str | None = None,
    expected_rows: int | None = None,
    expected_raw_objects: int | None = None,
) -> int:
    cursor, catalog, schema = trino_cursor()
    qualified_table = f"{catalog}.{schema}.{BRONZE_TABLE}"
    filters = [f"source_id = {sql_string(SOURCE_ID)}"]
    if raw_object_keys:
        raw_key_values = ", ".join(sql_string(key) for key in raw_object_keys)
        filters.append(f"raw_object_key IN ({raw_key_values})")
    elif raw_object_key:
        filters.append(f"raw_object_key = {sql_string(raw_object_key)}")
    if dag_run_id:
        filters.append(f"dag_run_id = {sql_string(dag_run_id)}")
    cursor.execute(
        f"""
        SELECT
            count(*) AS table_rows,
            count(DISTINCT raw_object_key) AS raw_object_count,
            max(collected_at) AS last_collected_at
        FROM {qualified_table}
        WHERE {" AND ".join(filters)}
        """
    )
    row = cursor.fetchone()
    table_rows = int(row[0])
    if expected_rows is not None and table_rows != expected_rows:
        raise TrafficCompletenessError(
            "Seoul traffic bronze verification failed: "
            f"expected_rows={expected_rows}, actual_rows={table_rows}"
        )
    if (
        expected_raw_objects is not None
        and expected_rows != 0
        and int(row[1]) != expected_raw_objects
    ):
        raise TrafficCompletenessError(
            "Seoul traffic bronze verification failed: "
            f"expected_raw_objects={expected_raw_objects}, actual_raw_objects={row[1]}"
        )
    audit_raw_object_keys = raw_object_keys or (
        [raw_object_key] if raw_object_key else []
    )
    if expected_rows == 0 and audit_raw_object_keys:
        audit_table = request_audit_table_for(qualified_table)
        audit_raw_key_values = ", ".join(
            sql_string(key) for key in audit_raw_object_keys
        )
        audit_filters = [
            f"source_id = {sql_string(SOURCE_ID)}",
            f"raw_object_key IN ({audit_raw_key_values})",
        ]
        if dag_run_id:
            audit_filters.append(f"dag_run_id = {sql_string(dag_run_id)}")
        cursor.execute(
            f"""
            SELECT count(*) AS request_audit_rows
            FROM {audit_table}
            WHERE {" AND ".join(audit_filters)}
            """
        )
        audit_row = cursor.fetchone()
        expected_audit_rows = expected_raw_objects or len(audit_raw_object_keys)
        if int(audit_row[0]) != expected_audit_rows:
            raise TrafficCompletenessError(
                "Seoul traffic bronze verification failed: "
                f"expected_request_audit_rows={expected_audit_rows}, "
                f"actual_request_audit_rows={audit_row[0]}"
            )
    if expected_rows and expected_raw_objects is None and int(row[1]) != 1:
        raise TrafficCompletenessError(
            f"Seoul traffic bronze verification failed: raw_object_count={row[1]}"
        )
    print(
        "traffic_incident_bronze "
        f"table_rows={row[0]} raw_object_count={row[1]} last_collected_at={row[2]}"
    )
    return table_rows


def _receipt_evidence_expectations(
    receipt_raw_results: Mapping[str, Mapping[str, object]],
) -> dict[str, tuple[int, tuple[tuple[str, str, int], ...]]]:
    expectations: dict[str, tuple[int, tuple[tuple[str, str, int], ...]]] = {}
    seen_raw_keys: set[str] = set()
    for snapshot_run_id, raw_result in receipt_raw_results.items():
        if not isinstance(snapshot_run_id, str) or not snapshot_run_id:
            raise TrafficCompletenessError(
                "Traffic receipt evidence requires a non-empty snapshot run ID"
            )
        raw_objects = raw_result.get("raw_objects")
        if not isinstance(raw_objects, list) or not raw_objects:
            raise TrafficCompletenessError(
                "Traffic receipt evidence requires at least one raw object"
            )
        descriptors: list[tuple[str, str, int]] = []
        for raw_object in raw_objects:
            if not isinstance(raw_object, Mapping):
                raise TrafficCompletenessError(
                    "Traffic receipt evidence contains a malformed raw object"
                )
            raw_object_key = str(raw_object.get("raw_object_key") or "")
            payload_hash = str(
                raw_object.get("raw_hash") or raw_object.get("payload_hash") or ""
            )
            try:
                row_count = int(raw_object.get("row_count"))
            except (TypeError, ValueError) as exc:
                raise TrafficCompletenessError(
                    "Traffic receipt evidence raw object requires an integer row_count"
                ) from exc
            if not raw_object_key or not payload_hash or row_count < 0:
                raise TrafficCompletenessError(
                    "Traffic receipt evidence raw object is incomplete"
                )
            if raw_object_key in seen_raw_keys:
                raise TrafficCompletenessError(
                    "Traffic receipt evidence reuses a raw object key"
                )
            seen_raw_keys.add(raw_object_key)
            descriptors.append((raw_object_key, payload_hash, row_count))
        try:
            expected_rows = int(raw_result.get("expected_rows"))
            page_count = int(raw_result.get("page_count", len(descriptors)))
        except (TypeError, ValueError) as exc:
            raise TrafficCompletenessError(
                "Traffic receipt evidence has malformed aggregate counts"
            ) from exc
        if (
            expected_rows < 0
            or page_count != len(descriptors)
            or expected_rows != sum(row_count for _key, _hash, row_count in descriptors)
        ):
            raise TrafficCompletenessError(
                "Traffic receipt evidence aggregate counts do not match raw objects"
            )
        expectations[snapshot_run_id] = (expected_rows, tuple(descriptors))
    return expectations


def find_verified_seoul_traffic_bronze_receipts(
    receipt_raw_results: Mapping[str, Mapping[str, object]],
    *,
    cursor_factory=trino_cursor,
) -> dict[str, int]:
    """Return only receipts whose prior Bronze and audit evidence is exact.

    A missing/mismatched row is deliberately not an error: its receipt must use
    the normal load-and-verify path.  Trino/catalog errors are left untouched so
    Airflow can retry rather than silently accepting unknown commit outcomes.
    """

    expectations = _receipt_evidence_expectations(receipt_raw_results)
    if not expectations:
        return {}
    raw_keys = [
        raw_key
        for _snapshot_run_id, (_expected_rows, descriptors) in expectations.items()
        for raw_key, _payload_hash, _row_count in descriptors
    ]
    run_ids = list(expectations)
    cursor, catalog, schema = cursor_factory()
    qualified_table = f"{catalog}.{schema}.{BRONZE_TABLE}"
    qualified_audit_table = request_audit_table_for(qualified_table)
    raw_key_values = ", ".join(sql_string(raw_key) for raw_key in raw_keys)
    run_id_values = ", ".join(sql_string(run_id) for run_id in run_ids)
    cursor.execute(
        f"""
        SELECT
            'bronze' AS evidence_kind,
            dag_run_id,
            raw_object_key,
            count(*) AS observed_rows,
            count(DISTINCT payload_hash) AS hash_variants,
            min(payload_hash) AS payload_hash,
            CAST(NULL AS bigint) AS declared_rows,
            CAST(NULL AS bigint) AS declared_row_variants
        FROM {qualified_table}
        WHERE source_id = {sql_string(SOURCE_ID)}
          AND dag_run_id IN ({run_id_values})
          AND raw_object_key IN ({raw_key_values})
        GROUP BY dag_run_id, raw_object_key

        UNION ALL

        SELECT
            'audit' AS evidence_kind,
            dag_run_id,
            raw_object_key,
            count(*) AS observed_rows,
            count(DISTINCT payload_hash) AS hash_variants,
            min(payload_hash) AS payload_hash,
            min(row_count) AS declared_rows,
            count(DISTINCT row_count) AS declared_row_variants
        FROM {qualified_audit_table}
        WHERE source_id = {sql_string(SOURCE_ID)}
          AND dag_run_id IN ({run_id_values})
          AND raw_object_key IN ({raw_key_values})
        GROUP BY dag_run_id, raw_object_key
        """
    )
    bronze_evidence: dict[tuple[str, str], tuple[int, int, str | None]] = {}
    audit_evidence: dict[tuple[str, str], tuple[int, int, str | None, int | None, int | None]] = {}
    for (
        evidence_kind,
        run_id,
        raw_key,
        observed_rows,
        hash_variants,
        payload_hash,
        declared_rows,
        declared_row_variants,
    ) in cursor.fetchall():
        evidence_key = (str(run_id), str(raw_key))
        if evidence_kind == "bronze":
            bronze_evidence[evidence_key] = (
                int(observed_rows),
                int(hash_variants),
                payload_hash,
            )
        elif evidence_kind == "audit":
            audit_evidence[evidence_key] = (
                int(observed_rows),
                int(hash_variants),
                payload_hash,
                int(declared_rows) if declared_rows is not None else None,
                int(declared_row_variants)
                if declared_row_variants is not None
                else None,
            )

    verified: dict[str, int] = {}
    for snapshot_run_id, (expected_rows, descriptors) in expectations.items():
        if all(
            _raw_object_evidence_matches(
                bronze_evidence=bronze_evidence.get((snapshot_run_id, raw_key)),
                audit_evidence=audit_evidence.get((snapshot_run_id, raw_key)),
                expected_hash=payload_hash,
                expected_rows=row_count,
            )
            for raw_key, payload_hash, row_count in descriptors
        ):
            verified[snapshot_run_id] = expected_rows
    print(
        "traffic_incident_bronze "
        f"preflight_verified_receipts={len(verified)} "
        f"preflight_checked_receipts={len(expectations)}"
    )
    return verified


def _raw_object_evidence_matches(
    *,
    bronze_evidence: tuple[int, int, str | None] | None,
    audit_evidence: tuple[int, int, str | None, int | None, int | None] | None,
    expected_hash: str,
    expected_rows: int,
) -> bool:
    if audit_evidence is None:
        return False
    audit_rows, audit_hash_variants, audit_hash, audit_declared_rows, audit_row_variants = audit_evidence
    if (
        audit_rows != 1
        or audit_hash_variants != 1
        or audit_hash != expected_hash
        or audit_declared_rows != expected_rows
        or audit_row_variants != 1
    ):
        return False
    if expected_rows == 0:
        return bronze_evidence is None
    if bronze_evidence is None:
        return False
    bronze_rows, bronze_hash_variants, bronze_hash = bronze_evidence
    return (
        bronze_rows == expected_rows
        and bronze_hash_variants == 1
        and bronze_hash == expected_hash
    )
