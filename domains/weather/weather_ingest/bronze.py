import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from weather_ingest.common.runtime import (
    create_schema_if_needed,
    r2_env,
    r2_env_name,
    sql_int,
    sql_string,
    sql_timestamp,
    trino_cursor,
)
from weather_ingest.kma import SOURCE_ID, request_params_json


BRONZE_TABLE = "bronze_kma_vilage_fcst"
KST = ZoneInfo("Asia/Seoul")
MAX_KMA_INSERT_QUERY_CHARS = 900_000
PYICEBERG_CHUNK_ROWS = 50_000
KMA_BRONZE_COLUMNS = (
    "request_id",
    "source_id",
    "request_params_json",
    "place_id",
    "base_date",
    "base_time",
    "nx",
    "ny",
    "category",
    "fcst_date",
    "fcst_time",
    "fcst_value",
    "raw_object_key",
    "payload_hash",
    "http_status",
    "result_code",
    "result_msg",
    "total_count",
    "item_count",
    "collected_at",
    "load_date",
    "dag_run_id",
)


def ensure_kma_bronze_schema(cursor, qualified_table: str) -> None:
    for column_name, column_type in (
        ("request_params_json", "varchar"),
        ("load_date", "varchar"),
    ):
        cursor.execute(
            f"ALTER TABLE {qualified_table} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        )


def create_kma_bronze_table(cursor, catalog: str, schema: str) -> str:
    qualified_schema = f"{catalog}.{schema}"
    qualified_table = f"{qualified_schema}.{BRONZE_TABLE}"
    create_schema_if_needed(cursor, qualified_schema)
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            request_id varchar,
            source_id varchar,
            request_params_json varchar,
            place_id varchar,
            base_date varchar,
            base_time varchar,
            nx integer,
            ny integer,
            category varchar,
            fcst_date varchar,
            fcst_time varchar,
            fcst_value varchar,
            raw_object_key varchar,
            payload_hash varchar,
            http_status integer,
            result_code varchar,
            result_msg varchar,
            total_count integer,
            item_count integer,
            collected_at timestamp(6),
            load_date varchar,
            dag_run_id varchar
        )
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['load_date']
        )
        """
    )
    ensure_kma_bronze_schema(cursor, qualified_table)
    return qualified_table


def metadata_int(metadata: dict, key: str) -> int:
    value = metadata.get(key)
    if value is None or value == "":
        return 0
    return int(value)


def validate_kma_row_count(
    rows: list[dict],
    metadata: dict,
    nx: int,
    ny: int,
    *,
    allow_partial_page: bool = False,
) -> None:
    if not rows:
        raise RuntimeError("KMA API returned no forecast rows.")

    total_count = metadata_int(metadata, "total_count")
    parsed_count = len(rows)
    if total_count > parsed_count and not allow_partial_page:
        raise RuntimeError(
            "KMA bronze validation failed: "
            f"total_count={total_count}, parsed row_count={parsed_count}, nx={nx}, ny={ny}"
        )


def validate_kma_bronze_row_batch(batch: dict) -> None:
    validate_kma_row_count(
        batch["rows"],
        batch["metadata"],
        int(batch["nx"]),
        int(batch["ny"]),
        allow_partial_page=True,
    )


def iter_kma_bronze_records(row_batches: list[dict], dag_run_id: str):
    for batch in row_batches:
        metadata = batch["metadata"]
        rows = batch["rows"]
        base_date = batch["base_date"]
        base_time = batch["base_time"]
        nx = int(batch["nx"])
        ny = int(batch["ny"])
        collected_at = batch["collected_at"]
        validate_kma_bronze_row_batch(batch)
        request_params = request_params_json(
            base_date,
            base_time,
            nx,
            ny,
            page_no=batch.get("page_no"),
            num_of_rows=batch.get("num_of_rows"),
        )
        load_date = collected_at.astimezone(KST).strftime("%Y-%m-%d")
        collected_at_utc = collected_at.astimezone(timezone.utc).replace(tzinfo=None)
        for row in rows:
            yield {
                "request_id": batch["request_id"],
                "source_id": SOURCE_ID,
                "request_params_json": request_params,
                "place_id": batch["place_id"],
                "base_date": row.get("baseDate"),
                "base_time": row.get("baseTime"),
                "nx": int(row.get("nx")),
                "ny": int(row.get("ny")),
                "category": row.get("category"),
                "fcst_date": row.get("fcstDate"),
                "fcst_time": row.get("fcstTime"),
                "fcst_value": row.get("fcstValue"),
                "raw_object_key": batch["raw_object_key"],
                "payload_hash": batch["raw_hash"],
                "http_status": int(batch["http_status"]),
                "result_code": metadata.get("result_code"),
                "result_msg": metadata.get("result_msg"),
                "total_count": metadata_int(metadata, "total_count"),
                "item_count": metadata_int(metadata, "row_count"),
                "collected_at": collected_at_utc,
                "load_date": load_date,
                "dag_run_id": dag_run_id,
            }


def _pyiceberg_catalog():
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(
        "weather",
        uri=r2_env("R2_DATA_CATALOG_URI"),
        warehouse=r2_env("R2_DATA_CATALOG_WAREHOUSE"),
        token=r2_env("R2_DATA_CATALOG_TOKEN"),
        **{
            "s3.endpoint": r2_env("R2_ENDPOINT"),
            "s3.access-key-id": r2_env("R2_ACCESS_KEY_ID"),
            "s3.secret-access-key": r2_env("R2_SECRET_ACCESS_KEY"),
            "s3.region": os.environ.get(r2_env_name("R2_REGION"), "auto"),
        },
    )


def _pyiceberg_table(schema: str):
    return _pyiceberg_catalog().load_table(f"{schema}.{BRONZE_TABLE}")


def _kma_pyiceberg_delete_filter(dag_run_id: str):
    from pyiceberg.expressions import And, EqualTo

    return And(EqualTo("source_id", SOURCE_ID), EqualTo("dag_run_id", dag_run_id))


def _arrow_table(rows: list[dict]):
    import pyarrow as pa

    types = {
        "nx": pa.int32(),
        "ny": pa.int32(),
        "http_status": pa.int32(),
        "total_count": pa.int32(),
        "item_count": pa.int32(),
        "collected_at": pa.timestamp("us"),
    }
    fields = []
    arrays = []
    for column in KMA_BRONZE_COLUMNS:
        arrow_type = types.get(column, pa.string())
        fields.append(pa.field(column, arrow_type))
        arrays.append(pa.array([row[column] for row in rows], type=arrow_type))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def append_kma_bronze_row_batches_pyiceberg(
    schema: str,
    row_batches: list[dict],
    dag_run_id: str,
    *,
    delete_existing: bool = True,
    chunk_rows: int = PYICEBERG_CHUNK_ROWS,
    table=None,
) -> int:
    if not row_batches:
        return 0
    if chunk_rows <= 0:
        raise ValueError(f"chunk_rows must be positive: {chunk_rows}")
    for batch in row_batches:
        validate_kma_bronze_row_batch(batch)

    iceberg_table = table or _pyiceberg_table(schema)
    total = 0
    chunk: list[dict] = []

    with iceberg_table.transaction() as txn:
        if delete_existing:
            txn.delete(_kma_pyiceberg_delete_filter(dag_run_id))

        def flush() -> None:
            nonlocal total
            if not chunk:
                return
            txn.append(_arrow_table(chunk))
            total += len(chunk)
            chunk.clear()

        for record in iter_kma_bronze_records(row_batches, dag_run_id):
            chunk.append(record)
            if len(chunk) >= chunk_rows:
                flush()
        flush()
    return total


def insert_kma_bronze_rows(
    cursor,
    qualified_table: str,
    rows: list[dict],
    metadata: dict,
    request_id: str,
    place_id: str,
    base_date: str,
    base_time: str,
    nx: int,
    ny: int,
    raw_object_key: str,
    raw_hash: str,
    http_status: int,
    collected_at: datetime,
    dag_run_id: str,
    page_no: int | None = None,
    num_of_rows: int | None = None,
    delete_existing: bool = True,
    allow_partial_page: bool = False,
) -> int:
    validate_kma_row_count(rows, metadata, nx, ny, allow_partial_page=allow_partial_page)

    if delete_existing:
        cursor.execute(
            f"""
            DELETE FROM {qualified_table}
            WHERE source_id = {sql_string(SOURCE_ID)}
                AND dag_run_id = {sql_string(dag_run_id)}
                AND base_date = {sql_string(base_date)}
                AND base_time = {sql_string(base_time)}
                AND nx = {sql_int(nx)}
                AND ny = {sql_int(ny)}
            """
        )

    load_date = collected_at.astimezone(KST).strftime("%Y-%m-%d")
    request_params = request_params_json(
        base_date,
        base_time,
        nx,
        ny,
        page_no=page_no,
        num_of_rows=num_of_rows,
    )
    values = []
    for row in rows:
        values.append(
            "("
            f"{sql_string(request_id)}, "
            f"{sql_string(SOURCE_ID)}, "
            f"{sql_string(request_params)}, "
            f"{sql_string(place_id)}, "
            f"{sql_string(row.get('baseDate'))}, "
            f"{sql_string(row.get('baseTime'))}, "
            f"{sql_int(row.get('nx'))}, "
            f"{sql_int(row.get('ny'))}, "
            f"{sql_string(row.get('category'))}, "
            f"{sql_string(row.get('fcstDate'))}, "
            f"{sql_string(row.get('fcstTime'))}, "
            f"{sql_string(row.get('fcstValue'))}, "
            f"{sql_string(raw_object_key)}, "
            f"{sql_string(raw_hash)}, "
            f"{sql_int(http_status)}, "
            f"{sql_string(metadata.get('result_code'))}, "
            f"{sql_string(metadata.get('result_msg'))}, "
            f"{sql_int(metadata.get('total_count'))}, "
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
            place_id,
            base_date,
            base_time,
            nx,
            ny,
            category,
            fcst_date,
            fcst_time,
            fcst_value,
            raw_object_key,
            payload_hash,
            http_status,
            result_code,
            result_msg,
            total_count,
            item_count,
            collected_at,
            load_date,
            dag_run_id
        )
        VALUES {", ".join(values)}
        """
    )
    return len(rows)


def insert_kma_bronze_row_batches(
    cursor,
    qualified_table: str,
    row_batches: list[dict],
    dag_run_id: str,
    *,
    delete_existing: bool = True,
    max_insert_query_chars: int = MAX_KMA_INSERT_QUERY_CHARS,
) -> int:
    if not row_batches:
        return 0

    values: list[str] = []
    grid_filters = []
    inserted = 0

    for batch in row_batches:
        metadata = batch["metadata"]
        rows = batch["rows"]
        request_id = batch["request_id"]
        place_id = batch["place_id"]
        base_date = batch["base_date"]
        base_time = batch["base_time"]
        nx = int(batch["nx"])
        ny = int(batch["ny"])
        raw_object_key = batch["raw_object_key"]
        raw_hash = batch["raw_hash"]
        http_status = int(batch["http_status"])
        collected_at = batch["collected_at"]
        page_no = batch.get("page_no")
        num_of_rows = batch.get("num_of_rows")

        validate_kma_row_count(rows, metadata, nx, ny, allow_partial_page=True)

        request_params = request_params_json(
            base_date,
            base_time,
            nx,
            ny,
            page_no=page_no,
            num_of_rows=num_of_rows,
        )
        inserted += len(rows)
        for row in rows:
            values.append(
                "("
                f"{sql_string(request_id)}, "
                f"{sql_string(SOURCE_ID)}, "
                f"{sql_string(request_params)}, "
                f"{sql_string(place_id)}, "
                f"{sql_string(row.get('baseDate'))}, "
                f"{sql_string(row.get('baseTime'))}, "
                f"{sql_int(row.get('nx'))}, "
                f"{sql_int(row.get('ny'))}, "
                f"{sql_string(row.get('category'))}, "
                f"{sql_string(row.get('fcstDate'))}, "
                f"{sql_string(row.get('fcstTime'))}, "
                f"{sql_string(row.get('fcstValue'))}, "
                f"{sql_string(raw_object_key)}, "
                f"{sql_string(raw_hash)}, "
                f"{sql_int(http_status)}, "
                f"{sql_string(metadata.get('result_code'))}, "
                f"{sql_string(metadata.get('result_msg'))}, "
                f"{sql_int(metadata.get('total_count'))}, "
                f"{sql_int(metadata.get('row_count'))}, "
                f"{sql_timestamp(collected_at)}, "
                f"{sql_string(collected_at.astimezone(KST).strftime('%Y-%m-%d'))}, "
                f"{sql_string(dag_run_id)}"
                ")"
            )

        load_date_filter = collected_at.astimezone(KST).strftime("%Y-%m-%d")
        grid_filters.append(
            f"(base_date = {sql_string(base_date)} AND base_time = {sql_string(base_time)} "
            f"AND nx = {sql_int(nx)} AND ny = {sql_int(ny)})"
        )

    if delete_existing:
        cursor.execute(
            f"""
            DELETE FROM {qualified_table}
            WHERE source_id = {sql_string(SOURCE_ID)}
                AND dag_run_id = {sql_string(dag_run_id)}
                AND ({' OR '.join(sorted(set(grid_filters)))})
            """
        )

    insert_prefix = f"""
        INSERT INTO {qualified_table} (
            request_id,
            source_id,
            request_params_json,
            place_id,
            base_date,
            base_time,
            nx,
            ny,
            category,
            fcst_date,
            fcst_time,
            fcst_value,
            raw_object_key,
            payload_hash,
            http_status,
            result_code,
            result_msg,
            total_count,
            item_count,
            collected_at,
            load_date,
            dag_run_id
        )
        VALUES """
    max_values_chars = max(1, max_insert_query_chars - len(insert_prefix))
    chunk: list[str] = []
    chunk_chars = 0
    for value in values:
        value_chars = len(value) + (2 if chunk else 0)
        if chunk and chunk_chars + value_chars > max_values_chars:
            cursor.execute(f"{insert_prefix}{', '.join(chunk)}")
            chunk = []
            chunk_chars = 0
            value_chars = len(value)
        chunk.append(value)
        chunk_chars += value_chars
    if chunk:
        cursor.execute(f"{insert_prefix}{', '.join(chunk)}")
    return inserted


def verify_kma_bronze_runtime(
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
            count(*) AS row_count,
            count(DISTINCT raw_object_key) AS raw_object_count,
            max(collected_at) AS last_collected_at
        FROM {qualified_table}
        WHERE {" AND ".join(filters)}
        """
    )
    row = cursor.fetchone()
    row_count = int(row[0])
    if expected_rows is not None and row_count != expected_rows:
        raise RuntimeError(
            f"KMA bronze verification failed: expected_rows={expected_rows}, actual_rows={row_count}"
        )
    if expected_raw_objects is not None and int(row[1]) != expected_raw_objects:
        raise RuntimeError(
            "KMA bronze verification failed: "
            f"expected_raw_objects={expected_raw_objects}, actual_raw_objects={row[1]}"
        )
    if expected_raw_objects is None and expected_rows and raw_object_key and int(row[1]) != 1:
        raise RuntimeError(f"KMA bronze verification failed: raw_object_count={row[1]}")
    print(
        "weather_vilage_fcst_bronze "
        f"row_count={row[0]} raw_object_count={row[1]} last_collected_at={row[2]}"
    )
    return row_count
