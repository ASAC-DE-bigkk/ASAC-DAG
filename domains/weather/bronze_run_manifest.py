from datetime import datetime, timezone


MANIFEST_TABLE = "bronze_collection_run_manifest"
STATUS_STARTED = "STARTED"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"


def sql_string(value: object) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def sql_int(value: object) -> str:
    if value is None or value == "":
        return "NULL"
    return str(int(value))


def sql_bool(value: bool) -> str:
    return "true" if value else "false"


def sql_timestamp(value: datetime | None) -> str:
    if value is None:
        return "NULL"
    utc_value = value.astimezone(timezone.utc)
    return "TIMESTAMP " + sql_string(utc_value.strftime("%Y-%m-%d %H:%M:%S.%f"))


def create_bronze_run_manifest_table(cursor, catalog: str, schema: str) -> str:
    qualified_schema = f"{catalog}.{schema}"
    qualified_table = f"{qualified_schema}.{MANIFEST_TABLE}"
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")
    except Exception as exc:
        if "Namespace already exists" not in str(exc):
            raise
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            source_id varchar,
            dag_id varchar,
            dag_run_id varchar,
            status varchar,
            is_publishable boolean,
            event_at timestamp(6),
            expected_rows integer,
            actual_rows integer,
            expected_raw_objects integer,
            actual_raw_objects integer,
            failure_reason varchar
        )
        WITH (
            format = 'PARQUET'
        )
        """
    )
    return qualified_table


def record_bronze_run_event(
    cursor,
    catalog: str,
    schema: str,
    *,
    source_id: str,
    dag_id: str,
    dag_run_id: str,
    status: str,
    is_publishable: bool = False,
    expected_rows: int | None = None,
    actual_rows: int | None = None,
    expected_raw_objects: int | None = None,
    actual_raw_objects: int | None = None,
    failure_reason: str | None = None,
    event_at: datetime | None = None,
) -> str:
    qualified_table = create_bronze_run_manifest_table(cursor, catalog, schema)
    cursor.execute(
        f"""
        DELETE FROM {qualified_table}
        WHERE source_id = {sql_string(source_id)}
            AND dag_run_id = {sql_string(dag_run_id)}
            AND status = {sql_string(status)}
        """
    )
    cursor.execute(
        f"""
        INSERT INTO {qualified_table} (
            source_id,
            dag_id,
            dag_run_id,
            status,
            is_publishable,
            event_at,
            expected_rows,
            actual_rows,
            expected_raw_objects,
            actual_raw_objects,
            failure_reason
        )
        VALUES (
            {sql_string(source_id)},
            {sql_string(dag_id)},
            {sql_string(dag_run_id)},
            {sql_string(status)},
            {sql_bool(is_publishable)},
            {sql_timestamp(event_at or datetime.now(timezone.utc))},
            {sql_int(expected_rows)},
            {sql_int(actual_rows)},
            {sql_int(expected_raw_objects)},
            {sql_int(actual_raw_objects)},
            {sql_string(failure_reason)}
        )
        """
    )
    return qualified_table


def failure_reason_from_context(context: dict) -> str:
    task = context.get("task_instance")
    task_id = getattr(task, "task_id", "unknown_task")
    exception = context.get("exception")
    exception_name = type(exception).__name__ if exception else "unknown_error"
    return f"{exception_name} in {task_id}"
