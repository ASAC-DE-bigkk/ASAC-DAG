from __future__ import annotations


class RecordingCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.statements = []

    def execute(self, sql):
        self.statements.append(" ".join(sql.split()))

    def fetchone(self):
        return self.rows.pop(0)


class _AirflowField:
    def __init__(self, name):
        self.name = name

    def __eq__(self, value):
        return (self.name, "eq", value)

    def __ge__(self, value):
        return (self.name, "ge", value)

    def __le__(self, value):
        return (self.name, "le", value)


class _ScheduledDagRun:
    dag_id = _AirflowField("dag_id")
    run_type = _AirflowField("run_type")
    logical_date = _AirflowField("logical_date")

    def __init__(self, *, state, logical_date, run_id, task_instances):
        self.state = state
        self.logical_date = logical_date
        self.run_id = run_id
        self._task_instances = task_instances

    def get_task_instances(self, session=None):
        return list(self._task_instances)


class _AirflowQuery:
    def __init__(self, rows):
        self.rows = rows
        self.filters = []

    def filter(self, *conditions):
        self.filters.extend(conditions)
        return self

    def order_by(self, *_columns):
        return self

    def all(self):
        return list(self.rows)


class _AirflowSession:
    def __init__(self, rows):
        self.query_calls = []
        self.query_result = _AirflowQuery(rows)

    def query(self, model):
        self.query_calls.append(model)
        return self.query_result


class _FailingAirflowSession:
    def __init__(self, secret):
        self.secret = secret

    def query(self, _model):
        raise RuntimeError(f"metadata connection failed: token={self.secret}")


class _SessionContext:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc, tb):
        return False


def stub_report_dependencies(monkeypatch):
    from traffic_ingest.reliability import report as composition

    monkeypatch.setattr(
        composition,
        "collect_dag_run_summary",
        lambda *args: {
            "dag_id": args[2],
            "success": 3,
            "failed": 0,
            "running": 1,
            "last_success_at": "2026-07-02 08:55:00+00:00",
            "last_publishable_at": "2026-07-02 08:55:00+00:00",
            "latest_dag_run_id": "scheduled__2026-07-02T08:55:00+00:00",
            "latest_status": "SUCCESS",
            "latest_is_publishable": True,
            "latest_event_at": "2026-07-02 08:55:00+00:00",
            "latest_terminal_dag_run_id": "scheduled__2026-07-02T08:55:00+00:00",
            "latest_terminal_status": "SUCCESS",
            "latest_terminal_is_publishable": True,
            "latest_terminal_event_at": "2026-07-02 08:55:00+00:00",
        },
    )
    monkeypatch.setattr(
        composition,
        "collect_scheduled_run_summary",
        lambda *args: {
            "expected": 4,
            "success": 3,
            "failed": 0,
            "running": 1,
            "failures": [],
        },
    )
