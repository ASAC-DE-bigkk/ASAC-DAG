from __future__ import annotations


class RecordingCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.statements = []

    def execute(self, sql):
        self.statements.append(" ".join(sql.split()))

    def fetchone(self):
        return self.rows.pop(0)


def stub_report_dependencies(monkeypatch):
    from weather_ingest.reliability import report as composition

    monkeypatch.setattr(
        composition,
        "collect_pipeline_stages",
        lambda **_kwargs: {"source": "marquez", "status": "PASS", "stages": []},
    )
    monkeypatch.setattr(composition, "load_recent_history", lambda *_args: [])

    monkeypatch.setattr(
        composition,
        "collect_dag_run_summary",
        lambda *args: {
            "dag_id": args[2],
            "success": 2,
            "failed": 1,
            "running": 0,
            "expected_raw_objects": 160,
            "actual_raw_objects": 160,
            "last_success_at": "2026-07-02 08:20:00+00:00",
            "last_publishable_at": "2026-07-02 08:20:00+00:00",
            "latest_dag_run_id": "scheduled__2026-07-02T08:00:00+00:00",
            "latest_status": "SUCCESS",
            "latest_is_publishable": True,
            "latest_event_at": "2026-07-02 08:20:00+00:00",
        },
    )
