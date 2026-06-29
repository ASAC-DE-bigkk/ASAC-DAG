"""ASK SEOUL ingestion framework.

Importable (not Airflow-DAG) code for source ingestion across domains.

Layout:
  ingestion/common/        domain-agnostic framework (R2 sink, http, run context)
  ingestion/domains/<d>/   per-domain sources, dataset registry, orchestration

DAG entry files live at the repo root (= Airflow dags_folder) and import from
here. This package is excluded from DAG scanning via ``.airflowignore`` but stays
importable.
"""
