"""Culture domain ingestion package.

Importable (not Airflow-DAG) code for the culture source ingestion.

Layout:
  culture_ingest/common/   domain-agnostic framework (R2 sink, http, run context)
  culture_ingest/source/   culture sources, dataset registry, orchestration

The DAG entry file ``culture_bronze_ingest.py`` lives one level up (in
``domains/culture/``, on the Airflow dags path) and imports from here. This
package is excluded from DAG scanning via ``.airflowignore`` but stays importable.
"""
