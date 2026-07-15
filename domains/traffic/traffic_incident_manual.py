"""Airflow entrypoints for manual Traffic Incident recollect and raw replay."""

from __future__ import annotations

import os
import sys


DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)
DOMAINS_DIR = os.path.dirname(DAG_DIR)
if DOMAINS_DIR not in sys.path:
    sys.path.insert(0, DOMAINS_DIR)
DAGS_ROOT_DIR = os.path.dirname(DOMAINS_DIR)
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from traffic_ingest.manual_incident import (  # noqa: E402
    build_traffic_backfill_dag,
    build_traffic_recollect_dag,
)


recollect_dag = build_traffic_recollect_dag()
backfill_dag = build_traffic_backfill_dag()
