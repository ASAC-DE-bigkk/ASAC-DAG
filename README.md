# ASAC-DAG — Airflow DAGs & ingestion framework

This repo is mounted at `/opt/airflow/dags` (the Airflow dags folder, read-only).
Airflow scans it recursively for DAG files; `ingestion/` and `scripts/` are
excluded from DAG scanning via `.airflowignore` but stay importable.

## Layout (multi-domain)

```
ASAC-DAG/
├─ <domain>_*.py                 # DAG entry files at repo root (one per pipeline)
│   ├─ culture_bronze_ingest.py  #   culture raw -> R2 bronze/culture
│   └─ dbt_trino_iceberg_smoke.py
├─ ingestion/                    # importable framework (NOT scanned for DAGs)
│   ├─ common/                   #   domain-agnostic: R2 sink, http, run context
│   │   ├─ config.py             #     R2Settings, RunContext, landing_prefix
│   │   ├─ http.py               #     session/retry, Page
│   │   └─ landing.py            #     Sink (R2/Local), Landing, DatasetResult
│   └─ domains/
│       └─ culture/              #   culture domain
│           ├─ config.py         #     LANDING_ROOT, source API keys
│           ├─ clients.py        #     KOPIS (XML) + Seoul OA (JSON) clients
│           ├─ datasets.py       #     dataset registry (single source of truth)
│           └─ ingest.py         #     orchestration + run_batch / ingest_one
├─ scripts/run_culture_ingest.py # local CLI (dry-run / R2 landing)
└─ .airflowignore
```

## Secrets

DAGs read keys from the environment. `docker-compose` injects `sample/.env`
(`env_file:`) into every Airflow container, so `KOPIS_SERVICE_KEY`,
`SEOUL_OPENAPI_KEY`, and `R2_DEV_*` are available as `os.environ`. Never commit
keys; `.env` is gitignored in the `sample` superproject.

## R2 layout (bronze raw)

```
bronze/<domain>/<source>/<dataset>/load_date=<KST>/ingest_ts=<UTC>/page-NNNN.<xml|json>
                                                                   /_manifest.json
```
`ingest_ts` isolates each run, so retries/partial runs don't corrupt prior data.

## Run locally (culture)

```bash
# dry-run to local dir (no R2)
python scripts/run_culture_ingest.py --dry-run --local-dir ./_dryrun \
    --env-file ../sample/.env --date-from 20260601 --date-to 20260628

# real landing to seoul-dev
python scripts/run_culture_ingest.py --target dev --env-file ../sample/.env \
    --date-from 20260101 --date-to 20261231 --include-detail --max-detail 200
```

## Add a new domain

1. `ingestion/domains/<domain>/` with `config.py` (LANDING_ROOT + source keys),
   `clients.py`, `datasets.py` (registry), `ingest.py` (reuse `ingestion.common`).
2. A DAG entry file `<domain>_*_ingest.py` at the repo root (copy
   `culture_bronze_ingest.py`, swap the domain imports).
3. Add any new source keys to `sample/.env` (gitignored).
