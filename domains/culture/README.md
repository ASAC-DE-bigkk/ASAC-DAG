# culture domain — bronze ingestion

Self-contained Airflow pipeline that lands the culture domain's raw source data
(KOPIS + Seoul Open Data) into R2 under `bronze/culture/`. Everything for this
domain lives under `domains/culture/`, matching the repo's per-domain layout.

## Layout

```
domains/culture/
├─ culture_bronze_ingest.py        # DAG entry (the ONLY file Airflow scans here)
├─ culture_ingest/                 # importable package (excluded from DAG scan)
│  ├─ common/                      #   domain-agnostic: R2 sink, http, run context
│  │  ├─ config.py                 #     R2Settings, RunContext, landing_prefix
│  │  ├─ http.py                   #     session/retry, Page
│  │  └─ landing.py                #     Sink (R2/Local), Landing, DatasetResult
│  └─ source/                      #   culture source layer
│     ├─ config.py                 #     LANDING_ROOT, source API keys
│     ├─ clients.py                #     KOPIS (XML) + Seoul OA (JSON) clients
│     ├─ datasets.py               #     dataset registry (single source of truth)
│     └─ ingest.py                 #     orchestration + run_batch / ingest_one
├─ scripts/run_culture_ingest.py   # local CLI (dry-run / R2 landing)
└─ .airflowignore                  # keep only the DAG file in DAG scanning
```

Airflow scans the dags folder recursively, so this DAG is picked up at
`domains/culture/culture_bronze_ingest.py`. `culture_ingest/` and `scripts/`
are excluded from DAG scanning via `.airflowignore` but stay importable — the
DAG puts `domains/culture/` on `sys.path` and imports `culture_ingest.*`.

## Secrets

The DAG reads keys from the environment. `docker-compose` injects `sample/.env`
(`env_file:`) into every Airflow container, so `KOPIS_SERVICE_KEY`,
`SEOUL_OPENAPI_KEY`, and `R2_DEV_*` are available as `os.environ`. Never commit
keys; `.env` is gitignored in the `sample` superproject.

## R2 layout (bronze raw)

```
bronze/culture/<source>/<dataset>/load_date=<KST>/ingest_ts=<UTC>/page-NNNN.<xml|json>
                                                                 /_manifest.json
```
`ingest_ts` isolates each run, so retries/partial runs don't corrupt prior data.

## Run locally

```bash
# from domains/culture/ — dry-run to a local dir (no R2)
python scripts/run_culture_ingest.py --dry-run --local-dir ./_dryrun \
    --env-file ../../../sample/.env --date-from 20260601 --date-to 20260628

# real landing to seoul-dev
python scripts/run_culture_ingest.py --target dev --env-file ../../../sample/.env \
    --date-from 20260101 --date-to 20261231 --include-detail --max-detail 200
```
