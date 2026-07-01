# observability DAG

This domain owns cross-domain reliability reports for the weather and traffic Bronze pipelines.

## DAG

| File | Role |
|---|---|
| `seoul_weather_traffic_reliability_report.py` | Airflow DAG entrypoint. Runs once a day at 09:00 KST when the dev target and Discord webhook env are configured. |
| `reliability_report/report.py` | Trino queries, SLO checks, Discord message formatting, and webhook delivery. |

## Runtime env

| Env | Default | Meaning |
|---|---:|---|
| `ASK_SEOUL_DISCORD_WEBHOOK_URL` | unset | Discord webhook URL. Treat as a secret. Do not commit it. |
| `ASK_SEOUL_REPORT_DAG_SCHEDULE` | auto | Override DAG schedule. Empty string disables schedule. |
| `ASK_SEOUL_REPORT_LOOKBACK_HOURS` | `24` | Lookback window for weather/traffic report queries. |
| `ASK_SEOUL_REPORT_EXPECTED_KMA_GRIDS` | `80` | Expected KMA grid count for Seoul coverage. |
| `ASK_SEOUL_REPORT_WEATHER_FRESHNESS_MINUTES` | `240` | Weather freshness SLO threshold. |
| `ASK_SEOUL_REPORT_TRAFFIC_FRESHNESS_MINUTES` | `15` | Traffic freshness SLO threshold. |

## Report fields

- DAG/report status: PASS or FAIL
- Weather coverage: latest KMA base time, distinct grid count, raw object count, row count, freshness minutes
- Traffic coverage: TOPIS request audit count, parsed rows, list total count, requested end index, zero-row normal response count, freshness minutes
- Impact tables: Bronze weather table, Bronze traffic table, traffic request audit table

This report is intentionally read-only. It does not mutate Bronze/Silver/Gold tables.
