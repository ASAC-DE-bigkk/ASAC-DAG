# Weather·Traffic Gold delivery reliability pilot

Issue: [#431](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/431)

This pilot converts existing read-only execution evidence into a deterministic
seven-day report. It does not query an API, trigger an Airflow DAG, execute dbt,
write a warehouse table, or mutate R2.

## Evidence grain and source mapping

One input row means one scheduled Weather or Traffic run. Its unique key is
`domain × scheduled_run_id`.

| Input field | Operational evidence | Meaning |
| --- | --- | --- |
| `scheduled_run_id`, `scheduled_at` | Airflow scheduled run / Traffic R2 run ledger | Expected delivery slot |
| `bronze_status`, `is_publishable` | `bronze_collection_run_manifest` latest event | Bronze publication gate |
| `completeness_status` | request audit and manifest expected/actual counts | `COMPLETE`, `PARTIAL`, or `UNKNOWN` |
| `source_status` | request audit / source result | `SUCCESS`, `ZERO_ROW`, `API_FAILURE`, or `SOURCE_FAILURE` |
| `transform_status` | current-attempt dbt run/test artifact and transform task | `SUCCESS`, `FAILED`, or `CONTRACT_FAILURE` |
| `gold_query_status`, `gold_available_at`, `gold_row_count` | scoped final Trino query | Consumer-visible delivery evidence |
| `detected_at` | failure event or report observation time | MTTR start; falls back to `scheduled_at` |
| `api_failure_type`, `retry_count` | request/run metrics | Weekly failure profile |

Do not infer unavailable evidence as success, failure, or zero. Record JSON
`null`; the report renders it as `NOT_AVAILABLE` where a textual value is
required.

## Input document

All timestamps must contain a timezone. Use UTC `Z` timestamps when possible.

```json
{
  "observed_at": "2026-07-19T09:00:00Z",
  "lookback_days": 7,
  "runs": [
    {
      "domain": "traffic",
      "scheduled_run_id": "scheduled__2026-07-19T08:00:00+00:00",
      "scheduled_at": "2026-07-19T08:00:00Z",
      "bronze_status": "SUCCESS",
      "is_publishable": true,
      "completeness_status": "COMPLETE",
      "source_status": "ZERO_ROW",
      "transform_status": "SUCCESS",
      "gold_query_status": "SUCCESS",
      "gold_available_at": "2026-07-19T08:07:00Z",
      "gold_row_count": 0,
      "sla_minutes": 30,
      "detected_at": null,
      "api_failure_type": null,
      "retry_count": 0
    }
  ]
}
```

`ZERO_ROW` is a successful source result and requires a final row count of zero
when that count is available. `PARTIAL`, `API_FAILURE`, and `SOURCE_FAILURE`
cannot be marked publishable.

## Run the pilot

Run from the ASAC-DAG repository root:

```bash
python3 domains/weather/weather_ingest/delivery_reliability_pilot.py evidence.json --format markdown
python3 domains/weather/weather_ingest/delivery_reliability_pilot.py evidence.json --format json
python3 domains/weather/weather_ingest/delivery_reliability_pilot.py evidence.json --format csv
```

The command writes only to stdout. Redirecting output to a file is an explicit
operator action. Invalid status values, contradictory evidence, duplicate
grain, naive timestamps, and negative counts fail with exit code 2.

## Metric semantics

- Supply success: Bronze `SUCCESS + is_publishable=true`.
- Gold delivery: supply success, complete source evidence, successful dbt
  transform, and successful final query with an availability timestamp and row
  count.
- SLA delivery: Gold delivery latency is within the row's `sla_minutes`.
- Freshness: `gold_available_at - scheduled_at`; report exposes sample count,
  median, and maximum.
- MTTR: failure `detected_at` (or scheduled time when unavailable) to the first
  successful Gold delivery after that detection time in the same domain.
- Every rate includes its numerator and evaluable denominator. An empty
  denominator produces `NOT_AVAILABLE`, never `0%`.

## Pilot limitations

The first version deliberately accepts normalized evidence instead of adding a
new warehouse metrics table or a second orchestration framework. During the
seven-day pilot, compare sampled input rows with the source manifest, request
audit, current-attempt dbt artifact, and final Trino result. Automate source
adapters only after the team confirms that the definitions explain real runs.
