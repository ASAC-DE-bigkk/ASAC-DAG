# Weather reliability cadence and Airflow log URL

## Scope

- Restore the dev weather reliability report default schedule to 09:00 KST daily.
- Make generated Airflow task log URLs use the host-published port `30585`.
- Preserve the existing traffic metadata fail-closed behavior; do not change traffic logic in this patch.

## Tasks

1. Update the weather schedule test to require `0 9 * * *` and run it to confirm the current hourly behavior fails.
2. Change the weather default schedule and its README contract to daily 09:00 KST.
3. Set Airflow API `base_url` to `http://localhost:30585` in the root compose environment.
4. Run focused weather tests and validate the rendered Compose configuration without printing secrets.

## Verification

- `pytest -q dags/domains/weather/tests/test_weather_reliability_report.py`
- `docker compose config` with only the resolved Airflow base URL displayed
- Inspect the resulting diff and preserve unrelated dirty worktree changes.
