from pathlib import Path


AIRFLOWIGNORE_PATH = Path(__file__).resolve().parents[3] / ".airflowignore"


def test_airflowignore_keeps_weather_and_traffic_as_the_only_dag_scan_roots():
    patterns = [
        line.strip()
        for line in AIRFLOWIGNORE_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]

    assert patterns == [
        "plugins/",
        "/*",
        "!/domains/",
        "!/domains/weather/",
        "!/domains/weather/**",
        "!/domains/traffic/",
        "!/domains/traffic/**",
    ]
