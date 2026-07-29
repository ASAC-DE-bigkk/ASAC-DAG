"""#60 errors 존 이사 — 도메인별 env 로 지정, 미설정 시 구 위치 폴백 (#561).

`errors/` 는 6개 도메인이 공유한다. 공유 기본값을 바꾸면 오너 합의 없이 남의
도메인 경로까지 움직이므로, 도메인 스코프 env(`<DOMAIN>_ERROR_PREFIX`)만 본다.
설정한 도메인만 이동하고 나머지는 증명 가능하게 무변경 — 다른 오너는 자기 env
한 줄로 합류하면 된다. 컨벤션은 transit(#549)·commerce(#553)와 동일하다.
"""

from common.errors.sink import DEFAULT_PREFIX, errors_prefix


def test_falls_back_to_legacy_root_without_env():
    assert errors_prefix("weather", env={}) == DEFAULT_PREFIX == "errors"


def test_domain_scoped_env_moves_only_that_domain():
    env = {"WEATHER_ERROR_PREFIX": "ops/errors/weather"}

    assert errors_prefix("weather", env=env) == "ops/errors/weather"
    # 같은 env 라도 등록하지 않은 도메인은 그대로다.
    assert errors_prefix("traffic", env=env) == DEFAULT_PREFIX
    assert errors_prefix("citydata", env=env) == DEFAULT_PREFIX


def test_each_domain_is_configured_independently():
    env = {
        "WEATHER_ERROR_PREFIX": "ops/errors/weather",
        "TRAFFIC_ERROR_PREFIX": "ops/errors/traffic",
    }

    assert errors_prefix("weather", env=env) == "ops/errors/weather"
    assert errors_prefix("traffic", env=env) == "ops/errors/traffic"
    assert errors_prefix("culture", env=env) == DEFAULT_PREFIX


def test_blank_env_is_treated_as_unset():
    assert errors_prefix("weather", env={"WEATHER_ERROR_PREFIX": "   "}) == DEFAULT_PREFIX


def test_trailing_slash_is_stripped():
    env = {"TRAFFIC_ERROR_PREFIX": "ops/errors/traffic/"}

    assert errors_prefix("traffic", env=env) == "ops/errors/traffic"


def test_missing_domain_falls_back_to_legacy_root():
    env = {"WEATHER_ERROR_PREFIX": "ops/errors/weather"}

    assert errors_prefix(None, env=env) == DEFAULT_PREFIX
    assert errors_prefix("", env=env) == DEFAULT_PREFIX
