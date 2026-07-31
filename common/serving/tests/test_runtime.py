from common.serving.runtime import HttpSmokeTester


def test_missing_api_base_url_is_not_evaluated_smoke():
    assert HttpSmokeTester("").check("gold_weather_place_current_outlook") == "not_evaluated"
