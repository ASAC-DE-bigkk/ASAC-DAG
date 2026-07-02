"""mapped XCom pull 결과 정규화 (#87).

Airflow 3에서 매핑 태스크가 1개 인스턴스면 ``xcom_pull(task_ids=...)`` 이
리스트가 아니라 dict 하나를 돌려줄 수 있다. 그대로 순회하면 dict 키(문자열)가
summary 행세를 해 report 가 TypeError 로 죽는다 — 항상 list[dict] 로 정규화한다.
"""
from culture_ingest.source.ingest import normalize_mapped_results


def test_single_dict_becomes_one_element_list():
    s = {"name": "kopis_facility", "rows": 1682}
    assert normalize_mapped_results(s) == [s]


def test_list_passes_through_with_none_holes_removed():
    a, b = {"name": "a"}, {"name": "b"}
    assert normalize_mapped_results([a, None, b]) == [a, b]


def test_empty_inputs_give_empty_list():
    assert normalize_mapped_results(None) == []
    assert normalize_mapped_results([]) == []
