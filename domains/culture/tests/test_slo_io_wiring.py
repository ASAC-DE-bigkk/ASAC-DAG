"""slo.io v2 배선 — PostgresHook + Connection 경로 (#411).

호스트(에어플로 미설치)에서 상수·소스 배선만 검증한다. Hook import 는 함수 내부
(지연)라 모듈 import 는 에어플로 없이 성립하고, 실제 접속 경로는 컨테이너 라이브
AC(설계 §6)가 커버한다.
"""
import inspect

from culture_ingest.slo import io


def test_metadb_conn_id_is_domain_neutral():
    # 메타DB 는 팀 공용 — culture_ 접두어 없는 도메인 중립 id(§6.2 _shared 승격 대비).
    assert io.METADB_CONN_ID == "airflow_metadb"


def test_load_dag_runs_wires_hook_and_drops_env_path():
    src = inspect.getsource(io.load_dag_runs)
    assert "PostgresHook" in src, "v2 는 PostgresHook 경로여야 한다"
    assert "METADB_CONN_ID" in src, "conn id 는 모듈 상수를 써야 한다"
    # v1 의 차단된 env 직결 경로가 남아있으면 안 된다(#303).
    assert "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" not in src


def test_load_dag_runs_import_is_deferred():
    # 모듈 top-level 에 airflow import 가 없어야 호스트 pytest 가 성립한다.
    module_src = inspect.getsource(io)
    top_level = [
        line for line in module_src.splitlines()
        if line.startswith(("import airflow", "from airflow"))
    ]
    assert top_level == []
