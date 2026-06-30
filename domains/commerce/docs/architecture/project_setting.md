# 프로젝트 구조 규약 (heritage)

다른 프로젝트에서도 그대로 따라갈 수 있는 **dags 폴더 구성 규약**이다. 핵심 목표:
DAG 가 쓰는 모든 것(코드·설정·테스트·문서·런타임 인자)을 **카테고리별 자립 단위**로 묶어,
`dags/` 폴더만 다른 Airflow 프로젝트로 옮겨도 **PYTHONPATH 설정 없이 바로 실행**되게 한다.
(`dags/` 자체는 ASAC-DAG git 서브모듈이라 호스트 프로젝트와 분리돼 있다.)

## 1. 레이아웃 — `dags/domains/<category>/`

dags 하위 `domains/` 를 카테고리별 폴더로 나눈다(예: `commerce`). 각 카테고리는 자립 단위다.

```text
dags/
└─ domains/
   └─ <category>/                  # 예: commerce
      ├─ <name>_dag.py             # DAG 정의(들). 자기 include 를 sys.path 에 부트스트랩 + env 적재.
      ├─ include/                  # 이 카테고리의 import 루트(= PYTHONPATH 대상)
      │  ├─ common/                # 설정·env·스토리지·경로·스키마·레지스트리 등 공유
      │  ├─ bronze/                # 원본 수집 단계 패키지
      │  └─ silver/                # 가공 단계 패키지   (계층/관심사별로 패키지 분리)
      ├─ config/                   # 데이터 레지스트리 등(YAML, 코드 밖 외부화)
      ├─ tests/                    # 단위 테스트 (conftest 가 include 를 path 에 올림)
      ├─ docs/                     # 이 카테고리 전용 문서
      ├─ requirements.txt          # 번들 런타임 의존성 명세
      ├─ .env.commerce(.example)   # 런타임 환경변수(실파일은 gitignore)
      └─ .airflowignore            # include/ config/ tests/ docs/ 파싱 제외
```

> `include/` 아래는 **래퍼 패키지를 두지 않고** 관심사별 패키지(`common`/`bronze`/`silver` …)를
> 직접 둔다. 단계(category)별로 폴더를 나눠 수집/가공에 필요한 코드를 분리한다.
> 런타임 인자는 호스트 루트 `.env` 가 아니라 **번들의 `.env.commerce`** 로 자립시킨다
> ([configuration.md](../configuration/configuration.md)).

## 2. import 규약 (이식성의 핵심)

DAG 파일은 **자기 카테고리의 `include` 를 런타임에 sys.path 에 올린다.** 그래서 호스트
Airflow 의 PYTHONPATH 설정에 의존하지 않는다.

```python
# dags/domains/<category>/<name>_dag.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent / "include"))

from common.env import load_commerce_env   # 번들 .env 적재(프로세스 env 우선)
load_commerce_env()

from bronze import bronze_tasks        # include/bronze
from common import registry            # include/common
from silver import silver_tasks        # include/silver
```

- 패키지 간 참조도 top-level 로: `from common.settings import get_settings`.
- 부트스트랩 직후 `load_commerce_env()` 로 번들 `.env.commerce` 를 적재한다 — 호스트 루트
  `.env` 에 카테고리 변수를 넣지 않아도 `dags/` 와 함께 인자가 따라온다([configuration.md](../configuration/configuration.md)).
- 테스트는 `tests/conftest.py` 가 동일하게 `include` 를 path 에 올린다.
- 설정/env 파일 경로는 코드 위치 기준 상대(`Path(__file__).resolve().parents[N]/...`)로
  찾되, 환경변수로 override 가능하게 한다(`COMMERCE_REGISTRY_PATH`/`COMMERCE_ENV_FILE`).

## 3. `.airflowignore`

각 카테고리 폴더에 두고, DAG 가 아닌 디렉터리를 파싱에서 제외한다(정규식, 해당 폴더 기준).

```text
^include/
^config/
^tests/
^docs/
```

## 4. 컨테이너 설정

- 마운트: `./dags` 만 마운트(코드·설정·테스트·문서·`.env.commerce` 가 그 안에 자립).
- `PYTHONPATH`: 활성 카테고리 include(예: `/opt/airflow/dags/domains/commerce/include`)를
  **편의용**으로 지정. DAG 는 자체 부트스트랩하므로 필수는 아니다(이식성은 부트스트랩이 보장).
- 이미지: dags 를 baking 한다면 `COPY dags/ /opt/airflow/dags/` 한 줄. 카테고리가 늘어도 변경
  불필요(현재 호스트 컴포즈는 baking 대신 `./dags` 바인드 마운트).

## 5. 카테고리 추가법

1. `dags/domains/<new>/` 생성, `include/{common,bronze,silver,...}` 구성.
2. `<new>_dag.py` 에 §2 부트스트랩(+`load_commerce_env()`) 추가.
3. `config/`·`tests/`·`docs/`·`requirements.txt`·`.env.<new>(.example)`·`.airflowignore` 채움.
4. (선택) `PYTHONPATH` 를 새 카테고리로 바꾸거나, 테스트는 카테고리별로 실행.

## 6. 멀티 카테고리 주의

패키지명(`common`/`bronze`/`silver`)은 카테고리마다 같다. Airflow 는 **DAG 파일을 각각
별도 프로세스로 파싱/실행**하므로 카테고리 간 모듈명 충돌이 없다. 단, **한 프로세스에서 두
카테고리를 동시에 import 하지 말 것**(테스트는 카테고리별로 분리 실행). 충돌이 우려되면
카테고리 고유 접두 패키지(예: `commerce_common`)로 바꾼다.

## 7. 이식성 체크리스트

- [ ] DAG 가 `Path(__file__).parent/"include"` 를 sys.path 에 올리는가
- [ ] DAG 가 `load_commerce_env()` 로 번들 `.env` 를 적재하는가(호스트 루트 `.env` 비의존)
- [ ] 외부 절대경로/호스트 PYTHONPATH 에 의존하지 않는가
- [ ] config/env 경로가 코드 기준 상대 + env override 인가
- [ ] `.airflowignore` 로 include/config/tests/docs 가 제외되는가
- [ ] `dags/` 를 빈 Airflow 프로젝트에 넣고 DagBag import 에러가 0 인가

이 규약의 공유 진입점은 [Share.md](../../Share.md) 이며, 에이전트용 요약은 [CLAUDE.md](../../CLAUDE.md) §19 에 있다.
