# Environments

> **현행 = 운영(prod) 단일 환경.** 아래 값은 루트 `.env` 활성 줄과 운영 버킷 실측
> (2026-08-01)을 대조해 적었다. 과거 문서가 설명하던 "dev/prod 두 환경을 버킷·토큰으로
> 가른다"는 구성은 **더 이상 동작 중인 구성이 아니다** — §3 참조.

## 1. 현행 런타임 (루트 `.env` 활성값)

| 축 | 값 | 출처 |
|---|---|---|
| 스토리지 백엔드 | `r2` | `COMMERCE_STORAGE_BACKEND=r2` |
| 오브젝트 버킷 | **`seoul`** | `R2_BUCKET_NAME=seoul` → `.env.commerce` 가 `R2_BUCKET` 으로 매핑 |
| 버킷 안 공통 접두 | (없음) | `COMMERCE_STORAGE_PREFIX=` (빈 값) |
| Iceberg 카탈로그 | **`iceberg`** | `TRINO_ICEBERG_CATALOG=iceberg` |
| 실행 타깃 | **`prod`** | `DBT_TARGET=prod` · `ASK_SEOUL_TARGET=prod` · `COMMERCE_DBT_TARGET=prod` (셋 다 일치) |
| R2 자격증명·엔드포인트 | 루트 `R2_ENDPOINT` / `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` | 이름이 같아 `.env.commerce` 매핑 없이 **프로세스 env 를 그대로 상속** |

**`R2_DEV_*` 는 폐지됐다.** 호스트가 ENV2 개편에서 그 키를 없앴고(Trino 카탈로그 파일도
canonical `R2_*` 한 세트만 읽는다), 코드에 남아 있던 dev 우선 해석 규칙도 제거했다
(ASAC-DAG#647). 지금은 **자격증명 키 이름이 배포 환경을 담지 않는다** — `R2_*` 한 벌이고
어느 버킷을 가리키는지는 그 **값**이 정한다.

> ⚠️ `R2_DEV_*` 를 되살리지 말 것. 채우는 순간 같은 날짜의 운영 기록이 두 버킷으로 갈린다
> (ASK-Seoul#78 `Z-7`). dev 로 되돌릴 때는 키를 바꾸는 게 아니라 위 4개 키의 **값**을 dev 것으로
> 채우고 `TRINO_ICEBERG_CATALOG` 를 바꾼다(§3).

### 타깃 이름이 세 개인 이유

- `DBT_TARGET` — 공유(전 도메인) 계약. dbt 프로필과 `runtime_guard` 가 본다.
- `ASK_SEOUL_TARGET` — weather/traffic 계열이 쓰던 별칭. `runtime_guard.resolve_runtime_target()`
  은 둘이 **엇갈리면 거부**한다.
- `COMMERCE_DBT_TARGET` — commerce 전용 우선값(`warehouse._is_dev()`·silver/gold DAG).

현재 셋 다 `prod` 라 어느 경로로 읽어도 같은 답이 나온다. **셋 중 하나만 바꾸면 안 된다** —
바꿀 때는 셋을 함께 바꾼다.

## 2. 운영 버킷 실측 (2026-08-01, `seoul`)

```text
seoul/
├─ raw/          # 원본 랜딩 — commerce 는 load_date= 파티션 28개
├─ ops/          # 운영 산출물 (아래 표)
├─ reference/    # 파이프라인 입력
└─ __r2_data_catalog/   # Iceberg 창고 (사람이 손대지 않음)
```

| ops 카테고리 | 건수 | 축 순서 | 날짜 칸 | 도메인 |
|---|---:|---|---|---|
| `control` | 7,908 (2.6GB) | — | 없음(정상, `P-5`) | state 7,511 · checkpoints 397 |
| `metrics` | 20,031 | 도메인 우선 | `observed_date=` | transit 17,338 · weather 1,678 · traffic 1,011 |
| `runs` | 9,391 | **날짜 우선** (`P-7` 미전환) | `observed_date=` | citydata |
| `reports` | 4,222 | 도메인 우선 | **`load_date=` 4,151** · `observed_date=` 66 · `date=` 5 | citydata 4,151 · culture 66 · traffic 3 · weather 2 |
| `product-events` | 2,577 | **날짜 우선** | `observed_date=` | 공통 모듈 |
| `logs` | 206 (12.6MB) | 도메인 우선 | **`load_date=`** (신규는 `observed_date=` 로 전환됨) | **commerce 단독** |
| `errors` | 95 | 도메인 우선 | `observed_date=` | transit 40 · citydata 22 · traffic 17 · weather 13 · culture 3 |
| `recovery` | 7 | 도메인 우선 | `observed_date=` | traffic |
| `product-health` | 7 | **날짜 우선** | `observed_date=` | 공통 모듈 |

- 관측 계열 합계 **36,536건**, 최근 일별 **8,000~10,000건**대(7-29 이후). 7-28 이전은 하루 200건 남짓.
- `raw/commerce/` 는 **`load_date=` 파티션 28개뿐** — 금지 표기(`2026/06/30` 분절 · `20260630`
  무구분)는 **0건**이다(`P-1`·`P-2` 준수).
- ops 존 밖 구경로(`runs/`·`errors/`·`metrics/` 루트)도 **0건**.
- 운영 기록 적재기(`common/ops/ingest.py`)의 경로 판독률 **36,536/36,536 = 100%**.

## 3. dev(`seoul-dev` / `iceberg_dev`)는 **동결된 롤백 지점**이다

2026-07-28 전환(change-log §79) 이후 **신규 쓰기는 전부 `seoul` 로만** 간다. `seoul-dev` 는
전환 직전 상태가 그대로 남아 있고 **현행과 저장 구조가 다르다**(구 `YYYY/MM/DD` raw 레이아웃,
ops 존 밖 루트 `runs/`·`errors/` 등).

- **정리·삭제 대상이 아니다.** 운영 이관이 완전히 종결될 때까지 **되돌아갈 지점**으로 보존한다.
  정리 시점은 이관 종결 후 오너가 정한다.
- 그렇다고 **현행도 아니다.** 현황 판단·감사·적재의 기준은 항상 `seoul` 이며, `seoul-dev` 수치를
  현황으로 인용하지 않는다. 문서에서 dev 를 "prod 와 짝을 이루는 동작 중인 환경"으로 쓰지 않는다.
- 되돌릴 때는 **키 이름이 아니라 값을 바꾼다** — `R2_BUCKET_NAME`·`R2_ENDPOINT`·`R2_ACCESS_KEY_ID`·
  `R2_SECRET_ACCESS_KEY` 에 dev 값을 넣고 `TRINO_ICEBERG_CATALOG=iceberg_dev` 로 바꾼다.
  **`R2_DEV_*` 는 되살리지 않는다**(§1 참조 — 두 버킷 동시 기록을 만든다).

`local` 백엔드(`COMMERCE_STORAGE_BACKEND=local`)는 R2 자격증명 없이 도는 **코드 경로**로 남아
있다(신규 기여자 스모크용). 컨테이너 `/opt/airflow/data` 는 호스트 볼륨 마운트가 없어 산출물이
컨테이너 수명과 함께 사라진다 — 영속이 필요하면 `r2` 를 쓴다.

## 4. 환경변수가 흘러오는 경로

1. **호스트 프로세스 env** — 루트 `.env`(`env_file: .env`)와 compose `environment:`.
   Airflow/Postgres/Trino/dbt/R2 와 `commerce 전용값` 블록이 여기 있다.
2. **`.env.commerce`** — DAG 임포트 시 `load_commerce_env()` 가 루트 값을 **코드 이름으로 매핑**
   (프로세스 env 우선). 실제 매핑은 8줄뿐이고, 이름이 같은 R2 자격증명은 매핑하지 않는다.

키 전체와 기본값: [configuration.md](configuration.md). 저장 경로 규칙:
[storage.md](../architecture/storage.md). 배포: [deploy-prod.md](../operations/deploy-prod.md) ·
[deploy-local.md](../operations/deploy-local.md).
