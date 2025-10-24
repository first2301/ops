# DataOps - Hadoop + Airflow + PostgreSQL

이 프로젝트는 Hadoop HDFS에 저장된 음식점 데이터를 Airflow를 통해 전처리하고 PostgreSQL에 저장하는 데이터 파이프라인입니다.

## 아키텍처

```
Hadoop HDFS (CSV 데이터) → Airflow (전처리) → PostgreSQL (저장)
```

## 구성 요소

- **Hadoop**: HDFS에 CSV 데이터 저장
- **Airflow**: 데이터 전처리 워크플로우 관리
- **PostgreSQL**: 전처리된 데이터 저장

## 실행 방법

### 1. 네트워크 생성
```bash
docker network create dataops-net
```

### 2. Hadoop 클러스터 시작
```bash
cd dataops
docker-compose -f compose.hadoop.yaml up -d
```

### 3. 데이터 저장용 PostgreSQL 시작
```bash
docker-compose -f compose.postgresql.yaml up -d
```

### 4. Airflow 및 메타데이터용 PostgreSQL 시작
```bash
docker-compose -f compose.airflow.yaml up -d
```

### 5. 서비스 접속

- **Airflow UI**: http://localhost:8080
  - Username: airflow
  - Password: airflow

- **Hadoop NameNode**: http://localhost:9870

- **데이터 저장용 PostgreSQL**: localhost:5432
  - Database: dataops
  - Username: dataops
  - Password: dataops
  - 용도: 전처리된 음식점 데이터 저장

- **Airflow 메타데이터용 PostgreSQL**: 내부 네트워크
  - Database: airflow
  - Username: airflow
  - Password: airflow
  - 용도: Airflow 워크플로우 메타데이터 저장

## 데이터 경로

### HDFS 경로
- 원본 데이터: `hdfs://namenode:8020/data/restaurant/fulldata_07_24_04_P_일반음식점.csv`
- 전처리된 데이터: `hdfs://namenode:8020/data/restaurant/restaurant_YYYY.csv`

### PostgreSQL 테이블
- 테이블명: `restaurant_data`
- 컬럼: id, 개방서비스명, 인허가일자, 폐업일자, 영업상태명, 소재지, 시설명, 구분, 남성종사자수, 여성종사자수, 시도, 시군구, 읍면동, 도로명, 번지, num, year, created_at

## Airflow Connection 설정

Airflow UI에서 데이터 저장용 PostgreSQL 연결을 설정해야 합니다:

1. Airflow UI → Admin → Connections
2. Connection 추가:
   - **Connection Id**: `postgres_dataops`
   - **Connection Type**: `Postgres`
   - **Host**: `postgresql` (컨테이너명)
   - **Port**: `5432`
   - **Database**: `dataops`
   - **Login**: `dataops`
   - **Password**: `dataops`

## DAG 실행

1. Airflow UI에 접속
2. `restaurant_data_preprocessing_v2` DAG 활성화
3. DAG 실행

## 주요 기능

- **데이터 로딩**: HDFS에서 원본 CSV 파일 읽기
- **전처리**: 연도별 데이터 분리 및 주소 파싱
- **저장**: PostgreSQL에 전처리된 데이터 저장
- **검증**: 데이터 품질 검증

## 문제 해결

### Hadoop 연결 오류
- Hadoop 클러스터가 실행 중인지 확인
- 네트워크 연결 상태 확인
- HDFS 경로가 올바른지 확인

### Airflow DAG 오류
- 로그에서 상세 오류 메시지 확인
- PostgreSQL 연결 설정 확인
- 필요한 패키지가 설치되었는지 확인