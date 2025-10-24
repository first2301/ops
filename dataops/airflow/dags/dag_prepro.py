from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import random
import os
from typing import Dict
import requests
from io import StringIO
import logging
import tempfile

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DAG 기본 설정
default_args = {
    'owner': 'dataops',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# WebHDFS 설정
HDFS_CONFIG = {
    'namenode_host': 'namenode',
    'namenode_port': 9870,
    'timeout': 60,
    'max_retries': 3
}

def split_address(address):
    """주소를 분리하는 함수"""
    if not address or pd.isna(address):
        return None, None, None, None, None
        
    parts = address.split()
    
    if len(parts) >= 4:
        city_name = parts[0]  # 예: 전라북도
        district = parts[1]  # 예: 남원시
        town = parts[2]  # 예: 대강면
        road_name = ' '.join(parts[3:-1])  # 예: 대강월산길
        bungee = parts[-1]  # 예: 37-16
        
        return city_name, district, town, road_name, bungee
    else:
        return None, None, None, None, None

def read_csv_from_hdfs_webhdfs(hdfs_path: str) -> pd.DataFrame:
    """WebHDFS API를 사용하여 HDFS에서 CSV 파일 읽기"""
    start_time = datetime.now()
    logger.info(f"WebHDFS 파일 읽기 시작: {hdfs_path}")
    
    try:
        # 파일 상태 확인
        status_url = f"http://{HDFS_CONFIG['namenode_host']}:{HDFS_CONFIG['namenode_port']}/webhdfs/v1{hdfs_path}?op=GETFILESTATUS"
        status_response = requests.get(status_url, timeout=HDFS_CONFIG['timeout'])
        
        if status_response.status_code == 404:
            raise FileNotFoundError(f"HDFS 파일을 찾을 수 없습니다: {hdfs_path}")
        
        # 파일 읽기
        url = f"http://{HDFS_CONFIG['namenode_host']}:{HDFS_CONFIG['namenode_port']}/webhdfs/v1{hdfs_path}?op=OPEN"
        response = requests.get(url, timeout=HDFS_CONFIG['timeout'])
        response.raise_for_status()
        
        # 인코딩 처리
        try:
            df = pd.read_csv(StringIO(response.text), encoding='cp949')
        except UnicodeDecodeError:
            df = pd.read_csv(StringIO(response.text), encoding='utf-8')
        
        # 성능 로깅
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"WebHDFS 파일 읽기 완료: {len(df)}건, 소요시간: {duration:.2f}초")
        
        return df
        
    except requests.exceptions.Timeout:
        raise Exception(f"HDFS 연결 시간 초과: {hdfs_path}")
    except requests.exceptions.ConnectionError:
        raise Exception(f"HDFS 연결 실패: {hdfs_path}")
    except Exception as e:
        logger.error(f"WebHDFS 파일 읽기 오류: {e}")
        raise

def upload_csv_to_hdfs_webhdfs(local_file_path: str, hdfs_path: str):
    """WebHDFS API를 사용하여 HDFS에 파일 업로드"""
    try:
        # 파일 읽기
        with open(local_file_path, 'r', encoding='utf-8') as f:
            data = f.read()
        
        # 1단계: 업로드 위치 요청
        url = f"http://{HDFS_CONFIG['namenode_host']}:{HDFS_CONFIG['namenode_port']}/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true"
        response = requests.put(url, timeout=HDFS_CONFIG['timeout'])
        response.raise_for_status()
        
        # 2단계: 실제 데이터 업로드
        upload_response = requests.put(response.url, data=data, timeout=HDFS_CONFIG['timeout'])
        upload_response.raise_for_status()
        
        logger.info(f"WebHDFS 업로드 완료: {hdfs_path}")
        
    except Exception as e:
        logger.error(f"WebHDFS 업로드 오류: {e}")
        raise

def preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """개별 데이터프레임 전처리"""
    random_value = random.randint(10120, 10200)
    df = df[:random_value].loc[:, ['개방서비스명', '인허가일자', '폐업일자','영업상태명', '도로명전체주소', '사업장명', '업태구분명', '남성종사자수', '여성종사자수']]
    df['도로명전체주소'] = df['도로명전체주소'].fillna('')
    df['폐업일자'] = df['폐업일자'].fillna('운영중')
    df.loc[:, '도로명전체주소'] = [address.split(',')[0] for address in df['도로명전체주소']]

    df[['시도', '시군구', '읍면동', '도로명', '번지']] = df['도로명전체주소'].apply(lambda x: pd.Series(split_address(x)))
    df.loc[:, 'num'] = 1
    df = df.rename(columns={'도로명전체주소': '소재지', '사업장명': '시설명', '업태구분명': '구분'})
    df = df.reset_index().drop(columns='index')
    return df

@dag(
    dag_id='restaurant_data_preprocessing_v3',
    default_args=default_args,
    description='음식점 데이터 전처리 및 PostgreSQL 저장 (WebHDFS 최적화)',
    schedule='@daily',
    catchup=False,
    tags=['dataops', 'preprocessing', 'restaurant', 'webhdfs'],
)
def restaurant_data_preprocessing():
    """최적화된 음식점 데이터 전처리 DAG"""
    
    @task
    def load_and_preprocess_data() -> Dict[int, pd.DataFrame]:
        """WebHDFS에서 데이터 로딩 및 전처리 Task"""
        # HDFS 경로 설정
        HDFS_BASE_PATH = "/data/restaurant"
        
        try:
            # WebHDFS에서 원본 데이터 파일 읽기
            original_file_path = f"{HDFS_BASE_PATH}/fulldata_07_24_04_P_일반음식점.csv"
            df = read_csv_from_hdfs_webhdfs(original_file_path)
            df = df.copy()
            
            # 날짜별 정렬 및 전처리
            sorted_day_df = df.sort_values(by='인허가일자', ascending=False)
            sorted_day_df['인허가일자'] = pd.to_datetime(sorted_day_df['인허가일자'], format='%Y-%m-%d', errors='coerce')
            # pandas 2.0+ 호환성을 위해 ffill() 메서드 사용
            sorted_day_df['인허가일자'] = sorted_day_df['인허가일자'].ffill()
            
            # 연도별 데이터 분리
            years_data = {}
            for year in range(2020, 2026):
                year_df = sorted_day_df[sorted_day_df['인허가일자'].dt.year.astype(int) == year]
                if not year_df.empty:
                    years_data[year] = year_df.reset_index().drop('index', axis=1)
            
            logger.info(f"WebHDFS 데이터 로딩 완료: {len(df)}건")
            return years_data
            
        except Exception as e:
            logger.error(f"WebHDFS 데이터 로딩 오류: {e}")
            raise

    @task
    def preprocess_yearly_data(years_data: Dict[int, pd.DataFrame]) -> Dict[int, pd.DataFrame]:
        """연도별 데이터 전처리 Task"""
        HDFS_BASE_PATH = "/data/restaurant"
        
        try:
            processed_data = {}
            
            for year, df in years_data.items():
                processed_df = preprocess_dataframe(df)
                processed_data[year] = processed_df
                
                # 임시 로컬 파일로 저장
                with tempfile.NamedTemporaryFile(mode='w+', suffix=f'_restaurant_{year}.csv', delete=False) as temp_file:
                    temp_path = temp_file.name
                
                # CSV 파일 저장
                processed_df.to_csv(temp_path, index=False, encoding='utf-8')
                
                # WebHDFS에 업로드
                hdfs_path = f"{HDFS_BASE_PATH}/restaurant_{year}.csv"
                upload_csv_to_hdfs_webhdfs(temp_path, hdfs_path)
                
                # 임시 파일 삭제
                os.unlink(temp_path)
                
                logger.info(f"{year}년 데이터 전처리 완료: {len(processed_df)}건")
            
            return processed_data
            
        except Exception as e:
            logger.error(f"데이터 전처리 오류: {e}")
            raise

    @task
    def create_postgresql_tables():
        """PostgreSQL 테이블 생성 Task (데이터 저장용 PostgreSQL)"""
        postgres_hook = PostgresHook(postgres_conn_id='postgres_dataops')
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS restaurant_data (
            id SERIAL PRIMARY KEY,
            개방서비스명 VARCHAR(255),
            인허가일자 DATE,
            폐업일자 VARCHAR(50),
            영업상태명 VARCHAR(50),
            소재지 TEXT,
            시설명 VARCHAR(255),
            구분 VARCHAR(100),
            남성종사자수 INTEGER,
            여성종사자수 INTEGER,
            시도 VARCHAR(50),
            시군구 VARCHAR(50),
            읍면동 VARCHAR(50),
            도로명 VARCHAR(255),
            번지 VARCHAR(50),
            num INTEGER,
            year INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_restaurant_year ON restaurant_data(year);
        CREATE INDEX IF NOT EXISTS idx_restaurant_location ON restaurant_data(시도, 시군구);
        """
        
        try:
            postgres_hook.run(create_table_query)
            logger.info("PostgreSQL 테이블 생성 완료")
            return "테이블 생성 완료"
        except Exception as e:
            logger.error(f"테이블 생성 오류: {e}")
            raise

    @task
    def insert_data_to_postgresql(processed_data: Dict[int, pd.DataFrame]):
        """PostgreSQL에 데이터 삽입 Task (데이터 저장용 PostgreSQL)"""
        postgres_hook = PostgresHook(postgres_conn_id='postgres_dataops')
        
        try:
            total_inserted = 0
            for year, df in processed_data.items():
                # 기존 연도 데이터 삭제
                delete_query = "DELETE FROM restaurant_data WHERE year = %s"
                postgres_hook.run(delete_query, parameters=(year,))
                
                # 데이터 삽입
                df['year'] = year
                df['created_at'] = datetime.now()
                
                # pandas to_sql 사용
                engine = postgres_hook.get_sqlalchemy_engine()
                df.to_sql('restaurant_data', engine, if_exists='append', index=False, method='multi')
                
                logger.info(f"{year}년 데이터 {len(df)}건 PostgreSQL 저장 완료")
                total_inserted += len(df)
            
            return f"총 {total_inserted}건 데이터 저장 완료"
            
        except Exception as e:
            logger.error(f"PostgreSQL 데이터 삽입 오류: {e}")
            raise

    @task
    def validate_data(insert_result: str) -> int:
        """데이터 검증 Task (데이터 저장용 PostgreSQL)"""
        postgres_hook = PostgresHook(postgres_conn_id='postgres_dataops')
        
        try:
            # 각 연도별 데이터 건수 확인
            validation_query = """
            SELECT year, COUNT(*) as count 
            FROM restaurant_data 
            GROUP BY year 
            ORDER BY year
            """
            
            results = postgres_hook.get_records(validation_query)
            
            logger.info("=== 데이터 검증 결과 ===")
            total_count = 0
            for year, count in results:
                logger.info(f"{year}년: {count:,}건")
                total_count += count
            
            logger.info(f"총 데이터 건수: {total_count:,}건")
            logger.info(f"삽입 결과: {insert_result}")
            
            # 최소 데이터 건수 검증
            if total_count < 1000:
                raise ValueError(f"데이터 건수가 너무 적습니다: {total_count}건")
                
            return total_count
            
        except Exception as e:
            logger.error(f"데이터 검증 오류: {e}")
            raise

    # Task 실행 및 의존성 설정
    years_data = load_and_preprocess_data()
    processed_data = preprocess_yearly_data(years_data)
    table_result = create_postgresql_tables()
    insert_result = insert_data_to_postgresql(processed_data)
    validation_result = validate_data(insert_result)
    
    # 의존성 설정 (TaskFlow API에서는 자동으로 처리되지만 명시적으로 표현)
    table_result >> insert_result
    insert_result >> validation_result

# DAG 인스턴스 생성
restaurant_data_preprocessing_dag = restaurant_data_preprocessing()