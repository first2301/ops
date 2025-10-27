from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='simple_data_pipeline',
    default_args=default_args,
    description='A simple data collection and preprocessing pipeline',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    @task
    def fetch_data():
        # 예시: 간단히 리스트로 데이터 제공
        return [1, 2, 2, 3, 4, 4, 5]

    @task
    def clean_data(data):
        # set으로 중복 제거 및 정렬
        cleaned = list(sorted(set(data)))
        return cleaned

    @task
    def preprocess_data(data):
        preprocessed = [x ** 2 for x in data]
        print("Preprocessed Data:", preprocessed)
        # 실제로는 저장 또는 다음 단계 전달

    # Task Dependency 정의 (XCom 자동 전달)
    raw = fetch_data()
    cleaned = clean_data(raw)
    preprocess_data(cleaned)


