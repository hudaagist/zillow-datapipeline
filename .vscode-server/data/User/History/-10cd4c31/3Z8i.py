from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 5),
    'email_on_failure': 'hudaagista@gmail.com',
    'email_on_retry': 'hudaagista@gmail.com',
    'retreis': 2,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    'zillow_analytics',
    default_args=default_args,
    description='A simple DAG for Zillow analytics',
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    extract_zillow_data = PythonOperator()