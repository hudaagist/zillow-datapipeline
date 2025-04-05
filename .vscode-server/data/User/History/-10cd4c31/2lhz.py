from airflow import DAG
from datetime import timedelta, datetime


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 5),
    'email_on_failure': 'hudaagista@gmail.com',
    'email_on_retry': 'hudaagista@gmail.com',
    'retreis': 2,
    'retry_delay': timedelta(seconds=15),
}