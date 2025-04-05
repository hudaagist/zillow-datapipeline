from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import json

#load json file
with open('/home/ubuntu/airflow/dags/config_api.json', 'r') as config_file:
    api_host_key = json.load(config_file) 

now = datetime.now()
dt_now_string = now.strftime("%Y-%m-%dT%H%M%S")

def extract_zillow_data(**kwargs):
    url = kwargs['url']
    headers = kwargs['headers']
    querystring = kwargs['querystring'] 
    df_string = kwargs['date_string']

    #return headers
    response = requests.get(url, headers = headers, params = querystring)
    response_data = response.json()

    #specify the output file path
    output_file_path = f"/home/ubuntu/resonse_data_{df_string}.json"
    file_str = f'response_data_{df_string}.csv'

    #write the json response to a file
    with open(output_file_path, 'w') as output_file:
        json.dump(response_data, output_file, indent = 4)
    output_list = [output_file_path,file_str]
    return output_list



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 5),
    'email_on_failure': False,
    'email_on_retry': False,
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
    
    extract_zillow_data = PythonOperator(
        task_id = 'task_extract_zillow_data',
        python_callable = extract_zillow_data,
        op_kwargs = {'url': "https://zillow56.p.rapidapi.com/search", 'querystring': {'location': 'houston, TX'},'headers': api_host_key, 'date_string': dt_now_string}
    )

    load_to_s3 = BashOperator(
        task_id = 'load_to_s3',
        bash_command = 'aws s3 mv {{ti.xcom_pull("task_extract_zillow_data")[0]}} s3://zillow-data-pipeline-gista'
    )

    extract_zillow_data >> load_to_s3

