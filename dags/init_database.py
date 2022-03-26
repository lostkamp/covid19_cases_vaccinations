from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from resources.load_static_data import preprocess_county_data

conf = {
}


default_args = {
    'depends_on_past': False,
    'email_on_retry': False,
    'email_on_failure': False,
    'start_date': datetime(2022, 3, 1)
}

with DAG(dag_id='init_database',
         description='Run once to create tables and load static data',
         default_args=default_args,
         schedule_interval=None,
         max_active_runs=1,
         concurrency=15,
         catchup=False) as dag:

    preprocess_county_data_task = PythonOperator(
        task_id='preprocess_county_data',
        python_callable=preprocess_county_data
    )
